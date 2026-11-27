// Package friendhttp serves authenticated friend operations over TLS.
package friendhttp

import (
	"bytes"
	"context"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/netip"
	"strconv"
	"strings"

	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/friend"
	"github.com/m-ice/NewIM/server/userhttp"
)

const (
	applyRoute            = "/api/v1/friends/applications"
	respondRoute          = "/api/v1/friends/applications/respond"
	listApplicationsRoute = "/api/v1/friends/applications/list"
	listRoute             = "/api/v1/friends/list"
	remarkRoute           = "/api/v1/friends/remark"
	removeRoute           = "/api/v1/friends/remove"
	changesRoute          = "/api/v1/friends/changes"
)

type Handler struct {
	service *friend.Service
	auth    userhttp.Authenticator
	budget  userhttp.Budget
}

func New(service *friend.Service, auth userhttp.Authenticator, budget userhttp.Budget) (*Handler, error) {
	if service == nil || auth == nil || budget == nil {
		return nil, friend.StorageUnavailable
	}
	return &Handler{service: service, auth: auth, budget: budget}, nil
}

func ContainsRoute(path string) bool {
	switch path {
	case applyRoute, respondRoute, listApplicationsRoute, listRoute, remarkRoute, removeRoute, changesRoute:
		return true
	default:
		return false
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	path := r.URL.EscapedPath()
	if !ContainsRoute(path) {
		writeError(w, http.StatusNotFound, "HTTP_NOT_FOUND")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "HTTP_METHOD_NOT_ALLOWED")
		return
	}
	addr, err := netip.ParseAddrPort(r.RemoteAddr)
	if r.TLS == nil || err != nil || r.URL.RawQuery != "" || r.URL.ForceQuery {
		writeServiceError(w, friend.InvalidInput)
		return
	}
	if !h.budget.AllowAddress(addr.Addr().Unmap()) {
		writeError(w, http.StatusTooManyRequests, "FRIEND_RATE_LIMITED")
		return
	}
	token, ok := bearerhttp.ParseBearerHeader(r.Header.Values("Authorization"))
	if !ok {
		writeUnauthorized(w)
		return
	}
	identity, err := h.auth.AuthenticateBearer(r.Context(), token)
	if err != nil {
		status, _ := bearerhttp.MapAuthenticationError(err)
		if status == http.StatusUnauthorized {
			writeUnauthorized(w)
		} else {
			writeServiceError(w, friend.StorageUnavailable)
		}
		return
	}
	if !h.budget.AllowAccount(identity.UserID()) {
		writeError(w, http.StatusTooManyRequests, "FRIEND_RATE_LIMITED")
		return
	}
	raw, ok := readBody(w, r)
	if !ok {
		return
	}
	field, value, err := h.dispatch(r.Context(), path, identity.UserID(), raw)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": 1, field: value})
}

func (h *Handler) dispatch(ctx context.Context, path, owner string, raw []byte) (string, any, error) {
	switch path {
	case applyRoute:
		var req friend.ApplyRequest
		if decode(raw, &req) {
			result, err := h.service.Apply(ctx, owner, req)
			return "application", result, err
		}
	case respondRoute:
		var req friend.ResponseRequest
		if decode(raw, &req) {
			result, err := h.service.Respond(ctx, owner, req)
			return "application", result, err
		}
	case listApplicationsRoute:
		var req friend.ApplicationListRequest
		if decode(raw, &req) {
			result, err := h.service.ListApplications(ctx, owner, req)
			return "page", result, err
		}
	case listRoute:
		var req friend.ListRequest
		if decode(raw, &req) {
			result, err := h.service.List(ctx, owner, req)
			return "page", result, err
		}
	case remarkRoute:
		var req struct {
			PeerID string  `json:"peerId"`
			Remark *string `json:"remark"`
		}
		if decode(raw, &req) && req.Remark != nil {
			result, err := h.service.Remark(ctx, owner, req.PeerID, *req.Remark)
			return "friend", result, err
		}
	case removeRoute:
		var req struct {
			PeerID string `json:"peerId"`
		}
		if decode(raw, &req) {
			result, err := h.service.Remove(ctx, owner, req.PeerID)
			return "removed", result, err
		}
	case changesRoute:
		var req struct {
			AfterRevision string `json:"afterRevision"`
			Limit         int    `json:"limit"`
		}
		if decode(raw, &req) {
			revision, err := strconv.ParseInt(req.AfterRevision, 10, 64)
			if err == nil && revision >= 0 && strconv.FormatInt(revision, 10) == req.AfterRevision {
				result, err := h.service.Changes(ctx, owner, revision, req.Limit)
				return "page", result, err
			}
		}
	}
	return "", nil, friend.InvalidInput
}

func decode(raw []byte, dest any) bool {
	return len(raw) > 0 && raw[0] == '{' && jsonv2.Unmarshal(raw, dest, jsonv2.RejectUnknownMembers(true)) == nil
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r.ContentLength > userhttp.BodyLimit {
		writeError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
		return nil, false
	}
	values := r.Header.Values("Content-Type")
	if len(values) != 1 {
		writeError(w, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
		return nil, false
	}
	media, params, err := mime.ParseMediaType(values[0])
	charset, hasCharset := params["charset"]
	if err != nil || media != "application/json" || (len(params) != 0 &&
		(len(params) != 1 || !hasCharset || !strings.EqualFold(charset, "utf-8"))) {
		writeError(w, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
		return nil, false
	}
	if r.Body == nil {
		writeServiceError(w, friend.InvalidInput)
		return nil, false
	}
	reader := http.MaxBytesReader(w, r.Body, userhttp.BodyLimit)
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err == nil {
		return bytes.Trim(raw, " \t\r\n"), true
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
	} else {
		writeServiceError(w, friend.InvalidInput)
	}
	return nil, false
}

func writeServiceError(w http.ResponseWriter, err error) {
	code := friend.ErrorCode(err)
	status := http.StatusServiceUnavailable
	switch code {
	case friend.InvalidInput:
		status = http.StatusBadRequest
	case friend.NotFound:
		status = http.StatusNotFound
	case friend.Pending, friend.Conflict:
		status = http.StatusConflict
	}
	writeError(w, status, string(code))
}

func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeError(w, http.StatusUnauthorized, string(bearerhttp.CodeInvalidToken))
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
