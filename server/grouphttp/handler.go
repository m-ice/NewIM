// Package grouphttp serves authenticated small-group operations over TLS.
package grouphttp

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
	"github.com/m-ice/NewIM/server/group"
	"github.com/m-ice/NewIM/server/userhttp"
)

const bodyLimit = 256 << 10

const (
	createRoute   = "/api/v1/groups/create"
	getRoute      = "/api/v1/groups/get"
	updateRoute   = "/api/v1/groups/update"
	joinRoute     = "/api/v1/groups/join"
	respondRoute  = "/api/v1/groups/respond"
	membersRoute  = "/api/v1/groups/members"
	memberRoute   = "/api/v1/groups/member/action"
	muteRoute     = "/api/v1/groups/mute"
	dissolveRoute = "/api/v1/groups/dissolve"
	requestsRoute = "/api/v1/groups/requests"
	changesRoute  = "/api/v1/groups/changes"
)

type Handler struct {
	service *group.Service
	auth    userhttp.Authenticator
	budget  userhttp.Budget
}

func New(service *group.Service, auth userhttp.Authenticator, budget userhttp.Budget) (*Handler, error) {
	if service == nil || auth == nil || budget == nil {
		return nil, group.StorageUnavailable
	}
	return &Handler{service: service, auth: auth, budget: budget}, nil
}

func ContainsRoute(path string) bool {
	switch path {
	case createRoute, getRoute, updateRoute, joinRoute, respondRoute, membersRoute,
		memberRoute, muteRoute, dissolveRoute, requestsRoute, changesRoute:
		return true
	default:
		return false
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.EscapedPath()
	switch {
	case !ContainsRoute(path):
		writeError(w, http.StatusNotFound, "HTTP_NOT_FOUND")
		return
	case r.Method != http.MethodPost:
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "HTTP_METHOD_NOT_ALLOWED")
		return
	case r.TLS == nil || r.URL.RawQuery != "" || r.URL.ForceQuery:
		writeServiceError(w, group.InvalidInput)
		return
	}
	addr, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		writeServiceError(w, group.InvalidInput)
		return
	}
	if !h.budget.AllowAddress(addr.Addr().Unmap()) {
		writeError(w, http.StatusTooManyRequests, "GROUP_RATE_LIMITED")
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
			writeServiceError(w, group.StorageUnavailable)
		}
		return
	}
	if !h.budget.AllowAccount(identity.UserID()) {
		writeError(w, http.StatusTooManyRequests, "GROUP_RATE_LIMITED")
		return
	}
	raw, ok := readBody(w, r)
	if !ok {
		return
	}
	field, result, err := h.dispatch(r.Context(), path, identity.UserID(), raw)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": 1, field: result})
}

type groupIDRequest struct {
	GroupID string `json:"groupId"`
}

type muteRequest struct {
	GroupID string `json:"groupId"`
	Muted   *bool  `json:"muted"`
}

type changeRequest struct {
	AfterRevision string `json:"afterRevision"`
	Limit         int    `json:"limit"`
}

func (h *Handler) dispatch(ctx context.Context, path, owner string, raw []byte) (string, any, error) {
	switch path {
	case createRoute:
		return call(ctx, owner, raw, "view", h.service.Create)
	case getRoute:
		return call(ctx, owner, raw, "view", func(ctx context.Context, owner string, req groupIDRequest) (group.View, error) {
			return h.service.Get(ctx, owner, req.GroupID)
		})
	case updateRoute:
		return call(ctx, owner, raw, "view", h.service.Update)
	case joinRoute:
		return call(ctx, owner, raw, "request", h.service.RequestEntry)
	case respondRoute:
		return call(ctx, owner, raw, "request", h.service.Respond)
	case membersRoute:
		return call(ctx, owner, raw, "page", h.service.ListMembers)
	case memberRoute:
		return call(ctx, owner, raw, "membership", h.service.ChangeMember)
	case muteRoute:
		return call(ctx, owner, raw, "view", func(ctx context.Context, owner string, req muteRequest) (group.View, error) {
			if req.Muted == nil {
				return group.View{}, group.InvalidInput
			}
			return h.service.SetMuted(ctx, owner, req.GroupID, *req.Muted)
		})
	case dissolveRoute:
		return call(ctx, owner, raw, "dissolved", func(ctx context.Context, owner string, req groupIDRequest) (bool, error) {
			return h.service.Dissolve(ctx, owner, req.GroupID)
		})
	case requestsRoute:
		return call(ctx, owner, raw, "page", h.service.ListEntries)
	case changesRoute:
		return call(ctx, owner, raw, "page", func(ctx context.Context, owner string, req changeRequest) (group.ChangePage, error) {
			revision, err := strconv.ParseInt(req.AfterRevision, 10, 64)
			if err != nil || revision < 0 || strconv.FormatInt(revision, 10) != req.AfterRevision {
				return group.ChangePage{}, group.InvalidInput
			}
			return h.service.Changes(ctx, owner, revision, req.Limit)
		})
	default:
		return "", nil, group.InvalidInput
	}
}

func call[Req, Result any](ctx context.Context, owner string, raw []byte, field string, operation func(context.Context, string, Req) (Result, error)) (string, any, error) {
	var req Req
	if len(raw) == 0 || raw[0] != '{' || jsonv2.Unmarshal(raw, &req, jsonv2.RejectUnknownMembers(true)) != nil {
		return "", nil, group.InvalidInput
	}
	result, err := operation(ctx, owner, req)
	return field, result, err
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r.ContentLength > bodyLimit {
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
	if err != nil || media != "application/json" ||
		(len(params) != 0 && (len(params) != 1 || !hasCharset || !strings.EqualFold(charset, "utf-8"))) {
		writeError(w, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
		return nil, false
	}
	if r.Body == nil {
		writeServiceError(w, group.InvalidInput)
		return nil, false
	}
	reader := http.MaxBytesReader(w, r.Body, bodyLimit)
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err == nil {
		return bytes.Trim(raw, " \t\r\n"), true
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
	} else {
		writeServiceError(w, group.InvalidInput)
	}
	return nil, false
}

func writeServiceError(w http.ResponseWriter, err error) {
	code := group.ErrorCode(err)
	status := http.StatusServiceUnavailable
	switch code {
	case group.InvalidInput:
		status = http.StatusBadRequest
	case group.Forbidden:
		status = http.StatusForbidden
	case group.NotFound:
		status = http.StatusNotFound
	case group.Conflict, group.CapacityExceeded:
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
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
