// Package userhttp adapts the authenticated account and profile APIs to HTTP.
package userhttp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/netip"
	"strings"

	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/user"
)

const (
	SelfRoute   = "/api/v1/users/me"
	UpdateRoute = "/api/v1/users/me/update"
	LookupRoute = "/api/v1/users/lookup"
)

type Authenticator interface {
	AuthenticateBearer(context.Context, string) (session.BearerSession, error)
}

type Budget interface {
	AllowAddress(netip.Addr) bool
	AllowAccount(string) bool
}

type Handler struct {
	service *user.Service
	auth    Authenticator
	budget  Budget
}

func New(service *user.Service, auth Authenticator, budget Budget) (*Handler, error) {
	if service == nil || auth == nil || budget == nil {
		return nil, user.Fail(user.StorageUnavailable)
	}
	return &Handler{service: service, auth: auth, budget: budget}, nil
}

func ContainsRoute(path string) bool {
	return path == SelfRoute || path == UpdateRoute || path == LookupRoute
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	path := r.URL.EscapedPath()
	if !ContainsRoute(path) {
		writeError(w, http.StatusNotFound, "HTTP_NOT_FOUND")
		return
	}
	method := http.MethodPost
	if path == SelfRoute {
		method = http.MethodGet
	}
	if r.Method != method {
		w.Header().Set("Allow", method)
		writeError(w, http.StatusMethodNotAllowed, "HTTP_METHOD_NOT_ALLOWED")
		return
	}
	addr, err := netip.ParseAddrPort(r.RemoteAddr)
	if r.TLS == nil || err != nil || r.URL.RawQuery != "" || r.URL.ForceQuery {
		writeError(w, http.StatusBadRequest, string(user.InvalidInput))
		return
	}
	if !h.budget.AllowAddress(addr.Addr().Unmap()) {
		writeError(w, http.StatusTooManyRequests, "USER_RATE_LIMITED")
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
			return
		}
		WriteServiceError(w, user.Fail(user.StorageUnavailable))
		return
	}
	if !h.budget.AllowAccount(identity.UserID()) {
		writeError(w, http.StatusTooManyRequests, "USER_RATE_LIMITED")
		return
	}
	if path == SelfRoute {
		if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			writeError(w, http.StatusBadRequest, string(user.InvalidInput))
			return
		}
		profile, err := h.service.Get(r.Context(), identity.UserID())
		writeProfileResult(w, profile, err)
		return
	}

	body, ok := readBody(w, r)
	if !ok {
		return
	}
	if path == UpdateRoute {
		patch, valid := decodeUpdate(body)
		if !valid {
			writeError(w, http.StatusBadRequest, string(user.InvalidInput))
			return
		}
		profile, err := h.service.Update(r.Context(), identity.UserID(), patch)
		writeProfileResult(w, profile, err)
		return
	}
	ids, valid := decodeLookup(body)
	if !valid {
		writeError(w, http.StatusBadRequest, string(user.InvalidInput))
		return
	}
	profiles, err := h.service.Lookup(r.Context(), ids)
	if err != nil {
		WriteServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Version  int            `json:"version"`
		Profiles []user.Profile `json:"profiles"`
	}{Version: 1, Profiles: profiles})
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r.ContentLength > BodyLimit {
		writeError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
		return nil, false
	}
	contentTypes := r.Header.Values("Content-Type")
	if len(contentTypes) != 1 {
		writeError(w, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
		return nil, false
	}
	media, params, err := mime.ParseMediaType(contentTypes[0])
	charset, hasCharset := params["charset"]
	if err != nil || media != "application/json" || (len(params) != 0 && (len(params) != 1 || !hasCharset || !strings.EqualFold(charset, "utf-8"))) {
		writeError(w, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
		return nil, false
	}
	if r.Body == nil {
		writeError(w, http.StatusBadRequest, string(user.InvalidInput))
		return nil, false
	}
	reader := http.MaxBytesReader(w, r.Body, BodyLimit)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err == nil {
		return body, true
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
		return nil, false
	}
	writeError(w, http.StatusBadRequest, string(user.InvalidInput))
	return nil, false
}

func writeProfileResult(w http.ResponseWriter, profile user.Profile, err error) {
	if err != nil {
		WriteServiceError(w, err)
		return
	}
	WriteProfile(w, profile)
}

func WriteProfile(w http.ResponseWriter, profile user.Profile) {
	writeJSON(w, http.StatusOK, struct {
		Version int          `json:"version"`
		Profile user.Profile `json:"profile"`
	}{Version: 1, Profile: profile})
}

func WriteServiceError(w http.ResponseWriter, err error) {
	switch user.ErrorCode(err) {
	case user.InvalidInput:
		writeError(w, http.StatusBadRequest, string(user.InvalidInput))
	case user.NotFound:
		writeError(w, http.StatusNotFound, string(user.NotFound))
	default:
		writeError(w, http.StatusServiceUnavailable, string(user.StorageUnavailable))
	}
}

func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeError(w, http.StatusUnauthorized, string(bearerhttp.CodeInvalidToken))
}

func writeError(w http.ResponseWriter, status int, code string) {
	type responseError struct {
		Code string `json:"code"`
	}
	writeJSON(w, status, struct {
		Error responseError `json:"error"`
	}{Error: responseError{Code: code}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
