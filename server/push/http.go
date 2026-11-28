package push

import (
	"context"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/netip"
	"strconv"

	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/auth/session"
)

const RegistrationRoute = "/api/v1/push/registration"
const maxRegistrationBody = 8192

type Authenticator interface {
	AuthenticateBearer(context.Context, string) (session.BearerSession, error)
}

type RegistrationBudget interface {
	AllowAddress(netip.Addr) bool
	AllowAccount(string) bool
}

type RegistrationHandler struct {
	auth   Authenticator
	store  RegistrationStore
	budget RegistrationBudget
}

func NewRegistrationHandler(auth Authenticator, store RegistrationStore, budget RegistrationBudget) (*RegistrationHandler, error) {
	if auth == nil || store == nil || budget == nil {
		return nil, ErrConfig
	}
	return &RegistrationHandler{auth: auth, store: store, budget: budget}, nil
}

func (h *RegistrationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.TLS == nil || r.URL.EscapedPath() != RegistrationRoute {
		failure(w, http.StatusNotFound, "PUSH_ROUTE_NOT_FOUND")
		return
	}
	if r.Method != http.MethodPut && r.Method != http.MethodDelete {
		w.Header().Set("Allow", "PUT, DELETE")
		failure(w, http.StatusMethodNotAllowed, "PUSH_METHOD_NOT_ALLOWED")
		return
	}
	if r.URL.ForceQuery || r.URL.RawQuery != "" {
		failure(w, http.StatusBadRequest, ErrInvalidInput.Error())
		return
	}
	address, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil || !h.budget.AllowAddress(address.Addr().Unmap()) {
		failure(w, http.StatusTooManyRequests, "PUSH_RATE_LIMITED")
		return
	}
	raw, ok := bearerhttp.ParseBearerHeader(r.Header.Values("Authorization"))
	if !ok {
		authenticationFailure(w, http.StatusUnauthorized, string(bearerhttp.CodeInvalidToken))
		return
	}
	bearer, err := h.auth.AuthenticateBearer(r.Context(), raw)
	if err != nil {
		status, code := bearerhttp.MapAuthenticationError(err)
		authenticationFailure(w, status, string(code))
		return
	}
	if !h.budget.AllowAccount(bearer.UserID()) {
		failure(w, http.StatusTooManyRequests, "PUSH_RATE_LIMITED")
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(r.Header.Values("Content-Type")) != 1 ||
		len(r.Header.Values("Content-Encoding")) != 0 {
		failure(w, http.StatusUnsupportedMediaType, "PUSH_UNSUPPORTED_MEDIA")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRegistrationBody))
	defer clear(body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			failure(w, http.StatusRequestEntityTooLarge, "PUSH_BODY_TOO_LARGE")
		} else {
			failure(w, http.StatusBadRequest, ErrInvalidInput.Error())
		}
		return
	}
	if r.Method == http.MethodPut {
		h.register(w, r.Context(), bearer, body)
	} else {
		h.unregister(w, r.Context(), bearer, body)
	}
}

func (h *RegistrationHandler) register(w http.ResponseWriter, ctx context.Context, auth session.BearerSession, body []byte) {
	var request struct {
		Channel Channel `json:"provider"`
		Token   string  `json:"token"`
	}
	if jsonv2.Unmarshal(body, &request, jsonv2.RejectUnknownMembers(true)) != nil {
		failure(w, http.StatusBadRequest, ErrInvalidInput.Error())
		return
	}
	token := []byte(request.Token)
	defer clear(token)
	version, err := h.store.Register(ctx, auth, request.Channel, token)
	if err != nil {
		registrationFailure(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Version int64 `json:"version,string"`
	}{version})
}

func (h *RegistrationHandler) unregister(w http.ResponseWriter, ctx context.Context, auth session.BearerSession, body []byte) {
	var request struct {
		Channel Channel `json:"provider"`
		Version string  `json:"version"`
	}
	if jsonv2.Unmarshal(body, &request, jsonv2.RejectUnknownMembers(true)) != nil {
		failure(w, http.StatusBadRequest, ErrInvalidInput.Error())
		return
	}
	version, err := strconv.ParseInt(request.Version, 10, 64)
	if err != nil || version < 1 || strconv.FormatInt(version, 10) != request.Version {
		failure(w, http.StatusBadRequest, ErrInvalidInput.Error())
		return
	}
	if err := h.store.Unregister(ctx, auth, request.Channel, version); err != nil {
		registrationFailure(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func registrationFailure(w http.ResponseWriter, err error) {
	var authErr *session.Error
	switch {
	case errors.As(err, &authErr):
		status, code := bearerhttp.MapAuthenticationError(err)
		authenticationFailure(w, status, string(code))
	case errors.Is(err, ErrInvalidInput):
		failure(w, http.StatusBadRequest, ErrInvalidInput.Error())
	default:
		failure(w, http.StatusServiceUnavailable, ErrStorage.Error())
	}
}

func authenticationFailure(w http.ResponseWriter, status int, code string) {
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="newim-session"`)
	}
	failure(w, status, code)
}

func failure(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Code string `json:"code"`
	}{code})
}
