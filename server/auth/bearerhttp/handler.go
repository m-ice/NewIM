// Package bearerhttp exposes a policy-neutral bearer session HTTP handler.
// bearerhttp 提供策略无关的 Bearer 会话 HTTP handler，不负责登录或路由装配。
package bearerhttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	app "github.com/m-ice/NewIM/server/auth/session"
)

const (
	// Route is the exact path handled by SessionHandler.
	// Route 是 SessionHandler 处理的精确路径。
	Route = "/api/v1/session"
	// Version is the frozen JSON response version.
	// Version 是冻结的 JSON 响应版本。
	Version = 1
)

// Code is a stable HTTP boundary error code with no sensitive data.
// Code 是不含敏感数据的稳定 HTTP 边界错误码。
type Code string

const (
	CodeInvalidToken     Code = "AUTH_INVALID_TOKEN"
	CodeUnavailable      Code = "AUTH_UNAVAILABLE"
	CodeInternalError    Code = "AUTH_INTERNAL_ERROR"
	CodeRouteNotFound    Code = "HTTP_ROUTE_NOT_FOUND"
	CodeMethodNotAllowed Code = "HTTP_METHOD_NOT_ALLOWED"
	CodeBodyNotAllowed   Code = "HTTP_BODY_NOT_ALLOWED"
)

// Error reports a handler construction or runtime boundary failure.
// Error 表示 handler 构造或运行时边界错误。
type Error struct{ Code Code }

func (e *Error) Error() string { return string(e.Code) }

// ErrorCode returns a stable code for handler errors.
// ErrorCode 返回 handler 的稳定错误码。
func ErrorCode(err error) Code {
	if err == nil {
		return ""
	}
	var known *Error
	if errors.As(err, &known) && known != nil {
		return known.Code
	}
	return CodeUnavailable
}

// Authenticator is implemented by the policy-neutral session service.
// Authenticator 由策略无关的 session service 实现。
type Authenticator interface {
	AuthenticateBearer(context.Context, string) (app.BearerSession, error)
}

// Handler handles exactly GET /api/v1/session when mounted by a future owner.
// Handler 在被后续装配后处理精确的 GET /api/v1/session。
type Handler struct {
	authenticator Authenticator
}

// NewHandler validates the required trusted authenticator.
// NewHandler 校验必需的受信 authenticator。
func NewHandler(authenticator Authenticator) (*Handler, error) {
	if authenticator == nil {
		return nil, &Error{Code: CodeUnavailable}
	}
	return &Handler{authenticator: authenticator}, nil
}

// ServeHTTP applies route, method, body, bearer and authentication checks.
// ServeHTTP 依次执行 route、method、body、bearer 与 authentication 校验。
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	recorder := &responseRecorder{ResponseWriter: w}
	defer func() {
		if recover() != nil {
			if !recorder.wroteHeader {
				writeError(recorder, http.StatusInternalServerError, CodeInternalError)
			}
		}
	}()
	if h == nil || h.authenticator == nil {
		writeError(recorder, http.StatusServiceUnavailable, CodeUnavailable)
		return
	}
	if r == nil {
		writeError(recorder, http.StatusBadRequest, CodeInvalidToken)
		return
	}
	if r.URL == nil || r.URL.EscapedPath() != Route {
		writeError(recorder, http.StatusNotFound, CodeRouteNotFound)
		return
	}
	if r.Method != http.MethodGet {
		recorder.Header().Set("Allow", http.MethodGet)
		writeError(recorder, http.StatusMethodNotAllowed, CodeMethodNotAllowed)
		return
	}
	if requestHasBody(r) {
		writeError(recorder, http.StatusBadRequest, CodeBodyNotAllowed)
		return
	}
	rawToken, ok := parseBearerHeader(r.Header.Values("Authorization"))
	if !ok {
		writeUnauthorized(recorder)
		return
	}
	session, err := h.authenticator.AuthenticateBearer(r.Context(), rawToken)
	if err != nil {
		status, code := mapAuthenticationError(err)
		if status == http.StatusUnauthorized {
			writeUnauthorized(recorder)
			return
		}
		writeError(recorder, status, code)
		return
	}
	if session.UserID() == "" || session.DeviceID() == "" || session.SessionID() == "" || session.TokenID() == "" || session.ExpiresAt().IsZero() {
		writeError(recorder, http.StatusServiceUnavailable, CodeUnavailable)
		return
	}
	writeSuccess(recorder, session)
}

func mapAuthenticationError(err error) (int, Code) {
	switch app.ErrorCode(err) {
	case app.AuthInvalidInput, app.AuthTokenMalformed, app.AuthTokenUnknown, app.AuthTokenExpired,
		app.AuthTokenRevoked, app.AuthSessionRevoked, app.AuthForbidden:
		return http.StatusUnauthorized, CodeInvalidToken
	default:
		return http.StatusServiceUnavailable, CodeUnavailable
	}
}

func parseBearerHeader(values []string) (string, bool) {
	if len(values) != 1 {
		return "", false
	}
	value := values[0]
	const scheme = "Bearer "
	if len(value) <= len(scheme) || !strings.EqualFold(value[:len(scheme)], scheme) {
		return "", false
	}
	token := value[len(scheme):]
	if token == "" || strings.TrimSpace(token) != token || strings.ContainsAny(token, " \t\r\n,") {
		return "", false
	}
	return token, true
}

func requestHasBody(r *http.Request) bool {
	return r.ContentLength > 0 || r.ContentLength < 0 || len(r.TransferEncoding) != 0
}

type sessionResponse struct {
	Version int         `json:"version"`
	Session sessionView `json:"session"`
}

type sessionView struct {
	UserID    string    `json:"userId"`
	DeviceID  string    `json:"deviceId"`
	SessionID string    `json:"sessionId"`
	TokenID   string    `json:"tokenId"`
	ExpiresAt time.Time `json:"expiresAt"`
}

func writeSuccess(w http.ResponseWriter, session app.BearerSession) {
	body, err := json.Marshal(sessionResponse{
		Version: Version,
		Session: sessionView{
			UserID: session.UserID(), DeviceID: session.DeviceID(), SessionID: session.SessionID(),
			TokenID: session.TokenID(), ExpiresAt: session.ExpiresAt().UTC(),
		},
	})
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, CodeUnavailable)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(body, '\n'))
}

func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="newim-session"`)
	writeError(w, http.StatusUnauthorized, CodeInvalidToken)
}

func writeError(w http.ResponseWriter, status int, code Code) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":{"code":"` + string(code) + `"}}` + "\n"))
}

type responseRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status = status
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(data []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(data)
}
