package bearerhttp

import (
	"context"
	"net/http"

	app "github.com/m-ice/NewIM/server/auth/session"
)

const (
	// TokenRoute revokes exactly the bearer token presented in the request.
	// TokenRoute 仅撤销请求中提交的 Bearer 令牌。
	TokenRoute = "/api/v1/session/tokens/current"
)

// TokenRevoker is implemented by the policy-neutral session service.
// TokenRevoker 由策略无关的 session service 实现。
type TokenRevoker interface {
	RevokeBearer(context.Context, string) (app.RevocationOutcome, error)
}

// TokenRevocationHandler handles the exact idempotent token revocation route.
// TokenRevocationHandler 处理精确的幂等令牌撤销路由。
type TokenRevocationHandler struct {
	revoker TokenRevoker
}

// NewTokenRevocationHandler validates the required trusted revoker.
// NewTokenRevocationHandler 校验必需的受信 revoker。
func NewTokenRevocationHandler(revoker TokenRevoker) (*TokenRevocationHandler, error) {
	if revoker == nil {
		return nil, &Error{Code: CodeUnavailable}
	}
	return &TokenRevocationHandler{revoker: revoker}, nil
}

// ServeHTTP applies route, method, body, bearer and idempotent revocation checks.
// ServeHTTP 依次执行 route、method、body、bearer 与幂等撤销校验。
func (h *TokenRevocationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	recorder := &responseRecorder{ResponseWriter: w}
	defer func() {
		if recovered := recover(); recovered != nil {
			if !recorder.wroteHeader {
				writeError(recorder, http.StatusInternalServerError, CodeInternalError)
				return
			}
			panic(recovered)
		}
	}()
	if h == nil || h.revoker == nil {
		writeError(recorder, http.StatusServiceUnavailable, CodeUnavailable)
		return
	}
	if r == nil {
		writeError(recorder, http.StatusUnauthorized, CodeInvalidToken)
		return
	}
	if r.URL == nil || r.URL.EscapedPath() != TokenRoute {
		writeError(recorder, http.StatusNotFound, CodeRouteNotFound)
		return
	}
	if r.Method != http.MethodDelete {
		recorder.Header().Set("Allow", http.MethodDelete)
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
	outcome, err := h.revoker.RevokeBearer(r.Context(), rawToken)
	if err != nil {
		status, code := mapRevocationError(err)
		if status == http.StatusUnauthorized {
			writeUnauthorized(recorder)
			return
		}
		writeError(recorder, status, code)
		return
	}
	if outcome != app.RevokeOK && outcome != app.RevokeNoop {
		writeError(recorder, http.StatusServiceUnavailable, CodeUnavailable)
		return
	}
	writeNoContent(recorder)
}

func mapRevocationError(err error) (int, Code) {
	switch app.ErrorCode(err) {
	case app.AuthInvalidInput, app.AuthTokenMalformed, app.AuthTokenUnknown, app.AuthForbidden:
		return http.StatusUnauthorized, CodeInvalidToken
	default:
		return http.StatusServiceUnavailable, CodeUnavailable
	}
}

func writeNoContent(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusNoContent)
}
