package bearerhttp

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"

	app "github.com/m-ice/NewIM/server/auth/session"
)

type revokerFunc func(context.Context, string) (app.RevocationOutcome, error)

func (f revokerFunc) RevokeBearer(ctx context.Context, token string) (app.RevocationOutcome, error) {
	return f(ctx, token)
}

func TestTokenRevocationHandlerContract(t *testing.T) {
	raw := app.TokenPrefix + strings.Repeat("a", app.TokenIDHexLen) + "_" + strings.Repeat("A", app.TokenSecretLen)
	var mu sync.Mutex
	calls := 0
	handler, err := NewTokenRevocationHandler(revokerFunc(func(_ context.Context, token string) (app.RevocationOutcome, error) {
		if token != raw {
			t.Fatalf("token=%q", token)
		}
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return app.RevokeOK, nil
		}
		return app.RevokeNoop, nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{string(app.RevokeOK), string(app.RevokeNoop)} {
		recorder := serve(t, handler, request(t, http.MethodDelete, TokenRoute, nil, map[string][]string{"Authorization": {"Bearer " + raw}}))
		if recorder.Code != http.StatusNoContent || recorder.Body.Len() != 0 {
			t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
		}
		if recorder.Header().Get("Cache-Control") != "no-store" || recorder.Header().Get("X-Content-Type-Options") != "nosniff" ||
			recorder.Header().Get("WWW-Authenticate") != "" || strings.Contains(recorder.Body.String(), raw) {
			t.Fatalf("headers=%v body=%q want=%s", recorder.Header(), recorder.Body.String(), want)
		}
	}

	for name, tc := range map[string]struct {
		request *http.Request
		status  int
		code    Code
		allow   string
	}{
		"route": {
			request: request(t, http.MethodDelete, "/api/v1/session", nil, map[string][]string{"Authorization": {"Bearer " + raw}}),
			status:  http.StatusNotFound, code: CodeRouteNotFound,
		},
		"method": {
			request: request(t, http.MethodGet, TokenRoute, nil, map[string][]string{"Authorization": {"Bearer " + raw}}),
			status:  http.StatusMethodNotAllowed, code: CodeMethodNotAllowed, allow: http.MethodDelete,
		},
		"body": {
			request: request(t, http.MethodDelete, TokenRoute, strings.NewReader("body"), map[string][]string{"Authorization": {"Bearer " + raw}}),
			status:  http.StatusBadRequest, code: CodeBodyNotAllowed,
		},
	} {
		t.Run(name, func(t *testing.T) {
			recorder := serve(t, handler, tc.request)
			if recorder.Code != tc.status || decodeCode(t, recorder.Body.Bytes()) != tc.code || recorder.Header().Get("Allow") != tc.allow {
				t.Fatalf("status=%d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
			}
		})
	}

	for name, values := range map[string][]string{
		"missing":      nil,
		"empty":        {"Bearer "},
		"wrong-scheme": {"Token " + raw},
		"duplicate":    {"Bearer " + raw, "Bearer " + raw},
		"whitespace":   {"Bearer  " + raw},
	} {
		t.Run(name, func(t *testing.T) {
			headers := map[string][]string{}
			if values != nil {
				headers["Authorization"] = values
			}
			recorder := serve(t, handler, request(t, http.MethodDelete, TokenRoute, nil, headers))
			if recorder.Code != http.StatusUnauthorized || recorder.Header().Get("WWW-Authenticate") != `Bearer realm="newim-session"` ||
				decodeCode(t, recorder.Body.Bytes()) != CodeInvalidToken {
				t.Fatalf("status=%d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
			}
		})
	}
}

func TestTokenRevocationHandlerSecurity(t *testing.T) {
	raw := app.TokenPrefix + strings.Repeat("a", app.TokenIDHexLen) + "_" + strings.Repeat("A", app.TokenSecretLen)
	for name, tc := range map[string]struct {
		revoker revokerFunc
		status  int
		code    Code
	}{
		"unknown": {func(context.Context, string) (app.RevocationOutcome, error) {
			return "", app.Fail(app.AuthTokenUnknown)
		}, http.StatusUnauthorized, CodeInvalidToken},
		"invalid": {func(context.Context, string) (app.RevocationOutcome, error) {
			return "", app.Fail(app.AuthInvalidInput)
		}, http.StatusUnauthorized, CodeInvalidToken},
		"unavailable": {func(context.Context, string) (app.RevocationOutcome, error) {
			return "", app.Fail(app.AuthStorageUnavailable)
		}, http.StatusServiceUnavailable, CodeUnavailable},
		"invalid-outcome": {func(context.Context, string) (app.RevocationOutcome, error) {
			return "OTHER", nil
		}, http.StatusServiceUnavailable, CodeUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			handler, err := NewTokenRevocationHandler(tc.revoker)
			if err != nil {
				t.Fatal(err)
			}
			recorder := serve(t, handler, request(t, http.MethodDelete, TokenRoute, nil, map[string][]string{"Authorization": {"Bearer " + raw}}))
			if recorder.Code != tc.status || decodeCode(t, recorder.Body.Bytes()) != tc.code || strings.Contains(recorder.Body.String(), raw) {
				t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
			}
		})
	}

	panicHandler, err := NewTokenRevocationHandler(revokerFunc(func(context.Context, string) (app.RevocationOutcome, error) {
		panic("sensitive panic sentinel")
	}))
	if err != nil {
		t.Fatal(err)
	}
	recorder := serve(t, panicHandler, request(t, http.MethodDelete, TokenRoute, nil, map[string][]string{"Authorization": {"Bearer " + raw}}))
	if recorder.Code != http.StatusInternalServerError || decodeCode(t, recorder.Body.Bytes()) != CodeInternalError ||
		strings.Contains(recorder.Body.String(), "sensitive panic sentinel") {
		t.Fatalf("panic status=%d body=%q", recorder.Code, recorder.Body.String())
	}

	if _, err := NewTokenRevocationHandler(nil); ErrorCode(err) != CodeUnavailable {
		t.Fatalf("nil revoker got %v", err)
	}
}
