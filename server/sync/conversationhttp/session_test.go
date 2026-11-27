package conversationhttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m-ice/NewIM/server/auth/session"
)

type pageAuthenticator func(context.Context, string) (session.BearerSession, error)

func (authenticate pageAuthenticator) AuthenticateBearer(ctx context.Context, token string) (session.BearerSession, error) {
	return authenticate(ctx, token)
}

func TestVerifiedPageRequiresCurrentCredential(t *testing.T) {
	for _, test := range []struct {
		name   string
		reason session.Code
		status int
		body   string
	}{
		{"revoked", session.AuthTokenRevoked, http.StatusUnauthorized, `{"error":{"code":"AUTH_INVALID_TOKEN"}}`},
		{"unavailable", session.AuthStorageUnavailable, http.StatusServiceUnavailable, `{"error":{"code":"AUTH_UNAVAILABLE"}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := Handler{auth: pageAuthenticator(func(context.Context, string) (session.BearerSession, error) {
				return session.BearerSession{}, session.Fail(test.reason)
			})}
			response := httptest.NewRecorder()
			handler.writeVerifiedPage(response, t.Context(), "fixture-token", session.BearerSession{}, []byte(`{"items":[]}`))
			if response.Code != test.status || response.Body.String() != test.body {
				t.Fatalf("page was not withheld: HTTP %d %q", response.Code, response.Body)
			}
		})
	}
}
