//go:build integration

package accountrevocation_test

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestAccountRevocationSecurity(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(13)
	const user = "revoke_security_owner"
	f.user(user)
	disabled := f.server("", nil)
	enabled := f.server("1", nil)
	active := admit(t, f, disabled, user, "initial")
	wire := revokeBody(t, user, "security_test", passwordChanged)

	f.call(disabled, http.MethodPost, revokePath, f.issuerKey, wire, nil).requireError(t, http.StatusNotFound, "HTTP_ROUTE_NOT_FOUND")
	assertBearer(t, f, disabled, active.Token, http.StatusOK)
	requireStartupFailure(t, f.startServer("1", map[string]string{"NEWIM_AUTH_DEVICE_ADMISSION": "0"}))
	requireStartupFailure(t, f.startServer("1", map[string]string{"NEWIM_AUTH_ISSUER_TOKEN_FILE": "", "NEWIM_AUTH_ISSUER_TTL_SECONDS": ""}))

	for _, item := range []struct {
		name, token string
		extra       http.Header
	}{
		{"missing", "", nil},
		{"forged-application-key", strings.Repeat("2", 64), nil},
		{"valid-session-bearer", active.Token, nil},
		{"duplicated", "", http.Header{"Authorization": {"Bearer " + f.issuerKey, "Bearer " + f.issuerKey}}},
	} {
		t.Run(item.name, func(t *testing.T) {
			got := f.call(enabled, http.MethodPost, revokePath, item.token, wire, item.extra)
			got.requireError(t, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
			if got.headers.Get("WWW-Authenticate") != `Bearer realm="newim-issuer"` {
				t.Fatal("revocation uses an unexpected challenge instead of the issuer boundary")
			}
		})
	}
	assertBearer(t, f, enabled, f.issuerKey, http.StatusUnauthorized)
	for _, alias := range []string{revokePath + "/", "/api/v1/internal/session/revoke%2daccount"} {
		f.call(enabled, http.MethodPost, alias, f.issuerKey, wire, nil).requireError(t, http.StatusNotFound, "HTTP_ROUTE_NOT_FOUND")
	}
	reply := f.call(enabled, http.MethodGet, revokePath, f.issuerKey, nil, nil)
	reply.requireError(t, http.StatusMethodNotAllowed, "HTTP_METHOD_NOT_ALLOWED")
	if reply.headers.Get("Allow") != "POST" {
		t.Fatal("method denial omitted the POST allowance")
	}
	f.call(enabled, http.MethodPost, revokePath+"?x=1", f.issuerKey, wire, nil).requireError(t, http.StatusBadRequest, "HTTP_INVALID_QUERY")
	f.call(enabled, http.MethodPost, revokePath, f.issuerKey, wire, http.Header{"Content-Encoding": {"gzip"}}).
		requireError(t, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
	f.call(enabled, http.MethodPost, revokePath, f.issuerKey, wire, http.Header{"Content-Type": {"text/plain"}}).
		requireError(t, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
	f.call(enabled, http.MethodPost, revokePath, f.issuerKey, bytes.Repeat([]byte{' '}, 1025), nil).
		requireError(t, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")

	cases := map[string][]byte{
		"invalid-reason-case":      revokeBody(t, user, "uppercase", "PASSWORD_CHANGED"),
		"reserved-device-eviction": revokeBody(t, user, "device_limit", "device_limit"),
		"invalid-request-id":       revokeBody(t, user, "spaces in request", passwordChanged),
		"unknown-actor-field":      []byte(`{"userId":"revoke_security_owner","requestId":"x","reason":"signed_out_all","actorId":"other"}`),
		"duplicate-reason":         []byte(`{"userId":"revoke_security_owner","requestId":"x","reason":"signed_out_all","reason":"account_blocked"}`),
		"escaped-duplicate":        []byte(`{"userId":"revoke_security_owner","requestId":"x","reason":"signed_out_all","reas\u006fn":"account_blocked"}`),
		"null-reason":              []byte(`{"userId":"revoke_security_owner","requestId":"x","reason":null}`),
		"missing-user":             []byte(`{"requestId":"x","reason":"signed_out_all"}`),
		"trailing-json":            append(append([]byte(nil), wire...), []byte(" {}")...),
	}
	for name, invalid := range cases {
		t.Run(name, func(t *testing.T) {
			f.call(enabled, http.MethodPost, revokePath, f.issuerKey, invalid, nil).requireError(t, http.StatusBadRequest, "AUTH_INVALID_INPUT")
		})
	}
	revocation(t, f, enabled, "revoke_missing_user", "missing_account", passwordChanged).
		requireError(t, http.StatusForbidden, "AUTH_SESSION_NOT_ELIGIBLE")
	requireUnaffected(t, f, user)
	var receipts int
	must(t, f.db.QueryRow(f.ctx, "SELECT count(*) FROM newim.im_account_revocation_requests").Scan(&receipts))
	if receipts != 0 {
		t.Fatal("a denied request recorded a successful receipt")
	}
	assertBearer(t, f, enabled, active.Token, http.StatusOK)
	enabled.requireRedacted(t, f.issuerKey, active.Token, user, "security_test", "revoke_missing_user", testDSN)
	disabled.requireRedacted(t, f.issuerKey, active.Token, user, testDSN)
}

func requireStartupFailure(t *testing.T, p *process) {
	t.Helper()
	select {
	case <-p.done:
		log := p.log.String()
		if p.waitErr == nil || !strings.Contains(log, "SERVER_INVALID_AUTH_CONFIG") {
			t.Fatalf("invalid account revocation startup did not fail with its fixed code: %v", p.waitErr)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("invalid account revocation startup did not terminate")
	}
}
