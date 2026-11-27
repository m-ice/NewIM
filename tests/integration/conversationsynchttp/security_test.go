//go:build integration

package conversationsynchttp_test

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

func TestSyncHTTPSecurity(t *testing.T) {
	t.Run("request-shape-and-bearer-boundary", testSyncRequestPolicy)
	t.Run("ready-and-history-permissions", testSyncReadinessAndHistory)
	t.Run("authenticated-expiry-and-rolling-keys", testSyncExpiryAndRotation)
	t.Run("key-file-validation", testSyncKeyFiles)
	t.Run("separate-ip-and-account-budgets", testSyncBudgets)
}

func testSyncRequestPolicy(t *testing.T) {
	f := openFixture(t)
	loopback := f.server("1", nil)
	keyPath := writeKeys(t, "old", "old")
	off := f.gateway("1", nil)
	gateway := f.syncGateway(keyPath, nil)
	const owner = "synchttp_security_owner"
	const outsider = "synchttp_security_outsider"
	provision(t, f, loopback, owner)
	provision(t, f, loopback, outsider)
	token := f.admit(owner, "phone")
	other := f.admit(outsider, "mobile")
	for _, path := range []string{bootstrapPath, deltaPath} {
		requireAbsent(t, f.call(off, http.MethodPost, path, token, []byte("{}"), nil))
	}
	createGroup(t, f, off, token, "synchttp_security_a", nil, true)
	createGroup(t, f, gateway, token, "synchttp_security_b", nil, true)
	start := readPage(t, f, gateway, bootstrapPath, token, map[string]int{"limit": 1})
	body := encode(t, continueWith(start.NextCursor))
	for _, test := range []struct {
		token   string
		headers http.Header
	}{
		{"", nil},
		{"synthetic_invalid_session", nil},
		{f.issuerKey, nil},
		{"", http.Header{"Authorization": {"Bearer " + token, "Bearer " + token}}},
	} {
		requireSyncError(t, f.call(gateway, http.MethodPost, bootstrapPath, test.token, body, test.headers), 401, "AUTH_INVALID_TOKEN")
	}
	requireSyncError(t, f.call(gateway, http.MethodPost, bootstrapPath, other, body, nil), 400, "SYNC_INVALID_CURSOR")
	if page := readPage(t, f, gateway, bootstrapPath, other, map[string]any{}); len(page.Items) != 0 {
		t.Fatal("different authenticated account saw another account's directory")
	}
	bootstrap := []byte("{}")
	for _, path := range []string{bootstrapPath, deltaPath} {
		badMethod := f.call(gateway, http.MethodGet, path, token, nil, nil)
		requireSyncError(t, badMethod, 405, "HTTP_METHOD_NOT_ALLOWED")
		if badMethod.headers.Get("Allow") != "POST" {
			t.Fatal("method error omitted the POST allowance")
		}
		requireSyncError(t, f.call(gateway, http.MethodPost, path+"?x=1", token, bootstrap, nil), 400, "SYNC_INVALID_INPUT")
		requireSyncError(t, f.call(gateway, http.MethodPost, path+"?", token, bootstrap, nil), 400, "SYNC_INVALID_INPUT")
		requireSyncError(t, f.call(gateway, http.MethodPost, path, token, bootstrap, http.Header{"Content-Type": {"text/plain"}}), 415, "HTTP_UNSUPPORTED_MEDIA_TYPE")
		requireSyncError(t, f.call(gateway, http.MethodPost, path, token, bytes.Repeat([]byte{' '}, 4097), nil), 413, "HTTP_BODY_TOO_LARGE")
		requireAbsent(t, f.call(gateway, http.MethodPost, path+"/", token, bootstrap, nil))
	}
	requireAbsent(t, f.call(gateway, http.MethodPost, "/api/v1/sync/%62ootstrap", token, bootstrap, nil))
	stream := io.NopCloser(bytes.NewReader(bytes.Repeat([]byte{' '}, 4097)))
	request, err := http.NewRequestWithContext(f.ctx, http.MethodPost, gateway.url+bootstrapPath, stream)
	must(t, err)
	request.ContentLength = -1
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	requireSyncError(t, f.execute(request), 413, "HTTP_BODY_TOO_LARGE")
	for _, test := range []struct {
		name, path string
		body       []byte
		code       string
	}{
		{"null-body", bootstrapPath, []byte("null"), "SYNC_INVALID_INPUT"},
		{"empty-cursor", bootstrapPath, []byte(`{"cursor":""}`), "SYNC_INVALID_INPUT"},
		{"null-limit", bootstrapPath, []byte(`{"limit":null}`), "SYNC_INVALID_INPUT"},
		{"noninteger-limit", bootstrapPath, []byte(`{"limit":1.2}`), "SYNC_INVALID_INPUT"},
		{"large-limit", bootstrapPath, []byte(`{"limit":101}`), "SYNC_LIMIT_EXCEEDED"},
		{"negative-limit", bootstrapPath, []byte(`{"limit":-1}`), "SYNC_LIMIT_EXCEEDED"},
		{"duplicate-field", bootstrapPath, []byte(`{"limit":1,"limi\u0074":2}`), "SYNC_INVALID_INPUT"},
		{"caller-identity-field", bootstrapPath, []byte(`{"userId":"synchttp_security_owner"}`), "SYNC_INVALID_INPUT"},
		{"mixed-bootstrap", bootstrapPath, encode(t, map[string]any{"cursor": start.NextCursor, "limit": 1}), "SYNC_INVALID_INPUT"},
		{"bootstrap-checkpoint-field", bootstrapPath, encode(t, map[string]any{"checkpoint": start.NextCursor}), "SYNC_INVALID_INPUT"},
		{"missing-checkpoint", deltaPath, bootstrap, "SYNC_INVALID_INPUT"},
		{"mixed-delta", deltaPath, encode(t, map[string]any{"cursor": start.NextCursor, "checkpoint": start.NextCursor}), "SYNC_INVALID_INPUT"},
		{"mixed-delta-limit", deltaPath, encode(t, map[string]any{"cursor": start.NextCursor, "limit": 1}), "SYNC_INVALID_INPUT"},
		{"trailing-json", bootstrapPath, []byte("{} {}"), "SYNC_INVALID_INPUT"},
		{"invalid-utf8", bootstrapPath, []byte{'{', '"', 0xff, '"', ':', '0', '}'}, "SYNC_INVALID_INPUT"},
		{"invalid-signature", bootstrapPath, encode(t, continueWith(tamperCursor(start.NextCursor))), "SYNC_INVALID_CURSOR"},
		{"wrong-cursor-kind", deltaPath, encode(t, map[string]string{"checkpoint": start.NextCursor}), "SYNC_INVALID_CURSOR"},
		{"oversized-cursor", bootstrapPath, encode(t, continueWith(strings.Repeat("a", app.MaxCursorBytes+1))), "SYNC_INVALID_CURSOR"},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireSyncError(t, f.call(gateway, http.MethodPost, test.path, token, test.body, nil), 400, test.code)
		})
	}
	if page := readPage(t, f, gateway, bootstrapPath, token, map[string]int{"limit": 0}); len(page.Items) != 2 || page.HasMore {
		t.Fatal("explicit integer 0 did not preserve the default page limit")
	}
	assertAuthUnavailable(t, f, gateway, token)
	_, err = f.auth.RevokeBearer(f.ctx, token)
	must(t, err)
	requireSyncError(t, f.call(gateway, http.MethodPost, bootstrapPath, token, bootstrap, nil), 401, "AUTH_INVALID_TOKEN")
	gateway.requireRedacted(t, token, other, f.issuerKey, keyPath, fixtureKeyHex("old"), start.NextCursor, testDSN)
}

func tamperCursor(cursor string) string {
	end := len(cursor) - 5
	value := byte('A')
	if cursor[end] == value {
		value = 'B'
	}
	return cursor[:end] + string(value) + cursor[end+1:]
}

func assertAuthUnavailable(t *testing.T, f *fixture, gateway *process, token string) {
	t.Helper()
	conn, err := pgx.Connect(f.ctx, testDSN)
	must(t, err)
	defer conn.Close(f.ctx)
	tx, err := conn.Begin(f.ctx)
	must(t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, "LOCK TABLE newim.im_auth_tokens IN ACCESS EXCLUSIVE MODE")
	must(t, err)
	requireSyncError(t, f.call(gateway, http.MethodPost, bootstrapPath, token, []byte("{}"), nil), 503, "AUTH_UNAVAILABLE")
	must(t, tx.Rollback(f.ctx))
	readPage(t, f, gateway, bootstrapPath, token, map[string]any{})
}

func testSyncKeyFiles(t *testing.T) {
	f := openFixture(t)
	valid := writeKeys(t, "old", "old")
	link := filepath.Join(t.TempDir(), "link.json")
	must(t, os.Symlink(valid, link))
	world := keyFileContents(t, encode(t, map[string]any{"activeKeyId": "old", "keys": map[string]string{"old": fixtureKeyHex("old")}}), 0o644)
	badActive := writeKeys(t, "missing", "old")
	upper := keyFileContents(t, encode(t, map[string]any{"activeKeyId": "uppercase", "keys": map[string]string{"uppercase": strings.Repeat("A", 64)}}), 0o600)
	large := keyFileContents(t, bytes.Repeat([]byte{' '}, 4097), 0o600)
	dupe := keyFileContents(t, []byte(fmt.Sprintf(`{"activeKeyId":"old","keys":{"old":%q,"old":%q}}`, fixtureKeyHex("old"), fixtureKeyHex("new"))), 0o600)
	nine := writeKeys(t, "old", "old", "a", "b", "c", "d", "e", "f", "g", "h")
	for _, test := range []struct{ name, path string }{
		{"missing", ""}, {"relative", filepath.Base(valid)}, {"symlink", link}, {"readable-by-group-or-other", world},
		{"directory", t.TempDir()}, {"wrong-active-id", badActive}, {"uppercase-key", upper}, {"oversized-file", large}, {"duplicate-key-id", dupe},
		{"too-many-keys", nine},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := f.startGateway("1", syncSettings(test.path))
			select {
			case <-p.done:
				if p.waitErr == nil || strings.Contains(p.log.String(), "newim-gateway listening on ") {
					t.Fatal("invalid sync key file reached readiness")
				}
				p.requireRedacted(t, fixtureKeyHex("old"), fixtureKeyHex("new"), testDSN)
			case <-time.After(8 * time.Second):
				t.Fatal("invalid sync key file did not reject startup")
			}
		})
	}
	f.syncGateway(valid, nil)
}

func keyFileContents(t *testing.T, data []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "synthetic-keyring.json")
	must(t, os.WriteFile(path, data, mode))
	return path
}

func testSyncBudgets(t *testing.T) {
	f := openFixture(t)
	loopback := f.server("1", nil)
	const account = "synchttp_budget_owner"
	const peer = "synchttp_budget_peer"
	provision(t, f, loopback, account)
	provision(t, f, loopback, peer)
	first := f.admit(account, "phone")
	second := f.admit(account, "tablet")
	other := f.admit(peer, "phone")
	keys := writeKeys(t, "old", "old")
	perAccount := f.syncGateway(keys, map[string]string{"NEWIM_CONVERSATION_SYNC_ACCOUNT_ATTEMPTS_PER_MINUTE": "1"})
	waitRateWindow()
	readPage(t, f, perAccount, bootstrapPath, first, map[string]any{})
	requireSyncError(t, f.call(perAccount, http.MethodPost, bootstrapPath, second, []byte("{}"), nil), 429, "SYNC_RATE_LIMITED")
	readPage(t, f, perAccount, bootstrapPath, other, map[string]any{})
	requireSocket(t, f, perAccount, first, http.StatusSwitchingProtocols)
	createGroup(t, f, perAccount, first, "synchttp_budget_group", nil, true)

	address := f.syncGateway(keys, map[string]string{
		"NEWIM_CONVERSATION_SYNC_IP_ATTEMPTS_PER_MINUTE": "1", "NEWIM_GATEWAY_IP_ATTEMPTS_PER_MINUTE": "1",
	})
	waitRateWindow()
	requireSocket(t, f, address, first, http.StatusSwitchingProtocols)
	requireSocket(t, f, address, other, http.StatusTooManyRequests)
	_ = requirePage(t, f.call(address, http.MethodPost, bootstrapPath, first, []byte("{}"), http.Header{"X-Forwarded-For": {"192.0.2.1"}}))
	requireSyncError(t, f.call(address, http.MethodPost, bootstrapPath, other, []byte("{}"), http.Header{"X-Forwarded-For": {"192.0.2.2"}}), 429, "SYNC_RATE_LIMITED")
}

func requireSocket(t *testing.T, f *fixture, gateway *process, token string, status int) {
	t.Helper()
	conn, reply, err := websocket.Dial(f.ctx, "wss://"+strings.TrimPrefix(gateway.url, "https://")+"/api/v1/gateway",
		&websocket.DialOptions{HTTPClient: f.client, HTTPHeader: http.Header{"Authorization": {"Bearer " + token}}, Subprotocols: []string{"newim.v1"}})
	if conn != nil {
		defer conn.CloseNow()
	}
	if err != nil && reply != nil && reply.Body != nil {
		defer reply.Body.Close()
	}
	if reply == nil || reply.StatusCode != status || (status == http.StatusSwitchingProtocols) != (conn != nil && err == nil) {
		t.Fatalf("existing WSS route did not return HTTP %d, err=%v", status, err)
	}
	if reply.TLS == nil || reply.TLS.Version < tls.VersionTLS12 || len(reply.TLS.VerifiedChains) == 0 {
		t.Fatal("existing WSS route did not establish verified TLS")
	}
}

func waitRateWindow() {
	remaining := time.Until(time.Now().Truncate(time.Minute).Add(time.Minute))
	if remaining < 3*time.Second {
		time.Sleep(remaining + 20*time.Millisecond)
	}
}

func requireAbsent(t *testing.T, reply response) {
	t.Helper()
	if reply.err != nil || reply.status != http.StatusNotFound || !reply.verified {
		t.Fatalf("sync route alias/default-off status=%d err=%v", reply.status, reply.err)
	}
}

func requireSyncError(t *testing.T, reply response, status int, code string) {
	t.Helper()
	reply.requireError(t, status, code)
	if !reply.verified {
		t.Fatal("sync error lacked verified TLS")
	}
}
