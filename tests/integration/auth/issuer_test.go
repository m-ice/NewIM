//go:build integration

package auth_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	app "github.com/m-ice/NewIM/server/auth/session"
)

const (
	issuerRoute   = "/api/v1/internal/session/token"
	issuerFileEnv = "NEWIM_AUTH_ISSUER_TOKEN_FILE"
	issuerTTLEnv  = "NEWIM_AUTH_ISSUER_TTL_SECONDS"
)

func TestAuthIssuer(t *testing.T) {
	if os.Getenv("NEWIM_SERVER_BINARY") == "" {
		t.Fatal("NEWIM_SERVER_BINARY is required")
	}
	t.Run("disabled-preserves-existing-routes", testAuthIssuerDisabled)
	t.Run("invalid-startup-before-bind", testAuthIssuerStartup)
	t.Run("qualified-binding-and-credential-isolation", testAuthIssuerRoundTrip)
	t.Run("deferred-commit-failure-and-recovery", testAuthIssuerCommitFailure)
}

func testAuthIssuerDisabled(t *testing.T) {
	f := openFixture(t)
	f.now.Store(time.Now().Unix())
	binding := f.seedBinding("issuer_disabled")
	token := f.issue(f.service(&captureObserver{}, nil, nil), binding, time.Hour)
	before := f.tokenCount()
	server := startAuthServer(t, issuerEnvironment("", ""))
	defer server.stop(t)
	server.waitHealthy(t)

	response := callIssuer(t, server, http.MethodPost, issuerRoute, issuerRequestBody(t, binding), "Bearer "+token.RawToken())
	requireIssuerError(t, response, http.StatusNotFound, "HTTP_ROUTE_NOT_FOUND")
	base := "http://" + server.api
	if _, status, _ := doSessionRequest(t, base, "Bearer "+token.RawToken()); status != http.StatusOK {
		t.Fatalf("legacy session query status=%d", status)
	}
	if _, status, _ := doTokenRevokeRequest(t, base, "Bearer "+token.RawToken()); status != http.StatusNoContent {
		t.Fatalf("legacy token revocation status=%d", status)
	}
	if f.tokenCount() != before || !f.tokenRevoked(binding, token.TokenID()) || f.sessionRevoked(binding) {
		t.Fatal("disabled issuer changed legacy token or session behavior")
	}
}

func testAuthIssuerStartup(t *testing.T) {
	f := openFixture(t)
	key := newIssuerKey(t)
	file := writeIssuerKey(t, key, 0o600)
	worldReadable := writeIssuerKey(t, key, 0o644)
	upper := writeIssuerKey(t, "A"+key[1:], 0o600)
	short := writeIssuerKey(t, key[:63], 0o600)
	repeatedLF := writeIssuerKey(t, key+"\n\n", 0o600)
	alias := filepath.Join(t.TempDir(), "alias")
	must(t, os.Symlink(file, alias))
	for _, test := range []struct {
		name, file, ttl string
	}{
		{"file-only", file, ""},
		{"ttl-only", "", "60"},
		{"ttl-zero", file, "0"},
		{"ttl-over-limit", file, "86401"},
		{"ttl-fractional", file, "1.5"},
		{"relative-file", filepath.Base(file), "60"},
		{"open-permissions", worldReadable, "60"},
		{"directory", t.TempDir(), "60"},
		{"symlink", alias, "60"},
		{"uppercase", upper, "60"},
		{"short", short, "60"},
		{"second-newline", repeatedLF, "60"},
	} {
		t.Run(test.name, func(t *testing.T) {
			expectIssuerConfigFailure(t, issuerEnvironment(test.file, test.ttl), false, key)
		})
	}
	t.Run("requires-storage", func(t *testing.T) {
		env := issuerEnvironment(file, "60")
		env["NEWIM_AUTH_DSN"] = ""
		expectIssuerConfigFailure(t, env, false, key)
	})
	t.Run("malformed-storage-config", func(t *testing.T) {
		env := issuerEnvironment(file, "60")
		env["NEWIM_AUTH_DSN"] = "postgres://user:%zz@127.0.0.1/newim"
		expectIssuerConfigFailure(t, env, false, key)
	})
	t.Run("requires-loopback", func(t *testing.T) {
		expectIssuerConfigFailure(t, issuerEnvironment(file, "60"), true, key)
	})
	if got := activeAuthConnections(t, f); got != 0 {
		t.Fatalf("unsafe issuer config left %d connections", got)
	}
}

func testAuthIssuerRoundTrip(t *testing.T) {
	f := openFixture(t)
	binding := f.seedBinding("issuer_roundtrip")
	other := f.seedBinding("issuer_other")
	revoked := f.seedBinding("issuer_revoked")
	f.revokeSessionSQL(revoked, time.Now())
	beforeBindings := issuerBindingState(f)
	beforeTokens := f.tokenCount()

	key := newIssuerKey(t)
	file := writeIssuerKey(t, key+"\n", 0o400)
	server := startAuthServer(t, issuerEnvironment(file, "120"))
	defer server.stop(t)
	server.waitHealthy(t)
	body := issuerRequestBody(t, binding)
	auth := "Bearer " + key

	for _, headers := range [][]string{
		nil,
		{"Bearer " + newIssuerKey(t)},
		{auth, auth},
	} {
		requireIssuerError(t, callIssuer(t, server, http.MethodPost, issuerRoute, body, headers...), http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	}
	requireIssuerError(t, callIssuer(t, server, http.MethodPost, issuerRoute+"?userId="+other.UserID, body, auth),
		http.StatusBadRequest, "HTTP_INVALID_QUERY")
	wrongMethod := callIssuer(t, server, http.MethodGet, issuerRoute, nil, auth)
	requireIssuerError(t, wrongMethod, http.StatusMethodNotAllowed, "HTTP_METHOD_NOT_ALLOWED")
	if wrongMethod.headers.Get("Allow") != http.MethodPost {
		t.Fatal("issuer has an unexpected method contract")
	}
	requireIssuerError(t, callIssuer(t, server, http.MethodPost, issuerRoute, []byte(strings.Repeat(" ", 1025)), auth),
		http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
	chunked := newIssuerRequest(t, server, http.MethodPost, issuerRoute, []byte(strings.Repeat(" ", 1025)), auth)
	chunked.ContentLength = -1
	chunked.TransferEncoding = []string{"chunked"}
	requireIssuerError(t, sendIssuerRequest(t, chunked), http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
	foreignEncoding := newIssuerRequest(t, server, http.MethodPost, issuerRoute, body, auth)
	foreignEncoding.Header.Set("Content-Type", "application/json; charset=iso-8859-1")
	requireIssuerError(t, sendIssuerRequest(t, foreignEncoding), http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")

	for _, malformed := range []string{
		`{}`,
		`{"userId":"a","deviceId":"x","sessionId":"z","userId":"b"}`,
		`{"userId":"a","deviceId":"x","sessionId":"z","ttl":180}`,
		`{"userId":"a","deviceId":"x","sessionId":42}`,
		`{"userId":"a","deviceId":"x","sessionId":"z"}{}`,
	} {
		requireIssuerError(t, callIssuer(t, server, http.MethodPost, issuerRoute, []byte(malformed), auth),
			http.StatusBadRequest, "AUTH_INVALID_INPUT")
	}
	for _, unavailable := range []app.SessionBinding{
		newBinding("issuer_missing"),
		{UserID: other.UserID, DeviceID: other.DeviceID, SessionID: binding.SessionID},
		{UserID: binding.UserID, DeviceID: other.DeviceID, SessionID: binding.SessionID},
		{UserID: binding.UserID, DeviceID: binding.DeviceID, SessionID: other.SessionID},
		revoked,
	} {
		requireIssuerError(t, callIssuer(t, server, http.MethodPost, issuerRoute, issuerRequestBody(t, unavailable), auth),
			http.StatusForbidden, "AUTH_SESSION_NOT_ELIGIBLE")
	}
	if f.tokenCount() != beforeTokens || issuerBindingState(f) != beforeBindings {
		t.Fatal("rejected issue changed database state")
	}

	maxBody := append(bytes.Clone(body), bytes.Repeat([]byte(" "), 1024-len(body))...)
	first := requireIssuedToken(t, callIssuer(t, server, http.MethodPost, issuerRoute, maxBody, auth))
	second := requireIssuedToken(t, callIssuer(t, server, http.MethodPost, issuerRoute, body, auth))
	firstID := inspectIssuedToken(t, f, binding, first, 2*time.Minute)
	secondID := inspectIssuedToken(t, f, binding, second, 2*time.Minute)
	if firstID == secondID || f.tokenCount() != beforeTokens+2 || issuerBindingState(f) != beforeBindings {
		t.Fatal("issuer did not commit exactly two independent tokens for an existing binding")
	}

	requireIssuerError(t, callIssuer(t, server, http.MethodPost, issuerRoute, body, "Bearer "+first.Token),
		http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	base := "http://" + server.api
	if _, status, _ := doSessionRequest(t, base, auth); status != http.StatusUnauthorized {
		t.Fatalf("application credential accepted on session route, status=%d", status)
	}
	if _, status, _ := doTokenRevokeRequest(t, base, auth); status != http.StatusUnauthorized {
		t.Fatalf("application credential accepted on revoke route, status=%d", status)
	}
	sessionBody, status, _ := doSessionRequest(t, base, "Bearer "+first.Token)
	if status != http.StatusOK {
		t.Fatalf("issued token session status=%d", status)
	}
	assertIssuerSessionBinding(t, sessionBody, binding, firstID)
	if _, status, _ = doTokenRevokeRequest(t, base, "Bearer "+first.Token); status != http.StatusNoContent {
		t.Fatalf("issued token revoke status=%d", status)
	}
	if _, status, _ = doSessionRequest(t, base, "Bearer "+first.Token); status != http.StatusUnauthorized {
		t.Fatalf("revoked issued token remained effective, status=%d", status)
	}
	if _, status, _ = doSessionRequest(t, base, "Bearer "+second.Token); status != http.StatusOK {
		t.Fatalf("sibling issued token did not remain effective, status=%d", status)
	}
	if !f.tokenRevoked(binding, firstID) || f.tokenRevoked(binding, secondID) || f.sessionRevoked(binding) || f.tokenCount() != beforeTokens+2 {
		t.Fatal("credential separation or token-only revocation changed durable state")
	}
	assertIssuerRedaction(t, server, key, first.Token, second.Token, hashToken(key), hashToken(first.Token), hashToken(second.Token), localDSN)
}

func testAuthIssuerCommitFailure(t *testing.T) {
	f := openFixture(t)
	binding := f.seedBinding("issuer_commit")
	key := newIssuerKey(t)
	server := startAuthServer(t, issuerEnvironment(writeIssuerKey(t, key, 0o600), "60"))
	defer server.stop(t)
	server.waitHealthy(t)

	// The empty reference table causes commit, not INSERT, to fail for any
	// generated token. The constraint exists only in this owned test database.
	f.sql("CREATE TABLE newim.test_issuer_commit(token_id text PRIMARY KEY)")
	f.sql(`ALTER TABLE newim.im_auth_tokens ADD CONSTRAINT test_issuer_commit_guard
		FOREIGN KEY (token_id) REFERENCES newim.test_issuer_commit(token_id)
		DEFERRABLE INITIALLY DEFERRED NOT VALID`)
	body := issuerRequestBody(t, binding)
	auth := "Bearer " + key
	before := f.tokenCount()
	beforeBindings := issuerBindingState(f)
	requireIssuerError(t, callIssuer(t, server, http.MethodPost, issuerRoute, body, auth),
		http.StatusServiceUnavailable, "AUTH_UNAVAILABLE")
	if f.tokenCount() != before || issuerBindingState(f) != beforeBindings {
		t.Fatal("commit failure left a token or changed a binding")
	}
	f.sql("ALTER TABLE newim.im_auth_tokens DROP CONSTRAINT test_issuer_commit_guard")
	f.sql("DROP TABLE newim.test_issuer_commit")
	recovered := requireIssuedToken(t, callIssuer(t, server, http.MethodPost, issuerRoute, body, auth))
	id := inspectIssuedToken(t, f, binding, recovered, time.Minute)
	if f.tokenCount() != before+1 || issuerBindingState(f) != beforeBindings {
		t.Fatal("recovered issuer did not commit exactly one token")
	}
	sessionBody, status, _ := doSessionRequest(t, "http://"+server.api, "Bearer "+recovered.Token)
	if status != http.StatusOK {
		t.Fatalf("recovered token status=%d", status)
	}
	assertIssuerSessionBinding(t, sessionBody, binding, id)
	assertIssuerRedaction(t, server, key, recovered.Token, hashToken(key), hashToken(recovered.Token), "test_issuer_commit_guard", localDSN)
}

func newIssuerKey(t *testing.T) string {
	t.Helper()
	var raw [32]byte
	_, err := rand.Read(raw[:])
	must(t, err)
	return hex.EncodeToString(raw[:])
}

func writeIssuerKey(t *testing.T, key string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "issuer-key")
	must(t, os.WriteFile(path, []byte(key), 0o600))
	must(t, os.Chmod(path, mode))
	return path
}

func issuerEnvironment(file, ttl string) map[string]string {
	return map[string]string{
		"NEWIM_AUTH_DSN":                localDSN,
		"NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1",
		"NEWIM_MESSAGE_HTTP":            "",
		"NEWIM_PRIVATE_MESSAGE_POLICY":  "",
		issuerFileEnv:                   file,
		issuerTTLEnv:                    ttl,
	}
}

func expectIssuerConfigFailure(t *testing.T, env map[string]string, wildcard bool, key string) {
	t.Helper()
	addr, ops := twoFreeAddrs(t)
	apiAddr := addr
	if wildcard {
		_, port, err := net.SplitHostPort(addr)
		must(t, err)
		apiAddr = net.JoinHostPort("0.0.0.0", port)
	}
	commandCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	command := exec.CommandContext(commandCtx, os.Getenv("NEWIM_SERVER_BINARY"), "--api-addr", apiAddr, "--ops-addr", ops)
	command.Env = os.Environ()
	for key, value := range env {
		command.Env = append(command.Env, key+"="+value)
	}
	out, err := command.CombinedOutput()
	if bytes.Contains(out, []byte(key)) || bytes.Contains(out, []byte(hashToken(key))) {
		t.Fatal("invalid config exposed the issuer credential")
	}
	var exit *exec.ExitError
	if commandCtx.Err() != nil || !errors.As(err, &exit) || !bytes.Contains(out, []byte("SERVER_INVALID_AUTH_CONFIG")) {
		t.Fatalf("issuer configuration did not fail before startup: err=%v output=%q", err, out)
	}
	for _, released := range []string{addr, ops} {
		listener, err := net.Listen("tcp", released)
		must(t, err)
		must(t, listener.Close())
	}
}

func issuerRequestBody(t *testing.T, binding app.SessionBinding) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]string{
		"userId": binding.UserID, "deviceId": binding.DeviceID, "sessionId": binding.SessionID,
	})
	must(t, err)
	return body
}

type issuerResponse struct {
	body    []byte
	status  int
	headers http.Header
}

func callIssuer(t *testing.T, server *runningServer, method, route string, body []byte, auth ...string) issuerResponse {
	t.Helper()
	return sendIssuerRequest(t, newIssuerRequest(t, server, method, route, body, auth...))
}

func newIssuerRequest(t *testing.T, server *runningServer, method, route string, body []byte, auth ...string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+server.api+route, bytes.NewReader(body))
	must(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for _, value := range auth {
		req.Header.Add("Authorization", value)
	}
	return req
}

func sendIssuerRequest(t *testing.T, req *http.Request) issuerResponse {
	t.Helper()
	res, err := (&http.Client{Timeout: 8 * time.Second}).Do(req)
	must(t, err)
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	must(t, err)
	return issuerResponse{data, res.StatusCode, res.Header}
}

func requireIssuerError(t *testing.T, response issuerResponse, status int, want string) {
	t.Helper()
	var result map[string]map[string]string
	if err := json.Unmarshal(response.body, &result); err != nil || len(result) != 1 || len(result["error"]) != 1 {
		t.Fatalf("issuer returned a nonstandard error body, status=%d", response.status)
	}
	if code := result["error"]["code"]; response.status != status || code != want {
		t.Fatalf("issuer error status=%d code=%s, want %d %s", response.status, code, status, want)
	}
	if response.headers.Get("Cache-Control") != "no-store" {
		t.Fatal("issuer error is missing no-store")
	}
}

type issuedResponse struct {
	Version   int       `json:"version"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expiresAt"`
}

func requireIssuedToken(t *testing.T, response issuerResponse) issuedResponse {
	t.Helper()
	if response.status != http.StatusOK || response.headers.Get("Cache-Control") != "no-store" ||
		response.headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("issuer success status=%d/cache=%q/content-type=%q", response.status, response.headers.Get("Cache-Control"), response.headers.Get("Content-Type"))
	}
	var fields map[string]json.RawMessage
	must(t, json.Unmarshal(response.body, &fields))
	if len(fields) != 3 || fields["version"] == nil || fields["token"] == nil || fields["expiresAt"] == nil {
		t.Fatal("issuer success shape differs from v1 contract")
	}
	var issued issuedResponse
	must(t, json.Unmarshal(response.body, &issued))
	if issued.Version != 1 || issued.ExpiresAt.IsZero() || len(issued.Token) != app.RawTokenLen || !strings.HasPrefix(issued.Token, app.TokenPrefix) {
		t.Fatal("issuer returned an invalid token shape or expiry")
	}
	return issued
}

func inspectIssuedToken(t *testing.T, f *fixture, binding app.SessionBinding, issued issuedResponse, ttl time.Duration) string {
	t.Helper()
	id := issued.Token[len(app.TokenPrefix) : len(app.TokenPrefix)+app.TokenIDHexLen]
	var sessionID, storedDigest, record string
	var created, expires time.Time
	must(t, f.db.QueryRow(ctx, `SELECT session_id,encode(token_digest,'hex'),row_to_json(t)::text,created_at,expires_at
		FROM newim.im_auth_tokens AS t WHERE token_id=$1`, id).Scan(&sessionID, &storedDigest, &record, &created, &expires))
	if sessionID != binding.SessionID || storedDigest != hashToken(issued.Token) || strings.Contains(record, issued.Token) {
		t.Fatal("issued token storage does not contain the exact session and digest-only binding")
	}
	if expires.Sub(created) != ttl || !expires.Equal(issued.ExpiresAt.Truncate(time.Microsecond)) {
		t.Fatalf("issued token TTL or persisted expiry mismatch")
	}
	return id
}

func issuerBindingState(f *fixture) string {
	return f.scalarString(`SELECT
		(SELECT md5(COALESCE(string_agg(t::text,',' ORDER BY user_id),'')) FROM newim.im_users AS t) || '/' ||
		(SELECT md5(COALESCE(string_agg(t::text,',' ORDER BY user_id,device_id),'')) FROM newim.im_devices AS t) || '/' ||
		(SELECT md5(COALESCE(string_agg(t::text,',' ORDER BY session_id),'')) FROM newim.im_sessions AS t)`)
}

func assertIssuerSessionBinding(t *testing.T, body []byte, binding app.SessionBinding, tokenID string) {
	t.Helper()
	var result struct {
		Version int
		Session struct {
			UserID    string
			DeviceID  string
			SessionID string
			TokenID   string
		}
	}
	must(t, json.Unmarshal(body, &result))
	if result.Version != 1 || result.Session.UserID != binding.UserID || result.Session.DeviceID != binding.DeviceID ||
		result.Session.SessionID != binding.SessionID || result.Session.TokenID != tokenID {
		t.Fatal("session query did not return the exact issued binding")
	}
}

func assertIssuerRedaction(t *testing.T, server *runningServer, fragments ...string) {
	t.Helper()
	metrics, status, _ := doRawRequest(t, http.MethodGet, "http://"+server.ops+"/metrics", nil, "")
	if status != http.StatusOK || !bytes.Contains(metrics, []byte(`route="auth_issuer"`)) {
		t.Fatal("issuer route is missing its stable metric label")
	}
	log := server.stdout.String() + server.stderr.String()
	for _, value := range fragments {
		if strings.Contains(log, value) || bytes.Contains(metrics, []byte(value)) {
			t.Fatal("issuer logs or metrics contain a credential, digest or storage detail")
		}
	}
}
