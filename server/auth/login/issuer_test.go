package login

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	app "github.com/m-ice/NewIM/server/auth/session"
)

const testIssuerSecret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
const testIssueBody = `{"userId":"user","deviceId":"phone","sessionId":"session"}`

func writeTestCredential(t *testing.T, body string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "issuer")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadIssuerDigest(t *testing.T) {
	want := sha256.Sum256([]byte(testIssuerSecret))
	for _, file := range []struct {
		body string
		mode os.FileMode
	}{
		{testIssuerSecret, 0o400},
		{testIssuerSecret + "\n", 0o600},
	} {
		path := writeTestCredential(t, file.body, file.mode)
		got, err := loadIssuerDigest(path)
		if err != nil || got != want {
			t.Fatalf("valid credential failed to load: %v", err)
		}
	}
	for _, test := range []struct {
		name string
		body string
		mode os.FileMode
	}{
		{"group read", testIssuerSecret, 0o640},
		{"executable", testIssuerSecret, 0o700},
		{"uppercase", strings.ToUpper(testIssuerSecret), 0o600},
		{"too short", testIssuerSecret[1:], 0o600},
		{"extra character", testIssuerSecret + "0", 0o600},
		{"two newlines", testIssuerSecret + "\n\n", 0o600},
		{"CRLF", testIssuerSecret + "\r\n", 0o600},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := writeTestCredential(t, test.body, test.mode)
			_, err := loadIssuerDigest(path)
			if err != errInvalidIssuerConfig {
				t.Fatal("invalid credential did not produce the fixed configuration error")
			}
		})
	}
}

func TestLoadIssuerDigestRejectsNonRegularPaths(t *testing.T) {
	target := writeTestCredential(t, testIssuerSecret, 0o600)
	dir := t.TempDir()
	link := filepath.Join(dir, "issuer-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{link, dir, "relative", filepath.Join(dir, "missing")} {
		if _, err := loadIssuerDigest(path); err != errInvalidIssuerConfig {
			t.Fatalf("invalid path %q did not produce the fixed error", filepath.Base(path))
		}
	}

	fifo := filepath.Join(dir, "issuer-fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := loadIssuerDigest(fifo)
		result <- err
	}()
	select {
	case err := <-result:
		if err != errInvalidIssuerConfig {
			t.Fatal("FIFO was not rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("credential load waited for a FIFO writer")
	}
}

type issueStore struct {
	app.Store
	binding app.SessionBinding
	calls   int
}

func (s *issueStore) Issue(_ context.Context, binding app.SessionBinding, generate func() (app.IssuedToken, error)) (app.IssuedToken, error) {
	s.binding = binding
	s.calls++
	return generate()
}

type noopObserver struct{}

func (noopObserver) Observe(app.Observation) {}

func issueRequest(body io.Reader, token string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, IssuerRoute, body)
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")
	request.Header.Set("Authorization", "Bearer "+token)
	return request
}

func TestIssuerUsesConfiguredBindingAndTTL(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	store := &issueStore{}
	service, err := app.NewService(store, app.Config{
		Clock:    app.ClockFunc(func() time.Time { return now }),
		Observer: noopObserver{},
	})
	if err != nil {
		t.Fatal(err)
	}
	path := writeTestCredential(t, testIssuerSecret+"\n", 0o400)
	for _, invalid := range []time.Duration{0, time.Second + time.Millisecond, 25 * time.Hour} {
		if handler, err := NewIssuerHandler(service, path, invalid); handler != nil || err != errInvalidIssuerConfig {
			t.Fatalf("invalid TTL %v was accepted", invalid)
		}
	}
	handler, err := NewIssuerHandler(service, path, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, issueRequest(strings.NewReader(testIssueBody), testIssuerSecret))
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("unexpected issuance response status=%d", response.Code)
	}
	var token struct {
		Version   int       `json:"version"`
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expiresAt"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &token); err != nil {
		t.Fatal("invalid issuance response")
	}
	if store.calls != 1 || store.binding != (app.SessionBinding{UserID: "user", DeviceID: "phone", SessionID: "session"}) {
		t.Fatal("session service did not receive the exact binding")
	}
	if token.Version != 1 || !strings.HasPrefix(token.Token, app.TokenPrefix) || token.Token == testIssuerSecret ||
		len(token.Token) != app.RawTokenLen || !token.ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatal("unexpected issued token envelope")
	}
}

func TestDecodeIssueBindingRejectsInvalidFields(t *testing.T) {
	tests := []string{
		`{"UserId":"user","deviceId":"phone","sessionId":"session"}`,
		`{"userId":"user","userId":"other","deviceId":"phone","sessionId":"session"}`,
		`{"userId":"user","\u0075serId":"other","deviceId":"phone","sessionId":"session"}`,
		`{"userId":"user","deviceId":"phone","sessionId":"session","role":"admin"}`,
		`{"userId":"user","deviceId":"phone","sessionId":null}`,
		`{"userId":"user","deviceId":"phone","sessionId":1}`,
		`{"userId":"user","deviceId":"phone"}`,
		`{"userId":"bad id","deviceId":"phone","sessionId":"session"}`,
		testIssueBody + "[]",
		testIssueBody + "garbage",
		"[]",
		"\xff" + testIssueBody,
	}
	for _, input := range tests {
		if _, valid := decodeIssueBinding([]byte(input)); valid {
			t.Errorf("accepted invalid binding %q", input)
		}
	}
	if _, valid := decodeIssueBinding([]byte(" \r\n" + testIssueBody + "\t")); !valid {
		t.Fatal("invalid handling of JSON whitespace")
	}
}

type unreadBody struct{ reads int }

func (r *unreadBody) Read([]byte) (int, error) {
	r.reads++
	return 0, io.EOF
}

func TestIssuerRejectsInvalidCredentialBeforeReadingBody(t *testing.T) {
	for _, tokens := range [][]string{
		nil,
		{"Bearer " + strings.Repeat("f", issuerCredentialLength)},
		{"Bearer " + app.TokenPrefix + strings.Repeat("0", app.RawTokenLen-len(app.TokenPrefix))},
		{"Bearer " + testIssuerSecret, "Bearer " + testIssuerSecret},
	} {
		handler := &IssuerHandler{
			digest: sha256.Sum256([]byte(testIssuerSecret)),
			issue: func(context.Context, app.IssueRequest) (app.IssuedToken, error) {
				t.Fatal("issue called with invalid issuer authentication")
				return app.IssuedToken{}, nil
			},
		}
		body := &unreadBody{}
		request := issueRequest(body, "")
		request.Header["Authorization"] = tokens
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if body.reads != 0 || response.Code != http.StatusUnauthorized ||
			response.Body.String() != "{\"error\":{\"code\":\"AUTH_INVALID_TOKEN\"}}\n" {
			t.Fatal("issuer authentication did not precede request body reading")
		}
	}
}

func TestIssuerFailureEnvelopes(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"invalid", app.Fail(app.AuthInvalidInput), 400, "AUTH_INVALID_INPUT"},
		{"binding", app.Fail(app.AuthForbidden), 403, "AUTH_SESSION_NOT_ELIGIBLE"},
		{"missing", app.Fail(app.AuthSessionNotFound), 403, "AUTH_SESSION_NOT_ELIGIBLE"},
		{"revoked", app.Fail(app.AuthSessionRevoked), 403, "AUTH_SESSION_NOT_ELIGIBLE"},
		{"storage", errors.New("synthetic driver diagnostic"), 503, "AUTH_UNAVAILABLE"},
		{"zero token", nil, 503, "AUTH_UNAVAILABLE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := &IssuerHandler{
				digest: sha256.Sum256([]byte(testIssuerSecret)),
				issue: func(context.Context, app.IssueRequest) (app.IssuedToken, error) {
					return app.IssuedToken{}, test.err
				},
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, issueRequest(strings.NewReader(testIssueBody), testIssuerSecret))
			if response.Code != test.status || response.Header().Get("Cache-Control") != "no-store" ||
				response.Body.String() != (`{"error":{"code":"`+test.code+`"}}`+"\n") {
				t.Fatalf("unexpected error response status=%d", response.Code)
			}
		})
	}
}

func TestIssuerBoundsBodyAndChecksTransport(t *testing.T) {
	handler := &IssuerHandler{
		digest: sha256.Sum256([]byte(testIssuerSecret)),
		issue: func(context.Context, app.IssueRequest) (app.IssuedToken, error) {
			t.Fatal("issue called with invalid transport")
			return app.IssuedToken{}, nil
		},
	}
	tests := []struct {
		name   string
		change func(*http.Request)
		status int
	}{
		{"method", func(r *http.Request) { r.Method = http.MethodGet }, http.StatusMethodNotAllowed},
		{"path", func(r *http.Request) { r.URL.Path += "/" }, http.StatusNotFound},
		{"query", func(r *http.Request) { r.URL.ForceQuery = true }, http.StatusBadRequest},
		{"unsupported media", func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }, http.StatusUnsupportedMediaType},
		{"unsupported encoding", func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }, http.StatusUnsupportedMediaType},
		{"known oversize", func(r *http.Request) { r.ContentLength = IssuerBodyLimit + 1 }, http.StatusRequestEntityTooLarge},
		{"streamed oversize", func(r *http.Request) {
			r.ContentLength = -1
			r.Body = io.NopCloser(bytes.NewReader(bytes.Repeat([]byte{' '}, IssuerBodyLimit+1)))
		}, http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := issueRequest(strings.NewReader(testIssueBody), testIssuerSecret)
			test.change(request)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status got %d want %d", response.Code, test.status)
			}
		})
	}
}
