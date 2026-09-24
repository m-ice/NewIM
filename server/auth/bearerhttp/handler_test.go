package bearerhttp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	app "github.com/m-ice/NewIM/server/auth/session"
)

type authenticatorFunc func(context.Context, string) (app.BearerSession, error)

func (f authenticatorFunc) AuthenticateBearer(ctx context.Context, token string) (app.BearerSession, error) {
	return f(ctx, token)
}

type testStore struct {
	mu       sync.Mutex
	bindings map[string]app.SessionBinding
	digests  map[string][32]byte
	expires  map[string]time.Time
}

func newTestStore() *testStore {
	return &testStore{
		bindings: make(map[string]app.SessionBinding),
		digests:  make(map[string][32]byte),
		expires:  make(map[string]time.Time),
	}
}

func (s *testStore) Issue(_ context.Context, binding app.SessionBinding, generate func() (app.IssuedToken, error)) (app.IssuedToken, error) {
	if !binding.Valid() || generate == nil {
		return app.IssuedToken{}, app.Fail(app.AuthInvalidInput)
	}
	issued, err := generate()
	if err != nil {
		return app.IssuedToken{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bindings[issued.TokenID()] = binding
	s.digests[issued.TokenID()] = issued.Digest()
	s.expires[issued.TokenID()] = issued.ExpiresAt()
	return issued, nil
}

func (s *testStore) Authenticate(_ context.Context, tokenID string, verify func(app.TokenSnapshot) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	binding, ok := s.bindings[tokenID]
	if !ok {
		return app.Fail(app.AuthTokenUnknown)
	}
	digest := s.digests[tokenID]
	snapshot, err := app.NewTokenSnapshot(tokenID, digest[:], binding, s.expires[tokenID], nil, nil)
	if err != nil {
		return app.Fail(app.AuthStorageUnavailable)
	}
	if err := verify(snapshot); err != nil {
		return err
	}
	return nil
}

func (s *testStore) LookupSession(context.Context, string) (app.SessionSnapshot, error) {
	return app.SessionSnapshot{}, app.Fail(app.AuthStorageUnavailable)
}

func (s *testStore) RevokeSession(context.Context, app.SessionBinding, time.Time) (app.RevocationOutcome, error) {
	return "", app.Fail(app.AuthStorageUnavailable)
}

func (s *testStore) RevokeToken(context.Context, string, string, time.Time) (app.RevocationOutcome, error) {
	return "", app.Fail(app.AuthStorageUnavailable)
}

type testObserver struct{}

func (testObserver) Observe(app.Observation) {}

func newTestService(t *testing.T, store app.Store) (*app.Service, app.SessionBinding, app.IssuedToken) {
	t.Helper()
	service, err := app.NewService(store, app.Config{
		Clock:    app.ClockFunc(func() time.Time { return time.Unix(1_800_000_000, 0).UTC() }),
		Observer: testObserver{},
		Entropy:  bytes.NewReader(randBytes(t, app.TokenIDHexLen/2+32)),
	})
	if err != nil {
		t.Fatal(err)
	}
	binding := app.SessionBinding{UserID: "user_1", DeviceID: "device_1", SessionID: "session_1"}
	token, err := service.Issue(context.Background(), app.IssueRequest{Binding: binding, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return service, binding, token
}

func randBytes(t *testing.T, size int) []byte {
	t.Helper()
	value := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, value); err != nil {
		t.Fatal(err)
	}
	return value
}

func request(t *testing.T, method, target string, body io.Reader, headers map[string][]string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	return req
}

func serve(t *testing.T, handler http.Handler, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder
}

func decodeCode(t *testing.T, body []byte) Code {
	t.Helper()
	var envelope struct {
		Error struct {
			Code Code `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode error response: %v body=%q", err, body)
	}
	return envelope.Error.Code
}

func TestSessionHandlerContract(t *testing.T) {
	store := newTestStore()
	service, _, token := newTestService(t, store)
	handler, err := NewSessionHandler(service)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("valid", func(t *testing.T) {
		recorder := serve(t, handler, request(t, http.MethodGet, Route, nil, map[string][]string{
			"Authorization": {"Bearer " + token.RawToken()},
		}))
		if recorder.Code != http.StatusOK {
			t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
		}
		if got := recorder.Header().Get("WWW-Authenticate"); got != "" {
			t.Fatalf("unexpected WWW-Authenticate %q", got)
		}
		if strings.Contains(recorder.Body.String(), token.RawToken()) {
			t.Fatal("raw token reflected in success response")
		}
		var envelope struct {
			Version int `json:"version"`
			Session struct {
				UserID    string    `json:"userId"`
				DeviceID  string    `json:"deviceId"`
				SessionID string    `json:"sessionId"`
				TokenID   string    `json:"tokenId"`
				ExpiresAt time.Time `json:"expiresAt"`
			} `json:"session"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Version != Version || envelope.Session.UserID != "user_1" || envelope.Session.DeviceID != "device_1" ||
			envelope.Session.SessionID != "session_1" || envelope.Session.TokenID != token.TokenID() ||
			!envelope.Session.ExpiresAt.Equal(token.ExpiresAt()) {
			t.Fatalf("unexpected response: %+v", envelope)
		}
	})

	t.Run("route-method-body", func(t *testing.T) {
		recorder := serve(t, handler, request(t, http.MethodGet, "/wrong", nil, nil))
		if recorder.Code != http.StatusNotFound || decodeCode(t, recorder.Body.Bytes()) != CodeRouteNotFound {
			t.Fatalf("route response status=%d body=%q", recorder.Code, recorder.Body.String())
		}
		recorder = serve(t, handler, request(t, http.MethodPost, Route, nil, nil))
		if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != http.MethodGet || decodeCode(t, recorder.Body.Bytes()) != CodeMethodNotAllowed {
			t.Fatalf("method response status=%d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
		}
		recorder = serve(t, handler, request(t, http.MethodGet, Route, strings.NewReader("body"), nil))
		if recorder.Code != http.StatusBadRequest || decodeCode(t, recorder.Body.Bytes()) != CodeBodyNotAllowed {
			t.Fatalf("body response status=%d body=%q", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("invalid-bearer", func(t *testing.T) {
		for name, values := range map[string][]string{
			"missing":      nil,
			"empty":        {"Bearer "},
			"wrong-scheme": {"Token " + token.RawToken()},
			"duplicate":    {"Bearer " + token.RawToken(), "Bearer " + token.RawToken()},
			"whitespace":   {"Bearer  " + token.RawToken()},
		} {
			t.Run(name, func(t *testing.T) {
				headers := map[string][]string{}
				if values != nil {
					headers["Authorization"] = values
				}
				recorder := serve(t, handler, request(t, http.MethodGet, Route, nil, headers))
				if recorder.Code != http.StatusUnauthorized || recorder.Header().Get("WWW-Authenticate") != `Bearer realm="newim-session"` ||
					decodeCode(t, recorder.Body.Bytes()) != CodeInvalidToken {
					t.Fatalf("status=%d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
				}
			})
		}
	})
}

func TestSessionHandlerSecurity(t *testing.T) {
	sentinel := "Bearer n1_sentinel-sentinel-sentinel-sentinel"
	handler, err := NewSessionHandler(authenticatorFunc(func(context.Context, string) (app.BearerSession, error) {
		return app.BearerSession{}, app.Fail(app.AuthTokenUnknown)
	}))
	if err != nil {
		t.Fatal(err)
	}
	recorder := serve(t, handler, request(t, http.MethodGet, Route, nil, map[string][]string{"Authorization": {sentinel}}))
	if recorder.Code != http.StatusUnauthorized || strings.Contains(recorder.Body.String(), sentinel) {
		t.Fatalf("sentinel response status=%d body=%q", recorder.Code, recorder.Body.String())
	}

	panicHandler, err := NewSessionHandler(authenticatorFunc(func(context.Context, string) (app.BearerSession, error) {
		panic("sensitive panic sentinel")
	}))
	if err != nil {
		t.Fatal(err)
	}
	recorder = serve(t, panicHandler, request(t, http.MethodGet, Route, nil, map[string][]string{
		"Authorization": {"Bearer n1_" + strings.Repeat("a", app.TokenIDHexLen) + "_" + strings.Repeat("A", app.TokenSecretLen)},
	}))
	if recorder.Code != http.StatusInternalServerError || decodeCode(t, recorder.Body.Bytes()) != CodeInternalError ||
		strings.Contains(recorder.Body.String(), "sensitive panic sentinel") {
		t.Fatalf("panic response status=%d body=%q", recorder.Code, recorder.Body.String())
	}

	unavailableHandler, err := NewSessionHandler(authenticatorFunc(func(context.Context, string) (app.BearerSession, error) {
		return app.BearerSession{}, app.Fail(app.AuthStorageUnavailable)
	}))
	if err != nil {
		t.Fatal(err)
	}
	recorder = serve(t, unavailableHandler, request(t, http.MethodGet, Route, nil, map[string][]string{
		"Authorization": {"Bearer n1_" + strings.Repeat("a", app.TokenIDHexLen) + "_" + strings.Repeat("A", app.TokenSecretLen)},
	}))
	if recorder.Code != http.StatusServiceUnavailable || decodeCode(t, recorder.Body.Bytes()) != CodeUnavailable {
		t.Fatalf("unavailable response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestNewHandlerRequiresAuthenticator(t *testing.T) {
	if _, err := NewSessionHandler(nil); ErrorCode(err) != CodeUnavailable {
		t.Fatalf("missing authenticator got %v", err)
	}
}
