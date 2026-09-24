package api

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAPIRouteComposition(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/session" {
			t.Fatalf("handler path=%q", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
	server, err := NewWithRoutes(
		Config{APIAddr: "127.0.0.1:0", OpsAddr: "127.0.0.1:0", ServerVersion: "0.1.0-test"},
		nil,
		[]APIRoute{{Name: "session", Path: "/api/v1/session", Handler: handler}},
	)
	if err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()
	server.makeHandler("api").ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/session", nil))
	if recorder.Code != http.StatusOK || recorder.Body.String() != `{"ok":true}` {
		t.Fatalf("route response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	server.makeHandler("api").ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/session", nil))
	if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("method response status=%d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	server.makeHandler("api").ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/session", strings.NewReader("body")))
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), HTTPBodyNotAllowed) {
		t.Fatalf("body response status=%d body=%q", recorder.Code, recorder.Body.String())
	}

	metrics := httptest.NewRecorder()
	server.makeHandler("ops").ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(metrics.Body.String(), `route="session"`) {
		t.Fatalf("session route metric missing: %s", metrics.Body.String())
	}
}

func TestAPIRouteValidation(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	valid := APIRoute{Name: "session", Path: "/api/v1/session", Handler: handler}
	for name, routes := range map[string][]APIRoute{
		"bad-name":       {{Name: "Session", Path: valid.Path, Handler: handler}},
		"bad-path":       {{Name: valid.Name, Path: "/session", Handler: handler}},
		"trailing-slash": {{Name: valid.Name, Path: "/api/v1/session/", Handler: handler}},
		"nil-handler":    {{Name: valid.Name, Path: valid.Path, Handler: nil}},
		"duplicate-path": {valid, valid},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewWithRoutes(Config{APIAddr: "127.0.0.1:0", OpsAddr: "127.0.0.1:0", ServerVersion: "0.1.0-test"}, nil, routes)
			var known *Error
			if !errors.As(err, &known) || known.Code != CodeInvalidConfig {
				t.Fatalf("error=%v want %s", err, CodeInvalidConfig)
			}
		})
	}
}
