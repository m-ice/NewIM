package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAPIRouteComposition(t *testing.T) {
	sessionHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/session" {
			t.Fatalf("session handler path=%q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	tokenMethods := []string{http.MethodDelete}
	tokenHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/session/tokens/current" {
			t.Fatalf("token handler path=%q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	server, err := NewWithRoutes(
		Config{APIAddr: "127.0.0.1:0", OpsAddr: "127.0.0.1:0", ServerVersion: "0.1.0-test"},
		nil,
		[]APIRoute{
			{Name: "session", Path: "/api/v1/session", Handler: sessionHandler},
			{Name: "session_token_revoke", Path: "/api/v1/session/tokens/current", AllowedMethods: tokenMethods, Handler: tokenHandler},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	tokenMethods[0] = http.MethodGet

	recorder := httptest.NewRecorder()
	server.makeHandler("api").ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/session", nil))
	if recorder.Code != http.StatusOK || recorder.Body.String() != `{"ok":true}` {
		t.Fatalf("session response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	server.makeHandler("api").ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/session", nil))
	if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("session method response status=%d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	server.makeHandler("api").ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/session", strings.NewReader("body")))
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), HTTPBodyNotAllowed) {
		t.Fatalf("session body response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	server.makeHandler("api").ServeHTTP(recorder, httptest.NewRequest(http.MethodDelete, "/api/v1/session/tokens/current", nil))
	if recorder.Code != http.StatusNoContent || recorder.Body.Len() != 0 {
		t.Fatalf("token revoke response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	server.makeHandler("api").ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/session/tokens/current", nil))
	if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != http.MethodDelete {
		t.Fatalf("token method response status=%d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
	}

	metrics := httptest.NewRecorder()
	server.makeHandler("ops").ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, route := range []string{`route="session"`, `route="session_token_revoke"`} {
		if !strings.Contains(metrics.Body.String(), route) {
			t.Fatalf("route metric %s missing: %s", route, metrics.Body.String())
		}
	}
}

func TestAPIRouteValidation(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	valid := APIRoute{Name: "session", Path: "/api/v1/session", Handler: handler}
	for name, routes := range map[string][]APIRoute{
		"bad-name":         {{Name: "Session", Path: valid.Path, Handler: handler}},
		"bad-path":         {{Name: valid.Name, Path: "/session", Handler: handler}},
		"trailing-slash":   {{Name: valid.Name, Path: "/api/v1/session/", Handler: handler}},
		"nil-handler":      {{Name: valid.Name, Path: valid.Path, Handler: nil}},
		"duplicate-path":   {valid, valid},
		"empty-methods":    {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{}, Handler: handler}},
		"duplicate-method": {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{http.MethodGet, http.MethodGet}, Handler: handler}},
		"unknown-method":   {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{http.MethodPost}, Handler: handler}},
		"wrong-order":      {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{http.MethodDelete, http.MethodGet}, Handler: handler}},
		"head-method":      {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{http.MethodHead}, Handler: handler}},
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

func TestAPIRouteAllowedMethodsAreCopied(t *testing.T) {
	routes, err := validateAPIRoutes([]APIRoute{{
		Name: "session", Path: "/api/v1/session", AllowedMethods: []string{http.MethodGet, http.MethodDelete}, Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	route := routes["/api/v1/session"]
	if got := strings.Join(route.AllowedMethods, ", "); got != "GET, DELETE" {
		t.Fatalf("canonical Allow=%q", got)
	}
}
