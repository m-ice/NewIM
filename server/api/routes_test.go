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
		"bad-name":              {{Name: "Session", Path: valid.Path, Handler: handler}},
		"bad-path":              {{Name: valid.Name, Path: "/session", Handler: handler}},
		"trailing-slash":        {{Name: valid.Name, Path: "/api/v1/session/", Handler: handler}},
		"nil-handler":           {{Name: valid.Name, Path: valid.Path, Handler: nil}},
		"duplicate-path":        {valid, valid},
		"empty-methods":         {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{}, Handler: handler}},
		"duplicate-method":      {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{http.MethodGet, http.MethodGet}, Handler: handler}},
		"unknown-method":        {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{http.MethodPut}, Handler: handler}},
		"wrong-order":           {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{http.MethodDelete, http.MethodGet}, Handler: handler}},
		"post-before-get-order": {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{http.MethodPost, http.MethodGet}, Handler: handler}},
		"head-method":           {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{http.MethodHead}, Handler: handler}},
		"negative-body-limit":   {{Name: valid.Name, Path: valid.Path, MaxBodyBytes: -1, Handler: handler}},
		"body-limit-get":        {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{http.MethodGet}, MaxBodyBytes: 8, Handler: handler}},
		"body-limit-mixed":      {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{http.MethodGet, http.MethodPost}, MaxBodyBytes: 8, Handler: handler}},
		"reject-query-get":      {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{http.MethodGet}, RejectQuery: true, Handler: handler}},
		"reject-query-mixed":    {{Name: valid.Name, Path: valid.Path, AllowedMethods: []string{http.MethodPost, http.MethodDelete}, RejectQuery: true, Handler: handler}},
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
	methods := []string{http.MethodGet, http.MethodPost, http.MethodDelete}
	routes, err := validateAPIRoutes([]APIRoute{{
		Name: "session", Path: "/api/v1/session", AllowedMethods: methods, Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	methods[0] = http.MethodDelete
	route := routes["/api/v1/session"]
	if got := strings.Join(route.AllowedMethods, ", "); got != "GET, POST, DELETE" {
		t.Fatalf("canonical Allow=%q", got)
	}
}

func TestAPIRouteBodyPolicy(t *testing.T) {
	const maxBodyBytes = int64(8)

	tests := []struct {
		name            string
		method          string
		target          string
		body            string
		contentLength   int64
		transfer        bool
		contentType     string
		contentEncoding string
		wantStatus      int
		wantCode        string
		wantAllow       string
		wantHandled     bool
		wantRead        bool
	}{
		{
			name: "known size within limit reaches handler", method: http.MethodPost, target: "/api/v1/messages", body: "12345678",
			contentLength: 8, contentType: "application/json", wantStatus: http.StatusOK, wantHandled: true, wantRead: true,
		},
		{
			name: "known size over limit precedes invalid headers", method: http.MethodPost, target: "/api/v1/messages", body: "123456789",
			contentLength: 9, contentType: "text/plain", contentEncoding: "gzip", wantStatus: http.StatusRequestEntityTooLarge,
			wantCode: HTTPBodyTooLarge,
		},
		{
			name: "query precedes known size", method: http.MethodPost, target: "/api/v1/messages?x=1", body: "123456789",
			contentLength: 9, contentType: "text/plain", wantStatus: http.StatusBadRequest, wantCode: HTTPInvalidQuery,
		},
		{
			name: "force query precedes known size", method: http.MethodPost, target: "/api/v1/messages?", body: "123456789",
			contentLength: 9, contentType: "text/plain", wantStatus: http.StatusBadRequest, wantCode: HTTPInvalidQuery,
		},
		{
			name: "method precedes query and size", method: http.MethodGet, target: "/api/v1/messages?x=1", body: "123456789",
			contentLength: 9, contentType: "text/plain", wantStatus: http.StatusMethodNotAllowed, wantCode: HTTPMethodNotAllowed,
			wantAllow: http.MethodPost,
		},
		{
			name: "invalid headers are checked by handler before read", method: http.MethodPost, target: "/api/v1/messages",
			body: "12345678", contentLength: 8, contentType: "text/plain", wantStatus: http.StatusUnsupportedMediaType,
			wantCode: HTTPUnsupportedMediaType, wantHandled: true,
		},
		{
			name: "chunked invalid encoding is rejected before read", method: http.MethodPost, target: "/api/v1/messages",
			body: "123456789", contentLength: -1, transfer: true, contentType: "application/json", contentEncoding: "gzip",
			wantStatus: http.StatusUnsupportedMediaType, wantCode: HTTPUnsupportedMediaType, wantHandled: true,
		},
		{
			name: "chunked valid headers are bounded in handler", method: http.MethodPost, target: "/api/v1/messages",
			body: "123456789", contentLength: -1, transfer: true, contentType: "application/json",
			wantStatus: http.StatusRequestEntityTooLarge, wantCode: HTTPBodyTooLarge, wantHandled: true, wantRead: true,
		},
		{
			name: "transfer encoding overrides reported length", method: http.MethodPost, target: "/api/v1/messages",
			body: "123456789", contentLength: 9, transfer: true, contentType: "text/plain",
			wantStatus: http.StatusUnsupportedMediaType, wantCode: HTTPUnsupportedMediaType, wantHandled: true,
		},
		{
			name: "reported size under limit still uses bounded reader", method: http.MethodPost, target: "/api/v1/messages",
			body: "123456789", contentLength: 8, contentType: "application/json",
			wantStatus: http.StatusRequestEntityTooLarge, wantCode: HTTPBodyTooLarge, wantHandled: true, wantRead: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var handled bool
			reader := &countingReadCloser{Reader: strings.NewReader(tc.body)}
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handled = true
				if r.Header.Get("Content-Type") != "application/json" || (r.Header.Get("Content-Encoding") != "" && r.Header.Get("Content-Encoding") != "identity") {
					writeError(w, http.StatusUnsupportedMediaType, HTTPUnsupportedMediaType)
					return
				}
				_, err := io.ReadAll(r.Body)
				if err != nil {
					var tooLarge *http.MaxBytesError
					if errors.As(err, &tooLarge) {
						writeError(w, http.StatusRequestEntityTooLarge, HTTPBodyTooLarge)
						return
					}
					writeError(w, http.StatusInternalServerError, HTTPInternalError)
					return
				}
				w.WriteHeader(http.StatusOK)
			})
			server, err := NewWithRoutes(
				Config{APIAddr: "127.0.0.1:0", OpsAddr: "127.0.0.1:0", ServerVersion: "0.1.0-test"},
				nil,
				[]APIRoute{{
					Name: "message_send", Path: "/api/v1/messages", AllowedMethods: []string{http.MethodPost},
					MaxBodyBytes: maxBodyBytes, RejectQuery: true, Handler: handler,
				}},
			)
			if err != nil {
				t.Fatal(err)
			}

			request := httptest.NewRequest(tc.method, tc.target, reader)
			request.ContentLength = tc.contentLength
			if tc.transfer {
				request.TransferEncoding = []string{"chunked"}
			}
			request.Header.Set("Content-Type", tc.contentType)
			if tc.contentEncoding != "" {
				request.Header.Set("Content-Encoding", tc.contentEncoding)
			}
			recorder := httptest.NewRecorder()
			server.makeHandler("api").ServeHTTP(recorder, request)

			if recorder.Code != tc.wantStatus {
				t.Fatalf("status=%d want=%d body=%q", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			if tc.wantCode != "" && !strings.Contains(recorder.Body.String(), `"code":"`+tc.wantCode+`"`) {
				t.Fatalf("body=%q missing code %q", recorder.Body.String(), tc.wantCode)
			}
			if got := recorder.Header().Get("Allow"); got != tc.wantAllow {
				t.Fatalf("Allow=%q want=%q", got, tc.wantAllow)
			}
			if handled != tc.wantHandled {
				t.Fatalf("handler called=%v want=%v", handled, tc.wantHandled)
			}
			if got := reader.ReadCount() > 0; got != tc.wantRead {
				t.Fatalf("body read=%v want=%v calls=%d", got, tc.wantRead, reader.ReadCount())
			}
		})
	}
}

func TestAPIRouteBodylessPOST(t *testing.T) {
	var handled bool
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { handled = true })
	server, err := NewWithRoutes(
		Config{APIAddr: "127.0.0.1:0", OpsAddr: "127.0.0.1:0", ServerVersion: "0.1.0-test"},
		nil,
		[]APIRoute{{
			Name: "bodyless_post", Path: "/api/v1/bodyless", AllowedMethods: []string{http.MethodPost}, Handler: handler,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	server.makeHandler("api").ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/bodyless", strings.NewReader("body")))
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), HTTPBodyNotAllowed) {
		t.Fatalf("bodyless POST response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	if handled {
		t.Fatal("bodyless POST reached handler")
	}
}

type countingReadCloser struct {
	io.Reader
	reads int
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	r.reads++
	return r.Reader.Read(p)
}

func (r *countingReadCloser) Close() error { return nil }

func (r *countingReadCloser) ReadCount() int { return r.reads }
