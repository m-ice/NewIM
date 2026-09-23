package api

import (
	"bytes"
	"context"
	"errors"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestHTTPContracts(t *testing.T) {
	var logs bytes.Buffer
	server, err := New(Config{APIAddr: "127.0.0.1:0", OpsAddr: "127.0.0.1:0", ServerVersion: "0.1.0-test"}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server.ready.Store(true)
	apiHandler := server.makeHandler("api")
	opsHandler := server.makeHandler("ops")

	cases := []struct {
		name        string
		handler     http.Handler
		method      string
		target      string
		body        io.Reader
		wantStatus  int
		wantBody    string
		wantCode    string
		wantAllow   string
		wantContent string
	}{
		{name: "health", handler: apiHandler, method: http.MethodGet, target: "/api/v1/health", wantStatus: 200, wantBody: "{\"status\":\"ok\"}\n", wantContent: "application/json; charset=utf-8"},
		{name: "ready", handler: opsHandler, method: http.MethodGet, target: "/ready", wantStatus: 200, wantBody: "{\"status\":\"ready\"}\n", wantContent: "application/json; charset=utf-8"},
		{name: "not ready", handler: opsHandler, method: http.MethodGet, target: "/ready", wantStatus: 503, wantCode: ServerNotReady, wantContent: "application/json; charset=utf-8"},
		{name: "api unknown", handler: apiHandler, method: http.MethodGet, target: "/api/v1/health/", wantStatus: 404, wantCode: HTTPRouteNotFound, wantContent: "application/json; charset=utf-8"},
		{name: "encoded slash is not an alias", handler: apiHandler, method: http.MethodGet, target: "/api%2Fv1%2Fhealth", wantStatus: 404, wantCode: HTTPRouteNotFound, wantContent: "application/json; charset=utf-8"},
		{name: "encoded health is not an alias", handler: apiHandler, method: http.MethodGet, target: "/api/v1/%68ealth", wantStatus: 404, wantCode: HTTPRouteNotFound, wantContent: "application/json; charset=utf-8"},
		{name: "api surface isolation", handler: apiHandler, method: http.MethodGet, target: "/ready", wantStatus: 404, wantCode: HTTPRouteNotFound, wantContent: "application/json; charset=utf-8"},
		{name: "ops surface isolation", handler: opsHandler, method: http.MethodGet, target: "/api/v1/health", wantStatus: 404, wantCode: HTTPRouteNotFound, wantContent: "application/json; charset=utf-8"},
		{name: "method", handler: apiHandler, method: http.MethodPost, target: "/api/v1/health", wantStatus: 405, wantCode: HTTPMethodNotAllowed, wantAllow: http.MethodGet, wantContent: "application/json; charset=utf-8"},
		{name: "head is not get", handler: apiHandler, method: http.MethodHead, target: "/api/v1/health", wantStatus: 405, wantCode: HTTPMethodNotAllowed, wantAllow: http.MethodGet, wantContent: "application/json; charset=utf-8"},
		{name: "body", handler: apiHandler, method: http.MethodGet, target: "/api/v1/health", body: strings.NewReader("private-body"), wantStatus: 400, wantCode: HTTPBodyNotAllowed, wantContent: "application/json; charset=utf-8"},
		{name: "chunked", handler: apiHandler, method: http.MethodGet, target: "/api/v1/health", body: strings.NewReader("private-body"), wantStatus: 400, wantCode: HTTPBodyNotAllowed, wantContent: "application/json; charset=utf-8"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, tc.target, tc.body)
			if tc.name == "chunked" {
				request.TransferEncoding = []string{"chunked"}
				request.ContentLength = -1
			}
			if tc.name == "not ready" {
				server.ready.Store(false)
			} else {
				server.ready.Store(true)
			}
			recorder := httptest.NewRecorder()
			tc.handler.ServeHTTP(recorder, request)
			response := recorder.Result()
			defer response.Body.Close()
			body, _ := io.ReadAll(response.Body)
			if response.StatusCode != tc.wantStatus {
				t.Fatalf("status=%d want=%d body=%q", response.StatusCode, tc.wantStatus, body)
			}
			if tc.wantBody != "" && string(body) != tc.wantBody {
				t.Fatalf("body=%q want=%q", body, tc.wantBody)
			}
			if tc.wantCode != "" && !strings.Contains(string(body), `"code":"`+tc.wantCode+`"`) {
				t.Fatalf("body=%q missing code %q", body, tc.wantCode)
			}
			if got := response.Header.Get("Allow"); got != tc.wantAllow {
				t.Fatalf("Allow=%q want=%q", got, tc.wantAllow)
			}
			if got := response.Header.Get("Content-Type"); got != tc.wantContent {
				t.Fatalf("Content-Type=%q want=%q", got, tc.wantContent)
			}
			if response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("X-Content-Type-Options") != "nosniff" {
				t.Fatalf("security headers missing: %v", response.Header)
			}
		})
	}
}

func TestServerRecovery(t *testing.T) {
	t.Run("real lifecycle", func(t *testing.T) {
		var logs bytes.Buffer
		server, err := New(Config{APIAddr: "127.0.0.1:0", OpsAddr: "127.0.0.1:0", ServerVersion: "0.1.0-test"}, slog.New(slog.NewTextHandler(&logs, nil)))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- server.Run(ctx) }()
		waitForReady(t, server)
		if !server.Ready() {
			t.Fatal("server did not become ready")
		}
		for _, endpoint := range []string{"http://" + server.APIAddr() + "/api/v1/health", "http://" + server.OpsAddr() + "/ready"} {
			response, err := http.Get(endpoint)
			if err != nil {
				t.Fatalf("GET %s: %v", endpoint, err)
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("GET %s status=%d", endpoint, response.StatusCode)
			}
		}
		cancel()
		readyDeadline := time.Now().Add(time.Second)
		for server.Ready() && time.Now().Before(readyDeadline) {
			time.Sleep(time.Millisecond)
		}
		if server.Ready() {
			t.Fatal("readiness did not fail closed during drain")
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("graceful shutdown failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("server did not stop after cancellation")
		}
		if server.Ready() {
			t.Fatal("server remained ready after shutdown")
		}
		if err := server.Run(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
			t.Fatalf("second run error=%v want=%v", err, ErrAlreadyStarted)
		}
	})

	t.Run("shutdown waits for active request", func(t *testing.T) {
		server, listener, release := blockingServer(t)
		defer listener.Close()
		entered := make(chan struct{})
		server.apiServer.Handler = blockingHandler(t, release, entered)
		serveDone := make(chan error, 1)
		go func() { serveDone <- server.apiServer.Serve(listener) }()
		requestDone := make(chan error, 1)
		go func() {
			response, err := http.Get("http://" + listener.Addr().String() + "/")
			if err == nil {
				_ = response.Body.Close()
			}
			requestDone <- err
		}()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("active request did not enter handler")
		}
		shutdownDone := make(chan error, 1)
		go func() { shutdownDone <- server.Shutdown(context.Background()) }()
		select {
		case err := <-shutdownDone:
			t.Fatalf("shutdown returned before active request completed: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		close(release)
		if err := <-shutdownDone; err != nil {
			t.Fatalf("graceful shutdown failed: %v", err)
		}
		if err := <-requestDone; err != nil {
			t.Fatalf("active request failed: %v", err)
		}
		select {
		case err := <-serveDone:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Fatalf("serve return: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("serve did not return after shutdown")
		}
	})

	t.Run("shutdown timeout force-closes active request", func(t *testing.T) {
		server, listener, release := blockingServer(t)
		defer listener.Close()
		entered := make(chan struct{})
		server.apiServer.Handler = blockingHandler(t, release, entered)
		serveDone := make(chan error, 1)
		go func() { serveDone <- server.apiServer.Serve(listener) }()
		requestDone := make(chan error, 1)
		go func() {
			response, err := http.Get("http://" + listener.Addr().String() + "/")
			if err == nil {
				_ = response.Body.Close()
			}
			requestDone <- err
		}()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("active request did not enter handler")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err := server.Shutdown(ctx)
		if codeOf(err) != CodeShutdownFailed {
			t.Fatalf("shutdown timeout code=%q err=%v", codeOf(err), err)
		}
		close(release)
		select {
		case <-requestDone:
		case <-time.After(time.Second):
			t.Fatal("forced-close request did not return")
		}
		select {
		case err := <-serveDone:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Fatalf("serve return: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("serve did not return after forced close")
		}
	})

	t.Run("second bind failure is atomic", func(t *testing.T) {
		held, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer held.Close()
		free, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		apiAddr := free.Addr().String()
		if err := free.Close(); err != nil {
			t.Fatal(err)
		}
		server, err := New(Config{APIAddr: apiAddr, OpsAddr: held.Addr().String(), ServerVersion: "0.1.0-test"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		err = server.Run(context.Background())
		if codeOf(err) != CodeBindFailed {
			t.Fatalf("bind failure code=%q err=%v", codeOf(err), err)
		}
		if server.APIAddr() != "" || server.OpsAddr() != "" {
			t.Fatalf("failed startup retained listeners: api=%q ops=%q", server.APIAddr(), server.OpsAddr())
		}
		probe, listenErr := net.Listen("tcp", apiAddr)
		if listenErr != nil {
			t.Fatalf("first listener remained open after second-bind failure: %v", listenErr)
		}
		_ = probe.Close()
	})
}

func TestHTTPSecurity(t *testing.T) {
	t.Run("redaction", func(t *testing.T) {
		const sentinel = "private-token-sentinel"
		var logs bytes.Buffer
		server, err := New(Config{APIAddr: "127.0.0.1:0", OpsAddr: "127.0.0.1:0", ServerVersion: "0.1.0-test"}, slog.New(slog.NewTextHandler(&logs, nil)))
		if err != nil {
			t.Fatal(err)
		}
		server.ready.Store(true)
		for _, request := range []*http.Request{
			httptest.NewRequest(http.MethodGet, "/"+sentinel+"?token="+sentinel, nil),
			httptest.NewRequest(http.MethodPost, "/api/v1/health", strings.NewReader(sentinel)),
		} {
			request.Header.Set("X-Private", sentinel)
			recorder := httptest.NewRecorder()
			server.makeHandler("api").ServeHTTP(recorder, request)
			if strings.Contains(recorder.Body.String(), sentinel) {
				t.Fatalf("response leaked sentinel: %q", recorder.Body.String())
			}
		}
		if strings.Contains(logs.String(), sentinel) {
			t.Fatalf("logs leaked sentinel: %q", logs.String())
		}
		recorder := httptest.NewRecorder()
		server.makeHandler("ops").ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if strings.Contains(recorder.Body.String(), sentinel) {
			t.Fatalf("metrics leaked sentinel: %q", recorder.Body.String())
		}
	})

	t.Run("panic recovery redacts value", func(t *testing.T) {
		const sentinel = "private-panic-sentinel"
		var logs bytes.Buffer
		server, err := New(Config{APIAddr: "127.0.0.1:0", OpsAddr: "127.0.0.1:0", ServerVersion: "0.1.0-test"}, slog.New(slog.NewTextHandler(&logs, nil)))
		if err != nil {
			t.Fatal(err)
		}
		recorder := &responseRecorder{ResponseWriter: httptest.NewRecorder()}
		func() {
			defer server.recoverRequest("api", "health", http.MethodGet, recorder)
			panic(sentinel)
		}()
		response := recorder.ResponseWriter.(*httptest.ResponseRecorder)
		if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), HTTPInternalError) {
			t.Fatalf("panic response=%d %q", response.Code, response.Body.String())
		}
		if strings.Contains(logs.String(), sentinel) {
			t.Fatalf("panic log leaked sentinel: %q", logs.String())
		}
	})

	t.Run("metric labels are bounded", func(t *testing.T) {
		server, err := New(Config{APIAddr: "127.0.0.1:0", OpsAddr: "127.0.0.1:0", ServerVersion: "0.1.0-test"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		server.ready.Store(true)
		server.makeHandler("api").ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))
		server.makeHandler("api").ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/private-path", nil))
		server.makeHandler("ops").ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/ready", nil))
		recorder := httptest.NewRecorder()
		server.makeHandler("ops").ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		body := recorder.Body.String()
		for _, want := range []string{
			`newim_server_info{server_version="0.1.0-test"} 1`,
			`newim_process_start_time_seconds `,
			`newim_http_requests_in_flight{listener="ops",route="metrics"} 1`,
			`newim_http_request_duration_seconds_sum{listener="api",route="health",method="GET"} `,
			`newim_http_request_duration_seconds_count{listener="api",route="health",method="GET"} 1`,
			`newim_http_requests_total{listener="api",route="health",method="GET",status="200"} 1`,
			`newim_http_requests_total{listener="api",route="unmatched",method="GET",status="404"} 1`,
			`newim_http_requests_total{listener="ops",route="ready",method="POST",status="405"} 1`,
		} {
			if !strings.Contains(body, want) {
				t.Fatalf("metrics missing %q in:\n%s", want, body)
			}
		}
		if strings.Contains(body, "private-path") || strings.Contains(body, "?") {
			t.Fatalf("metrics leaked request-derived data: %s", body)
		}
		allowed := regexp.MustCompile(`(?:listener|route|method|status)="([^"]*)"`)
		for _, match := range allowed.FindAllStringSubmatch(body, -1) {
			value := match[1]
			switch {
			case value == "api" || value == "ops":
			case value == "health" || value == "ready" || value == "metrics" || value == "unmatched":
			case value == "GET" || value == "HEAD" || value == "POST" || value == "PUT" || value == "PATCH" || value == "DELETE" || value == "OPTIONS" || value == "OTHER":
			case len(value) == 3 && value >= "100" && value <= "503":
			default:
				t.Fatalf("unbounded metric label value %q", value)
			}
		}
	})
}

func TestImportBoundary(t *testing.T) {
	allowedModule := map[string]bool{
		"github.com/m-ice/NewIM/server/api":       true,
		"github.com/m-ice/NewIM/server/buildinfo": true,
	}
	for _, dir := range []string{".", "../cmd/newim-server"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, imported := range file.Imports {
				value := strings.Trim(imported.Path.Value, `"`)
				if strings.Contains(value, ".") && !allowedModule[value] {
					t.Fatalf("%s imports forbidden module %q", path, value)
				}
			}
		}
	}
}

func blockingServer(t *testing.T) (*Server, net.Listener, chan struct{}) {
	t.Helper()
	server, err := New(Config{APIAddr: "127.0.0.1:0", OpsAddr: "127.0.0.1:0", ServerVersion: "0.1.0-test"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return server, listener, make(chan struct{})
}

func blockingHandler(t *testing.T, release <-chan struct{}, entered chan<- struct{}) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusOK)
	})
}

func waitForReady(t *testing.T, server *Server) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if server.Ready() && server.APIAddr() != "" && server.OpsAddr() != "" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server did not become ready: api=%q ops=%q", server.APIAddr(), server.OpsAddr())
}

func codeOf(err error) ErrorCode {
	var known *Error
	if errors.As(err, &known) && known != nil {
		return known.Code
	}
	return ""
}
