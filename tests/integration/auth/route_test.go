//go:build integration

package auth_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	bearer "github.com/m-ice/NewIM/server/auth/bearerhttp"
	app "github.com/m-ice/NewIM/server/auth/session"
)

const localDSN = "host=/var/run/postgresql user=newim_test dbname=newim_test sslmode=disable"

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

type runningServer struct {
	cmd    *exec.Cmd
	stdout synchronizedBuffer
	stderr synchronizedBuffer
	api    string
	ops    string
}

func startAuthServer(t *testing.T, env map[string]string) *runningServer {
	t.Helper()
	apiAddr, opsAddr := twoFreeAddrs(t)
	cmd := exec.Command(os.Getenv("NEWIM_SERVER_BINARY"), "--api-addr", apiAddr, "--ops-addr", opsAddr)
	cmd.Env = append([]string{}, os.Environ()...)
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	server := &runningServer{cmd: cmd, api: apiAddr, ops: opsAddr}
	cmd.Stdout = &server.stdout
	cmd.Stderr = &server.stderr
	must(t, cmd.Start())
	return server
}

func (s *runningServer) waitHealthy(t *testing.T) {
	t.Helper()
	if err := waitForHTTPStatus("http://"+s.api+"/api/v1/health", http.StatusOK, 5*time.Second); err != nil {
		t.Fatalf("health failed: %v stderr=%q", err, s.stderr.String())
	}
}

func (s *runningServer) stop(t *testing.T) {
	t.Helper()
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return
	}
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SIGTERM exit: %v stderr=%q", err, s.stderr.String())
		}
	case <-time.After(10 * time.Second):
		_ = s.cmd.Process.Kill()
		t.Fatalf("SIGTERM exceeded 10s total shutdown bound; stderr=%q", s.stderr.String())
	}
}

func TestAuthRoute(t *testing.T) {
	if strings.TrimSpace(os.Getenv("NEWIM_SERVER_BINARY")) == "" {
		t.Fatal("NEWIM_SERVER_BINARY is required")
	}

	t.Run("without-dsn-route-is-absent", func(t *testing.T) {
		f := openFixture(t)
		server := startAuthServer(t, map[string]string{"NEWIM_AUTH_DSN": ""})
		defer server.stop(t)
		server.waitHealthy(t)
		body, status, _ := doSessionRequest(t, "http://"+server.api, "")
		if status != http.StatusNotFound || !strings.Contains(string(body), "HTTP_ROUTE_NOT_FOUND") {
			t.Fatalf("no-dsn session status=%d body=%q", status, body)
		}
		if got := activeAuthConnections(t, f); got != 0 {
			t.Fatalf("no-dsn opened %d auth connections", got)
		}
	})

	t.Run("unsafe-config-fails-before-bind", func(t *testing.T) {
		for name, env := range map[string]map[string]string{
			"wildcard":        {"NEWIM_AUTH_DSN": localDSN, "NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1"},
			"malformed-dsn":   {"NEWIM_AUTH_DSN": "postgres://user:%zz@127.0.0.1/newim", "NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1"},
			"socket-disabled": {"NEWIM_AUTH_DSN": localDSN, "NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "0"},
		} {
			t.Run(name, func(t *testing.T) {
				apiAddr := "127.0.0.1:0"
				if name == "wildcard" {
					apiAddr = "0.0.0.0:0"
				}
				cmd := exec.Command(os.Getenv("NEWIM_SERVER_BINARY"), "--api-addr", apiAddr, "--ops-addr", "127.0.0.1:0")
				cmd.Env = append([]string{}, os.Environ()...)
				for key, value := range env {
					cmd.Env = append(cmd.Env, key+"="+value)
				}
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				if err := cmd.Run(); err == nil {
					t.Fatalf("unsafe config started successfully: stderr=%q", stderr.String())
				}
				if !strings.Contains(stderr.String(), "SERVER_INVALID_AUTH_CONFIG") && !strings.Contains(stderr.String(), "AUTH_STORAGE_UNAVAILABLE") {
					t.Fatalf("unsafe config stderr=%q", stderr.String())
				}
			})
		}
	})

	t.Run("occupied-api-port-cleans-repository", func(t *testing.T) {
		f := openFixture(t)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		must(t, err)
		defer listener.Close()
		cmd := exec.Command(os.Getenv("NEWIM_SERVER_BINARY"), "--api-addr", listener.Addr().String(), "--ops-addr", "127.0.0.1:0")
		cmd.Env = append([]string{}, os.Environ()...)
		cmd.Env = append(cmd.Env, "NEWIM_AUTH_DSN="+localDSN, "NEWIM_AUTH_ALLOW_LOCAL_SOCKET=1")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err == nil {
			t.Fatalf("occupied port started successfully: stderr=%q", stderr.String())
		}
		if !strings.Contains(stderr.String(), "SERVER_BIND_FAILED") {
			t.Fatalf("occupied port stderr=%q", stderr.String())
		}
		if got := activeAuthConnections(t, f); got != 0 {
			t.Fatalf("occupied bind left %d auth connections", got)
		}
	})

	t.Run("loopback-route-real-postgresql", func(t *testing.T) {
		f := openFixture(t)
		service := f.service(&captureObserver{}, nil, nil)
		binding := f.seedBinding("route_valid")
		token := f.issue(service, binding, time.Hour)
		expiredBinding := f.seedBinding("route_expired")
		expired := f.issue(service, expiredBinding, time.Hour)
		f.sql("UPDATE newim.im_auth_tokens SET created_at=now() - interval '2 minutes', expires_at=now() - interval '1 minute' WHERE token_id=$1", expired.TokenID())
		disabledBinding := f.seedBinding("route_disabled")
		disabled := f.issue(service, disabledBinding, time.Hour)

		server := startAuthServer(t, map[string]string{
			"NEWIM_AUTH_DSN": localDSN, "NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1",
		})
		defer server.stop(t)
		server.waitHealthy(t)

		body, status, headers := doSessionRequest(t, "http://"+server.api, "Bearer "+token.RawToken())
		if status != http.StatusOK || strings.Contains(string(body), token.RawToken()) {
			t.Fatalf("valid status=%d body=%q", status, body)
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
		must(t, json.Unmarshal(body, &envelope))
		if envelope.Version != 1 || envelope.Session.UserID != binding.UserID || envelope.Session.DeviceID != binding.DeviceID ||
			envelope.Session.SessionID != binding.SessionID || envelope.Session.TokenID != token.TokenID() {
			t.Fatalf("valid response=%+v", envelope)
		}

		body, status, headers = doRawRequest(t, http.MethodGet, "http://"+server.api+"/api/v1/not-session", nil, "")
		if status != http.StatusNotFound || !strings.Contains(string(body), "HTTP_ROUTE_NOT_FOUND") {
			t.Fatalf("route precedence status=%d body=%q", status, body)
		}
		body, status, headers = doRawRequest(t, http.MethodPost, "http://"+server.api+bearerRoute, nil, "")
		if status != http.StatusMethodNotAllowed || headers.Get("Allow") != http.MethodGet || !strings.Contains(string(body), "HTTP_METHOD_NOT_ALLOWED") {
			t.Fatalf("method precedence status=%d headers=%v body=%q", status, headers, body)
		}
		body, status, headers = doRawRequest(t, http.MethodGet, "http://"+server.api+bearerRoute, strings.NewReader("body"), "")
		if status != http.StatusBadRequest || !strings.Contains(string(body), "HTTP_BODY_NOT_ALLOWED") {
			t.Fatalf("body precedence status=%d headers=%v body=%q", status, headers, body)
		}

		body, status, headers = doSessionRequest(t, "http://"+server.api, "Bearer "+expired.RawToken())
		if status != http.StatusUnauthorized || headers.Get("WWW-Authenticate") != `Bearer realm="newim-session"` {
			t.Fatalf("expired status=%d headers=%v body=%q", status, headers, body)
		}
		if _, err := service.RevokeSession(ctx, disabledBinding); err != nil {
			t.Fatal(err)
		}
		body, status, _ = doSessionRequest(t, "http://"+server.api, "Bearer "+disabled.RawToken())
		if status != http.StatusUnauthorized {
			t.Fatalf("revoked status=%d body=%q", status, body)
		}

		duplicate, err := http.NewRequest(http.MethodGet, "http://"+server.api+bearerRoute, nil)
		must(t, err)
		duplicate.Header.Add("Authorization", "Bearer "+token.RawToken())
		duplicate.Header.Add("Authorization", "Bearer "+token.RawToken())
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(duplicate)
		must(t, err)
		defer response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized || response.Header.Get("WWW-Authenticate") == "" {
			t.Fatalf("duplicate status=%d headers=%v", response.StatusCode, response.Header)
		}

		metricsResponse, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + server.ops + "/metrics")
		must(t, err)
		defer metricsResponse.Body.Close()
		metricsBody, err := io.ReadAll(metricsResponse.Body)
		must(t, err)
		if !strings.Contains(string(metricsBody), `route="session"`) || strings.Contains(string(metricsBody), token.RawToken()) {
			t.Fatalf("metrics route/redaction missing: %s", metricsBody)
		}
		for _, forbidden := range []string{token.RawToken(), localDSN, "SELECT ", "im_auth_tokens", "im_sessions"} {
			if strings.Contains(server.stdout.String(), forbidden) || strings.Contains(server.stderr.String(), forbidden) {
				t.Fatalf("server log leaked %q; stdout=%q stderr=%q", forbidden, server.stdout.String(), server.stderr.String())
			}
		}
	})

	t.Run("disconnect-recovers-and-shutdown-closes-pool", func(t *testing.T) {
		f := openFixture(t)
		service := f.service(&captureObserver{}, nil, nil)
		binding := f.seedBinding("route_disconnect")
		token := f.issue(service, binding, time.Hour)

		server := startAuthServer(t, map[string]string{
			"NEWIM_AUTH_DSN": localDSN, "NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1",
		})
		server.waitHealthy(t)

		blocker, err := pgx.Connect(ctx, f.dsn)
		must(t, err)
		defer blocker.Close(context.Background())
		tx, err := blocker.Begin(ctx)
		must(t, err)
		defer func() { _ = tx.Rollback(context.Background()) }()
		_, err = tx.Exec(ctx, `SELECT user_id FROM newim.im_sessions WHERE session_id=$1 FOR UPDATE`, binding.SessionID)
		must(t, err)

		type result struct {
			body   []byte
			status int
			err    error
		}
		resultCh := make(chan result, 1)
		go func() {
			req, err := http.NewRequest(http.MethodGet, "http://"+server.api+bearerRoute, nil)
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			req.Header.Set("Authorization", "Bearer "+token.RawToken())
			response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			resultCh <- result{body: body, status: response.StatusCode, err: err}
		}()

		deadline := time.Now().Add(5 * time.Second)
		backendPID := 0
		for time.Now().Before(deadline) {
			err = f.db.QueryRow(ctx, `SELECT pid FROM pg_stat_activity
				WHERE datname=current_database() AND usename=current_user
				AND pid<>pg_backend_pid() AND application_name='newim-auth-http'
				AND wait_event_type IS NOT NULL AND query ILIKE '%im_sessions%FOR SHARE%'
				ORDER BY query_start ASC LIMIT 1`).Scan(&backendPID)
			if err == nil {
				break
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("inspect blocked backend: %v", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		if backendPID == 0 {
			t.Fatal("auth server query did not block")
		}
		select {
		case got := <-resultCh:
			t.Fatalf("request completed before disconnect: status=%d err=%v body=%q", got.status, got.err, got.body)
		default:
		}
		_, err = f.db.Exec(ctx, `SELECT pg_terminate_backend($1)`, backendPID)
		must(t, err)
		_ = tx.Rollback(ctx)
		got := <-resultCh
		if got.err != nil || got.status != http.StatusServiceUnavailable {
			t.Fatalf("disconnect result status=%d err=%v body=%q", got.status, got.err, got.body)
		}

		deadline = time.Now().Add(5 * time.Second)
		for {
			_, status, _ := doSessionRequest(t, "http://"+server.api, "Bearer "+token.RawToken())
			if status == http.StatusOK {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("auth route did not recover from disconnect; last status=%d", status)
			}
			time.Sleep(50 * time.Millisecond)
		}
		server.stop(t)
		deadline = time.Now().Add(5 * time.Second)
		for activeAuthConnections(t, f) != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("auth pool remained open after shutdown")
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
}

func doRawRequest(t *testing.T, method, url string, body io.Reader, authorization string) ([]byte, int, http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	must(t, err)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	must(t, err)
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	must(t, err)
	return data, response.StatusCode, response.Header
}

func activeAuthConnections(t *testing.T, f *fixture) int {
	t.Helper()
	var count int
	must(t, f.db.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE application_name='newim-auth-http'`).Scan(&count))
	return count
}

func twoFreeAddrs(t *testing.T) (string, string) {
	t.Helper()
	for {
		a := freeAddr(t)
		b := freeAddr(t)
		if a != b {
			return a, b
		}
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	address := listener.Addr().String()
	must(t, listener.Close())
	return address
}

func waitForHTTPStatus(url string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 250 * time.Millisecond}
	var last error
	for time.Now().Before(deadline) {
		response, err := client.Get(url)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == want {
				return nil
			}
			last = errors.New(response.Status)
		} else {
			last = err
		}
		time.Sleep(25 * time.Millisecond)
	}
	return last
}

const bearerRoute = bearer.Route

var _ = app.ErrorCode
