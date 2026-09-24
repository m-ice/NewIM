//go:build integration

package auth_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	bearer "github.com/m-ice/NewIM/server/auth/bearerhttp"
	app "github.com/m-ice/NewIM/server/auth/session"
	store "github.com/m-ice/NewIM/server/storage/authsession"
)

func TestAuthHTTP(t *testing.T) {
	t.Run("valid-and-invalid-tokens", func(t *testing.T) {
		f := openFixture(t)
		observer := &captureObserver{}
		service := f.service(observer, nil, nil)
		binding := f.seedBinding("http_valid")
		token := f.issue(service, binding, time.Hour)
		handler, err := bearer.NewSessionHandler(service)
		must(t, err)
		server := httptest.NewServer(handler)
		defer server.Close()

		body, status, headers := doSessionRequest(t, server.URL, "Bearer "+token.RawToken())
		if status != http.StatusOK {
			t.Fatalf("valid status=%d body=%q", status, body)
		}
		if strings.Contains(string(body), token.RawToken()) {
			t.Fatal("raw token reflected in valid response")
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
		if envelope.Version != bearer.Version || envelope.Session.UserID != binding.UserID ||
			envelope.Session.DeviceID != binding.DeviceID || envelope.Session.SessionID != binding.SessionID ||
			envelope.Session.TokenID != token.TokenID() || !envelope.Session.ExpiresAt.Equal(token.ExpiresAt()) {
			t.Fatalf("unexpected valid response: %+v", envelope)
		}
		if headers.Get("WWW-Authenticate") != "" {
			t.Fatalf("success response has WWW-Authenticate=%q", headers.Get("WWW-Authenticate"))
		}

		for _, value := range []string{"", "Bearer bad", "Token " + token.RawToken(), "Bearer n1_" + strings.Repeat("a", 32) + "_" + strings.Repeat("A", 43)} {
			body, status, headers = doSessionRequest(t, server.URL, value)
			if status != http.StatusUnauthorized || headers.Get("WWW-Authenticate") != `Bearer realm="newim-session"` {
				t.Fatalf("invalid header %q status=%d headers=%v body=%q", value, status, headers, body)
			}
		}

		duplicateRequest, err := http.NewRequest(http.MethodGet, server.URL+bearer.Route, nil)
		must(t, err)
		duplicateRequest.Header.Add("Authorization", "Bearer "+token.RawToken())
		duplicateRequest.Header.Add("Authorization", "Bearer "+token.RawToken())
		duplicateResponse, err := (&http.Client{Timeout: 5 * time.Second}).Do(duplicateRequest)
		must(t, err)
		defer duplicateResponse.Body.Close()
		if duplicateResponse.StatusCode != http.StatusUnauthorized || duplicateResponse.Header.Get("WWW-Authenticate") != `Bearer realm="newim-session"` {
			t.Fatalf("duplicate header status=%d headers=%v", duplicateResponse.StatusCode, duplicateResponse.Header)
		}

		tamperedSecret := "B" + strings.Repeat("A", app.TokenSecretLen-1)
		originalSecret := token.RawToken()[len(app.TokenPrefix)+app.TokenIDHexLen+1:]
		if tamperedSecret == originalSecret {
			tamperedSecret = "C" + strings.Repeat("A", app.TokenSecretLen-1)
		}
		tampered := app.TokenPrefix + token.TokenID() + "_" + tamperedSecret
		if _, err := service.AuthenticateBearer(ctx, tampered); app.ErrorCode(err) != app.AuthTokenUnknown {
			t.Fatalf("canonical tampered token got %v want %s", err, app.AuthTokenUnknown)
		}
		body, status, headers = doSessionRequest(t, server.URL, "Bearer "+tampered)
		if status != http.StatusUnauthorized || headers.Get("WWW-Authenticate") != `Bearer realm="newim-session"` || strings.Contains(string(body), tampered) {
			t.Fatalf("tampered status=%d headers=%v body=%q", status, headers, body)
		}

		body, status, headers = doSessionRequest(t, server.URL, "")
		if status != http.StatusUnauthorized || headers.Get("WWW-Authenticate") != `Bearer realm="newim-session"` {
			t.Fatalf("missing header status=%d headers=%v body=%q", status, headers, body)
		}
	})

	t.Run("expiry-revoke-and-storage-failure", func(t *testing.T) {
		f := openFixture(t)
		service := f.service(&captureObserver{}, nil, nil)
		handler, err := bearer.NewSessionHandler(service)
		must(t, err)
		server := httptest.NewServer(handler)
		defer server.Close()

		expiringBinding := f.seedBinding("http_expiry")
		expiring := f.issue(service, expiringBinding, time.Second)
		f.now.Add(1)
		body, status, headers := doSessionRequest(t, server.URL, "Bearer "+expiring.RawToken())
		if status != http.StatusUnauthorized || headers.Get("WWW-Authenticate") == "" {
			t.Fatalf("expired status=%d headers=%v body=%q", status, headers, body)
		}

		revokedBinding := f.seedBinding("http_revoke")
		revoked := f.issue(service, revokedBinding, time.Hour)
		if _, err := service.RevokeSession(ctx, revokedBinding); err != nil {
			t.Fatal(err)
		}
		body, status, headers = doSessionRequest(t, server.URL, "Bearer "+revoked.RawToken())
		if status != http.StatusUnauthorized || headers.Get("WWW-Authenticate") == "" {
			t.Fatalf("revoked status=%d headers=%v body=%q", status, headers, body)
		}

		failureBinding := f.seedBinding("http_failure")
		failureToken := f.issue(service, failureBinding, time.Hour)
		closedRepo, err := store.Open(ctx, store.Config{
			DSN: f.dsn, AllowLocalSocket: true, MaxConnections: 1, ApplicationName: "nim_auth_http_closed",
		})
		must(t, err)
		closedRepo.Close()
		closedService := f.serviceWithStore(closedRepo, &captureObserver{}, nil, nil)
		closedHandler, err := bearer.NewSessionHandler(closedService)
		must(t, err)
		closedServer := httptest.NewServer(closedHandler)
		defer closedServer.Close()
		body, status, headers = doSessionRequest(t, closedServer.URL, "Bearer "+failureToken.RawToken())
		if status != http.StatusServiceUnavailable || headers.Get("WWW-Authenticate") != "" {
			t.Fatalf("storage failure status=%d headers=%v body=%q", status, headers, body)
		}
		if strings.Contains(string(body), failureToken.RawToken()) {
			t.Fatal("raw token reflected in storage-failure response")
		}
	})

	t.Run("concurrent-revoke-converges", func(t *testing.T) {
		f := openFixture(t)
		service := f.service(&captureObserver{}, nil, nil)
		binding := f.seedBinding("http_concurrent")
		token := f.issue(service, binding, time.Hour)
		handler, err := bearer.NewSessionHandler(service)
		must(t, err)
		server := httptest.NewServer(handler)
		defer server.Close()

		start := make(chan struct{})
		var wg sync.WaitGroup
		var mu sync.Mutex
		statuses := make([]int, 0, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, status, _ := doSessionRequest(t, server.URL, "Bearer "+token.RawToken())
				mu.Lock()
				statuses = append(statuses, status)
				mu.Unlock()
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := service.RevokeSession(ctx, binding); err != nil {
				t.Errorf("revoke failed: %v", err)
			}
		}()
		close(start)
		wg.Wait()
		if len(statuses) != 8 {
			t.Fatalf("status count=%d", len(statuses))
		}
		for _, status := range statuses {
			if status != http.StatusOK && status != http.StatusUnauthorized {
				t.Fatalf("concurrent status=%d want 200 or 401; all=%v", status, statuses)
			}
		}
		body, status, headers := doSessionRequest(t, server.URL, "Bearer "+token.RawToken())
		if status != http.StatusUnauthorized || headers.Get("WWW-Authenticate") == "" || len(body) == 0 {
			t.Fatalf("post-revoke status=%d headers=%v body=%q statuses=%v", status, headers, body, statuses)
		}
	})

	t.Run("authenticate-connection-termination", func(t *testing.T) {
		f := openFixture(t)
		service := f.service(&captureObserver{}, nil, nil)
		binding := f.seedBinding("http_disconnect")
		token := f.issue(service, binding, time.Hour)
		handler, err := bearer.NewSessionHandler(service)
		must(t, err)
		server := httptest.NewServer(handler)
		defer server.Close()

		blocker, err := pgx.Connect(ctx, f.dsn)
		must(t, err)
		defer blocker.Close(context.Background())
		tx, err := blocker.Begin(ctx)
		must(t, err)
		defer func() { _ = tx.Rollback(context.Background()) }()
		_, err = tx.Exec(ctx, `SELECT user_id FROM newim.im_sessions WHERE session_id=$1 FOR UPDATE`, binding.SessionID)
		must(t, err)

		type requestResult struct {
			body   []byte
			status int
			err    error
		}
		resultCh := make(chan requestResult, 1)
		go func() {
			req, err := http.NewRequest(http.MethodGet, server.URL+bearer.Route, nil)
			if err != nil {
				resultCh <- requestResult{err: err}
				return
			}
			req.Header.Set("Authorization", "Bearer "+token.RawToken())
			response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
			if err != nil {
				resultCh <- requestResult{err: err}
				return
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			resultCh <- requestResult{body: body, status: response.StatusCode, err: err}
		}()

		deadline := time.Now().Add(5 * time.Second)
		backendPID := 0
		for time.Now().Before(deadline) {
			err = f.db.QueryRow(ctx, `SELECT pid FROM pg_stat_activity
				WHERE datname=current_database() AND usename=current_user
				AND pid<>pg_backend_pid() AND application_name=$1
				AND wait_event_type IS NOT NULL AND query ILIKE '%im_sessions%FOR SHARE%'
				ORDER BY query_start ASC LIMIT 1`, "nim_auth_test").Scan(&backendPID)
			if err == nil {
				break
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("inspect blocked auth backend: %v", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		if backendPID == 0 {
			t.Fatal("bearer authentication did not block in PostgreSQL")
		}
		select {
		case result := <-resultCh:
			t.Fatalf("request completed before connection termination: status=%d err=%v body=%q", result.status, result.err, result.body)
		default:
		}
		_, err = f.db.Exec(ctx, `SELECT pg_terminate_backend($1)`, backendPID)
		must(t, err)
		_ = tx.Rollback(ctx)

		result := <-resultCh
		if result.err != nil {
			t.Fatalf("HTTP request after backend termination: %v", result.err)
		}
		if result.status != http.StatusServiceUnavailable || !strings.Contains(string(result.body), string(bearer.CodeUnavailable)) {
			t.Fatalf("disconnect response status=%d body=%q", result.status, result.body)
		}
	})
}

func doSessionRequest(t *testing.T, baseURL, authorization string) ([]byte, int, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, baseURL+bearer.Route, nil)
	if err != nil {
		t.Fatal(err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return body, response.StatusCode, response.Header
}
