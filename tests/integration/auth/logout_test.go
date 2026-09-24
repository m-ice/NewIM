//go:build integration

package auth_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	bearer "github.com/m-ice/NewIM/server/auth/bearerhttp"
	app "github.com/m-ice/NewIM/server/auth/session"
)

type ambiguousRevokeStore struct{ app.Store }

func (s ambiguousRevokeStore) RevokeBearer(ctx context.Context, tokenID string, verify func(app.TokenSnapshot) error, revokedAt time.Time) (app.RevocationOutcome, error) {
	if _, err := s.Store.RevokeBearer(ctx, tokenID, verify, revokedAt); err != nil {
		return "", err
	}
	return "", app.Fail(app.AuthStorageUnavailable)
}

type abortAfterHandler struct{ next http.Handler }

func (h abortAfterHandler) ServeHTTP(_ http.ResponseWriter, r *http.Request) {
	h.next.ServeHTTP(discardResponseWriter{header: make(http.Header)}, r)
	panic(http.ErrAbortHandler)
}

type discardResponseWriter struct{ header http.Header }

func (w discardResponseWriter) Header() http.Header          { return w.header }
func (discardResponseWriter) Write(data []byte) (int, error) { return len(data), nil }
func (discardResponseWriter) WriteHeader(int)                {}

func TestAuthLogout(t *testing.T) {
	t.Run("token-only-idempotent-isolation", func(t *testing.T) {
		f := openFixture(t)
		service := f.service(&captureObserver{}, nil, nil)
		binding := f.seedBinding("logout_keep")
		tokenA := f.issue(service, binding, time.Hour)
		tokenB := f.issue(service, binding, time.Hour)
		sessionHandler, err := bearer.NewSessionHandler(service)
		must(t, err)
		handler, err := bearer.NewTokenRevocationHandler(service)
		must(t, err)
		mux := http.NewServeMux()
		mux.Handle(bearer.Route, sessionHandler)
		mux.Handle(bearer.TokenRoute, handler)
		server := httptest.NewServer(mux)
		defer server.Close()

		body, status, headers := doTokenRevokeRequest(t, server.URL, "Bearer "+tokenA.RawToken())
		if status != http.StatusNoContent || len(body) != 0 || headers.Get("WWW-Authenticate") != "" {
			t.Fatalf("first revoke status=%d body=%q headers=%v", status, body, headers)
		}
		firstRevokedAt := f.scalarString("SELECT revoked_at::text FROM newim.im_auth_tokens WHERE token_id=$1", tokenA.TokenID())
		if firstRevokedAt == "" || f.tokenRevoked(binding, tokenB.TokenID()) || f.sessionRevoked(binding) {
			t.Fatalf("first revoke state revokedAt=%q tokenBRevoked=%t sessionRevoked=%t", firstRevokedAt, f.tokenRevoked(binding, tokenB.TokenID()), f.sessionRevoked(binding))
		}
		if _, status, _ = doSessionRequest(t, server.URL, "Bearer "+tokenB.RawToken()); status != http.StatusOK {
			t.Fatalf("sibling token status=%d", status)
		}
		if _, status, _ = doTokenRevokeRequest(t, server.URL, "Bearer "+tokenA.RawToken()); status != http.StatusNoContent {
			t.Fatalf("idempotent retry status=%d", status)
		}
		if got := f.scalarString("SELECT revoked_at::text FROM newim.im_auth_tokens WHERE token_id=$1", tokenA.TokenID()); got != firstRevokedAt {
			t.Fatalf("retry rewrote revoked_at got=%q want=%q", got, firstRevokedAt)
		}
	})

	t.Run("expired-revoked-tampered-and-unknown", func(t *testing.T) {
		f := openFixture(t)
		service := f.service(&captureObserver{}, nil, nil)
		handler, err := bearer.NewTokenRevocationHandler(service)
		must(t, err)
		server := httptest.NewServer(handler)
		defer server.Close()

		expiredBinding := f.seedBinding("logout_expired")
		expired := f.issue(service, expiredBinding, time.Hour)
		f.sql("UPDATE newim.im_auth_tokens SET created_at=now()-interval '2 hours', expires_at=now()-interval '1 hour' WHERE token_id=$1", expired.TokenID())
		if _, status, _ := doTokenRevokeRequest(t, server.URL, "Bearer "+expired.RawToken()); status != http.StatusNoContent {
			t.Fatalf("expired revoke status=%d", status)
		}
		if !f.tokenRevoked(expiredBinding, expired.TokenID()) {
			t.Fatal("expired token was not revoked")
		}

		revokedBinding := f.seedBinding("logout_revoked")
		revoked := f.issue(service, revokedBinding, time.Hour)
		if _, err := service.RevokeSession(ctx, revokedBinding); err != nil {
			t.Fatal(err)
		}
		if _, status, _ := doTokenRevokeRequest(t, server.URL, "Bearer "+revoked.RawToken()); status != http.StatusNoContent {
			t.Fatalf("session-revoked retry status=%d", status)
		}

		tamperedBinding := f.seedBinding("logout_tampered")
		original := f.issue(service, tamperedBinding, time.Hour)
		other := f.issue(service, tamperedBinding, time.Hour)
		parts := strings.Split(other.RawToken(), "_")
		tampered := app.TokenPrefix + original.TokenID() + "_" + parts[2]
		body, status, headers := doTokenRevokeRequest(t, server.URL, "Bearer "+tampered)
		if status != http.StatusUnauthorized || headers.Get("WWW-Authenticate") == "" || strings.Contains(string(body), tampered) {
			t.Fatalf("tampered status=%d headers=%v body=%q", status, headers, body)
		}
		if f.tokenRevoked(tamperedBinding, original.TokenID()) {
			t.Fatal("tampered proof revoked the target token")
		}

		unknownBinding := f.seedBinding("logout_unknown")
		unknown := f.issue(service, unknownBinding, time.Hour)
		f.sql("DELETE FROM newim.im_auth_tokens WHERE token_id=$1", unknown.TokenID())
		if _, status, _ := doTokenRevokeRequest(t, server.URL, "Bearer "+unknown.RawToken()); status != http.StatusUnauthorized {
			t.Fatalf("unknown revoke status=%d", status)
		}
	})

	t.Run("deterministic-ambiguous-commit", func(t *testing.T) {
		f := openFixture(t)
		realService := f.service(&captureObserver{}, nil, nil)
		binding := f.seedBinding("logout_ambiguous")
		token := f.issue(realService, binding, time.Hour)
		faultyService := f.serviceWithStore(ambiguousRevokeStore{Store: f.repo}, &captureObserver{}, nil, nil)
		handler, err := bearer.NewTokenRevocationHandler(faultyService)
		must(t, err)
		server := httptest.NewServer(handler)
		defer server.Close()

		if _, status, _ := doTokenRevokeRequest(t, server.URL, "Bearer "+token.RawToken()); status != http.StatusServiceUnavailable {
			t.Fatalf("ambiguous first status=%d", status)
		}
		if !f.tokenRevoked(binding, token.TokenID()) {
			t.Fatal("real transaction did not commit before the injected unavailable result")
		}
		firstRevokedAt := f.scalarString("SELECT revoked_at::text FROM newim.im_auth_tokens WHERE token_id=$1", token.TokenID())
		if outcome, err := realService.RevokeBearer(ctx, token.RawToken()); err != nil || outcome != app.RevokeNoop {
			t.Fatalf("retry outcome=%q err=%v", outcome, err)
		}
		if got := f.scalarString("SELECT revoked_at::text FROM newim.im_auth_tokens WHERE token_id=$1", token.TokenID()); got != firstRevokedAt {
			t.Fatalf("ambiguous retry rewrote revoked_at got=%q want=%q", got, firstRevokedAt)
		}
	})

	t.Run("deterministic-response-loss", func(t *testing.T) {
		f := openFixture(t)
		service := f.service(&captureObserver{}, nil, nil)
		binding := f.seedBinding("logout_response_loss")
		token := f.issue(service, binding, time.Hour)
		handler, err := bearer.NewTokenRevocationHandler(service)
		must(t, err)

		lossServer := httptest.NewServer(abortAfterHandler{next: handler})
		req, err := http.NewRequest(http.MethodDelete, lossServer.URL+bearer.TokenRoute, nil)
		must(t, err)
		req.Header.Set("Authorization", "Bearer "+token.RawToken())
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err == nil {
			_ = response.Body.Close()
		}
		lossServer.Close()
		if err == nil {
			t.Fatal("response-loss request unexpectedly returned a response")
		}
		deadline := time.Now().Add(5 * time.Second)
		for !f.tokenRevoked(binding, token.TokenID()) {
			if time.Now().After(deadline) {
				t.Fatal("response-loss handler did not commit revocation")
			}
			time.Sleep(20 * time.Millisecond)
		}
		retryServer := httptest.NewServer(handler)
		defer retryServer.Close()
		if _, status, _ := doTokenRevokeRequest(t, retryServer.URL, "Bearer "+token.RawToken()); status != http.StatusNoContent {
			t.Fatalf("response-loss retry status=%d", status)
		}
	})

	t.Run("backend-failure-recovers", func(t *testing.T) {
		f := openFixture(t)
		service := f.service(&captureObserver{}, nil, nil)
		binding := f.seedBinding("logout_backend_failure")
		token := f.issue(service, binding, time.Hour)
		handler, err := bearer.NewTokenRevocationHandler(service)
		must(t, err)
		server := httptest.NewServer(handler)
		defer server.Close()

		blocker, tx := lockSessionRow(t, f, binding)
		defer func() { _ = tx.Rollback(context.Background()); blocker.Close(context.Background()) }()
		type revokeResult struct {
			body   []byte
			status int
			header http.Header
			err    error
		}
		resultCh := make(chan revokeResult, 1)
		go func() {
			req, reqErr := http.NewRequest(http.MethodDelete, server.URL+bearer.TokenRoute, nil)
			if reqErr != nil {
				resultCh <- revokeResult{err: reqErr}
				return
			}
			req.Header.Set("Authorization", "Bearer "+token.RawToken())
			response, doErr := (&http.Client{Timeout: 5 * time.Second}).Do(req)
			if doErr != nil {
				resultCh <- revokeResult{err: doErr}
				return
			}
			defer response.Body.Close()
			body, readErr := io.ReadAll(response.Body)
			resultCh <- revokeResult{body: body, status: response.StatusCode, header: response.Header, err: readErr}
		}()
		backendPID := waitForAuthBackend(t, f)
		_, err = f.db.Exec(ctx, "SELECT pg_terminate_backend($1)", backendPID)
		must(t, err)
		_ = tx.Rollback(ctx)
		blocker.Close(context.Background())
		result := <-resultCh
		if result.err != nil || result.status != http.StatusServiceUnavailable {
			t.Fatalf("backend failure status=%d err=%v body=%q", result.status, result.err, result.body)
		}
		if f.tokenRevoked(binding, token.TokenID()) {
			t.Fatal("terminated backend committed a revocation")
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			body, status, _ := doTokenRevokeRequest(t, server.URL, "Bearer "+token.RawToken())
			if status == http.StatusNoContent {
				break
			}
			if status != http.StatusServiceUnavailable || time.Now().After(deadline) {
				t.Fatalf("recovery retry status=%d body=%q", status, body)
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !f.tokenRevoked(binding, token.TokenID()) || f.sessionRevoked(binding) {
			t.Fatalf("recovery state target=%t session=%t", f.tokenRevoked(binding, token.TokenID()), f.sessionRevoked(binding))
		}
	})

	t.Run("observable-concurrent-lock-order", func(t *testing.T) {
		f := openFixture(t)
		service := f.service(&captureObserver{}, nil, nil)

		t.Run("token-only", func(t *testing.T) {
			binding := f.seedBinding("logout_race_token")
			tokenA := f.issue(service, binding, time.Hour)
			tokenB := f.issue(service, binding, time.Hour)
			blocker, tx := lockSessionRow(t, f, binding)
			defer func() { _ = tx.Rollback(context.Background()); blocker.Close(context.Background()) }()

			start := make(chan struct{})
			revokeResult := make(chan error, 1)
			authResult := make(chan error, 1)
			go func() {
				<-start
				outcome, err := service.RevokeBearer(ctx, tokenA.RawToken())
				if err == nil && outcome != app.RevokeOK && outcome != app.RevokeNoop {
					err = errors.New("unexpected revoke outcome")
				}
				revokeResult <- err
			}()
			go func() {
				<-start
				_, err := service.AuthenticateBearer(ctx, tokenB.RawToken())
				authResult <- err
			}()
			close(start)
			waitForAuthWaiters(t, f, 2)
			_ = tx.Rollback(ctx)
			blocker.Close(context.Background())
			must(t, <-revokeResult)
			must(t, <-authResult)
			if !f.tokenRevoked(binding, tokenA.TokenID()) || f.tokenRevoked(binding, tokenB.TokenID()) || f.sessionRevoked(binding) {
				t.Fatalf("token-only race state A=%t B=%t session=%t", f.tokenRevoked(binding, tokenA.TokenID()), f.tokenRevoked(binding, tokenB.TokenID()), f.sessionRevoked(binding))
			}
		})

		t.Run("session-revoke", func(t *testing.T) {
			binding := f.seedBinding("logout_race_session")
			tokenA := f.issue(service, binding, time.Hour)
			tokenB := f.issue(service, binding, time.Hour)
			blocker, tx := lockSessionRow(t, f, binding)
			defer func() { _ = tx.Rollback(context.Background()); blocker.Close(context.Background()) }()

			start := make(chan struct{})
			revokeResult := make(chan error, 1)
			sessionResult := make(chan error, 1)
			go func() {
				<-start
				outcome, err := service.RevokeBearer(ctx, tokenA.RawToken())
				if err == nil && outcome != app.RevokeOK && outcome != app.RevokeNoop {
					err = errors.New("unexpected revoke outcome")
				}
				revokeResult <- err
			}()
			go func() {
				<-start
				outcome, err := service.RevokeSession(ctx, binding)
				if err == nil && outcome != app.RevokeOK && outcome != app.RevokeNoop {
					err = errors.New("unexpected session outcome")
				}
				sessionResult <- err
			}()
			close(start)
			waitForAuthWaiters(t, f, 2)
			_ = tx.Rollback(ctx)
			blocker.Close(context.Background())
			must(t, <-revokeResult)
			must(t, <-sessionResult)
			if !f.sessionRevoked(binding) || !f.tokenRevoked(binding, tokenA.TokenID()) || !f.tokenRevoked(binding, tokenB.TokenID()) {
				t.Fatalf("session race state session=%t A=%t B=%t", f.sessionRevoked(binding), f.tokenRevoked(binding, tokenA.TokenID()), f.tokenRevoked(binding, tokenB.TokenID()))
			}
		})
	})
}

func TestAuthLogoutRestore(t *testing.T) {
	f := openFixture(t)
	keepBinding := newBinding("logout_keep")
	if f.sessionRevoked(keepBinding) {
		t.Fatal("token-only session became revoked after restore")
	}
	if active, revoked := f.tokenRevocationCounts(keepBinding); active != 1 || revoked != 1 {
		t.Fatalf("token-only restore counts active=%d revoked=%d", active, revoked)
	}
	raceBinding := newBinding("logout_race_session")
	if !f.sessionRevoked(raceBinding) {
		t.Fatal("session-revoke scenario was not preserved after restore")
	}
	if active, revoked := f.tokenRevocationCounts(raceBinding); active != 0 || revoked != 2 {
		t.Fatalf("session-revoke restore counts active=%d revoked=%d", active, revoked)
	}
}

func (f *fixture) tokenRevoked(binding app.SessionBinding, tokenID string) bool {
	return f.scalarBool("SELECT revoked_at IS NOT NULL FROM newim.im_auth_tokens WHERE token_id=$1 AND session_id=$2", tokenID, binding.SessionID)
}

func (f *fixture) tokenRevocationCounts(binding app.SessionBinding) (active, revoked int) {
	f.t.Helper()
	must(f.t, f.db.QueryRow(ctx, `SELECT count(*) FILTER (WHERE revoked_at IS NULL), count(*) FILTER (WHERE revoked_at IS NOT NULL)
		FROM newim.im_auth_tokens WHERE session_id=$1`, binding.SessionID).Scan(&active, &revoked))
	return active, revoked
}

func doTokenRevokeRequest(t *testing.T, baseURL, authorization string) ([]byte, int, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, baseURL+bearer.TokenRoute, nil)
	must(t, err)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	must(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	must(t, err)
	return body, response.StatusCode, response.Header
}

func lockSessionRow(t *testing.T, f *fixture, binding app.SessionBinding) (*pgx.Conn, pgx.Tx) {
	t.Helper()
	conn, err := pgx.Connect(ctx, f.dsn)
	must(t, err)
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Close(context.Background())
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, "SELECT user_id FROM newim.im_sessions WHERE session_id=$1 FOR UPDATE", binding.SessionID); err != nil {
		_ = tx.Rollback(context.Background())
		conn.Close(context.Background())
		t.Fatal(err)
	}
	return conn, tx
}

func waitForAuthBackend(t *testing.T, f *fixture) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var pid int
		err := f.db.QueryRow(ctx, `SELECT pid FROM pg_stat_activity
			WHERE datname=current_database() AND usename=current_user
			AND pid<>pg_backend_pid() AND application_name='nim_auth_test'
			AND wait_event_type IS NOT NULL AND query ILIKE '%im_sessions%'
			ORDER BY query_start ASC LIMIT 1`).Scan(&pid)
		if err == nil {
			return pid
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("inspect blocked revoke backend: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("revoke backend did not block in PostgreSQL")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForAuthWaiters(t *testing.T, f *fixture, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var count int
		err := f.db.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname=current_database() AND usename=current_user
			AND application_name='nim_auth_test' AND wait_event_type IS NOT NULL
			AND query ILIKE '%im_sessions%'`).Scan(&count)
		must(t, err)
		if count >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("auth waiters=%d want at least %d", count, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
