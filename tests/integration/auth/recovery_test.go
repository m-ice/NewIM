//go:build integration

package auth_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	app "github.com/m-ice/NewIM/server/auth/session"
)

const recoveryStatePath = "/tmp/nim-srv-003-auth-recovery.json"

type recoveryState struct {
	Binding        app.SessionBinding `json:"binding"`
	PrimaryToken   string             `json:"primary_token"`
	PrimaryTokenID string             `json:"primary_token_id"`
	RevokedToken   string             `json:"revoked_token"`
	RevokedTokenID string             `json:"revoked_token_id"`
	SessionRevoked bool               `json:"session_revoked"`
}

func TestAuthRecovery(t *testing.T) {
	switch os.Getenv("NEWIM_AUTH_PHASE") {
	case "prepare":
		prepareRecovery(t)
	case "restart":
		restartRecovery(t)
	case "restore":
		restoreRecovery(t)
	default:
		t.Fatalf("unexpected recovery phase %q", os.Getenv("NEWIM_AUTH_PHASE"))
	}
}

func prepareRecovery(t *testing.T) {
	f := openFixture(t)
	binding := f.seedBinding("recovery_main")
	observer := &captureObserver{}
	service := f.service(observer, nil, nil)
	primary := f.issue(service, binding, time.Hour)
	revoked := f.issue(service, binding, time.Hour)
	if _, err := service.RevokeToken(ctx, binding.UserID, revoked.TokenID()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.authenticate(service, primary, binding, "connection_primary"); err != nil {
		t.Fatalf("primary token failed before restart: %v", err)
	}
	if _, err := f.authenticate(service, revoked, binding, "connection_revoked"); app.ErrorCode(err) != app.AuthTokenRevoked {
		t.Fatalf("revoked token before restart got %v", err)
	}

	testConcurrentAuthenticateRevoke(t, f, service)
	testCommitFailureAndDisconnect(t, f)

	state := recoveryState{
		Binding: binding, PrimaryToken: primary.RawToken(), PrimaryTokenID: primary.TokenID(),
		RevokedToken: revoked.RawToken(), RevokedTokenID: revoked.TokenID(),
	}
	writeRecoveryState(t, state)
}

func restartRecovery(t *testing.T) {
	f := openFixture(t)
	state := readRecoveryState(t)
	observer := &captureObserver{}
	service := f.service(observer, nil, nil)
	primary, err := app.NewConnectionIdentity(state.Binding.UserID, state.Binding.DeviceID, state.Binding.SessionID, "connection_primary", state.PrimaryTokenID)
	must(t, err)
	if primary.TokenID() != state.PrimaryTokenID {
		t.Fatal("identity mismatch")
	}
	if _, err = service.Authenticate(ctx, app.AuthenticateRequest{Token: state.PrimaryToken, Binding: state.Binding, ConnectionID: "connection_primary"}); err != nil {
		t.Fatalf("primary token did not survive restart: %v", err)
	}
	if _, err = service.Authenticate(ctx, app.AuthenticateRequest{Token: state.RevokedToken, Binding: state.Binding, ConnectionID: "connection_revoked"}); app.ErrorCode(err) != app.AuthTokenRevoked {
		t.Fatalf("revoked token resurrected after restart: %v", err)
	}
	if _, err = service.RevokeSession(ctx, state.Binding); err != nil {
		t.Fatalf("session revoke after restart failed: %v", err)
	}
	state.SessionRevoked = true
	writeRecoveryState(t, state)
}

func restoreRecovery(t *testing.T) {
	f := openFixture(t)
	state := readRecoveryState(t)
	service := f.service(&captureObserver{}, nil, nil)
	if _, err := service.Authenticate(ctx, app.AuthenticateRequest{Token: state.PrimaryToken, Binding: state.Binding, ConnectionID: "connection_primary"}); app.ErrorCode(err) != app.AuthTokenRevoked {
		t.Fatalf("restored revoked session resurrected (got %v, state revoked=%t)", err, state.SessionRevoked)
	}
	_ = os.Remove(recoveryStatePath)
}

func testConcurrentAuthenticateRevoke(t *testing.T, f *fixture, service *app.Service) {
	t.Helper()
	binding := f.seedBinding("recovery_concurrent")
	token := f.issue(service, binding, time.Hour)
	sleepFunction := installSessionSleepTrigger(t, f, binding.SessionID)
	defer dropSessionSleepTrigger(t, f, sleepFunction)

	revokeDone := make(chan error, 1)
	go func() {
		_, err := service.RevokeSession(context.Background(), binding)
		revokeDone <- err
	}()
	waitForSessionSleep(t, f)

	const attempts = 6
	authDone := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			_, err := service.Authenticate(context.Background(), app.AuthenticateRequest{Token: token.RawToken(), Binding: binding, ConnectionID: "connection_concurrent"})
			authDone <- err
		}()
	}
	waitForAuthLockWaiters(t, f, attempts)
	if err := <-revokeDone; err != nil {
		t.Fatalf("concurrent revoke failed: %v", err)
	}
	for i := 0; i < attempts; i++ {
		err := <-authDone
		if app.ErrorCode(err) != app.AuthTokenRevoked {
			t.Fatalf("post-revoke authentication got %v want %s", err, app.AuthTokenRevoked)
		}
	}
	if !f.sessionRevoked(binding) {
		t.Fatal("concurrent revoke did not commit durably")
	}
}

func waitForSessionSleep(t *testing.T, f *fixture) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		err := f.db.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name='nim_auth_test' AND wait_event='PgSleep' AND query LIKE 'UPDATE newim.im_sessions%'").Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("session revoke sleep barrier was not observed")
}

func waitForAuthLockWaiters(t *testing.T, f *fixture, want int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		err := f.db.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name='nim_auth_test' AND wait_event_type='Lock'").Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("auth lock waiters did not reach %d", want)
}

func installSessionSleepTrigger(t *testing.T, f *fixture, sessionID string) string {
	t.Helper()
	functionName := "test_auth_session_sleep_" + strings.ReplaceAll(sessionID, "-", "_")
	triggerName := functionName + "_trigger"
	f.sql(fmt.Sprintf("CREATE FUNCTION newim.%s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.session_id = '%s'::newim.identifier THEN PERFORM pg_sleep(0.5); END IF; RETURN NEW; END $$", functionName, sessionID))
	f.sql(fmt.Sprintf("CREATE TRIGGER %s BEFORE UPDATE ON newim.im_sessions FOR EACH ROW EXECUTE FUNCTION newim.%s()", triggerName, functionName))
	return functionName
}

func dropSessionSleepTrigger(t *testing.T, f *fixture, functionName string) {
	t.Helper()
	f.sql("DROP TRIGGER IF EXISTS " + functionName + "_trigger ON newim.im_sessions")
	f.sql("DROP FUNCTION IF EXISTS newim." + functionName + "()")
}

func testCommitFailureAndDisconnect(t *testing.T, f *fixture) {
	t.Helper()
	commitBinding := f.seedBinding("recovery_commit_failure")
	commitFunction := installAuthCommitTrigger(t, f, commitBinding.SessionID, "AUTH_TEST_COMMIT_FAILURE", false)
	defer dropAuthCommitTrigger(t, f, commitFunction)
	service := f.service(&captureObserver{}, nil, nil)
	_, err := service.Issue(ctx, app.IssueRequest{Binding: commitBinding, TTL: time.Hour})
	mustCode(t, err, app.AuthStorageUnavailable)
	if got := f.scalarString("SELECT count(*) FROM newim.im_auth_tokens WHERE session_id=$1", commitBinding.SessionID); got != "0" {
		t.Fatalf("commit failure left %s token rows", got)
	}
	dropAuthCommitTrigger(t, f, commitFunction)

	disconnectBinding := f.seedBinding("recovery_disconnect")
	disconnectFunction := installAuthCommitTrigger(t, f, disconnectBinding.SessionID, "AUTH_TEST_DISCONNECT", true)
	defer dropAuthCommitTrigger(t, f, disconnectFunction)
	done := make(chan error, 1)
	go func() {
		_, err := service.Issue(context.Background(), app.IssueRequest{Binding: disconnectBinding, TTL: time.Hour})
		done <- err
	}()
	waitForCommitSleep(t, f)
	if _, err = f.db.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name=$1 AND wait_event='PgSleep'", "nim_auth_test"); err != nil {
		t.Fatal(err)
	}
	mustCode(t, <-done, app.AuthStorageUnavailable)
	if got := f.scalarString("SELECT count(*) FROM newim.im_auth_tokens WHERE session_id=$1", disconnectBinding.SessionID); got != "0" {
		t.Fatalf("ambiguous commit left %s token rows", got)
	}
}

func installAuthCommitTrigger(t *testing.T, f *fixture, sessionID, message string, sleep bool) string {
	t.Helper()
	functionName := "test_auth_commit_" + strings.ReplaceAll(sessionID, "-", "_")
	triggerName := functionName + "_trigger"
	body := "RAISE EXCEPTION USING ERRCODE='P0001', MESSAGE='" + message + "'; RETURN NEW;"
	if sleep {
		body = "PERFORM pg_sleep(5); RETURN NEW;"
	}
	f.sql(fmt.Sprintf("CREATE FUNCTION newim.%s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.session_id = '%s'::newim.identifier THEN %s END IF; RETURN NEW; END $$", functionName, sessionID, body))
	f.sql(fmt.Sprintf("CREATE CONSTRAINT TRIGGER %s AFTER INSERT ON newim.im_auth_tokens DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION newim.%s()", triggerName, functionName))
	return functionName
}

func dropAuthCommitTrigger(t *testing.T, f *fixture, functionName string) {
	t.Helper()
	f.sql("DROP TRIGGER IF EXISTS " + functionName + "_trigger ON newim.im_auth_tokens")
	f.sql("DROP FUNCTION IF EXISTS newim." + functionName + "()")
}

func waitForCommitSleep(t *testing.T, f *fixture) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		err := f.db.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 AND wait_event='PgSleep' AND lower(query) LIKE 'commit%'", "nim_auth_test").Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	rows, err := f.db.Query(ctx, "SELECT pid,application_name,state,coalesce(wait_event_type,''),coalesce(wait_event,''),left(query,100) FROM pg_stat_activity WHERE datname=current_database() ORDER BY pid")
	if err == nil {
		var details []string
		for rows.Next() {
			var pid int
			var name, state, waitType, waitEvent, query string
			if err = rows.Scan(&pid, &name, &state, &waitType, &waitEvent, &query); err != nil {
				break
			}
			details = append(details, fmt.Sprintf("%d:%s:%s:%s/%s:%s", pid, name, state, waitType, waitEvent, query))
		}
		rows.Close()
		t.Fatalf("commit-time sleeper was not observed: %s", strings.Join(details, " | "))
	}
	t.Fatalf("commit-time sleeper was not observed")
}

func writeRecoveryState(t *testing.T, state recoveryState) {
	t.Helper()
	raw, err := json.Marshal(state)
	must(t, err)
	if err = os.WriteFile(recoveryStatePath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readRecoveryState(t *testing.T) recoveryState {
	t.Helper()
	raw, err := os.ReadFile(recoveryStatePath)
	must(t, err)
	var state recoveryState
	must(t, json.Unmarshal(raw, &state))
	return state
}
