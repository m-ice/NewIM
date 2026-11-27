//go:build integration

package accountrevocation_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func testAdmissionOrder(t *testing.T) {
	f := openFixture(t)
	const user = "revoke_serial_account"
	f.user(user)
	first, second := f.server("1", nil), f.server("1", nil)
	old := admit(t, f, first, user, "initial")
	locker, err := pgx.Connect(f.ctx, testDSN)
	must(t, err)
	defer locker.Close(f.ctx)
	tx, err := locker.Begin(f.ctx)
	must(t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, "SELECT user_id FROM newim.im_users WHERE user_id=$1 FOR UPDATE", user)
	must(t, err)

	revokeWire := revokeBody(t, user, "ordered_action", signedOutAll)
	admitWire := admitBody(t, user, "competing")
	revoked := make(chan response, 1)
	admitted := make(chan response, 1)
	go func() { revoked <- f.call(first, http.MethodPost, revokePath, f.issuerKey, revokeWire, nil) }()
	waitForAccountWaiters(t, f, 1)
	go func() { admitted <- f.call(second, http.MethodPost, admitPath, f.issuerKey, admitWire, nil) }()
	waitForAccountWaiters(t, f, 2)
	select {
	case got := <-revoked:
		t.Fatalf("revocation returned while account row was locked, status=%d", got.status)
	default:
	}
	select {
	case got := <-admitted:
		t.Fatalf("admission returned while account row was locked, status=%d", got.status)
	default:
	}
	must(t, tx.Rollback(f.ctx))

	result := <-revoked
	requireAPIStatus(t, result, http.StatusOK)
	var outcome struct {
		RevokedSessions int `json:"revokedSessions"`
	}
	must(t, json.Unmarshal(result.body, &outcome))
	fresh := expectAdmission(t, <-admitted, user, "competing")
	if n := outcome.RevokedSessions; n != 1 && n != 2 {
		t.Fatalf("serial result revoked %d sessions, want 1 or 2", n)
	}
	expectRevocation(t, result, outcome.RevokedSessions)
	if durable := receipt(t, f, user, "ordered_action", signedOutAll); durable != outcome.RevokedSessions {
		t.Fatal("serialized response disagrees with the committed receipt")
	}
	requireDurableRevocation(t, f, user, old.SessionID, signedOutAll)
	assertBearer(t, f, second, old.Token, http.StatusUnauthorized)
	if outcome.RevokedSessions == 2 {
		requireDurableRevocation(t, f, user, fresh.SessionID, signedOutAll)
		assertBearer(t, f, second, fresh.Token, http.StatusUnauthorized)
	} else {
		assertBearer(t, f, second, fresh.Token, http.StatusOK)
	}
}

func waitForAccountWaiters(t *testing.T, f *fixture, want int) {
	t.Helper()
	deadline := time.Now().Add(700 * time.Millisecond)
	for time.Now().Before(deadline) {
		var count int
		must(t, f.db.QueryRow(f.ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname=current_database() AND application_name='newim-auth-http' AND wait_event_type='Lock'
			AND query ILIKE '%im_users%' AND cardinality(pg_blocking_pids(pid))>0`).Scan(&count))
		if count == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not observe %d real requests waiting on the account row", want)
}

func testCommitRollback(t *testing.T) {
	f := openFixture(t)
	const user = "revoke_rollback_owner"
	const request = "commit_failure"
	f.user(user)
	server := f.server("1", nil)
	first := admit(t, f, server, user, "phone")
	second := admit(t, f, server, user, "other")
	// The missing reference only rejects the commit in this owned fixture.
	f.sql("CREATE TABLE newim.test_revocation_commit(request_id newim.identifier PRIMARY KEY)")
	f.sql(`ALTER TABLE newim.im_account_revocation_requests ADD CONSTRAINT test_revocation_commit_guard
		FOREIGN KEY (request_id) REFERENCES newim.test_revocation_commit(request_id)
		DEFERRABLE INITIALLY DEFERRED NOT VALID`)
	revocation(t, f, server, user, request, accountBlocked).requireError(t, http.StatusServiceUnavailable, "AUTH_UNAVAILABLE")
	requireUnaffected(t, f, user)
	assertBearer(t, f, server, first.Token, http.StatusOK)
	assertBearer(t, f, server, second.Token, http.StatusOK)
	f.sql("ALTER TABLE newim.im_account_revocation_requests DROP CONSTRAINT test_revocation_commit_guard")
	f.sql("DROP TABLE newim.test_revocation_commit")
	expectRevocation(t, revocation(t, f, server, user, request, accountBlocked), 2)
	for _, device := range []deviceReply{first, second} {
		requireDurableRevocation(t, f, user, device.SessionID, accountBlocked)
		assertBearer(t, f, server, device.Token, http.StatusUnauthorized)
	}
	if n := receipt(t, f, user, request, accountBlocked); n != 2 {
		t.Fatalf("recovered commit receipt=%d want 2", n)
	}
	server.requireRedacted(t, user, request, first.Token, second.Token, f.issuerKey, testDSN, "test_revocation_commit_guard")
}
