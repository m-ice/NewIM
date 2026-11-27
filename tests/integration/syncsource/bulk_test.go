//go:build integration

package syncsource_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	store "github.com/m-ice/NewIM/server/storage/conversationsync"
)

func testBulkBackfillDiscovery(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(14)
	// The fixture sorts before the other accounts in this suite.
	ready := bulkUsers(t, f, "00_sync_bulk_ready_", 201)
	f.sql(`INSERT INTO newim.im_conversation_sync_accounts(user_id,source_ready)
		SELECT unnest($1::text[]),true`, ready)
	const pending = "00_sync_bulk_z_pending"
	const conversation = "sync_bulk_later_projection"
	f.user(pending)
	f.sql("INSERT INTO newim.im_conversations(conversation_id,kind) VALUES($1,'group')", conversation)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", conversation, pending)

	repository, err := store.Open(f.ctx, store.Config{DSN: testDSN, AllowLocalSocket: true, RequireSourceReady: true})
	must(t, err)
	t.Cleanup(repository.Close)
	after := ""
	for _, end := range []int{99, 199} {
		next, accounts, err := repository.NextSourceAccounts(f.ctx, after)
		must(t, err)
		if next != ready[end] || len(accounts) != 0 {
			t.Fatalf("discovery page ending at fixture index %d did not advance across exactly 100 ready accounts", end)
		}
		after = next
	}
	_, accounts, err := repository.NextSourceAccounts(f.ctx, after)
	must(t, err)
	if len(accounts) == 0 || accounts[0] != pending {
		t.Fatal("discovery page after both ready-only pages omitted its first pending account")
	}

	runCLI(t, f, 0, "SYNC_SOURCE_COMPLETE", "run", "--limit", "1")
	if state := projectionState(t, f, pending); state != (accountState{ready: true, epoch: 1, head: 1}) {
		t.Fatal("bulk backfill stopped before reaching the pending account after the ready-only pages")
	}
	page := bootstrap(t, f, openSync(t, f, true), pending, 0)
	expectOnly(t, page, conversation, "upsert", 1, 0, "")
	if count := scalar(t, f, `SELECT count(*) FROM newim.im_conversation_sync_accounts
		WHERE user_id=ANY($1::text[]) AND source_ready AND source_after_key IS NULL
			AND epoch=1 AND last_change_seq=0`, ready); count != int64(len(ready)) {
		t.Fatal("bulk backfill changed a previously ready account")
	}
}

func testBulkBackfillResetInterlock(t *testing.T) {
	f := openFixture(t)
	const earlier = "00_sync_interlock_a_ready"
	const blocked = "00_sync_interlock_b_pending"
	f.user(earlier)
	f.user(blocked)
	f.sql(`INSERT INTO newim.im_conversation_sync_accounts(user_id,source_ready)
		VALUES($1,true),($2,false)`, earlier, blocked)
	before := projectionState(t, f, earlier)

	tx, lockerPID := lockBackfillAccount(t, f, blocked)
	active := make(chan commandResult, 1)
	go func() { active <- invokeCLI(f.ctx, nil, "run", "--limit", "1") }()
	waitForBackfillAccountLock(t, f, lockerPID)

	// The earlier account is unlocked and already behind bulk discovery.
	runCLI(t, f, 1, "SYNC_NOT_READY", "reset", "--user", earlier)
	if state := projectionState(t, f, earlier); state != before {
		t.Fatal("refused reset changed the ready account that bulk discovery already passed")
	}
	must(t, tx.Rollback(f.ctx))
	requireCLI(t, <-active, 0, "SYNC_SOURCE_COMPLETE")
	if state := projectionState(t, f, blocked); state != (accountState{ready: true, epoch: 1}) {
		t.Fatal("bulk backfill did not finish the previously blocked account")
	}

	runCLI(t, f, 0, "SYNC_SOURCE_RESET", "reset", "--user", earlier)
	if state := projectionState(t, f, earlier); state != (accountState{epoch: before.epoch + 1}) {
		t.Fatal("reset did not advance the account generation once bulk backfill finished")
	}
}

func testBulkBackfillLosesLockConnection(t *testing.T) {
	f := openFixture(t)
	const pending = "00_sync_loss_pending"
	const conversation = "sync_loss_projection"
	f.user(pending)
	f.sql("INSERT INTO newim.im_conversation_sync_accounts(user_id) VALUES($1)", pending)
	f.sql("INSERT INTO newim.im_conversations(conversation_id,kind) VALUES($1,'group')", conversation)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", conversation, pending)

	tx, lockerPID := lockBackfillAccount(t, f, pending)
	active := make(chan commandResult, 1)
	go func() { active <- invokeCLI(f.ctx, nil, "run", "--limit", "1") }()
	waitForBackfillAccountLock(t, f, lockerPID)

	var holders int
	var lockPID int32
	must(t, f.db.QueryRow(f.ctx, `SELECT count(DISTINCT l.pid),COALESCE(min(l.pid),0)
		FROM pg_locks l JOIN pg_stat_activity a USING(pid)
		WHERE a.datname=current_database() AND a.application_name='newim-sync-backfill'
			AND l.locktype='advisory' AND l.mode='ExclusiveLock' AND l.granted`).Scan(&holders, &lockPID))
	if holders != 1 || lockPID == lockerPID {
		t.Fatal("expected a single owned CLI backend holding the bulk lock")
	}
	var terminated bool
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_terminate_backend($1, 500)", lockPID).Scan(&terminated))
	if !terminated {
		t.Fatal("could not stop the owned CLI backend holding the bulk lock")
	}
	must(t, tx.Rollback(f.ctx))
	requireCLI(t, <-active, 1, "SYNC_STORAGE_UNAVAILABLE")
	state := projectionState(t, f, pending)
	keys := scalar(t, f, "SELECT count(*) FROM newim.im_conversation_sync_keys WHERE user_id=$1", pending)
	changes := scalar(t, f, "SELECT count(*) FROM newim.im_conversation_sync_changes WHERE user_id=$1", pending)
	if state != (accountState{epoch: 1}) || keys != 0 || changes != 0 {
		t.Fatalf("bulk backfill committed after lock connection loss: ready=%t head=%d keys=%d changes=%d", state.ready, state.head, keys, changes)
	}

	runCLI(t, f, 0, "SYNC_SOURCE_COMPLETE", "run", "--limit", "1")
	expectOnly(t, bootstrap(t, f, openSync(t, f, true), pending, 0), conversation, "upsert", 1, 0, "")
}

func lockBackfillAccount(t *testing.T, f *fixture, user string) (pgx.Tx, int32) {
	t.Helper()
	locker, err := pgx.Connect(f.ctx, testDSN)
	must(t, err)
	t.Cleanup(func() { _ = locker.Close(f.ctx) })
	tx, err := locker.Begin(f.ctx)
	must(t, err)
	t.Cleanup(func() { _ = tx.Rollback(f.ctx) })
	_, err = tx.Exec(f.ctx, "SELECT user_id FROM newim.im_conversation_sync_accounts WHERE user_id=$1 FOR UPDATE", user)
	must(t, err)
	return tx, int32(locker.PgConn().PID())
}

func waitForBackfillAccountLock(t *testing.T, f *fixture, lockerPID int32) {
	t.Helper()
	deadline := time.Now().Add(700 * time.Millisecond)
	for time.Now().Before(deadline) {
		if count := scalar(t, f, `SELECT count(*) FROM pg_stat_activity
			WHERE datname=current_database() AND application_name='newim-sync-backfill'
				AND wait_event_type='Lock' AND query ILIKE '%im_conversation_sync_accounts%'
				AND $1=ANY(pg_blocking_pids(pid))`, lockerPID); count == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("bulk backfill did not wait on the fixture's pending account row")
}
