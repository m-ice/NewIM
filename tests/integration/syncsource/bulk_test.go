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

	locker, err := pgx.Connect(f.ctx, testDSN)
	must(t, err)
	defer locker.Close(f.ctx)
	tx, err := locker.Begin(f.ctx)
	must(t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, "SELECT user_id FROM newim.im_conversation_sync_accounts WHERE user_id=$1 FOR UPDATE", blocked)
	must(t, err)

	active := make(chan commandResult, 1)
	go func() { active <- invokeCLI(f.ctx, nil, "run", "--limit", "1") }()
	waitForBackfillAccountLock(t, f)

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

func waitForBackfillAccountLock(t *testing.T, f *fixture) {
	t.Helper()
	deadline := time.Now().Add(700 * time.Millisecond)
	for time.Now().Before(deadline) {
		if count := scalar(t, f, `SELECT count(*) FROM pg_stat_activity
			WHERE datname=current_database() AND application_name='newim-sync-backfill'
				AND wait_event_type='Lock' AND query ILIKE '%im_conversation_sync_accounts%'
				AND cardinality(pg_blocking_pids(pid))>0`); count == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("bulk backfill did not wait on the fixture's pending account row")
}
