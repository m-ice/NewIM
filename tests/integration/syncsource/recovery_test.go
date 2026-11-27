//go:build integration

package syncsource_test

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/m-ice/NewIM/server/group"
	store "github.com/m-ice/NewIM/server/storage/conversationsync"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

const (
	recoveryUser  = "sync_recovery_member"
	recoveryOwner = "sync_recovery_owner"
	staleKey      = "sync_recovery_a_stale"
	activeKey     = "sync_recovery_b_active"
	racingKey     = "sync_recovery_c_membership"
	recoveryPath  = "/tmp/newim-sync-source-recovery.json"
)

type recoveryState struct {
	OwnerToken string
	Postmaster time.Time
}

func TestSyncSourceRecovery(t *testing.T) {
	f := openFixture(t)
	switch os.Getenv("NEWIM_SYNC_SOURCE_PHASE") {
	case "prepare":
		prepareRecovery(t, f)
	case "restart":
		resumeRecovery(t, f)
	default:
		t.Fatal("NEWIM_SYNC_SOURCE_PHASE must select prepare or restart")
	}
}

func prepareRecovery(t *testing.T, f *fixture) {
	t.Helper()
	f.requireMigration(13)
	f.user(recoveryUser)
	f.user(recoveryOwner)
	seedOldProjection(t, f)
	owner := f.admit(recoveryOwner, "phone")
	oldLoopback, oldGateway := f.server("", nil), f.gateway("", nil)
	send(t, f, oldLoopback, owner, activeKey, "legacy_before_backfill", "old message before migration")
	createGroup(t, f, oldGateway, owner, racingKey, []string{recoveryUser}, true)

	for _, process := range []*process{f.startServer("1", nil), f.startGateway("1", nil)} {
		select {
		case <-process.done:
			if process.waitErr == nil {
				t.Fatal("projection writer enabled without its migration")
			}
			process.requireRedacted(t, testDSN)
		case <-time.After(8 * time.Second):
			t.Fatal("projection writer did not fail fast without migration 014")
		}
	}

	const ledger = "SELECT string_agg(version::text||':'||sha256,',' ORDER BY version) FROM newim_meta.migrations WHERE version<=13"
	var oldLedger, newLedger string
	must(t, f.db.QueryRow(f.ctx, ledger).Scan(&oldLedger))
	f.migrateSources()
	f.requireMigration(14)
	must(t, f.db.QueryRow(f.ctx, ledger).Scan(&newLedger))
	if oldLedger == "" || oldLedger != newLedger {
		t.Fatal("migration 014 changed existing migration history")
	}

	client := openSync(t, f, true)
	p := app.Principal{UserID: recoveryUser}
	_, err := client.BeginBootstrap(f.ctx, p, 0)
	requireSyncError(t, err, app.NotReady)
	if got := projectionState(t, f, recoveryUser); got != (accountState{epoch: 1, head: 1}) {
		t.Fatal("old projection account started ready or lost its prior revision")
	}
	runCLI(t, f, 0, "SYNC_SOURCE_PENDING", "step", "--user", recoveryUser, "--limit", "1")
	before := projectionState(t, f, recoveryUser)
	if before != (accountState{epoch: 1, head: 2, after: staleKey}) {
		t.Fatal("first bounded backfill did not atomically append the stale tombstone and progress")
	}
	if n := scalar(t, f, `SELECT count(*) FROM newim.im_conversation_sync_changes WHERE user_id=$1 AND conversation_id=$2
		AND kind='remove' AND latest_seq IS NULL AND latest_server_msg_id IS NULL`, recoveryUser, staleKey); n != 1 {
		t.Fatal("backfill did not remove a previously projected inactive member")
	}
	_, err = client.BeginBootstrap(f.ctx, p, 0)
	requireSyncError(t, err, app.NotReady)

	installCommitFailure(f)
	runCLI(t, f, 1, "SYNC_STORAGE_UNAVAILABLE", "step", "--user", recoveryUser, "--limit", "1")
	if got := projectionState(t, f, recoveryUser); got != before {
		t.Fatal("failed backfill commit retained progress, readiness or projection head")
	}
	if n := scalar(t, f, "SELECT count(*) FROM newim.im_conversation_sync_keys WHERE user_id=$1 AND conversation_id=$2", recoveryUser, activeKey); n != 0 {
		t.Fatal("failed backfill commit retained the second directory key")
	}
	removeCommitFailure(f)

	var postmaster time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&postmaster))
	must(t, os.WriteFile(recoveryPath, encode(t, recoveryState{OwnerToken: owner, Postmaster: postmaster}), 0o600))
	oldLoopback.requireRedacted(t, owner, testDSN, "old message before migration")
}

func seedOldProjection(t *testing.T, f *fixture) {
	t.Helper()
	repo, err := store.Open(f.ctx, store.Config{DSN: testDSN, AllowLocalSocket: true})
	must(t, err)
	defer repo.Close()
	must(t, repo.InitializeEmptyAccount(f.ctx, recoveryUser))
	f.sql("INSERT INTO newim.im_conversations(conversation_id,kind) VALUES($1,'group'),($2,'group')", staleKey, activeKey)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$3),($2,$3),($2,$4)",
		staleKey, activeKey, recoveryUser, recoveryOwner)
	tx, err := repo.Begin(f.ctx)
	must(t, err)
	defer tx.Rollback(f.ctx)
	batch, err := repo.PrepareBatch(f.ctx, tx, []string{staleKey}, []string{recoveryUser})
	must(t, err)
	revision, err := batch.RecordUpsert(f.ctx, staleKey, recoveryUser)
	must(t, err)
	if revision != 1 {
		t.Fatal("legacy fixture did not publish its first directory member")
	}
	must(t, tx.Commit(f.ctx))
	f.sql("DELETE FROM newim.im_conversation_members WHERE conversation_id=$1 AND user_id=$2", staleKey, recoveryUser)
}

func resumeRecovery(t *testing.T, f *fixture) {
	t.Helper()
	f.requireMigration(14)
	data, err := os.ReadFile(recoveryPath)
	must(t, err)
	t.Cleanup(func() { _ = os.Remove(recoveryPath) })
	var saved recoveryState
	must(t, json.Unmarshal(data, &saved))
	if saved.OwnerToken == "" || saved.Postmaster.IsZero() {
		t.Fatal("backfill recovery fixture is incomplete")
	}
	var postmaster time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&postmaster))
	if !postmaster.After(saved.Postmaster) {
		t.Fatal("recovery did not start a new PostgreSQL postmaster")
	}
	if got := projectionState(t, f, recoveryUser); got != (accountState{epoch: 1, head: 2, after: staleKey}) {
		t.Fatal("restart did not resume the last committed backfill position")
	}
	currentLoopback, currentGateway := f.server("1", nil), f.gateway("1", nil)
	client := openSync(t, f, true)
	p := app.Principal{UserID: recoveryUser}
	_, err = client.BeginBootstrap(f.ctx, p, 0)
	requireSyncError(t, err, app.NotReady)

	sendWire := textBody(t, "live_during_recovery", activeKey, "concurrent source and backfill")
	progress, reply := raceBackfill(t, f, activeKey, func() response {
		return f.call(currentLoopback, http.MethodPost, sendPath, saved.OwnerToken, sendWire, nil)
	})
	requireCLI(t, progress, 0, "SYNC_SOURCE_PENDING")
	ack := expectACK(t, reply)
	if ack.ConversationSeq != "2" {
		t.Fatal("concurrent writer did not preserve existing message sequence")
	}
	if state := projectionState(t, f, recoveryUser); state.ready || state.after != activeKey || state.head < 3 || state.head > 4 {
		t.Fatal("live send and backfill did not commit a serial active-key result")
	}

	removeWire := encode(t, group.MemberRequest{GroupID: racingKey, UserID: recoveryUser, Action: group.Remove})
	progress, reply = raceBackfill(t, f, racingKey, func() response {
		return f.call(currentGateway, http.MethodPost, memberGroupPath, saved.OwnerToken, removeWire, nil)
	})
	requireCLI(t, progress, 0, "SYNC_SOURCE_READY")
	if reply.err != nil || reply.status != http.StatusOK || !reply.verified {
		t.Fatalf("concurrent member removal status=%d TLS=%t err=%v", reply.status, reply.verified, reply.err)
	}
	projection := projectionState(t, f, recoveryUser)
	if !projection.ready || projection.after != "" || projection.epoch != 1 {
		t.Fatal("terminal batch did not atomically advance readiness and clear progress")
	}
	visible := bootstrap(t, f, client, recoveryUser, 0)
	if len(visible.Items) != 1 {
		t.Fatalf("backfill exposed %d active directory items, want 1", len(visible.Items))
	}
	expectOnly(t, visible, activeKey, "upsert", visible.Items[0].Revision, 2, ack.ServerMsgID)
	runCLI(t, f, 0, "SYNC_SOURCE_COMPLETE", "run", "--limit", "1")
	runCLI(t, f, 0, "SYNC_SOURCE_READY", "step", "--user", recoveryUser, "--limit", "1")
	if unchanged := projectionState(t, f, recoveryUser); unchanged != projection {
		t.Fatal("rerun of a completed backfill changed an account projection")
	}

	runCLI(t, f, 0, "SYNC_SOURCE_RESET", "reset", "--user", recoveryUser)
	reset := projectionState(t, f, recoveryUser)
	if reset.ready || reset.after != "" || reset.epoch != projection.epoch+1 || reset.head != projection.head {
		t.Fatal("reset did not atomically invalidate readiness and advance its generation")
	}
	_, err = client.BeginBootstrap(f.ctx, p, 0)
	requireSyncError(t, err, app.NotReady)
	runCLI(t, f, 0, "SYNC_SOURCE_COMPLETE", "run", "--limit", "1")
	repaired := projectionState(t, f, recoveryUser)
	if !repaired.ready || repaired.head != projection.head || repaired.epoch != reset.epoch || repaired.after != "" {
		t.Fatal("same-state rebuild changed projection history or did not restore readiness")
	}
	_, err = client.BeginDelta(f.ctx, p, visible.NextCursor, 10)
	requireSyncError(t, err, app.CursorExpired)
	rechecked := bootstrap(t, f, client, recoveryUser, 0)
	if len(rechecked.Items) != 1 {
		t.Fatal("reset/rebuild exposed stale or removed directory entries")
	}
	expectOnly(t, rechecked, activeKey, "upsert", visible.Items[0].Revision, 2, ack.ServerMsgID)
	currentLoopback.requireRedacted(t, saved.OwnerToken, testDSN, "concurrent source and backfill")
	currentGateway.requireRedacted(t, saved.OwnerToken, testDSN, recoveryUser)
}

func raceBackfill(t *testing.T, f *fixture, conversation string, source func() response) (commandResult, response) {
	t.Helper()
	conn, err := pgx.Connect(f.ctx, testDSN)
	must(t, err)
	defer conn.Close(f.ctx)
	tx, err := conn.Begin(f.ctx)
	must(t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, "SELECT conversation_id FROM newim.im_conversations WHERE conversation_id=$1 FOR UPDATE", conversation)
	must(t, err)
	backfill := make(chan commandResult, 1)
	writer := make(chan response, 1)
	go func() { backfill <- invokeCLI(f.ctx, nil, "step", "--user", recoveryUser, "--limit", "1") }()
	waitConversationLocks(t, f, 1)
	go func() { writer <- source() }()
	waitConversationLocks(t, f, 2)
	must(t, tx.Rollback(f.ctx))
	return <-backfill, <-writer
}

func waitConversationLocks(t *testing.T, f *fixture, want int64) {
	t.Helper()
	deadline := time.Now().Add(700 * time.Millisecond)
	for time.Now().Before(deadline) {
		n := scalar(t, f, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()
			AND wait_event_type='Lock' AND query ILIKE '%im_conversation%'
			AND cardinality(pg_blocking_pids(pid))>0`)
		if n >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not observe %d backfill/source transactions waiting for the fixture lock", want)
}
