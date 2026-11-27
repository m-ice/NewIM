//go:build integration

package conversationsynchttp_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

const (
	recoveryFile  = "/tmp/newim-sync-http-recovery.json"
	recoveryOwner = "synchttp_recovery_owner"
	recoveryA     = "synchttp_recovery_a"
	recoveryB     = "synchttp_recovery_b"
	recoveryC     = "synchttp_recovery_c"
	recoveryD     = "synchttp_recovery_d"
)

type recoveryState struct {
	Token         string
	Cursor        string
	Checkpoint    string
	FirstServerID string
	Postmaster    time.Time
}

func TestSyncHTTPRecovery(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(14)
	switch os.Getenv("NEWIM_SYNC_HTTP_PHASE") {
	case "prepare":
		prepareSyncRecovery(t, f)
	case "restart":
		resumeSyncRecovery(t, f)
	default:
		t.Fatal("NEWIM_SYNC_HTTP_PHASE must be prepare or restart")
	}
}

func prepareSyncRecovery(t *testing.T, f *fixture) {
	t.Helper()
	loopback := f.server("1", nil)
	gateway := f.syncGateway(writeKeys(t, "old", "old", "new"), nil)
	provision(t, f, loopback, recoveryOwner)
	phone := f.admit(recoveryOwner, "phone")
	tablet := f.admit(recoveryOwner, "tablet")
	for _, id := range []string{recoveryA, recoveryB, recoveryC} {
		createGroup(t, f, gateway, phone, id, nil, true)
	}
	partial := readPage(t, f, gateway, bootstrapPath, phone, map[string]int{"limit": 1})
	expectItem(t, partial, true, recoveryA, "upsert", 1, 0, "")
	full := readPage(t, f, gateway, bootstrapPath, phone, map[string]any{})
	if full.HasMore || len(full.Items) != 3 {
		t.Fatal("did not create the original three-item checkpoint")
	}
	ack := send(t, f, loopback, phone, recoveryB, "recovery_after_checkpoint", "committed before database restart")
	createGroup(t, f, gateway, phone, recoveryD, nil, true)
	if !groupResult[bool](t, f, gateway, phone, dissolveGroupPath, "dissolved", map[string]string{"groupId": recoveryA}) {
		t.Fatal("failed to commit the pre-restart removal")
	}
	var postmaster time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&postmaster))
	must(t, os.WriteFile(recoveryFile, encode(t, recoveryState{Token: tablet, Cursor: partial.NextCursor,
		Checkpoint: full.NextCursor, FirstServerID: ack.ServerMsgID, Postmaster: postmaster}), 0o600))
	gateway.requireRedacted(t, phone, tablet, partial.NextCursor, full.NextCursor, fixtureKeyHex("old"), testDSN)
}

func resumeSyncRecovery(t *testing.T, f *fixture) {
	t.Helper()
	raw, err := os.ReadFile(recoveryFile)
	must(t, err)
	t.Cleanup(func() { _ = os.Remove(recoveryFile) })
	var saved recoveryState
	must(t, json.Unmarshal(raw, &saved))
	if saved.Token == "" || saved.Cursor == "" || saved.Checkpoint == "" || saved.FirstServerID == "" || saved.Postmaster.IsZero() {
		t.Fatal("sync HTTP restart fixture is incomplete")
	}
	var postmaster time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&postmaster))
	if !postmaster.After(saved.Postmaster) {
		t.Fatal("TLS recovery did not start a new PostgreSQL postmaster")
	}
	rotated := f.syncGateway(writeKeys(t, "new", "old", "new"), nil)
	overlap := f.syncGateway(writeKeys(t, "old", "old", "new"), nil)
	second := readPage(t, f, rotated, bootstrapPath, saved.Token, continueWith(saved.Cursor))
	expectItem(t, second, true, recoveryB, "upsert", 2, 0, "")
	third := readPage(t, f, overlap, bootstrapPath, saved.Token, continueWith(second.NextCursor))
	expectItem(t, third, false, recoveryC, "upsert", 3, 0, "")

	deltaFirst := readPage(t, f, rotated, deltaPath, saved.Token, map[string]any{"checkpoint": saved.Checkpoint, "limit": 1})
	expectItem(t, deltaFirst, true, recoveryB, "upsert", 4, 1, saved.FirstServerID)
	deltaSecond := readPage(t, f, overlap, deltaPath, saved.Token, continueWith(deltaFirst.NextCursor))
	expectItem(t, deltaSecond, true, recoveryD, "upsert", 5, 0, "")
	deltaThird := readPage(t, f, rotated, deltaPath, saved.Token, continueWith(deltaSecond.NextCursor))
	expectItem(t, deltaThird, false, recoveryA, "remove", 6, 0, "")
	quiet := readPage(t, f, overlap, deltaPath, saved.Token, checkpoint(deltaThird.NextCursor))
	if quiet.HasMore || len(quiet.Items) != 0 {
		t.Fatal("resumed checkpoint advanced twice")
	}

	newACK := send(t, f, rotated, saved.Token, recoveryC, "recovery_fresh_message", "committed after database restart")
	newDelta := readPage(t, f, overlap, deltaPath, saved.Token, checkpoint(quiet.NextCursor))
	expectItem(t, newDelta, false, recoveryC, "upsert", 7, 1, newACK.ServerMsgID)
	rotated.requireRedacted(t, saved.Token, saved.Cursor, saved.Checkpoint, fixtureKeyHex("new"), testDSN)
	overlap.requireRedacted(t, saved.Token, second.NextCursor, deltaFirst.NextCursor, fixtureKeyHex("old"), testDSN)
}
