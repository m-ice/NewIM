//go:build integration

package conversationsynchttp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	store "github.com/m-ice/NewIM/server/storage/conversationsync"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

func (f *fixture) readSyncGateway(keys string) *process {
	f.t.Helper()
	return f.syncGateway(keys, map[string]string{"NEWIM_CONVERSATION_READ_SYNC": "1", "NEWIM_READ_WATERMARK_HTTP": "1"})
}

func (f *fixture) readSyncServer() *process {
	f.t.Helper()
	return f.server("1", map[string]string{"NEWIM_CONVERSATION_READ_SYNC": "1"})
}

type readSyncPage struct {
	app.Page
	fields []map[string]json.RawMessage
}

func readSync(t *testing.T, f *fixture, gateway *process, path, token string, body any) readSyncPage {
	t.Helper()
	reply := f.call(gateway, http.MethodPost, path, token, encode(t, body), nil)
	page := requirePage(t, reply)
	var wire struct{ Items []map[string]json.RawMessage }
	must(t, json.Unmarshal(reply.body, &wire))
	return readSyncPage{Page: page, fields: wire.Items}
}

func expectReadSyncSeq(t *testing.T, page readSyncPage, index int, want string) {
	t.Helper()
	if index >= len(page.fields) {
		t.Fatal("projected read sequence did not have a corresponding sync item")
	}
	raw := page.fields[index]["lastReadSeq"]
	if want == "" {
		if raw != nil {
			t.Fatal("sync item serialized a read field where it must be absent")
		}
		return
	}
	if string(raw) != strconv.Quote(want) {
		t.Fatalf("lastReadSeq encoded as %s, expected string %q", raw, want)
	}
}

func expectReadSyncItem(t *testing.T, page readSyncPage, more bool, id, kind string, revision, latest uint64, msg, read string) {
	t.Helper()
	expectItem(t, page.Page, more, id, kind, revision, latest, msg)
	expectReadSyncSeq(t, page, 0, read)
}

func openSyncDirect(t *testing.T, f *fixture, gateway *process, owner, peerID string) string {
	t.Helper()
	reply := f.call(gateway, http.MethodPost, "/api/v1/conversations/direct", owner,
		encode(t, map[string]string{"peerUserId": peerID}), nil)
	requireStatus(t, reply, http.StatusOK)
	if !reply.verified {
		t.Fatal("direct conversation fixture did not use verified TLS")
	}
	var fields map[string]string
	must(t, json.Unmarshal(reply.body, &fields))
	if len(fields) != 1 || fields["conversationId"] == "" {
		t.Fatal("direct open did not return a committed conversation")
	}
	return fields["conversationId"]
}

func TestReadSyncHTTPCheck(t *testing.T) {
	t.Run("account-scoped-advancement-and-original-format", testReadSyncDevices)
	t.Run("paginated-snapshots-retain-revision-values", testReadSyncFence)
	t.Run("initialize-existing-empty-account", testReadSyncInitializeEmpty)
}

func testReadSyncInitializeEmpty(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(17)
	const userID = "readsync_initialize_empty"
	legacyServer := f.server("1", nil)
	provision(t, f, legacyServer, userID)
	before := readProjectionProgress(t, f, userID)
	if before.ReadEpoch != nil || before.Epoch != 1 || before.LastChange != 0 {
		t.Fatal("legacy provisioning did not create an empty projection")
	}

	repo, err := store.Open(f.ctx, store.Config{
		DSN: testDSN, AllowLocalSocket: true, RequireSourceReady: true, IncludeReadState: true,
	})
	must(t, err)
	defer repo.Close()
	must(t, repo.InitializeEmptyAccount(f.ctx, userID))
	must(t, repo.InitializeEmptyAccount(f.ctx, userID))
	after := readProjectionProgress(t, f, userID)
	if !after.Ready || after.ReadEpoch == nil || *after.ReadEpoch != before.Epoch ||
		after.Epoch != before.Epoch || after.LastChange != before.LastChange {
		t.Fatal("initializing an existing empty account changed its epoch or left it unready")
	}
}

func testReadSyncDevices(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(17)
	server := f.readSyncServer()
	keys := writeKeys(t, "old", "old")
	left, right := f.readSyncGateway(keys), f.readSyncGateway(keys)
	off := f.syncGateway(keys, nil)
	const ownerID, readerID = "readsync_devices_sender", "readsync_devices_reader"
	provision(t, f, server, ownerID)
	provision(t, f, server, readerID)
	owner := f.admit(ownerID, "phone")
	phone, tablet := f.admit(readerID, "phone"), f.admit(readerID, "tablet")
	empty := readSync(t, f, left, bootstrapPath, phone, map[string]any{})
	if empty.HasMore || len(empty.Items) != 0 {
		t.Fatal("read-enabled provisioning did not create an empty ready account")
	}
	id := openSyncDirect(t, f, left, owner, readerID)
	send(t, f, server, owner, id, "readsync_direct_one", "first message")
	last := send(t, f, left, owner, id, "readsync_direct_two", "second message")
	reader := readSync(t, f, right, bootstrapPath, phone, map[string]any{})
	expectReadSyncItem(t, reader, false, id, "upsert", 3, 2, last.ServerMsgID, "0")
	author := readSync(t, f, left, bootstrapPath, owner, map[string]any{})
	expectReadSyncItem(t, author, false, id, "upsert", 3, 2, last.ServerMsgID, "0")

	marked := advanceWatermark(t, f, left, phone, id, "1")
	requireWatermark(t, marked, id, "1", "0", "2", true)
	requireSameWatermarkTime(t, marked, getWatermark(t, f, right, tablet, id))
	updated := readSync(t, f, right, deltaPath, tablet, checkpoint(reader.NextCursor))
	expectReadSyncItem(t, updated, false, id, "upsert", 4, 2, last.ServerMsgID, "1")
	quietAuthor := readSync(t, f, right, deltaPath, owner, checkpoint(author.NextCursor))
	if quietAuthor.HasMore || len(quietAuthor.Items) != 0 {
		t.Fatal("another account received the reader's watermark revision")
	}
	for _, seq := range []string{"1", "0"} {
		stale := advanceWatermark(t, f, right, tablet, id, seq)
		requireWatermark(t, stale, id, "1", "0", "2", true)
		requireSameWatermarkTime(t, marked, stale)
	}
	quietReader := readSync(t, f, left, deltaPath, phone, checkpoint(updated.NextCursor))
	if quietReader.HasMore || len(quietReader.Items) != 0 {
		t.Fatal("equal or stale watermark reports appended a revision")
	}

	third := send(t, f, server, owner, id, "readsync_direct_three", "third message")
	delta := readSync(t, f, right, deltaPath, tablet, checkpoint(quietReader.NextCursor))
	expectReadSyncItem(t, delta, false, id, "upsert", 5, 3, third.ServerMsgID, "1")
	compat := readSync(t, f, off, bootstrapPath, tablet, map[string]any{})
	expectReadSyncItem(t, compat, false, id, "upsert", 5, 3, third.ServerMsgID, "")
	var persistedRead, projectedRead int64
	must(t, f.db.QueryRow(f.ctx, `SELECT r.read_seq,c.last_read_seq
		FROM newim.im_read_states r JOIN newim.im_conversation_sync_changes c USING(user_id,conversation_id)
		WHERE r.conversation_id=$1 AND r.user_id=$2 AND c.change_seq=5`, id, readerID).Scan(&persistedRead, &projectedRead))
	if persistedRead != 1 || projectedRead != 1 {
		t.Fatal("authoritative and immutable projected read values differ")
	}
	left.requireRedacted(t, phone, tablet, owner, reader.NextCursor, third.ServerMsgID, testDSN, "third message")
}

func testReadSyncFence(t *testing.T) {
	f := openFixture(t)
	server := f.readSyncServer()
	keys := writeKeys(t, "old", "old")
	left, right := f.readSyncGateway(keys), f.readSyncGateway(keys)
	const owner = "readsync_fence_owner"
	const a, b = "readsync_fence_a", "readsync_fence_b"
	provision(t, f, server, owner)
	phone, tablet := f.admit(owner, "phone"), f.admit(owner, "tablet")
	createGroup(t, f, left, phone, a, nil, true)
	createGroup(t, f, left, phone, b, nil, true)
	one := send(t, f, left, phone, a, "fence_a_one", "first group message")
	send(t, f, right, phone, b, "fence_b_one", "second group's first message")
	last := send(t, f, right, phone, b, "fence_b_two", "second group's next message")

	first := readSync(t, f, left, bootstrapPath, phone, map[string]int{"limit": 1})
	expectReadSyncItem(t, first, true, a, "upsert", 3, 1, one.ServerMsgID, "0")
	advanceWatermark(t, f, right, tablet, b, "1")
	second := readSync(t, f, right, bootstrapPath, tablet, continueWith(first.NextCursor))
	expectReadSyncItem(t, second, false, b, "upsert", 5, 2, last.ServerMsgID, "0")

	advanceWatermark(t, f, left, phone, a, "1")
	delta := readSync(t, f, right, deltaPath, tablet, map[string]any{"checkpoint": second.NextCursor, "limit": 1})
	expectReadSyncItem(t, delta, true, b, "upsert", 6, 2, last.ServerMsgID, "1")
	advanceWatermark(t, f, left, phone, b, "2")
	next := readSync(t, f, left, deltaPath, phone, continueWith(delta.NextCursor))
	expectReadSyncItem(t, next, false, a, "upsert", 7, 1, one.ServerMsgID, "1")
	final := readSync(t, f, right, deltaPath, tablet, checkpoint(next.NextCursor))
	expectReadSyncItem(t, final, false, b, "upsert", 8, 2, last.ServerMsgID, "2")
}

func TestReadSyncHTTPSecurity(t *testing.T) {
	t.Run("history-floor-removal-and-rejoin", testReadSyncHistory)
	t.Run("failed-commit-preserves-watermark-and-revision", testReadSyncRollback)
}

func testReadSyncHistory(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(17)
	server := f.readSyncServer()
	keys := writeKeys(t, "old", "old")
	left, right := f.readSyncGateway(keys), f.readSyncGateway(keys)
	const owner, member, stranger = "readsync_history_host", "readsync_history_member", "readsync_history_other"
	for _, user := range []string{owner, member, stranger} {
		provision(t, f, server, user)
	}
	host := f.admit(owner, "phone")
	peer := f.admit(member, "phone")
	unrelated := f.admit(stranger, "phone")
	empty := readSync(t, f, right, bootstrapPath, peer, map[string]any{})
	const group = "readsync_restricted_group"
	createGroup(t, f, left, host, group, nil, false)
	send(t, f, left, host, group, "readsync_history_hidden", "hidden before join")
	join(t, f, left, peer, group, "readsync_first_join")
	joined := readSync(t, f, right, deltaPath, peer, checkpoint(empty.NextCursor))
	expectReadSyncItem(t, joined, false, group, "upsert", 1, 0, "", "1")
	visible := send(t, f, left, host, group, "readsync_history_visible", "visible while a member")
	visiblePage := readSync(t, f, right, deltaPath, peer, checkpoint(joined.NextCursor))
	expectReadSyncItem(t, visiblePage, false, group, "upsert", 2, 2, visible.ServerMsgID, "1")
	advanceWatermark(t, f, right, peer, group, "2")
	marked := readSync(t, f, left, deltaPath, peer, checkpoint(visiblePage.NextCursor))
	expectReadSyncItem(t, marked, false, group, "upsert", 3, 2, visible.ServerMsgID, "2")

	request := encode(t, checkpoint(joined.NextCursor))
	requireSyncError(t, f.call(left, http.MethodPost, deltaPath, unrelated, request, nil), 400, "SYNC_INVALID_CURSOR")
	other := readSync(t, f, right, bootstrapPath, unrelated, map[string]any{})
	if len(other.Items) != 0 || other.HasMore {
		t.Fatal("unrelated account received a restricted group")
	}

	leave(t, f, left, peer, group)
	removed := readSync(t, f, right, deltaPath, peer, checkpoint(marked.NextCursor))
	expectReadSyncItem(t, removed, false, group, "remove", 4, 0, "", "")
	send(t, f, left, host, group, "readsync_history_gap", "hidden while departed")
	join(t, f, right, peer, group, "readsync_second_join")
	rejoined := readSync(t, f, left, deltaPath, peer, checkpoint(removed.NextCursor))
	expectReadSyncItem(t, rejoined, false, group, "upsert", 5, 0, "", "3")
	requireWatermark(t, getWatermark(t, f, right, peer, group), group, "3", "3", "3", false)
	requireSyncError(t, f.call(right, http.MethodPost, deltaPath, peer, request, nil), 409, "SYNC_CURSOR_EXPIRED")
}

func testReadSyncRollback(t *testing.T) {
	f := openFixture(t)
	server := f.readSyncServer()
	keys := writeKeys(t, "old", "old")
	left, right := f.readSyncGateway(keys), f.readSyncGateway(keys)
	const owner, peerID = "readsync_rollback_owner", "readsync_rollback_peer"
	provision(t, f, server, owner)
	provision(t, f, server, peerID)
	host, peer := f.admit(owner, "phone"), f.admit(peerID, "phone")
	id := openSyncDirect(t, f, left, host, peerID)
	msg := send(t, f, left, host, id, "readsync_rollback_message", "durable message")
	before := readSync(t, f, right, bootstrapPath, peer, map[string]any{})
	expectReadSyncItem(t, before, false, id, "upsert", 2, 1, msg.ServerMsgID, "0")

	f.sql(`CREATE FUNCTION newim.fixture_read_sync_commit_failure() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'fixture refused read projection commit';
		END;
		$$;
		CREATE CONSTRAINT TRIGGER fixture_read_sync_commit_failure
		AFTER INSERT ON newim.im_conversation_sync_changes DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW WHEN (NEW.last_read_seq=1)
		EXECUTE FUNCTION newim.fixture_read_sync_commit_failure();`)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := f.db.Exec(ctx, `DROP TRIGGER IF EXISTS fixture_read_sync_commit_failure ON newim.im_conversation_sync_changes;
			DROP FUNCTION IF EXISTS newim.fixture_read_sync_commit_failure()`)
		if err != nil {
			t.Error("could not remove the read projection fixture")
		}
	}()

	requireSyncError(t, f.call(left, http.MethodPost, watermarkPath(id), peer, watermarkBody(t, "1"), nil),
		503, "READ_STORAGE_UNAVAILABLE")
	requireWatermark(t, getWatermark(t, f, right, peer, id), id, "0", "0", "1", false)
	var rows int
	must(t, f.db.QueryRow(f.ctx, "SELECT count(*) FROM newim.im_read_states WHERE conversation_id=$1 AND user_id=$2", id, peerID).Scan(&rows))
	if rows != 0 {
		t.Fatal("failed projection commit left a durable watermark row")
	}
	quiet := readSync(t, f, right, deltaPath, peer, checkpoint(before.NextCursor))
	if quiet.HasMore || len(quiet.Items) != 0 {
		t.Fatal("failed projection commit exposed a revision")
	}
	f.sql("DROP TRIGGER fixture_read_sync_commit_failure ON newim.im_conversation_sync_changes")
	advanceWatermark(t, f, left, peer, id, "1")
	after := readSync(t, f, right, deltaPath, peer, checkpoint(quiet.NextCursor))
	expectReadSyncItem(t, after, false, id, "upsert", 3, 1, msg.ServerMsgID, "1")
	left.requireRedacted(t, peer, testDSN, "fixture refused read projection commit")
}

const (
	readSyncRecoveryFile    = "/tmp/newim-read-sync-recovery.json"
	readSyncOldUser         = "readsync_recovery_legacy"
	readSyncPartialUser     = "readsync_recovery_old_partial"
	readSyncCurrentUser     = "readsync_recovery_new"
	readSyncRebuildA        = "readsync_rebuild_a"
	readSyncRebuildB        = "readsync_rebuild_b"
	readSyncRebuildC        = "readsync_rebuild_c"
	readSyncNewConversation = "readsync_preserved_cursor"
)

type readSyncProgress struct {
	Epoch      int64
	LastChange int64
	ReadEpoch  *int64
	After      string
	Ready      bool
}

func readProjectionProgress(t *testing.T, f *fixture, user string) readSyncProgress {
	t.Helper()
	var p readSyncProgress
	must(t, f.db.QueryRow(f.ctx, `SELECT epoch,last_change_seq,read_state_epoch,COALESCE(source_after_key::text,''),source_ready
		FROM newim.im_conversation_sync_accounts WHERE user_id=$1`, user).
		Scan(&p.Epoch, &p.LastChange, &p.ReadEpoch, &p.After, &p.Ready))
	return p
}

func runReadSyncBackfill(t *testing.T, f *fixture, enabled, want string, args ...string) {
	t.Helper()
	path := os.Getenv("NEWIM_SYNC_BACKFILL_BINARY")
	if !filepath.IsAbs(path) {
		t.Fatal("absolute fixture backfill binary required")
	}
	ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = append(os.Environ(), "NEWIM_AUTH_DSN="+testDSN, "NEWIM_AUTH_ALLOW_LOCAL_SOCKET=1",
		"NEWIM_CONVERSATION_PROJECTION=1", "NEWIM_CONVERSATION_READ_SYNC="+enabled)
	output, err := cmd.CombinedOutput()
	if err != nil || string(output) != want+"\n" {
		t.Fatalf("owned read-sync backfill %s did not return %s: err=%v output=%q", args[0], want, err, output)
	}
}

type readSyncRecovery struct {
	OldToken       string
	OldCheckpoint  string
	OldHead        int64
	Prepared       readSyncProgress
	Partial        readSyncProgress
	CurrentToken   string
	CurrentCursor  string
	CurrentMessage string
	Postmaster     time.Time
}

func TestReadSyncHTTPRecovery(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(17)
	switch os.Getenv("NEWIM_SYNC_HTTP_PHASE") {
	case "prepare":
		prepareReadSyncRestart(t, f)
	case "restart":
		verifyReadSyncRestart(t, f)
	default:
		t.Fatal("NEWIM_SYNC_HTTP_PHASE must be prepare or restart")
	}
}

func prepareReadSyncRestart(t *testing.T, f *fixture) {
	t.Helper()
	keys := writeKeys(t, "old", "old")
	oldServer := f.server("1", nil)
	old := f.syncGateway(keys, map[string]string{"NEWIM_READ_WATERMARK_HTTP": "1"})
	provision(t, f, oldServer, readSyncOldUser)
	provision(t, f, oldServer, readSyncPartialUser)
	token := f.admit(readSyncOldUser, "phone")
	for _, id := range []string{readSyncRebuildA, readSyncRebuildB, readSyncRebuildC} {
		createGroup(t, f, old, token, id, []string{readSyncPartialUser}, true)
	}
	send(t, f, old, token, readSyncRebuildA, "rebuild_first", "legacy author's earlier message")
	send(t, f, old, token, readSyncRebuildA, "rebuild_second", "legacy author's latest message")
	requireWatermark(t, advanceWatermark(t, f, old, token, readSyncRebuildA, "2"), readSyncRebuildA, "2", "0", "2", true)
	f.sql("INSERT INTO newim.im_read_states(conversation_id,user_id,read_seq) VALUES($1,$2,0)", readSyncRebuildB, readSyncOldUser)
	legacyPage := readSync(t, f, old, bootstrapPath, token, map[string]any{})
	if len(legacyPage.Items) != 3 || legacyPage.HasMore {
		t.Fatal("legacy fixture did not establish the complete directory")
	}
	for i := range legacyPage.Items {
		expectReadSyncSeq(t, legacyPage, i, "")
	}
	before := readProjectionProgress(t, f, readSyncOldUser)
	if !before.Ready || before.ReadEpoch != nil {
		t.Fatal("old ready account unexpectedly included a read generation marker")
	}

	runReadSyncBackfill(t, f, "0", "SYNC_SOURCE_RESET", "reset", "--user", readSyncPartialUser)
	runReadSyncBackfill(t, f, "0", "SYNC_SOURCE_PENDING", "step", "--user", readSyncPartialUser, "--limit", "1")
	oldPartial := readProjectionProgress(t, f, readSyncPartialUser)
	if oldPartial.Ready || oldPartial.After != readSyncRebuildA || oldPartial.ReadEpoch != nil {
		t.Fatal("legacy partial fixture did not commit its first directory key")
	}

	gateway := f.readSyncGateway(keys)
	requireSyncError(t, f.call(gateway, http.MethodPost, bootstrapPath, token, []byte("{}"), nil), 503, "SYNC_NOT_READY")
	runReadSyncBackfill(t, f, "1", "SYNC_SOURCE_PENDING", "step", "--user", readSyncOldUser, "--limit", "1")
	prepared := readProjectionProgress(t, f, readSyncOldUser)
	requireReadSyncProgress(t, prepared, before.Epoch+1, readSyncRebuildA, false)
	runReadSyncBackfill(t, f, "1", "SYNC_SOURCE_PENDING", "step", "--user", readSyncPartialUser, "--limit", "1")
	partial := readProjectionProgress(t, f, readSyncPartialUser)
	requireReadSyncProgress(t, partial, oldPartial.Epoch+1, readSyncRebuildA, false)
	requireSyncError(t, f.call(gateway, http.MethodPost, bootstrapPath, token, []byte("{}"), nil), 503, "SYNC_NOT_READY")

	server := f.readSyncServer()
	provision(t, f, server, readSyncCurrentUser)
	current := f.admit(readSyncCurrentUser, "tablet")
	createGroup(t, f, gateway, current, readSyncNewConversation, nil, true)
	last := send(t, f, gateway, current, readSyncNewConversation, "rebuild_current", "message for current epoch")
	cursor := readSync(t, f, gateway, bootstrapPath, current, map[string]any{})
	expectReadSyncItem(t, cursor, false, readSyncNewConversation, "upsert", 2, 1, last.ServerMsgID, "0")
	advanceWatermark(t, f, gateway, current, readSyncNewConversation, "1")

	saved := readSyncRecovery{OldToken: token, OldCheckpoint: legacyPage.NextCursor, OldHead: before.LastChange,
		Prepared: prepared, Partial: partial, CurrentToken: current, CurrentCursor: cursor.NextCursor,
		CurrentMessage: last.ServerMsgID, Postmaster: watermarkPostmaster(t, f)}
	must(t, os.WriteFile(readSyncRecoveryFile, encode(t, saved), 0o600))
}

func verifyReadSyncRestart(t *testing.T, f *fixture) {
	t.Helper()
	raw, err := os.ReadFile(readSyncRecoveryFile)
	must(t, err)
	t.Cleanup(func() { _ = os.Remove(readSyncRecoveryFile) })
	var saved readSyncRecovery
	must(t, json.Unmarshal(raw, &saved))
	if saved.Postmaster.IsZero() || !watermarkPostmaster(t, f).After(saved.Postmaster) {
		t.Fatal("read sync recovery did not start a new PostgreSQL postmaster")
	}
	keys := writeKeys(t, "old", "old")
	left, right := f.readSyncGateway(keys), f.readSyncGateway(keys)
	baseline := readProjectionProgress(t, f, readSyncOldUser)
	requireReadSyncProgress(t, baseline, saved.Prepared.Epoch, readSyncRebuildA, false)
	if baseline.LastChange != saved.Prepared.LastChange {
		t.Fatal("restart lost the committed backfill revision")
	}
	partial := readProjectionProgress(t, f, readSyncPartialUser)
	requireReadSyncProgress(t, partial, saved.Partial.Epoch, readSyncRebuildA, false)
	if partial.LastChange != saved.Partial.LastChange {
		t.Fatal("restart lost the prepared legacy partial account")
	}

	requireSyncError(t, f.call(left, http.MethodPost, bootstrapPath, saved.OldToken, []byte("{}"), nil), 503, "SYNC_NOT_READY")
	delta := readSync(t, f, right, deltaPath, saved.CurrentToken, checkpoint(saved.CurrentCursor))
	expectReadSyncItem(t, delta, false, readSyncNewConversation, "upsert", 3, 1, saved.CurrentMessage, "1")
	runReadSyncBackfill(t, f, "1", "SYNC_SOURCE_PENDING", "step", "--user", readSyncOldUser, "--limit", "1")
	requireReadSyncProgress(t, readProjectionProgress(t, f, readSyncOldUser), saved.Prepared.Epoch, readSyncRebuildB, false)
	runReadSyncBackfill(t, f, "1", "SYNC_SOURCE_COMPLETE", "run", "--limit", "2")
	requireReadSyncProgress(t, readProjectionProgress(t, f, readSyncOldUser), saved.Prepared.Epoch, "", true)
	requireReadSyncProgress(t, readProjectionProgress(t, f, readSyncPartialUser), saved.Partial.Epoch, "", true)
	runReadSyncBackfill(t, f, "1", "SYNC_SOURCE_COMPLETE", "run", "--limit", "2")
	if p := readProjectionProgress(t, f, readSyncOldUser); p.LastChange != saved.Prepared.LastChange+2 {
		t.Fatal("resumed or repeated backfill did not publish exactly the remaining legacy revisions")
	}
	requireSyncError(t, f.call(left, http.MethodPost, deltaPath, saved.OldToken, encode(t, checkpoint(saved.OldCheckpoint)), nil),
		409, "SYNC_CURSOR_EXPIRED")

	rebuilt := readSync(t, f, right, bootstrapPath, saved.OldToken, map[string]any{})
	if len(rebuilt.Items) != 3 || rebuilt.HasMore {
		t.Fatal("resumed backfill did not publish the complete directory")
	}
	for i, want := range []struct{ id, seq string }{{readSyncRebuildA, "2"}, {readSyncRebuildB, "0"}, {readSyncRebuildC, "0"}} {
		if rebuilt.Items[i].ConversationID != want.id {
			t.Fatal("resumed read directory lost an expected conversation")
		}
		expectReadSyncSeq(t, rebuilt, i, want.seq)
	}
	requireWatermark(t, getWatermark(t, f, left, saved.OldToken, readSyncRebuildA), readSyncRebuildA, "2", "0", "2", true)
	var changedOldRows int
	must(t, f.db.QueryRow(f.ctx, `SELECT count(*) FROM newim.im_conversation_sync_changes
		WHERE user_id=$1 AND change_seq<=$2 AND last_read_seq IS NOT NULL`, readSyncOldUser, saved.OldHead).Scan(&changedOldRows))
	if changedOldRows != 0 {
		t.Fatal("backfill modified immutable pre-feature revisions")
	}
	left.requireRedacted(t, saved.OldToken, saved.CurrentToken, saved.OldCheckpoint, saved.CurrentCursor, testDSN)
}

func requireReadSyncProgress(t *testing.T, got readSyncProgress, epoch int64, after string, ready bool) {
	t.Helper()
	if got.Epoch != epoch || got.ReadEpoch == nil || *got.ReadEpoch != epoch || got.After != after || got.Ready != ready {
		t.Fatalf("read backfill epoch=%d marker=%v next key=%q ready=%t; expected epoch=%d next=%q ready=%t",
			got.Epoch, got.ReadEpoch != nil, got.After, got.Ready, epoch, after, ready)
	}
}

func TestReadSyncHTTPWide(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(17)
	const owner = "readsync_wide_owner"
	f.user(owner)
	token := f.admit(owner, "phone")
	const count = 205
	ids := make([]string, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("readsync_wide_%04d", i)
	}
	f.sql("INSERT INTO newim.im_conversations(conversation_id,kind) SELECT id,'group' FROM unnest($1::text[]) AS ids(id)", ids)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) SELECT id,$2 FROM unnest($1::text[]) AS ids(id)", ids, owner)
	f.sql(`SELECT newim.persist_message($2,id,id,'m_'||id,'text',1,$3,
		convert_to('{"text":"wide directory fixture"}','UTF8'),'evt_'||id)
		FROM unnest($1::text[]) AS ids(id)`, ids, owner, time.Now().UnixMilli())
	f.sql(`INSERT INTO newim.im_read_states(conversation_id,user_id,read_seq)
		SELECT id,$2,1 FROM unnest($1::text[]) WITH ORDINALITY AS ids(id,n) WHERE n % 2 = 1`, ids, owner)
	runReadSyncBackfill(t, f, "1", "SYNC_SOURCE_COMPLETE", "run", "--limit", "40")
	gateway := f.readSyncGateway(writeKeys(t, "old", "old"))

	var request any = map[string]int{"limit": app.MaxPageItems}
	for start := 0; start < len(ids); start += app.MaxPageItems {
		page := readSync(t, f, gateway, bootstrapPath, token, request)
		end := min(len(ids), start+app.MaxPageItems)
		if len(page.Items) != end-start || page.HasMore != (end < len(ids)) {
			t.Fatal("large read directory did not respect its bounded continuation")
		}
		for i, row := range page.Items {
			index := start + i
			if row.ConversationID != ids[index] {
				t.Fatal("read directory skipped or repeated a member key")
			}
			expectReadSyncSeq(t, page, i, strconv.Itoa(1-index%2))
		}
		request = continueWith(page.NextCursor)
	}
}
