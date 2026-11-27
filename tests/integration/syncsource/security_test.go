//go:build integration

package syncsource_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	app "github.com/m-ice/NewIM/server/sync/conversation"
)

func TestSyncSourceSecurity(t *testing.T) {
	t.Run("source-and-projection-rollback-together", testAtomicSource)
	t.Run("unpublished-membership-removal-and-readiness", testUnpublishedRemoval)
	t.Run("backfill-command-boundaries", testBackfillCLI)
	t.Run("bulk-backfill-crosses-ready-only-pages", testBulkBackfillDiscovery)
	t.Run("bulk-backfill-excludes-reset", testBulkBackfillResetInterlock)
}

func testAtomicSource(t *testing.T) {
	f := openFixture(t)
	loopback := f.server("1", nil)
	gateway := f.gateway("1", nil)
	const owner = "sync_atomic_owner"
	const peer = "sync_atomic_peer"
	provision(t, f, loopback, owner)
	provision(t, f, loopback, peer)
	ownerToken := f.admit(owner, "phone")
	reader := openSync(t, f, true)
	ownBefore := bootstrap(t, f, reader, owner, 0)
	peerBefore := bootstrap(t, f, reader, peer, 0)
	installCommitFailure(f)
	f.call(gateway, http.MethodPost, directPath, ownerToken, peerBody(t, peer), nil).
		requireError(t, http.StatusServiceUnavailable, "SERVER_TEMPORARY_UNAVAILABLE")
	const failedGroup = "sync_source_rejected_group"
	groupWire := encode(t, map[string]any{
		"groupId": failedGroup, "profile": map[string]string{"name": "Rejected fixture"}, "memberIds": []string{peer},
	})
	f.call(gateway, http.MethodPost, createGroupPath, ownerToken, groupWire, nil).
		requireError(t, http.StatusServiceUnavailable, "GROUP_STORAGE_UNAVAILABLE")
	if got := scalar(t, f, `SELECT (SELECT count(*) FROM newim.im_conversations WHERE conversation_id=$1)
		+ (SELECT count(*) FROM newim.im_groups WHERE group_id=$1)`, failedGroup); got != 0 {
		t.Fatal("failed projection commit retained managed group source state")
	}
	if got := scalar(t, f, "SELECT count(*) FROM newim.im_conversations WHERE direct_user_low=$1 OR direct_user_high=$1", owner); got != 0 {
		t.Fatal("failed projection commit retained the source conversation")
	}
	if got := scalar(t, f, "SELECT count(*) FROM newim.im_conversation_members WHERE user_id=$1 OR user_id=$2", owner, peer); got != 0 {
		t.Fatal("failed projection commit retained source membership")
	}
	for _, who := range []string{owner, peer} {
		if state := projectionState(t, f, who); state.head != 0 || !state.ready {
			t.Fatal("failed creation advanced a projection")
		}
	}
	removeCommitFailure(f)

	conversation := createDirect(t, f, gateway, ownerToken, peer)
	ownCreate := delta(t, f, reader, owner, ownBefore.NextCursor)
	peerCreate := delta(t, f, reader, peer, peerBefore.NextCursor)
	expectOnly(t, ownCreate, conversation, "upsert", 1, 0, "")
	expectOnly(t, peerCreate, conversation, "upsert", 1, 0, "")
	installCommitFailure(f)
	wire := textBody(t, "repair_after_rollback", conversation, "rolled back source text")
	f.call(loopback, http.MethodPost, sendPath, ownerToken, wire, nil).
		requireError(t, http.StatusServiceUnavailable, "SERVER_TEMPORARY_UNAVAILABLE")
	if got := scalar(t, f, "SELECT last_seq FROM newim.im_conversations WHERE conversation_id=$1", conversation); got != 0 {
		t.Fatal("failed projection commit allocated a source sequence")
	}
	if got := scalar(t, f, "SELECT count(*) FROM newim.im_messages WHERE conversation_id=$1", conversation); got != 0 {
		t.Fatal("failed projection commit retained a source message")
	}
	if got := scalar(t, f, "SELECT count(*) FROM newim.im_outbox_events e JOIN newim.im_messages m USING(server_msg_id) WHERE m.conversation_id=$1", conversation); got != 0 {
		t.Fatal("failed projection commit retained a source outbox event")
	}
	for _, who := range []string{owner, peer} {
		if state := projectionState(t, f, who); state.head != 1 || !state.ready {
			t.Fatal("rejected send changed a projection head")
		}
	}
	removeCommitFailure(f)
	repaired := expectACK(t, f.call(loopback, http.MethodPost, sendPath, ownerToken, wire, nil))
	expectOnly(t, delta(t, f, reader, owner, ownCreate.NextCursor), conversation, "upsert", 2, 1, repaired.ServerMsgID)
	expectOnly(t, delta(t, f, reader, peer, peerCreate.NextCursor), conversation, "upsert", 2, 1, repaired.ServerMsgID)
	loopback.requireRedacted(t, ownerToken, testDSN, "rolled back source text", "sync_fixture_projection_guard")
	gateway.requireRedacted(t, ownerToken, testDSN, "sync_fixture_projection_guard")
}

func testUnpublishedRemoval(t *testing.T) {
	f := openFixture(t)
	legacy := f.gateway("", nil)
	current := f.gateway("1", nil)
	provisioner := f.server("1", nil)
	const owner = "sync_unpublished_owner"
	const member = "sync_unpublished_member"
	const conversation = "sync_unpublished_group"
	f.user(owner)
	f.user(member)
	token := f.admit(owner, "phone")
	peer := f.admit(member, "phone")
	createGroup(t, f, legacy, token, conversation, []string{member}, true)
	provision(t, f, provisioner, member)
	client := openSync(t, f, true)
	_, err := client.BeginBootstrap(f.ctx, app.Principal{UserID: member}, 0)
	requireSyncError(t, err, app.NotReady)
	leave(t, f, current, peer, conversation)
	if got := scalar(t, f, "SELECT count(*) FROM newim.im_conversation_sync_keys WHERE user_id=$1 AND conversation_id=$2", member, conversation); got != 0 {
		t.Fatal("departing before a conversation was projected manufactured a tombstone key")
	}
	if got := scalar(t, f, "SELECT count(*) FROM newim.im_conversation_sync_changes WHERE user_id=$1 AND conversation_id=$2", member, conversation); got != 0 {
		t.Fatal("departing before a conversation was projected manufactured an event")
	}
	runCLI(t, f, 0, "SYNC_SOURCE_READY", "step", "--user", member, "--limit", "1")
	if page := bootstrap(t, f, client, member, 0); len(page.Items) != 0 {
		t.Fatal("completed backfill exposed an unauthorized, previously unpublished conversation")
	}
}

func testBackfillCLI(t *testing.T) {
	f := openFixture(t)
	const user = "sync_cli_private_sample"
	f.user(user)
	for _, args := range [][]string{
		{"unknown", "--user", user}, {"step", "--user", user, "--limit", "0"},
		{"step", "--user", user, "--limit", "101"}, {"step", "--limit", "1"},
		{"reset", "--user", user, "--limit", "1"}, {"run", "--user", user, "--limit", "1"},
	} {
		runCLI(t, f, 2, "SYNC_INVALID_ARGUMENT", args...)
	}
	result := invokeCLI(f.ctx, map[string]string{"NEWIM_AUTH_ALLOW_LOCAL_SOCKET": ""}, "step", "--user", user, "--limit", "1")
	requireCLI(t, result, 1, "SYNC_STORAGE_UNAVAILABLE")
	if got := scalar(t, f, "SELECT count(*) FROM newim.im_conversation_sync_accounts WHERE user_id=$1", user); got != 0 {
		t.Fatal("rejected operator command advanced account state")
	}
}

func installCommitFailure(f *fixture) {
	f.t.Helper()
	f.sql("CREATE TABLE newim.sync_fixture_allowed_conversations(conversation_id newim.identifier PRIMARY KEY)")
	f.sql(`ALTER TABLE newim.im_conversation_sync_changes ADD CONSTRAINT sync_fixture_projection_guard
		FOREIGN KEY (conversation_id) REFERENCES newim.sync_fixture_allowed_conversations(conversation_id)
		DEFERRABLE INITIALLY DEFERRED NOT VALID`)
}

func removeCommitFailure(f *fixture) {
	f.t.Helper()
	f.sql("ALTER TABLE newim.im_conversation_sync_changes DROP CONSTRAINT sync_fixture_projection_guard")
	f.sql("DROP TABLE newim.sync_fixture_allowed_conversations")
}

type commandResult struct {
	output   string
	exitCode int
	err      error
}

func invokeCLI(parent context.Context, overrides map[string]string, args ...string) commandResult {
	path := os.Getenv("NEWIM_SYNC_BACKFILL_BINARY")
	if !filepath.IsAbs(path) {
		return commandResult{err: errors.New("fixture requires an absolute backfill binary path")}
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	env := map[string]string{"NEWIM_AUTH_DSN": testDSN, "NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1"}
	for key, value := range overrides {
		env[key] = value
	}
	cmd.Env = os.Environ()
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	data, err := cmd.CombinedOutput()
	result := commandResult{output: string(data), err: err}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		result.exitCode = exit.ExitCode()
		result.err = nil
	}
	return result
}

func runCLI(t *testing.T, f *fixture, status int, line string, args ...string) {
	t.Helper()
	requireCLI(t, invokeCLI(f.ctx, nil, args...), status, line)
}

func requireCLI(t *testing.T, result commandResult, code int, line string) {
	t.Helper()
	if result.err != nil || result.exitCode != code || result.output != line+"\n" {
		t.Fatalf("backfill CLI exit=%d output bytes=%d err=%v, want %d and %s", result.exitCode, len(result.output), result.err, code, line)
	}
}
