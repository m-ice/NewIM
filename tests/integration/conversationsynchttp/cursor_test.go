//go:build integration

package conversationsynchttp_test

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	app "github.com/m-ice/NewIM/server/sync/conversation"
)

func testSyncReadinessAndHistory(t *testing.T) {
	f := openFixture(t)
	loopback := f.server("1", nil)
	gateway := f.syncGateway(writeKeys(t, "old", "old"), nil)
	const owner = "synchttp_history_owner"
	const member = "synchttp_history_late"
	provision(t, f, loopback, owner)
	f.user(member)
	host := f.admit(owner, "phone")
	client := f.admit(member, "tablet")
	requireSyncError(t, f.call(gateway, http.MethodPost, bootstrapPath, client, []byte("{}"), nil), 503, "SYNC_NOT_READY")
	const legacy = "synchttp_history_legacy"
	createGroup(t, f, gateway, host, legacy, []string{member}, true)
	requireSyncError(t, f.call(gateway, http.MethodPost, bootstrapPath, client, []byte("{}"), nil), 503, "SYNC_NOT_READY")
	makeReady(t, f, member)
	ready := readPage(t, f, gateway, bootstrapPath, client, map[string]any{})
	expectItem(t, ready, false, legacy, "upsert", 1, 0, "")

	const restricted = "synchttp_history_restricted"
	createGroup(t, f, gateway, host, restricted, nil, false)
	prior := send(t, f, gateway, host, restricted, "older_hidden", "before initial join")
	if prior.ConversationSeq != "1" {
		t.Fatal("did not establish an older hidden message")
	}
	join(t, f, gateway, client, restricted, "first_join")
	joined := readPage(t, f, gateway, deltaPath, client, checkpoint(ready.NextCursor))
	expectItem(t, joined, false, restricted, "upsert", 2, 0, "")
	visible := send(t, f, gateway, host, restricted, "first_visible", "while the member belonged")
	first := readPage(t, f, gateway, deltaPath, client, checkpoint(joined.NextCursor))
	expectItem(t, first, false, restricted, "upsert", 3, 2, visible.ServerMsgID)
	leave(t, f, gateway, client, restricted)
	removed := readPage(t, f, gateway, deltaPath, client, checkpoint(first.NextCursor))
	expectItem(t, removed, false, restricted, "remove", 4, 0, "")
	send(t, f, gateway, host, restricted, "hidden_gap", "message from before membership resumes")
	join(t, f, gateway, client, restricted, "next_join")
	rejoined := readPage(t, f, gateway, deltaPath, client, checkpoint(removed.NextCursor))
	expectItem(t, rejoined, false, restricted, "upsert", 5, 0, "")
	reply := f.call(gateway, http.MethodPost, deltaPath, client, encode(t, checkpoint(joined.NextCursor)), nil)
	requireSyncError(t, reply, 409, "SYNC_CURSOR_EXPIRED")
	gateway.requireRedacted(t, client, host, joined.NextCursor, visible.ServerMsgID, prior.ServerMsgID, testDSN)
}

func checkpoint(cursor string) map[string]string { return map[string]string{"checkpoint": cursor} }

func makeReady(t *testing.T, f *fixture, user string) {
	t.Helper()
	path := os.Getenv("NEWIM_SYNC_BACKFILL_BINARY")
	if !filepath.IsAbs(path) {
		t.Fatal("absolute fixture backfill binary required")
	}
	ctx, cancel := context.WithTimeout(f.ctx, 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, "step", "--user", user, "--limit", "10")
	cmd.Env = append(os.Environ(), "NEWIM_AUTH_DSN="+testDSN, "NEWIM_AUTH_ALLOW_LOCAL_SOCKET=1")
	output, err := cmd.CombinedOutput()
	if err != nil || string(output) != "SYNC_SOURCE_READY\n" {
		t.Fatalf("owned backfill did not complete its account: error=%v output length=%d", err, len(output))
	}
}

func testSyncExpiryAndRotation(t *testing.T) {
	f := openFixture(t)
	loopback := f.server("1", nil)
	const user = "synchttp_rotation_user"
	provision(t, f, loopback, user)
	token := f.admit(user, "phone")
	oldPath := writeKeys(t, "old", "old")
	old := f.syncGateway(oldPath, nil)
	ids := []string{"synchttp_rotation_a", "synchttp_rotation_b", "synchttp_rotation_c"}
	for _, id := range ids {
		createGroup(t, f, old, token, id, nil, true)
	}
	first := readPage(t, f, old, bootstrapPath, token, map[string]int{"limit": 1})
	expectItem(t, first, true, ids[0], "upsert", 1, 0, "")

	// The overlap instances can both verify either signing key.
	preload := f.syncGateway(writeKeys(t, "old", "old", "new"), nil)
	rotated := f.syncGateway(writeKeys(t, "new", "old", "new"), nil)
	oldSigned := readPage(t, f, preload, bootstrapPath, token, continueWith(first.NextCursor))
	expectItem(t, oldSigned, true, ids[1], "upsert", 2, 0, "")
	newSigned := readPage(t, f, rotated, bootstrapPath, token, continueWith(first.NextCursor))
	expectItem(t, newSigned, true, ids[1], "upsert", 2, 0, "")
	both := readPage(t, f, preload, bootstrapPath, token, continueWith(newSigned.NextCursor))
	expectItem(t, both, false, ids[2], "upsert", 3, 0, "")
	readPage(t, f, rotated, deltaPath, token, checkpoint(both.NextCursor))
	requireSyncError(t, f.call(old, http.MethodPost, bootstrapPath, token, encode(t, continueWith(newSigned.NextCursor)), nil), 400, "SYNC_INVALID_CURSOR")

	retired := f.syncGateway(writeKeys(t, "new", "new"), nil)
	requireSyncError(t, f.call(retired, http.MethodPost, bootstrapPath, token, encode(t, continueWith(oldSigned.NextCursor)), nil), 400, "SYNC_INVALID_CURSOR")
	current := readPage(t, f, retired, bootstrapPath, token, continueWith(newSigned.NextCursor))
	expectItem(t, current, false, ids[2], "upsert", 3, 0, "")
	readPage(t, f, preload, deltaPath, token, checkpoint(current.NextCursor))

	repository := openRepository(t, f)
	principal := app.Principal{UserID: user}
	base := time.Now()
	newService := func(age time.Duration) *app.Service {
		service, err := app.NewService(repository, app.Config{
			ActiveKeyID: "old", Keys: map[string][]byte{"old": fixtureKey("old")},
			Clock: func() time.Time { return base.Add(-age) },
		})
		must(t, err)
		return service
	}
	pageClock := newService(16 * time.Minute)
	expiredPage, err := pageClock.BeginBootstrap(f.ctx, principal, 1)
	must(t, err)
	if !expiredPage.HasMore {
		t.Fatal("backdated fixture did not issue a page token")
	}
	_, err = pageClock.ContinueBootstrap(f.ctx, principal, expiredPage.NextCursor)
	must(t, err)
	requireSyncError(t, f.call(rotated, http.MethodPost, bootstrapPath, token, encode(t, continueWith(expiredPage.NextCursor)), nil), 409, "SYNC_CURSOR_EXPIRED")

	checkpointClock := newService(25 * time.Hour)
	expiredTail, err := checkpointClock.BeginBootstrap(f.ctx, principal, 100)
	must(t, err)
	if expiredTail.HasMore {
		t.Fatal("backdated fixture did not issue a checkpoint")
	}
	_, err = checkpointClock.BeginDelta(f.ctx, principal, expiredTail.NextCursor, 100)
	must(t, err)
	requireSyncError(t, f.call(preload, http.MethodPost, deltaPath, token, encode(t, checkpoint(expiredTail.NextCursor)), nil), 409, "SYNC_CURSOR_EXPIRED")
	rotated.requireRedacted(t, token, fixtureKeyHex("old"), fixtureKeyHex("new"), newSigned.NextCursor, expiredPage.NextCursor, testDSN)
	preload.requireRedacted(t, token, fixtureKeyHex("old"), fixtureKeyHex("new"), expiredTail.NextCursor, testDSN)
}
