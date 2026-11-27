//go:build integration

package friend_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/m-ice/NewIM/server/friend"
)

const (
	recoveryAlice  = "friend_recovery_alice"
	recoveryBob    = "friend_recovery_bob"
	recoveryThird  = "friend_recovery_third"
	recoveryFile   = "/tmp/newim-friend-recovery-fixture.json"
	recoveryRemark = "private remark before restart"
)

type recoveryState struct {
	Alice  string
	Bob    string
	After  int64
	Latest int64
}

func TestFriendRecovery(t *testing.T) {
	f := openFixture(t)
	switch os.Getenv("NEWIM_FRIEND_PHASE") {
	case "prepare":
		prepareRecovery(t, f)
	case "restart":
		resumeRecovery(t, f)
	default:
		t.Fatal("NEWIM_FRIEND_PHASE must be prepare or restart")
	}
}

func prepareRecovery(t *testing.T, f *fixture) {
	t.Helper()
	f.requireMigration(10)
	alice := f.admit(recoveryAlice, "mobile")
	bob := f.admit(recoveryBob, "mobile")
	f.sql("INSERT INTO newim.im_users(user_id) VALUES($1)", recoveryThird)
	f.sql("INSERT INTO newim.im_user_profiles(user_id,nickname) VALUES($1,$2)", recoveryBob, "Bob before 011")
	f.sql("INSERT INTO newim.im_blocks(user_id,blocked_user_id) VALUES($1,$2)", recoveryAlice, recoveryThird)
	var previous, current string
	const ledger = "SELECT string_agg(version::text||':'||sha256,',' ORDER BY version) FROM newim_meta.migrations WHERE version<=10"
	must(t, f.db.QueryRow(f.ctx, ledger).Scan(&previous))
	f.migrateFriends()
	f.requireMigration(11)
	must(t, f.db.QueryRow(f.ctx, ledger).Scan(&current))
	if previous == "" || current != previous {
		t.Fatal("friend migration changed the prior migration ledger")
	}
	requireLegacyBlock(t, f)

	gateway := f.gateway("1", nil)
	if initial := friendPage(t, f, gateway, alice, map[string]any{}); initial.LatestRevision != 0 || len(initial.Items) != 0 {
		t.Fatal("pre-existing account did not bootstrap friends at revision zero")
	}
	sendApplication(t, f, gateway, alice, "recovery_request", recoveryBob, "sent before restart")
	cursor := changePage(t, f, gateway, alice, 0, 10)
	if len(cursor.Items) != 1 || cursor.LatestRevision != 1 || cursor.Items[0].Revision != 1 {
		t.Fatal("could not checkpoint the applied request revision")
	}
	other := f.gateway("1", nil)
	answer(t, f, other, bob, recoveryAlice, "recovery_request", "accept")
	setRemark(t, f, other, alice, recoveryBob, recoveryRemark)
	latest := friendPage(t, f, other, alice, map[string]any{}).LatestRevision
	if latest != cursor.LatestRevision+3 {
		t.Fatal("unexpected committed events before recovery")
	}
	data := encode(t, recoveryState{Alice: alice, Bob: bob, After: cursor.LatestRevision, Latest: latest})
	must(t, os.WriteFile(recoveryFile, data, 0o600))
	gateway.requireRedacted(t, alice, bob, testDSN)
	other.requireRedacted(t, alice, bob, testDSN, recoveryRemark)
}

func resumeRecovery(t *testing.T, f *fixture) {
	t.Helper()
	f.requireMigration(11)
	requireLegacyBlock(t, f)
	data, err := os.ReadFile(recoveryFile)
	must(t, err)
	t.Cleanup(func() { _ = os.Remove(recoveryFile) })
	var state recoveryState
	must(t, json.Unmarshal(data, &state))
	if state.Alice == "" || state.Bob == "" || state.After != 1 || state.Latest != state.After+3 {
		t.Fatal("friend recovery fixture incomplete")
	}

	gateway := f.gateway("1", nil)
	first := changePage(t, f, gateway, state.Alice, state.After, 1)
	second := changePage(t, f, gateway, state.Alice, state.After+1, 1)
	third := changePage(t, f, gateway, state.Alice, state.After+2, 1)
	for n, page := range []friend.ChangePage{first, second, third} {
		if len(page.Items) != 1 || page.Items[0].Revision != state.After+int64(n)+1 ||
			page.Items[0].PeerID != recoveryBob || page.HasMore != (n < 2) || page.LatestRevision != state.Latest {
			t.Fatal("resumed feed skipped, repeated or reordered a committed change")
		}
	}
	if accepted := first.Items[0]; accepted.Kind != friend.ApplicationChanged || accepted.Application == nil || accepted.Application.State != friend.StateAccepted {
		t.Fatal("accepted application was absent from the resumed feed")
	}
	if initial := second.Items[0]; initial.Kind != friend.FriendUpserted || initial.Edge == nil || initial.Edge.Remark != "" ||
		initial.Edge.Profile.Nickname != "Bob before 011" {
		t.Fatal("resumed feed lost the original friend record or pre-migration profile")
	}
	if updated := third.Items[0]; updated.Kind != friend.FriendUpserted || updated.Edge == nil || updated.Edge.Remark != recoveryRemark {
		t.Fatal("resumed feed lost the committed private remark")
	}
	if bob := requireSingleFriend(t, friendPage(t, f, gateway, state.Bob, map[string]any{}), recoveryAlice); bob.Remark != "" {
		t.Fatal("restart exposed the other owner's private remark")
	}
	answer(t, f, gateway, state.Bob, recoveryAlice, "recovery_request", "accept")
	if page := changePage(t, f, gateway, state.Alice, state.Latest, 1); len(page.Items) != 0 || page.LatestRevision != state.Latest {
		t.Fatal("retry after restart duplicated a committed acceptance")
	}
	if !remove(t, f, gateway, state.Alice, recoveryBob) {
		t.Fatal("owner could not continue the committed relationship after restart")
	}
	if page := changePage(t, f, gateway, state.Alice, state.Latest, 1); len(page.Items) != 1 ||
		page.Items[0].Kind != friend.FriendRemoved || page.Items[0].Revision != state.Latest+1 ||
		page.LatestRevision != state.Latest+1 || page.HasMore {
		t.Fatal("post-restart mutation did not advance the existing revision")
	}
	gateway.requireRedacted(t, state.Alice, state.Bob, testDSN, recoveryRemark)
}

func requireLegacyBlock(t *testing.T, f *fixture) {
	t.Helper()
	var retained bool
	must(t, f.db.QueryRow(f.ctx, `SELECT EXISTS (
		SELECT 1 FROM newim.im_blocks WHERE user_id=$1 AND blocked_user_id=$2
	) AND NOT EXISTS (
		SELECT 1 FROM newim.im_blocks WHERE user_id=$2 AND blocked_user_id=$1
	)`, recoveryAlice, recoveryThird).Scan(&retained))
	if !retained {
		t.Fatal("upgrade or restart changed the legacy direction of a block")
	}
}
