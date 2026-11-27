//go:build integration

package syncsource_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/group"
	store "github.com/m-ice/NewIM/server/storage/conversationsync"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

const (
	provisionPath     = "/api/v1/internal/users/provision"
	createGroupPath   = "/api/v1/groups/create"
	joinGroupPath     = "/api/v1/groups/join"
	memberGroupPath   = "/api/v1/groups/member/action"
	dissolveGroupPath = "/api/v1/groups/dissolve"
)

func TestSyncSourceCheck(t *testing.T) {
	t.Run("live-direct-writers-and-unchanged-receipt", testLiveDirect)
	t.Run("group-history-and-removal", testGroupSources)
	t.Run("managed-and-legacy-membership-scale", testMemberScale)
}

func testLiveDirect(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(14)
	server := f.server("1", nil)
	gateway := f.gateway("1", nil)
	client := openSync(t, f, true)
	const left = "sync_direct_left"
	const right = "sync_direct_right"
	provision(t, f, server, left)
	provision(t, f, server, right)
	alice := f.admit(left, "phone")
	bob := f.admit(right, "phone")
	leftCursor := bootstrap(t, f, client, left, 0).NextCursor
	rightCursor := bootstrap(t, f, client, right, 0).NextCursor
	if got := projectionState(t, f, left); got != (accountState{ready: true, epoch: 1}) {
		t.Fatal("trusted provisioning did not initialize a ready empty projection")
	}

	direct := createDirect(t, f, gateway, alice, right)
	if again := createDirect(t, f, server, bob, left); again != direct {
		t.Fatal("direct source retry created a second conversation")
	}
	createLeft := delta(t, f, client, left, leftCursor)
	createRight := delta(t, f, client, right, rightCursor)
	expectOnly(t, createLeft, direct, "upsert", 1, 0, "")
	expectOnly(t, createRight, direct, "upsert", 1, 0, "")

	wire := textBody(t, "stable_direct_send", direct, "synthetic private source message")
	first := f.call(gateway, http.MethodPost, sendPath, alice, wire, nil)
	ack := expectACK(t, first)
	if !first.verified || ack.ConversationSeq != "1" || ack.SenderID != left {
		t.Fatal("TLS writer did not return the committed private ACK")
	}
	retry := f.call(server, http.MethodPost, sendPath, alice, wire, nil)
	expectACK(t, retry)
	if retry.verified || !bytes.Equal(retry.body, first.body) {
		t.Fatal("loopback replay did not preserve the original TLS receipt")
	}
	nextLeft := delta(t, f, client, left, createLeft.NextCursor)
	nextRight := delta(t, f, client, right, createRight.NextCursor)
	expectOnly(t, nextLeft, direct, "upsert", 2, 1, ack.ServerMsgID)
	expectOnly(t, nextRight, direct, "upsert", 2, 1, ack.ServerMsgID)
	reply := send(t, f, server, bob, direct, "fresh_loopback_write", "synthetic loopback source message")
	expectOnly(t, delta(t, f, client, left, nextLeft.NextCursor), direct, "upsert", 3, 2, reply.ServerMsgID)
	expectOnly(t, delta(t, f, client, right, nextRight.NextCursor), direct, "upsert", 3, 2, reply.ServerMsgID)
	if got := scalar(t, f, "SELECT count(*) FROM newim.im_messages WHERE conversation_id=$1", direct); got != 2 {
		t.Fatal("replay unexpectedly changed message storage")
	}
	if got := scalar(t, f, "SELECT count(*) FROM newim.im_outbox_events e JOIN newim.im_messages m USING(server_msg_id) WHERE m.conversation_id=$1", direct); got != 2 {
		t.Fatal("replay unexpectedly changed outbox storage")
	}
	server.requireRedacted(t, alice, bob, f.issuerKey, testDSN, "synthetic loopback source message")
	gateway.requireRedacted(t, alice, bob, testDSN, "synthetic private source message")
}

func testGroupSources(t *testing.T) {
	f := openFixture(t)
	server := f.server("1", nil)
	gateway := f.gateway("1", nil)
	client := openSync(t, f, true)
	const groupID = "sync_hidden_history_group"
	const owner = "sync_history_owner"
	const late = "sync_history_late"
	provision(t, f, server, owner)
	provision(t, f, server, late)
	ownerToken := f.admit(owner, "phone")
	lateToken := f.admit(late, "phone")
	base := bootstrap(t, f, client, late, 0)
	createGroup(t, f, gateway, ownerToken, groupID, nil, false)
	old := send(t, f, gateway, ownerToken, groupID, "before_late_join", "older private history")
	if old.ConversationSeq != "1" {
		t.Fatal("group source fixture missed the first message")
	}
	join(t, f, gateway, lateToken, groupID, "first_entry")
	joined := delta(t, f, client, late, base.NextCursor)
	expectOnly(t, joined, groupID, "upsert", 1, 0, "")

	visible := send(t, f, gateway, ownerToken, groupID, "first_visible", "visible after first join")
	visiblePage := delta(t, f, client, late, joined.NextCursor)
	expectOnly(t, visiblePage, groupID, "upsert", 2, 2, visible.ServerMsgID)
	leave(t, f, gateway, lateToken, groupID)
	tombstone := delta(t, f, client, late, visiblePage.NextCursor)
	expectOnly(t, tombstone, groupID, "remove", 3, 0, "")
	send(t, f, gateway, ownerToken, groupID, "between_memberships", "hidden after departure")
	if got := projectionState(t, f, late); got.head != 3 {
		t.Fatal("message source advanced a departed member's projection")
	}

	join(t, f, gateway, lateToken, groupID, "second_entry")
	rejoined := delta(t, f, client, late, tombstone.NextCursor)
	expectOnly(t, rejoined, groupID, "upsert", 4, 0, "")
	_, err := client.BeginDelta(f.ctx, app.Principal{UserID: late}, joined.NextCursor, 10)
	requireSyncError(t, err, app.CursorExpired)
	visibleAgain := send(t, f, gateway, ownerToken, groupID, "visible_again", "visible after re-entry")
	again := delta(t, f, client, late, rejoined.NextCursor)
	expectOnly(t, again, groupID, "upsert", 5, 4, visibleAgain.ServerMsgID)
	if !groupResult[bool](t, f, gateway, ownerToken, dissolveGroupPath, "dissolved", map[string]string{"groupId": groupID}) {
		t.Fatal("group dissolution did not commit")
	}
	expectOnly(t, delta(t, f, client, late, again.NextCursor), groupID, "remove", 6, 0, "")
}

func testMemberScale(t *testing.T) {
	f := openFixture(t)
	gateway := f.gateway("1", nil)
	loopback := f.server("1", nil)
	const owner = "sync_large_owner"
	f.user(owner)
	token := f.admit(owner, "phone")
	managed := bulkUsers(t, f, "sync_large_managed_", 999)
	const managedGroup = "sync_managed_large_group"
	createGroup(t, f, gateway, token, managedGroup, managed, true)
	requireFanout(t, f, managedGroup, 1000, 0, 1)
	send(t, f, gateway, token, managedGroup, "large_group_send", "managed fanout")
	requireFanout(t, f, managedGroup, 1000, 1, 2)

	const legacyGroup = "sync_legacy_large_group"
	legacy := append(bulkUsers(t, f, "sync_large_legacy_", 1000), owner)
	f.sql("INSERT INTO newim.im_conversations(conversation_id,kind) VALUES($1,'group')", legacyGroup)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) SELECT $1,unnest($2::text[])", legacyGroup, legacy)
	send(t, f, loopback, token, legacyGroup, "legacy_group_send", "legacy fanout")
	requireFanout(t, f, legacyGroup, 1001, 1, 1)
}

func requireFanout(t *testing.T, f *fixture, id string, accounts, latest, each int64) {
	t.Helper()
	total := scalar(t, f, "SELECT count(*) FROM newim.im_conversation_sync_changes WHERE conversation_id=$1", id)
	latestRows := scalar(t, f, `SELECT count(*) FROM (
		SELECT DISTINCT ON (user_id) kind,latest_seq,latest_server_msg_id
		FROM newim.im_conversation_sync_changes WHERE conversation_id=$1 ORDER BY user_id,change_seq DESC
	) AS current WHERE kind='upsert' AND latest_seq=$2 AND (latest_server_msg_id IS NOT NULL)=($2>0)`, id, latest)
	if latestRows != accounts || total != accounts*each {
		t.Fatalf("source fanout latest=%d rows=%d want latest=%d rows=%d", latestRows, total, accounts, accounts*each)
	}
}

func bulkUsers(t *testing.T, f *fixture, prefix string, n int) []string {
	t.Helper()
	users := make([]string, n)
	for i := range users {
		users[i] = fmt.Sprintf("%s%04d", prefix, i)
	}
	f.sql("INSERT INTO newim.im_users(user_id) SELECT unnest($1::text[])", users)
	return users
}

func provision(t *testing.T, f *fixture, server *process, user string) {
	t.Helper()
	reply := f.call(server, http.MethodPost, provisionPath, f.issuerKey, encode(t, map[string]string{"userId": user}), nil)
	requireStatus(t, reply, http.StatusOK)
	if reply.verified {
		t.Fatal("trusted provisioning must use the loopback app credential boundary")
	}
}

func createGroup(t *testing.T, f *fixture, gateway *process, token, id string, members []string, history bool) {
	t.Helper()
	wire := map[string]any{"groupId": id, "profile": map[string]string{"name": "Projection fixture"}, "joinMode": 2, "historyEnabled": history}
	if members != nil {
		wire["memberIds"] = members
	}
	got := groupResult[group.View](t, f, gateway, token, createGroupPath, "view", wire)
	if got.Group.GroupID != id || got.Membership.Role != group.Owner {
		t.Fatal("managed group source did not establish its owner")
	}
}

func join(t *testing.T, f *fixture, gateway *process, token, id, request string) {
	t.Helper()
	entry := groupResult[group.Entry](t, f, gateway, token, joinGroupPath, "request", group.JoinRequest{GroupID: id, RequestID: request, Kind: group.Join})
	if entry.State != group.Accepted {
		t.Fatal("group join source did not accept an open join")
	}
}

func leave(t *testing.T, f *fixture, gateway *process, token, id string) {
	t.Helper()
	member := groupResult[group.Membership](t, f, gateway, token, memberGroupPath, "membership", group.MemberRequest{GroupID: id, Action: group.Leave})
	if member.DepartedThroughSeq == nil {
		t.Fatal("group leave source did not store its ending sequence")
	}
}

func groupResult[T any](t *testing.T, f *fixture, gateway *process, token, path, field string, body any) T {
	t.Helper()
	reply := f.call(gateway, http.MethodPost, path, token, encode(t, body), nil)
	if reply.err != nil || reply.status != http.StatusOK || !reply.verified {
		t.Fatalf("group API %s status=%d TLS=%t err=%v", path, reply.status, reply.verified, reply.err)
	}
	var envelope map[string]json.RawMessage
	must(t, json.Unmarshal(reply.body, &envelope))
	if string(envelope["version"]) != "1" || len(envelope) != 2 || len(envelope[field]) == 0 || string(envelope[field]) == "null" {
		t.Fatal("group source operation did not return its versioned result")
	}
	var result T
	must(t, json.Unmarshal(envelope[field], &result))
	return result
}

func encode(t *testing.T, body any) []byte {
	t.Helper()
	raw, err := json.Marshal(body)
	must(t, err)
	return raw
}

type accountState struct {
	ready       bool
	epoch, head int64
	after       string
}

func projectionState(t *testing.T, f *fixture, user string) accountState {
	t.Helper()
	var result accountState
	must(t, f.db.QueryRow(f.ctx, `SELECT source_ready,epoch,last_change_seq,COALESCE(source_after_key::text,'')
		FROM newim.im_conversation_sync_accounts WHERE user_id=$1`, user).Scan(&result.ready, &result.epoch, &result.head, &result.after))
	return result
}

func scalar(t *testing.T, f *fixture, query string, args ...any) int64 {
	t.Helper()
	var value int64
	must(t, f.db.QueryRow(f.ctx, query, args...).Scan(&value))
	return value
}

func openSync(t *testing.T, f *fixture, ready bool) *app.Service {
	t.Helper()
	repo, err := store.Open(f.ctx, store.Config{DSN: testDSN, AllowLocalSocket: true, MaxConnections: 3, RequireSourceReady: ready})
	must(t, err)
	t.Cleanup(repo.Close)
	appService, err := app.NewService(repo, app.Config{ActiveKeyID: "fixture", Keys: map[string][]byte{
		"fixture": []byte("fixed synthetic restart key with 32 bytes"),
	}, Clock: time.Now})
	must(t, err)
	return appService
}

func bootstrap(t *testing.T, f *fixture, client *app.Service, user string, limit int) app.Page {
	t.Helper()
	got, err := client.BeginBootstrap(f.ctx, app.Principal{UserID: user}, limit)
	must(t, err)
	if got.NextCursor == "" {
		t.Fatal("successful bootstrap response omitted its cursor")
	}
	return got
}

func delta(t *testing.T, f *fixture, client *app.Service, user, cursor string) app.Page {
	t.Helper()
	got, err := client.BeginDelta(f.ctx, app.Principal{UserID: user}, cursor, 100)
	must(t, err)
	if got.NextCursor == "" {
		t.Fatal("successful delta response omitted its cursor")
	}
	return got
}

func expectOnly(t *testing.T, page app.Page, id, kind string, revision, seq uint64, server string) {
	t.Helper()
	if page.HasMore || len(page.Items) != 1 {
		t.Fatalf("expected exactly one projection change, got %d more=%t", len(page.Items), page.HasMore)
	}
	got := page.Items[0]
	if got.ConversationID != id || got.Kind != kind || got.Revision != revision {
		t.Fatalf("projection kind=%s revision=%d want kind=%s revision=%d", got.Kind, got.Revision, kind, revision)
	}
	if kind == "remove" {
		if got.LatestConversationSeq != nil || got.LatestServerMsgID != nil {
			t.Fatal("tombstone exposed a summary")
		}
		return
	}
	if got.LatestConversationSeq == nil || *got.LatestConversationSeq != seq {
		t.Fatal("projection has the wrong visible sequence")
	}
	if server == "" && got.LatestServerMsgID != nil ||
		server != "" && (got.LatestServerMsgID == nil || *got.LatestServerMsgID != server) {
		t.Fatal("projection has the wrong visible message pointer")
	}
}

func requireSyncError(t *testing.T, err error, code app.Code) {
	t.Helper()
	if err == nil || app.ErrorCode(err) != code {
		t.Fatalf("sync error=%v want=%s", err, code)
	}
}
