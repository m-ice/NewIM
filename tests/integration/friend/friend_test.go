//go:build integration

package friend_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/m-ice/NewIM/server/friend"
)

const (
	applyPath        = "/api/v1/friends/applications"
	respondPath      = "/api/v1/friends/applications/respond"
	applicationsPath = "/api/v1/friends/applications/list"
	listPath         = "/api/v1/friends/list"
	remarkPath       = "/api/v1/friends/remark"
	removePath       = "/api/v1/friends/remove"
	changesPath      = "/api/v1/friends/changes"
)

func TestFriendCheck(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(11)
	alice := f.admit("friend_alice", "mobile")
	aliceOther := f.admit("friend_alice", "desktop")
	bob := f.admit("friend_bob", "mobile")
	cara := f.admit("friend_cara", "mobile")
	dan := f.admit("friend_dan", "mobile")
	f.sql("INSERT INTO newim.im_user_profiles(user_id, nickname, face_url, ex) VALUES($1,$2,$3,$4)",
		"friend_bob", "Bob", "https://newim.example.test/bob", "public biography")
	first, second := f.gateway("1", nil), f.gateway("1", nil)

	pending := sendApplication(t, f, first, alice, "a01", "friend_bob", "hello Bob")
	requireApplication(t, pending, "a01", "friend_alice", "friend_bob", "hello Bob", friend.StatePending)
	if got := sendApplication(t, f, second, aliceOther, "a01", "friend_bob", "hello Bob"); got != pending {
		t.Fatal("an application changed during an idempotent retry on another gateway")
	}
	if got := changePage(t, f, second, alice, 0, 2); len(got.Items) != 1 || got.LatestRevision != 1 || got.HasMore ||
		got.Items[0].Kind != friend.ApplicationChanged || got.Items[0].Application == nil || *got.Items[0].Application != pending {
		t.Fatal("initial application or its retry produced the wrong creator change")
	}
	reverse := sendApplication(t, f, second, bob, "b01", "friend_alice", "hello Alice")
	requireApplication(t, reverse, "b01", "friend_bob", "friend_alice", "hello Alice", friend.StatePending)
	sendApplication(t, f, first, alice, "a02", "friend_cara", "hello Cara")
	requireApplication(t, answer(t, f, second, cara, "friend_alice", "a02", "reject"),
		"a02", "friend_alice", "friend_cara", "hello Cara", friend.StateRejected)

	left := applicationPage(t, f, first, alice, map[string]any{"direction": "outgoing", "limit": 1})
	if len(left.Items) != 1 || left.Items[0] != pending || !left.HasMore {
		t.Fatal("outgoing application pagination returned the wrong first page")
	}
	right := applicationPage(t, f, second, alice, map[string]any{
		"direction": "outgoing", "limit": 1, "afterApplicantId": pending.ApplicantID, "afterRequestId": pending.RequestID,
	})
	if len(right.Items) != 1 || right.Items[0].RequestID != "a02" || right.Items[0].State != friend.StateRejected ||
		right.HasMore || right.LatestRevision != left.LatestRevision {
		t.Fatal("application pagination did not resume from the exclusive applicant/request cursor")
	}

	answerBody := encode(t, friend.ResponseRequest{ApplicantID: "friend_alice", RequestID: "a01", Decision: "accept"})
	start := make(chan struct{})
	replies := make(chan response, 2)
	for _, gateway := range []*process{first, second} {
		go func() {
			<-start
			replies <- f.call(gateway, http.MethodPost, respondPath, bob, answerBody, nil)
		}()
	}
	close(start)
	for range 2 {
		requireApplication(t, readResult[friend.Application](t, <-replies, "application"),
			"a01", "friend_alice", "friend_bob", "hello Bob", friend.StateAccepted)
	}
	beforeRetry := changePage(t, f, first, bob, 0, 100).LatestRevision
	requireApplication(t, answer(t, f, second, bob, "friend_alice", "a01", "accept"),
		"a01", "friend_alice", "friend_bob", "hello Bob", friend.StateAccepted)
	conflictingDecision := encode(t, friend.ResponseRequest{ApplicantID: "friend_alice", RequestID: "a01", Decision: "reject"})
	f.call(second, http.MethodPost, respondPath, bob, conflictingDecision, nil).requireError(t, 409, "FRIEND_APPLICATION_CONFLICT")
	if retry := changePage(t, f, first, bob, beforeRetry, 100); len(retry.Items) != 0 || retry.LatestRevision != beforeRetry {
		t.Fatal("replaying a terminal decision appended a change or changed the result")
	}
	incoming := applicationPage(t, f, first, alice, map[string]any{"direction": "incoming"})
	if len(incoming.Items) != 1 {
		t.Fatal("reverse application was absent after accepting")
	}
	requireApplication(t, incoming.Items[0], "b01", "friend_bob", "friend_alice", "hello Alice", friend.StateAccepted)
	if got := friendPage(t, f, second, cara, map[string]any{}); len(got.Items) != 0 {
		t.Fatal("rejected application created a friend record")
	}

	sendApplication(t, f, second, alice, "a03", "friend_dan", "")
	answer(t, f, first, dan, "friend_alice", "a03", "accept")
	aliceFirst := friendPage(t, f, first, alice, map[string]any{"limit": 1})
	if len(aliceFirst.Items) != 1 || !aliceFirst.HasMore || aliceFirst.Items[0].Profile.UserID != "friend_bob" ||
		aliceFirst.Items[0].Profile.Nickname != "Bob" || aliceFirst.Items[0].Profile.FaceURL != "https://newim.example.test/bob" ||
		aliceFirst.Items[0].Profile.Ex != "public biography" || aliceFirst.Items[0].Remark != "" {
		t.Fatal("list did not return the first ordered friend with the public profile")
	}
	aliceNext := friendPage(t, f, second, alice, map[string]any{"limit": 1, "afterPeerId": "friend_bob"})
	if got := requireSingleFriend(t, aliceNext, "friend_dan"); got.Remark != "" || aliceNext.HasMore ||
		aliceNext.LatestRevision != aliceFirst.LatestRevision {
		t.Fatal("friend list did not resume from the exclusive peer cursor")
	}
	if got := requireSingleFriend(t, friendPage(t, f, second, bob, map[string]any{}), "friend_alice"); got.Remark != "" {
		t.Fatal("accept did not grant an independent reverse friend record")
	}
	f.sql("INSERT INTO newim.im_blocks(user_id, blocked_user_id) VALUES($1,$2)", "friend_alice", "friend_bob")
	requireSingleFriend(t, friendPage(t, f, first, alice, map[string]any{"excludeBlocked": true}), "friend_dan")
	requireSingleFriend(t, friendPage(t, f, second, bob, map[string]any{"excludeBlocked": true}), "friend_alice")

	verifyOwnerChanges(t, f, first, second, alice, aliceOther, bob)
	sendApplication(t, f, second, alice, "a04", "friend_bob", "restore missing record")
	answer(t, f, first, bob, "friend_alice", "a04", "accept")
	if got := requireSingleFriend(t, friendPage(t, f, second, bob, map[string]any{}), "friend_alice"); got.Remark != "private Bob remark" {
		t.Fatal("reaccepting a one-way relationship replaced the surviving owner's remark")
	}
	if restored := friendPage(t, f, second, alice, map[string]any{"limit": 1}); len(restored.Items) != 1 ||
		restored.Items[0].Profile.UserID != "friend_bob" || restored.Items[0].Remark != "" {
		t.Fatal("reaccepting a one-way relationship did not restore the missing owner's record")
	}
	first.requireRedacted(t, alice, aliceOther, bob, cara, dan, "private Alice remark", testDSN)
	second.requireRedacted(t, alice, aliceOther, bob, cara, dan, "private Bob remark", testDSN)
}

func verifyOwnerChanges(t *testing.T, f *fixture, first, second *process, alice, aliceOther, bob string) {
	t.Helper()
	if got := setRemark(t, f, second, bob, "friend_alice", "private Bob remark"); got.Remark != "private Bob remark" {
		t.Fatal("Bob's remark did not persist")
	}
	beforeAlice := friendPage(t, f, first, alice, map[string]any{"limit": 1})
	beforeBob := friendPage(t, f, second, bob, map[string]any{})
	if len(beforeAlice.Items) != 1 || beforeAlice.Items[0].Remark != "" {
		t.Fatal("one account observed another account's private remark")
	}
	if got := setRemark(t, f, first, alice, "friend_bob", "private Alice remark"); got.Remark != "private Alice remark" {
		t.Fatal("Alice's remark did not persist")
	}
	if got := setRemark(t, f, second, aliceOther, "friend_bob", ""); got.Remark != "" {
		t.Fatal("explicit empty remark did not clear from another device")
	}
	if !remove(t, f, first, alice, "friend_bob") || remove(t, f, second, aliceOther, "friend_bob") {
		t.Fatal("one-way removal did not distinguish the initial delete from its retry")
	}
	requireSingleFriend(t, friendPage(t, f, second, aliceOther, map[string]any{}), "friend_dan")
	if got := requireSingleFriend(t, friendPage(t, f, first, bob, map[string]any{}), "friend_alice"); got.Remark != "private Bob remark" {
		t.Fatal("one-way removal changed the other owner's friend record")
	}

	a := changePage(t, f, first, aliceOther, beforeAlice.LatestRevision, 1)
	requireChange(t, a, beforeAlice.LatestRevision+1, beforeAlice.LatestRevision+3, true, friend.FriendUpserted, "private Alice remark")
	b := changePage(t, f, second, alice, a.Items[0].Revision, 1)
	requireChange(t, b, beforeAlice.LatestRevision+2, a.LatestRevision, true, friend.FriendUpserted, "")
	c := changePage(t, f, first, aliceOther, b.Items[0].Revision, 1)
	requireChange(t, c, beforeAlice.LatestRevision+3, a.LatestRevision, false, friend.FriendRemoved, "")
	quiet := changePage(t, f, second, bob, beforeBob.LatestRevision, 1)
	if len(quiet.Items) != 0 || quiet.HasMore || quiet.LatestRevision != beforeBob.LatestRevision {
		t.Fatal("remark or removal leaked to the related account's revision feed")
	}
}

func requireChange(t *testing.T, page friend.ChangePage, revision, latest int64, more bool, kind friend.ChangeKind, remark string) {
	t.Helper()
	if len(page.Items) != 1 || page.HasMore != more || page.LatestRevision != latest {
		t.Fatal("unexpected friend change page bounds")
	}
	item := page.Items[0]
	if item.Revision != revision || item.Kind != kind || item.PeerID != "friend_bob" || item.Application != nil {
		t.Fatal("friend change had incorrect owner revision, type or peer")
	}
	if kind == friend.FriendUpserted {
		if item.Edge == nil || item.Edge.Remark != remark || item.Edge.Profile.UserID != item.PeerID {
			t.Fatal("friend upsert omitted the expected private record")
		}
	} else if item.Edge != nil {
		t.Fatal("friend deletion returned an unexpected record")
	}
}

func requireApplication(t *testing.T, got friend.Application, id, from, to, message string, state friend.State) {
	t.Helper()
	if got.RequestID != id || got.ApplicantID != from || got.RecipientID != to || got.Message != message || got.State != state {
		t.Fatal("unexpected friend application identity, message or state")
	}
}

func requireSingleFriend(t *testing.T, page friend.EdgePage, id string) friend.Edge {
	t.Helper()
	if len(page.Items) != 1 || page.Items[0].Profile.UserID != id {
		t.Fatal("friend list did not contain the expected single peer")
	}
	return page.Items[0]
}

func sendApplication(t *testing.T, f *fixture, gateway *process, token, requestID, peer, message string) friend.Application {
	t.Helper()
	return post[friend.Application](t, f, gateway, token, applyPath, "application",
		friend.ApplyRequest{RequestID: requestID, PeerID: peer, Message: message})
}

func answer(t *testing.T, f *fixture, gateway *process, token, applicant, requestID, decision string) friend.Application {
	t.Helper()
	return post[friend.Application](t, f, gateway, token, respondPath, "application",
		friend.ResponseRequest{ApplicantID: applicant, RequestID: requestID, Decision: decision})
}

func setRemark(t *testing.T, f *fixture, gateway *process, token, peer, remark string) friend.Edge {
	t.Helper()
	return post[friend.Edge](t, f, gateway, token, remarkPath, "friend", map[string]string{"peerId": peer, "remark": remark})
}

func remove(t *testing.T, f *fixture, gateway *process, token, peer string) bool {
	t.Helper()
	return post[bool](t, f, gateway, token, removePath, "removed", map[string]string{"peerId": peer})
}

func friendPage(t *testing.T, f *fixture, gateway *process, token string, request map[string]any) friend.EdgePage {
	t.Helper()
	page := post[friend.EdgePage](t, f, gateway, token, listPath, "page", request)
	if page.Items == nil {
		t.Fatal("friend list items must be an array")
	}
	return page
}

func applicationPage(t *testing.T, f *fixture, gateway *process, token string, request map[string]any) friend.ApplicationPage {
	t.Helper()
	page := post[friend.ApplicationPage](t, f, gateway, token, applicationsPath, "page", request)
	if page.Items == nil {
		t.Fatal("application list items must be an array")
	}
	return page
}

func changePage(t *testing.T, f *fixture, gateway *process, token string, after int64, limit int) friend.ChangePage {
	t.Helper()
	page := post[friend.ChangePage](t, f, gateway, token, changesPath, "page",
		map[string]any{"afterRevision": strconv.FormatInt(after, 10), "limit": limit})
	if page.Items == nil {
		t.Fatal("change list items must be an array")
	}
	return page
}

func post[T any](t *testing.T, f *fixture, gateway *process, token, path, key string, payload any) T {
	t.Helper()
	return readResult[T](t, f.call(gateway, http.MethodPost, path, token, encode(t, payload), nil), key)
}

func encode(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	must(t, err)
	return data
}

func readResult[T any](t *testing.T, reply response, key string) T {
	t.Helper()
	if reply.err != nil || reply.status != http.StatusOK || !reply.verified {
		t.Fatalf("friend API status=%d TLS verified=%t err=%v", reply.status, reply.verified, reply.err)
	}
	if reply.headers.Get("Cache-Control") != "no-store" {
		t.Fatal("friend API response allowed caching")
	}
	var envelope map[string]json.RawMessage
	must(t, json.Unmarshal(reply.body, &envelope))
	if len(envelope) != 2 || string(envelope["version"]) != "1" || len(envelope[key]) == 0 || string(envelope[key]) == "null" {
		t.Fatalf("friend API did not use the versioned %s result shape", key)
	}
	var result T
	must(t, json.Unmarshal(envelope[key], &result))
	return result
}
