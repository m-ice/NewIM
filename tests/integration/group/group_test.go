//go:build integration

package group_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"testing"

	"github.com/m-ice/NewIM/server/group"
)

const (
	createGroupPath   = "/api/v1/groups/create"
	getGroupPath      = "/api/v1/groups/get"
	updateGroupPath   = "/api/v1/groups/update"
	joinGroupPath     = "/api/v1/groups/join"
	respondGroupPath  = "/api/v1/groups/respond"
	membersGroupPath  = "/api/v1/groups/members"
	actionGroupPath   = "/api/v1/groups/member/action"
	muteGroupPath     = "/api/v1/groups/mute"
	dissolveGroupPath = "/api/v1/groups/dissolve"
	requestsGroupPath = "/api/v1/groups/requests"
	changesGroupPath  = "/api/v1/groups/changes"
)

func TestGroupCheck(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(12)
	const id = "grp_check_lifecycle"
	const ownerID = "grp_check_owner"
	const adminID = "grp_check_admin"
	const memberID = "grp_check_invited"
	const requesterID = "grp_check_application"
	const openID = "grp_check_open"
	const excessID = "grp_check_excess"
	owner := f.admit(ownerID, "mobile")
	ownerOther := f.admit(ownerID, "desktop")
	admin := f.admit(adminID, "mobile")
	member := f.admit(memberID, "mobile")
	requester := f.admit(requesterID, "mobile")
	openUser := f.admit(openID, "mobile")
	excess := f.admit(excessID, "mobile")
	first, second := f.gateway("1", nil), f.gateway("1", nil)
	input := map[string]any{"groupId": id, "profile": map[string]string{"name": "Small group", "description": "initial description"},
		"memberIds": []string{adminID}, "memberLimit": 5}
	created := post[group.View](t, f, first, owner, createGroupPath, "view", input)
	if created.Group.GroupID != id || created.Group.OwnerID != ownerID || created.Membership.UserID != ownerID ||
		created.Membership.Role != group.Owner || created.Group.JoinMode != group.ApproveApplications || !created.Group.HistoryEnabled ||
		!created.Group.MembersCanInvite || created.Group.MemberLimit != 5 {
		t.Fatal("group creation did not establish the default policy or owner")
	}
	if replay := post[group.View](t, f, second, ownerOther, createGroupPath, "view", input); replay.Group != created.Group || replay.Membership.Role != group.Owner {
		t.Fatal("repeat create on another gateway changed group identity")
	}
	requireMembership(t, changeMember(t, f, first, owner, id, adminID, group.Promote, 0), adminID, group.Admin)

	invited := join(t, f, second, admin, id, "direct_invite", group.Invite, memberID)
	requireEntry(t, invited, adminID, memberID, "direct_invite", group.Invite, group.Accepted)
	request := join(t, f, first, requester, id, "request_to_join", group.Join, "")
	requireEntry(t, request, requesterID, requesterID, "request_to_join", group.Join, group.Pending)
	if got := join(t, f, second, requester, id, "request_to_join", group.Join, requesterID); got != request {
		t.Fatal("repeated join on a separate gateway changed the pending request")
	}
	f.call(second, http.MethodPost, joinGroupPath, requester, encode(t, group.JoinRequest{
		GroupID: id, RequestID: "competing_join", Kind: group.Join,
	}), nil).requireError(t, 409, "GROUP_CONFLICT")
	list := post[group.Page[group.Entry]](t, f, first, admin, requestsGroupPath, "page", map[string]any{"groupId": id, "limit": 100})
	if !slices.ContainsFunc(list.Items, func(e group.Entry) bool { return e.RequestID == request.RequestID && e.State == group.Pending }) {
		t.Fatal("administrator could not see the waiting join request")
	}
	approved := respond(t, f, second, admin, id, requesterID, "request_to_join", group.Accepted)
	requireEntry(t, approved, requesterID, requesterID, "request_to_join", group.Join, group.Accepted)
	if approved.RespondedBy != adminID {
		t.Fatal("accepted request did not record the trusted approver")
	}
	if retry := respond(t, f, first, admin, id, requesterID, "request_to_join", group.Accepted); retry != approved {
		t.Fatal("replaying accepted request changed its terminal state")
	}

	modeOne := group.ApproveOrdinaryInvites
	description := ""
	updated := post[group.View](t, f, second, admin, updateGroupPath, "view", group.UpdateRequest{GroupID: id, JoinMode: &modeOne, Description: &description})
	if updated.Group.JoinMode != modeOne || updated.Group.Profile.Description != "" || updated.Group.Profile.Name != created.Group.Profile.Name {
		t.Fatal("administrator could not update policy and clear exactly one profile field")
	}
	ordinary := join(t, f, first, member, id, "ordinary_invite", group.Invite, openID)
	requireEntry(t, ordinary, memberID, openID, "ordinary_invite", group.Invite, group.Pending)
	if feed := changes(t, f, second, openUser, 0, 100); len(feed.Items) != 1 || feed.Items[0].Kind != group.RequestChanged ||
		feed.Items[0].Entry == nil || *feed.Items[0].Entry != ordinary {
		t.Fatal("invite target did not receive its own pending request event")
	}
	rejected := respond(t, f, second, owner, id, memberID, "ordinary_invite", group.Rejected)
	requireEntry(t, rejected, memberID, openID, "ordinary_invite", group.Invite, group.Rejected)
	modeTwo := group.OpenJoining
	post[group.View](t, f, first, owner, updateGroupPath, "view", group.UpdateRequest{GroupID: id, JoinMode: &modeTwo})
	openJoin := join(t, f, second, openUser, id, "open_join", group.Join, openID)
	requireEntry(t, openJoin, openID, openID, "open_join", group.Join, group.Accepted)
	requestPage := post[group.Page[group.Entry]](t, f, first, openUser, requestsGroupPath, "page", group.EntryListRequest{
		GroupID: id, Limit: 1,
	})
	if !requestPage.HasMore || len(requestPage.Items) != 1 || requestPage.Items[0] != rejected {
		t.Fatal("participant page did not return the first invitation")
	}
	requestPage = post[group.Page[group.Entry]](t, f, second, openUser, requestsGroupPath, "page", group.EntryListRequest{
		GroupID: id, AfterActorID: rejected.ActorID, AfterRequestID: rejected.RequestID, Limit: 1,
	})
	if requestPage.HasMore || len(requestPage.Items) != 1 || requestPage.Items[0] != openJoin {
		t.Fatal("participant cursor did not continue through the member's own application")
	}
	f.call(second, http.MethodPost, joinGroupPath, excess, encode(t, group.JoinRequest{GroupID: id, RequestID: "full_group", Kind: group.Join}), nil).
		requireError(t, 409, "GROUP_CAPACITY_EXCEEDED")

	page := members(t, f, first, owner, id, "", 2)
	if len(page.Items) != 2 || !page.HasMore || page.Items[0].UserID != adminID || page.Items[1].UserID != requesterID {
		t.Fatal("member page did not use stable user ordering")
	}
	next := members(t, f, second, owner, id, page.Items[1].UserID, 100)
	if len(next.Items) != 3 || next.HasMore || next.Items[0].UserID != memberID || next.Items[1].UserID != openID || next.Items[2].UserID != ownerID {
		t.Fatal("member page did not resume after the exclusive cursor")
	}
	got := getGroup(t, f, second, member, id)
	requireMembership(t, got.Membership, memberID, group.Member)
	if got.Group.Profile != updated.Group.Profile || got.Group.JoinMode != modeTwo {
		t.Fatal("member did not see the committed group profile and policy")
	}

	conn := subscribe(t, f, second, member, id, 0)
	ack := send(t, f, first, owner, id, "first_managed_group_message", "committed group text")
	if ack.SenderID != ownerID || ack.ConversationSeq != "1" {
		t.Fatal("managed group send ACK does not identify the owner and first sequence")
	}
	requireDelivered(t, f, conn, ack, "committed group text")
	requireMessageRows(t, f, id, 1)
	revisions := changes(t, f, first, owner, 0, 1)
	if len(revisions.Items) != 1 || revisions.Items[0].Revision != 1 || !revisions.HasMore || revisions.LatestRevision < 3 {
		t.Fatal("owner change feed did not bootstrap a paginated revision sequence")
	}
	rest := changes(t, f, second, ownerOther, revisions.Items[0].Revision, 100)
	if rest.HasMore || rest.LatestRevision != revisions.LatestRevision || int64(len(rest.Items)) != rest.LatestRevision-1 {
		t.Fatal("second device could not continue the existing account revision")
	}
	for i, item := range rest.Items {
		if item.Revision != int64(i)+2 || item.GroupID != id {
			t.Fatal("owner change feed skipped or reordered a revision")
		}
	}
	verifyPrivateAt012(t, f, first)
	first.requireRedacted(t, owner, ownerOther, admin, member, requester, openUser, excess, testDSN, "committed group text")
	second.requireRedacted(t, owner, ownerOther, admin, member, requester, openUser, excess, testDSN)
}

func verifyPrivateAt012(t *testing.T, f *fixture, gateway *process) {
	t.Helper()
	const leftID = "grp_private_left"
	const rightID = "grp_private_right"
	left := f.admit(leftID, "mobile")
	right := f.admit(rightID, "mobile")
	id := createDirect(t, f, gateway, left, rightID)
	before := f.call(gateway, http.MethodPost, sendPath, left, textBody(t, "private_original", id, "private fixture"), nil)
	ack := expectACK(t, before)
	if ack.ConversationSeq != "1" {
		t.Fatal("private send after migration did not commit")
	}
	setBlock(t, f, gateway, blockPath, right, leftID)
	f.call(gateway, http.MethodPost, sendPath, left, textBody(t, "private_denied", id, "blocked"), nil).requireError(t, 403, "BLOCKED_BY_RECIPIENT")
	replay := f.call(gateway, http.MethodPost, sendPath, left, textBody(t, "private_original", id, "private fixture"), nil)
	expectACK(t, replay)
	if !bytes.Equal(replay.body, before.body) {
		t.Fatal("group migration changed private receipt retries")
	}
	receive := subscribe(t, f, gateway, right, id, 0)
	requireDelivered(t, f, receive, ack, "private fixture")
	setBlock(t, f, gateway, unblockPath, right, leftID)
	send(t, f, gateway, left, id, "private_unblocked", "unblocked")
	requireMessageRows(t, f, id, 2)
	gateway.requireRedacted(t, left, right, "private fixture")
}

func requireMessageRows(t *testing.T, f *fixture, id string, count int) {
	t.Helper()
	var seq, messages, outbox int
	must(t, f.db.QueryRow(f.ctx, `SELECT c.last_seq,
		(SELECT count(*) FROM newim.im_messages m WHERE m.conversation_id=c.conversation_id),
		(SELECT count(*) FROM newim.im_outbox_events e JOIN newim.im_messages m ON e.server_msg_id=m.server_msg_id WHERE m.conversation_id=c.conversation_id)
		FROM newim.im_conversations c WHERE c.conversation_id=$1`, id).Scan(&seq, &messages, &outbox))
	if seq != count || messages != count || outbox != count {
		t.Fatalf("group durable state seq=%d messages=%d outbox=%d want=%d", seq, messages, outbox, count)
	}
}

func requireEntry(t *testing.T, actual group.Entry, actor, user, id string, kind group.RequestKind, state group.RequestState) {
	t.Helper()
	if actual.ActorID != actor || actual.UserID != user || actual.RequestID != id || actual.Kind != kind || actual.State != state {
		t.Fatal("unexpected group request actor, user or state")
	}
}

func requireMembership(t *testing.T, actual group.Membership, user string, role group.Role) {
	t.Helper()
	if actual.UserID != user || actual.Role != role || actual.DepartedThroughSeq != nil {
		t.Fatal("unexpected current group membership")
	}
}

func getGroup(t *testing.T, f *fixture, gateway *process, raw, id string) group.View {
	t.Helper()
	return post[group.View](t, f, gateway, raw, getGroupPath, "view", map[string]string{"groupId": id})
}

func join(t *testing.T, f *fixture, gateway *process, raw, id, request string, kind group.RequestKind, user string) group.Entry {
	t.Helper()
	return post[group.Entry](t, f, gateway, raw, joinGroupPath, "request", group.JoinRequest{GroupID: id, RequestID: request, Kind: kind, UserID: user})
}

func respond(t *testing.T, f *fixture, gateway *process, raw, id, actor, request string, decision group.RequestState) group.Entry {
	t.Helper()
	return post[group.Entry](t, f, gateway, raw, respondGroupPath, "request", group.ResponseRequest{GroupID: id, ActorID: actor, RequestID: request, Decision: decision})
}

func changeMember(t *testing.T, f *fixture, gateway *process, raw, id, user string, action group.MemberAction, seconds int64) group.Membership {
	t.Helper()
	return post[group.Membership](t, f, gateway, raw, actionGroupPath, "membership", group.MemberRequest{GroupID: id, UserID: user, Action: action, MuteSeconds: seconds})
}

func members(t *testing.T, f *fixture, gateway *process, raw, id, after string, limit int) group.Page[group.Membership] {
	t.Helper()
	page := post[group.Page[group.Membership]](t, f, gateway, raw, membersGroupPath, "page", group.MemberListRequest{GroupID: id, AfterUserID: after, Limit: limit})
	if page.Items == nil {
		t.Fatal("group membership page omitted its items array")
	}
	return page
}

func changes(t *testing.T, f *fixture, gateway *process, raw string, after int64, limit int) group.ChangePage {
	t.Helper()
	page := post[group.ChangePage](t, f, gateway, raw, changesGroupPath, "page", map[string]any{"afterRevision": strconv.FormatInt(after, 10), "limit": limit})
	if page.Items == nil {
		t.Fatal("group changes page omitted its items array")
	}
	return page
}

func post[T any](t *testing.T, f *fixture, gateway *process, raw, path, key string, input any) T {
	t.Helper()
	reply := f.call(gateway, http.MethodPost, path, raw, encode(t, input), nil)
	if reply.err != nil || reply.status != http.StatusOK || !reply.verified || reply.headers.Get("Cache-Control") != "no-store" {
		t.Fatalf("group API %s status=%d TLS verified=%t err=%v", path, reply.status, reply.verified, reply.err)
	}
	var envelope map[string]json.RawMessage
	must(t, json.Unmarshal(reply.body, &envelope))
	if len(envelope) != 2 || string(envelope["version"]) != "1" || len(envelope[key]) == 0 || string(envelope[key]) == "null" {
		t.Fatalf("group API did not use the versioned %s envelope", key)
	}
	var output T
	must(t, json.Unmarshal(envelope[key], &output))
	return output
}

func encode(t *testing.T, input any) []byte {
	t.Helper()
	raw, err := json.Marshal(input)
	must(t, err)
	return raw
}
