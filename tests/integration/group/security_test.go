//go:build integration

package group_test

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/group"
)

func TestGroupSecurity(t *testing.T) {
	t.Run("roles-mutes-and-concurrent-removal", testRolesAndMutes)
	t.Run("historical-window-and-media-membership", testHistoryAndMedia)
	t.Run("request-and-rate-bounds", testGroupRequestLimits)
}

func testRolesAndMutes(t *testing.T) {
	f := openFixture(t)
	const id = "grp_security_roles"
	const ownerID = "grp_roles_owner"
	const adminID = "grp_roles_admin"
	const memberID = "grp_roles_member"
	const strangerID = "grp_roles_stranger"
	owner := f.admit(ownerID, "mobile")
	admin := f.admit(adminID, "mobile")
	member := f.admit(memberID, "mobile")
	stranger := f.admit(strangerID, "mobile")
	first, second := f.gateway("1", nil), f.gateway("1", nil)
	post[group.View](t, f, first, owner, createGroupPath, "view", map[string]any{
		"groupId": id, "profile": map[string]string{"name": "Permission fixture"}, "memberIds": []string{adminID, memberID},
		"membersCanInvite": false,
	})
	requireMembership(t, changeMember(t, f, first, owner, id, adminID, group.Promote, 0), adminID, group.Admin)
	idWire := encode(t, map[string]string{"groupId": id})
	f.call(second, http.MethodPost, getGroupPath, stranger, idWire, nil).requireError(t, 403, "GROUP_FORBIDDEN")
	f.call(second, http.MethodPost, membersGroupPath, stranger, idWire, nil).requireError(t, 403, "GROUP_FORBIDDEN")
	application := join(t, f, first, stranger, id, "stranger_pending", group.Join, "")
	requireEntry(t, application, strangerID, strangerID, "stranger_pending", group.Join, group.Pending)
	requestList := map[string]string{"groupId": id}
	if page := post[group.Page[group.Entry]](t, f, second, member, requestsGroupPath, "page", requestList); page.Items == nil || len(page.Items) != 0 {
		t.Fatal("regular member saw another account's pending application")
	}
	if page := post[group.Page[group.Entry]](t, f, first, stranger, requestsGroupPath, "page", requestList); len(page.Items) != 1 || page.Items[0] != application {
		t.Fatal("applicant could not see its own unapproved request")
	}
	f.call(second, http.MethodPost, dissolveGroupPath, admin, idWire, nil).requireError(t, 403, "GROUP_FORBIDDEN")
	f.call(second, http.MethodPost, updateGroupPath, member, encode(t, map[string]string{"groupId": id, "name": "denied edit"}), nil).
		requireError(t, 403, "GROUP_FORBIDDEN")
	f.call(second, http.MethodPost, joinGroupPath, member, encode(t, group.JoinRequest{GroupID: id, RequestID: "denied_invite", Kind: group.Invite, UserID: strangerID}), nil).
		requireError(t, 403, "GROUP_FORBIDDEN")
	for _, item := range []struct {
		actor   string
		request group.MemberRequest
	}{
		{admin, group.MemberRequest{GroupID: id, UserID: ownerID, Action: group.Remove}},
		{admin, group.MemberRequest{GroupID: id, UserID: ownerID, Action: group.MuteMember, MuteSeconds: 120}},
		{admin, group.MemberRequest{GroupID: id, UserID: memberID, Action: group.Promote}},
		{member, group.MemberRequest{GroupID: id, UserID: adminID, Action: group.MuteMember, MuteSeconds: 120}},
		{owner, group.MemberRequest{GroupID: id, Action: group.Leave}},
	} {
		f.call(first, http.MethodPost, actionGroupPath, item.actor, encode(t, item.request), nil).requireError(t, 403, "GROUP_FORBIDDEN")
	}

	memberWire := textBody(t, "member_before_mute", id, "committed before mute")
	committed := f.call(first, http.MethodPost, sendPath, member, memberWire, nil)
	expectACK(t, committed)
	setGroupMute(t, f, first, admin, id, true)
	f.call(second, http.MethodPost, sendPath, member, textBody(t, "member_global_muted", id, "denied by group mute"), nil).
		requireError(t, 403, "SEND_UNAUTHORIZED")
	requireSameReceipt(t, f, second, member, memberWire, committed)
	local := f.loopback()
	localReplay := f.call(local, http.MethodPost, sendPath, member, memberWire, nil)
	if localReplay.err != nil || localReplay.status != http.StatusOK || localReplay.verified || !bytes.Equal(localReplay.body, committed.body) {
		t.Fatal("loopback writer did not retain the same committed receipt during group mute")
	}
	f.call(local, http.MethodPost, sendPath, member, textBody(t, "local_member_muted", id, "denied on loopback"), nil).
		requireError(t, 403, "SEND_UNAUTHORIZED")
	adminWire := textBody(t, "admin_before_personal_mute", id, "administrator can write during group mute")
	adminResult := f.call(second, http.MethodPost, sendPath, admin, adminWire, nil)
	expectACK(t, adminResult)
	muted := changeMember(t, f, first, owner, id, adminID, group.MuteMember, 300)
	if muted.MuteUntil == nil || !muted.MuteUntil.After(time.Now()) {
		t.Fatal("owner did not retain administrator's personal mute")
	}
	f.call(second, http.MethodPost, sendPath, admin, textBody(t, "admin_personal_muted", id, "denied by personal mute"), nil).
		requireError(t, 403, "SEND_UNAUTHORIZED")
	requireSameReceipt(t, f, second, admin, adminWire, adminResult)

	setGroupMute(t, f, first, owner, id, false)
	changeMember(t, f, first, owner, id, adminID, group.UnmuteMember, 0)
	send(t, f, second, admin, id, "admin_after_unmute", "administrator can write again")
	changeMember(t, f, first, admin, id, memberID, group.MuteMember, 300)
	f.call(second, http.MethodPost, sendPath, member, textBody(t, "member_personal_muted", id, "denied by personal mute"), nil).
		requireError(t, 403, "SEND_UNAUTHORIZED")
	changeMember(t, f, second, admin, id, memberID, group.UnmuteMember, 0)
	requireMessageRows(t, f, id, 3)

	changeMember(t, f, second, owner, id, adminID, group.TransferOwner, 0)
	prior := getGroup(t, f, first, owner, id)
	current := getGroup(t, f, second, admin, id)
	requireMembership(t, prior.Membership, ownerID, group.Admin)
	requireMembership(t, current.Membership, adminID, group.Owner)
	if prior.Group.OwnerID != adminID || current.Group.OwnerID != adminID {
		t.Fatal("owner transfer did not change both account views")
	}
	f.call(first, http.MethodPost, dissolveGroupPath, owner, idWire, nil).requireError(t, 403, "GROUP_FORBIDDEN")
	requireMembership(t, changeMember(t, f, first, admin, id, memberID, group.Promote, 0), memberID, group.Admin)
	requireMembership(t, changeMember(t, f, second, admin, id, memberID, group.Demote, 0), memberID, group.Member)
	checkRemovalRace(t, f, first, second, admin, member, id, memberID)
	f.call(local, http.MethodPost, sendPath, member, memberWire, nil).requireError(t, 403, "SEND_UNAUTHORIZED")
	first.requireRedacted(t, owner, admin, member, stranger, testDSN, "committed before mute")
	second.requireRedacted(t, owner, admin, member, stranger, testDSN, "administrator can write during group mute")
}

func setGroupMute(t *testing.T, f *fixture, gateway *process, raw, id string, muted bool) {
	t.Helper()
	got := post[group.View](t, f, gateway, raw, muteGroupPath, "view", map[string]any{"groupId": id, "muted": muted})
	if got.Group.Muted != muted {
		t.Fatal("group mute policy did not commit")
	}
}

func requireSameReceipt(t *testing.T, f *fixture, gateway *process, raw string, wire []byte, original response) {
	t.Helper()
	replay := f.call(gateway, http.MethodPost, sendPath, raw, wire, nil)
	expectACK(t, replay)
	if !bytes.Equal(replay.body, original.body) {
		t.Fatal("muting lost the previously committed receipt")
	}
}

func checkRemovalRace(t *testing.T, f *fixture, one, two *process, owner, member, id, memberID string) {
	t.Helper()
	ready := make(chan struct{})
	sendResult := make(chan response, 1)
	removeResult := make(chan response, 1)
	wire := textBody(t, "racing_member_send", id, "concurrent send")
	body := encode(t, group.MemberRequest{GroupID: id, UserID: memberID, Action: group.Remove})
	go func() { <-ready; sendResult <- f.call(one, http.MethodPost, sendPath, member, wire, nil) }()
	go func() { <-ready; removeResult <- f.call(two, http.MethodPost, actionGroupPath, owner, body, nil) }()
	close(ready)
	removal := <-removeResult
	if removal.err != nil || removal.status != http.StatusOK || !removal.verified {
		t.Fatalf("concurrent removal status=%d err=%v", removal.status, removal.err)
	}
	sent := <-sendResult
	want := 3
	if sent.status == http.StatusOK {
		if got := expectACK(t, sent); got.ConversationSeq != "4" || got.SenderID != memberID {
			t.Fatal("send ordered before removal did not return its committed sequence")
		}
		want = 4
	} else {
		sent.requireError(t, 403, "SEND_UNAUTHORIZED")
	}
	var last *int
	must(t, f.db.QueryRow(f.ctx, "SELECT departed_through_seq FROM newim.im_conversation_members WHERE conversation_id=$1 AND user_id=$2", id, memberID).Scan(&last))
	if last == nil || *last != want {
		t.Fatal("removal did not order the history bound with the competing send")
	}
	f.call(one, http.MethodPost, sendPath, member, wire, nil).requireError(t, 403, "SEND_UNAUTHORIZED")
	requireMessageRows(t, f, id, want)
}

func testGroupRequestLimits(t *testing.T) {
	f := openFixture(t)
	const id = "grp_security_budgets"
	const ownerID = "grp_budget_owner"
	const otherID = "grp_budget_other"
	owner := f.admit(ownerID, "device1")
	ownerOther := f.admit(ownerID, "device2")
	other := f.admit(otherID, "mobile")
	disabled := f.gateway("", nil)
	body := encode(t, map[string]string{"groupId": id})
	if reply := f.call(disabled, http.MethodPost, getGroupPath, owner, body, nil); reply.err != nil || reply.status != http.StatusNotFound || !reply.verified {
		t.Fatal("disabled group routes must remain unmounted")
	}
	private := createDirect(t, f, disabled, owner, otherID)
	send(t, f, disabled, owner, private, "private_with_group_disabled", "still available")

	enabled := f.gateway("1", nil)
	f.call(enabled, http.MethodPost, getGroupPath, "", body, nil).requireError(t, 401, "AUTH_INVALID_TOKEN")
	f.call(enabled, http.MethodPost, getGroupPath, "", body, http.Header{"Authorization": {"Bearer " + owner, "Bearer " + owner}}).
		requireError(t, 401, "AUTH_INVALID_TOKEN")
	f.call(enabled, http.MethodGet, getGroupPath, owner, nil, nil).requireError(t, 405, "HTTP_METHOD_NOT_ALLOWED")
	f.call(enabled, http.MethodPost, getGroupPath+"?other=1", owner, body, nil).requireError(t, 400, "GROUP_INVALID_INPUT")
	f.call(enabled, http.MethodPost, getGroupPath, owner, []byte(`{"groupId":"grp_security_budgets","actorId":"grp_budget_other"}`), nil).
		requireError(t, 400, "GROUP_INVALID_INPUT")
	f.call(enabled, http.MethodPost, getGroupPath, owner, body, http.Header{"Content-Type": {"text/plain"}}).
		requireError(t, 415, "HTTP_UNSUPPORTED_MEDIA_TYPE")
	f.call(enabled, http.MethodPost, getGroupPath, owner, bytes.Repeat([]byte{' '}, 256*1024+1), nil).
		requireError(t, 413, "HTTP_BODY_TOO_LARGE")
	f.call(enabled, http.MethodPost, changesGroupPath, owner, []byte(`{"afterRevision":0}`), nil).
		requireError(t, 400, "GROUP_INVALID_INPUT")

	account := f.gateway("1", map[string]string{"NEWIM_GROUP_ACCOUNT_ATTEMPTS_PER_MINUTE": "1"})
	waitRateWindow()
	create := map[string]any{"groupId": id, "profile": map[string]string{"name": "Budget fixture"}, "memberIds": []string{otherID}}
	post[group.View](t, f, account, owner, createGroupPath, "view", create)
	f.call(account, http.MethodPost, getGroupPath, ownerOther, body, nil).requireError(t, 429, "GROUP_RATE_LIMITED")
	getGroup(t, f, account, other, id)
	deliver := subscribe(t, f, account, other, id, 0)
	ack := send(t, f, account, owner, id, "independent_send", "message after group management rate limit")
	requireDelivered(t, f, deliver, ack, "message after group management rate limit")

	address := f.gateway("1", map[string]string{"NEWIM_GROUP_IP_ATTEMPTS_PER_MINUTE": "1", "NEWIM_GATEWAY_IP_ATTEMPTS_PER_MINUTE": "1"})
	waitRateWindow()
	conn, reply, err := dialSocket(t, f, address, other)
	requireSocketStatus(t, conn, reply, err, http.StatusSwitchingProtocols)
	defer conn.CloseNow()
	any, reply, err := dialSocket(t, f, address, owner)
	requireSocketStatus(t, any, reply, err, http.StatusTooManyRequests)
	got := f.call(address, http.MethodPost, getGroupPath, owner, body, http.Header{"X-Forwarded-For": {"192.0.2.1"}})
	if got.err != nil || got.status != http.StatusOK || !got.verified {
		t.Fatal("WSS quota leaked into group HTTP")
	}
	f.call(address, http.MethodPost, getGroupPath, other, body, http.Header{"X-Forwarded-For": {"192.0.2.2"}}).
		requireError(t, 429, "GROUP_RATE_LIMITED")
}

func waitRateWindow() {
	remaining := time.Until(time.Now().Truncate(time.Minute).Add(time.Minute))
	if remaining < 3*time.Second {
		time.Sleep(remaining + 20*time.Millisecond)
	}
}
