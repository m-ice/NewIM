//go:build integration

package conversationsynchttp_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/coder/websocket"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/messagehttp"
	app "github.com/m-ice/NewIM/server/sync/conversation"
	syncmessage "github.com/m-ice/NewIM/server/sync/message"
)

func requireUnread(t *testing.T, page app.Page, id string, read, unread, visibility uint64) {
	t.Helper()
	if len(page.Items) != 1 || page.HasMore {
		t.Fatalf("expected one directory upsert, got %d; hasMore=%t", len(page.Items), page.HasMore)
	}
	item := page.Items[0]
	if item.ConversationID != id || item.Kind != "upsert" ||
		item.LastReadSeq == nil || *item.LastReadSeq != read ||
		item.UnreadCount == nil || *item.UnreadCount != unread ||
		item.VisibilityRevision == nil || *item.VisibilityRevision != visibility {
		t.Fatalf("unexpected unread projection (want read=%d unread=%d visibility=%d): %+v", read, unread, visibility, item)
	}
}

func TestMessageVisibilityProjection(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(21)
	keys := writeKeys(t, "visibility", "visibility")
	gateway, second := f.accountSettingsGateway(keys), f.accountSettingsGateway(keys)
	pair := accountPair(t, f, gateway, "visibility")
	id := pair.conversation

	start := readPage(t, f, gateway, bootstrapPath, pair.owner, map[string]any{})
	requireUnread(t, start, id, 0, 0, 0)
	first := send(t, f, gateway, pair.owner, id, "visibility_own", "outgoing")
	secondMessage := send(t, f, second, pair.peer, id, "visibility_peer", "incoming")

	owner := readPage(t, f, gateway, bootstrapPath, pair.owner, map[string]any{})
	peer := readPage(t, f, second, bootstrapPath, pair.peer, map[string]any{})
	requireUnread(t, owner, id, 0, 1, 0)
	requireUnread(t, peer, id, 0, 1, 0)

	advanceWatermark(t, f, gateway, pair.owner, id, "1")
	read := readPage(t, f, second, deltaPath, pair.tablet, checkpoint(owner.NextCursor))
	requireUnread(t, read, id, 1, 1, 0)
	advanceWatermark(t, f, second, pair.tablet, id, "0")
	requireAccountQuiet(t, f, gateway, pair.owner, read.NextCursor)

	hide := protocol.VisibilityRequest{
		ConversationID: id, ServerMsgID: secondMessage.ServerMsgID,
		RequestID: "hidden_incoming", Action: protocol.DeleteForMe,
	}
	event := messageVisibility(t, f, gateway, pair.owner, hide, http.StatusOK)
	if event.ConversationSeq != 2 || event.Revision != 1 {
		t.Fatalf("unexpected hide event: %+v", event)
	}
	if retry := messageVisibility(t, f, second, pair.tablet, hide, http.StatusOK); retry != event {
		t.Fatal("same-account retry returned a different event")
	}
	conflict := hide
	conflict.ServerMsgID = first.ServerMsgID
	messageVisibility(t, f, gateway, pair.owner, conflict, http.StatusConflict)
	hidden := readPage(t, f, second, deltaPath, pair.tablet, checkpoint(read.NextCursor))
	requireUnread(t, hidden, id, 1, 0, 1)
	requireAccountQuiet(t, f, gateway, pair.peer, peer.NextCursor)
	floor, tail, personal := accountMessageHistory(t, f, second, pair.tablet, id)
	if floor != "0" || tail != "2" || len(personal) != 2 {
		t.Fatal("personal deletion changed the shared sequence")
	}
	requireHiddenMessage(t, personal[1], secondMessage.ServerMsgID, "2", protocol.DeleteForMe)
	_, _, shared := accountMessageHistory(t, f, gateway, pair.peer, id)
	if len(shared) != 2 || shared[1].Type != "text" || !bytes.Contains(shared[1].Payload, []byte("incoming")) {
		t.Fatal("personal deletion altered the peer's history")
	}

	recall := protocol.VisibilityRequest{
		ConversationID: id, ServerMsgID: first.ServerMsgID, RequestID: "recall_outgoing", Action: protocol.Recall,
	}
	messageVisibility(t, f, second, pair.peer, recall, http.StatusForbidden)
	admin := recall
	admin.Action = protocol.AdminRemove
	messageVisibility(t, f, gateway, pair.owner, admin, http.StatusForbidden)
	event = messageVisibility(t, f, gateway, pair.owner, recall, http.StatusOK)
	if event.ConversationSeq != 1 || event.Revision != 2 {
		t.Fatalf("unexpected recall event: %+v", event)
	}
	recalled := readPage(t, f, second, deltaPath, pair.peer, checkpoint(peer.NextCursor))
	requireUnread(t, recalled, id, 0, 0, 2)
	privatePage := visibilityPage(t, f, gateway, pair.tablet, id, 0, 2, 1, http.StatusOK)
	if len(privatePage.Items) != 1 || privatePage.Items[0].Revision != 1 ||
		privatePage.Items[0].Action != protocol.DeleteForMe || !privatePage.HasNext || privatePage.NextAfterRevision != 1 {
		t.Fatalf("unexpected personal event page: %+v", privatePage)
	}
	terminal := visibilityPage(t, f, second, pair.tablet, id, privatePage.NextAfterRevision, 2, 1, http.StatusOK)
	peerEvents := visibilityPage(t, f, gateway, pair.peer, id, 0, 2, 1, http.StatusOK)
	for _, page := range []syncmessage.VisibilityPage{terminal, peerEvents} {
		if len(page.Items) != 1 || page.Items[0].ServerMsgID != first.ServerMsgID ||
			page.Items[0].Action != protocol.Recall || page.Items[0].CountAsUnread ||
			page.HasNext || page.NextAfterRevision != 2 {
			t.Fatalf("unexpected shared event page: %+v", page)
		}
	}
	visibilityPage(t, f, gateway, pair.peer, id, 0, 3, 1, http.StatusBadRequest)
	_, _, shared = accountMessageHistory(t, f, gateway, pair.peer, id)
	requireHiddenMessage(t, shared[0], first.ServerMsgID, "1", protocol.Recall)
	socket := accountHistorySocket(t, f, second, pair.peer, id)
	kind, raw, err := socket.Read(f.ctx)
	must(t, err)
	frame, err := protocol.DecodeServerFrame(raw)
	must(t, err)
	if kind != websocket.MessageText || frame.Message == nil {
		t.Fatal("WSS replay omitted the sequence slot for a recalled message")
	}
	requireHiddenMessage(t, *frame.Message, first.ServerMsgID, "1", protocol.Recall)
	send(t, f, second, pair.peer, id, "visibility_peer_late", "incoming after recall")
	requireUnread(t, readPage(t, f, gateway, bootstrapPath, pair.peer, map[string]any{}), id, 0, 0, 2)
	requireUnread(t, readPage(t, f, second, bootstrapPath, pair.tablet, map[string]any{}), id, 1, 1, 2)

	accountState(t, f, gateway, http.MethodPost, pair.owner, id, "clear", settingsVersion(0))
	cleared := visibilityPage(t, f, second, pair.tablet, id, 0, 2, 1, http.StatusOK)
	if len(cleared.Items) != 0 || cleared.HasNext || cleared.NextAfterRevision != 2 {
		t.Fatalf("clear did not bound the event page: %+v", cleared)
	}
	requireUnread(t, readPage(t, f, second, bootstrapPath, pair.tablet, map[string]any{}), id, 1, 0, 2)
	send(t, f, second, pair.peer, id, "visibility_peer_new", "incoming after clear")
	requireUnread(t, readPage(t, f, gateway, bootstrapPath, pair.owner, map[string]any{}), id, 1, 1, 2)

	const groupID = "visibility_group"
	createGroup(t, f, gateway, pair.owner, groupID, []string{"visibility_peer"}, true)
	groupMessage := send(t, f, gateway, pair.peer, groupID, "group_visibility", "group message")
	remove := protocol.VisibilityRequest{
		ConversationID: groupID, ServerMsgID: groupMessage.ServerMsgID,
		RequestID: "remove_group_message", Action: protocol.AdminRemove,
	}
	messageVisibility(t, f, gateway, pair.peer, remove, http.StatusForbidden)
	messageVisibility(t, f, second, pair.owner, remove, http.StatusOK)
	_, _, shared = accountMessageHistory(t, f, gateway, pair.peer, groupID)
	if len(shared) != 1 {
		t.Fatal("group management removal changed the sequence")
	}
	requireHiddenMessage(t, shared[0], groupMessage.ServerMsgID, "1", protocol.AdminRemove)
}

func messageVisibility(t *testing.T, f *fixture, gateway *process, token string, request protocol.VisibilityRequest, want int) protocol.VisibilityEvent {
	t.Helper()
	reply := f.call(gateway, http.MethodPost, messagehttp.VisibilityRoute, token, encode(t, request), nil)
	requireStatus(t, reply, want)
	if !reply.verified {
		t.Fatal("message visibility did not use verified TLS")
	}
	if want != http.StatusOK {
		return protocol.VisibilityEvent{}
	}
	var event protocol.VisibilityEvent
	must(t, json.Unmarshal(reply.body, &event))
	if event.ConversationID != request.ConversationID || event.ServerMsgID != request.ServerMsgID ||
		event.Action != request.Action || event.CountAsUnread || !bytes.Contains(reply.body, []byte(`"countAsUnread":false`)) {
		t.Fatalf("unexpected visibility response: %+v", event)
	}
	return event
}

func requireHiddenMessage(t *testing.T, frame protocol.Message, id, sequence string, action protocol.VisibilityAction) {
	t.Helper()
	want, err := protocol.EncodeHiddenPayload(action)
	must(t, err)
	if frame.ServerMsgID != id || frame.ConversationSeq != sequence ||
		frame.Type != protocol.HiddenMessageType || !bytes.Equal(frame.Payload, want) {
		t.Fatalf("missing the body-free placeholder for sequence %s", sequence)
	}
}

func visibilityPage(t *testing.T, f *fixture, gateway *process, token, id string, after, through int64, limit int, status int) syncmessage.VisibilityPage {
	t.Helper()
	wire := encode(t, map[string]any{
		"conversationId": id, "afterRevision": strconv.FormatInt(after, 10),
		"throughRevision": strconv.FormatInt(through, 10), "limit": limit,
	})
	reply := f.call(gateway, http.MethodPost, syncmessage.VisibilityRoute, token, wire, nil)
	requireStatus(t, reply, status)
	if !reply.verified {
		t.Fatal("visibility events were not read over verified TLS")
	}
	var page syncmessage.VisibilityPage
	if status == http.StatusOK {
		must(t, json.Unmarshal(reply.body, &page))
		if page.Items == nil {
			t.Fatal("visibility response omitted its event list")
		}
	}
	return page
}
