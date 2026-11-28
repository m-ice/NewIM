//go:build integration

package conversationsynchttp_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/messagehttp"
	app "github.com/m-ice/NewIM/server/sync/conversation"
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
	send(t, f, second, pair.peer, id, "visibility_peer_late", "incoming after recall")
	requireUnread(t, readPage(t, f, gateway, bootstrapPath, pair.peer, map[string]any{}), id, 0, 0, 2)
	requireUnread(t, readPage(t, f, second, bootstrapPath, pair.tablet, map[string]any{}), id, 1, 1, 2)

	accountState(t, f, gateway, http.MethodPost, pair.owner, id, "clear", settingsVersion(0))
	requireUnread(t, readPage(t, f, second, bootstrapPath, pair.tablet, map[string]any{}), id, 1, 0, 2)
	send(t, f, second, pair.peer, id, "visibility_peer_new", "incoming after clear")
	requireUnread(t, readPage(t, f, gateway, bootstrapPath, pair.owner, map[string]any{}), id, 1, 1, 2)
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
