//go:build integration

package conversationsynchttp_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

const receiptRecoveryPath = "/tmp/newim-read-receipt-recovery.json"

func receiptPath(conversation, message string) string {
	return "/api/v1/conversations/" + conversation + "/messages/" + message + "/read-receipt"
}

func requireReceipt(t *testing.T, f *fixture, gateway *process, token, conversation, message string, read bool) {
	t.Helper()
	reply := f.call(gateway, http.MethodGet, receiptPath(conversation, message), token, nil, nil)
	requireStatus(t, reply, http.StatusOK)
	if !reply.verified || reply.headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatal("read receipt did not use verified TLS and JSON")
	}
	var fields map[string]json.RawMessage
	must(t, json.Unmarshal(reply.body, &fields))
	var got string
	must(t, json.Unmarshal(fields["serverMsgId"], &got))
	if len(fields) != 2 || got != message || string(fields["read"]) != strconv.FormatBool(read) {
		t.Fatal("read receipt did not return the exact committed message ID and expected boolean")
	}
}

func TestReadReceiptHTTPCheck(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(15)
	left, right := f.watermarkGateway(nil), f.watermarkGateway(nil)
	id, sender, senderOther, reader := setupWatermarkDirect(t, f, left, "receipt_check", 0)
	first := send(t, f, left, sender, id, "receipt_check_first", "first direct message")
	second := send(t, f, left, sender, id, "receipt_check_second", "second direct message")
	requireReceipt(t, f, right, senderOther, id, first.ServerMsgID, false)
	requireReceipt(t, f, left, sender, id, second.ServerMsgID, false)

	requireWatermark(t, advanceWatermark(t, f, right, reader, id, "1"), id, "1", "0", "2", true)
	requireReceipt(t, f, left, sender, id, first.ServerMsgID, true)
	requireReceipt(t, f, right, senderOther, id, first.ServerMsgID, true)
	requireReceipt(t, f, left, sender, id, second.ServerMsgID, false)

	reverse := send(t, f, right, reader, id, "receipt_check_reverse", "return direct message")
	const ownerID = "receipt_check_owner"
	const peerID = "receipt_check_peer"
	// A pre-timestamp row is still a durable account read watermark.
	f.sql("INSERT INTO newim.im_read_states(conversation_id,user_id,read_seq) VALUES($1,$2,3)", id, ownerID)
	requireWatermark(t, getWatermark(t, f, left, sender, id), id, "3", "0", "3", false)
	requireReceipt(t, f, left, reader, id, reverse.ServerMsgID, true)

	for _, pair := range []struct{ actor, target string }{{reader, ownerID}, {sender, peerID}} {
		peer := encode(t, map[string]string{"peerUserId": pair.target})
		requireStatus(t, f.call(right, http.MethodPost, "/api/v1/relationships/block", pair.actor, peer, nil), http.StatusNoContent)
		requireReceipt(t, f, left, sender, id, first.ServerMsgID, true)
		requireReceipt(t, f, right, sender, id, second.ServerMsgID, false)
		requireReceipt(t, f, left, reader, id, reverse.ServerMsgID, true)
		requireStatus(t, f.call(left, http.MethodPost, "/api/v1/relationships/unblock", pair.actor, peer, nil), http.StatusNoContent)
	}
	requireWatermark(t, advanceWatermark(t, f, right, reader, id, "2"), id, "2", "0", "3", true)
	requireReceipt(t, f, left, senderOther, id, second.ServerMsgID, true)
	left.requireRedacted(t, sender, senderOther, reader, testDSN, "first direct message")
	right.requireRedacted(t, sender, senderOther, reader, testDSN)
}

func TestReadReceiptHTTPSecurity(t *testing.T) {
	t.Run("opaque-visibility-and-private-pair", testReceiptVisibility)
	t.Run("canonical-get-revocation-and-shared-budgets", testReceiptTransport)
}

func testReceiptVisibility(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(15)
	left, right := f.watermarkGateway(nil), f.watermarkGateway(nil)
	id, sender, _, reader := setupWatermarkDirect(t, f, left, "receipt_visibility", 0)
	const ownerID = "receipt_visibility_owner"
	const peerID = "receipt_visibility_peer"
	const outsiderID = "receipt_visibility_outside"
	f.user(outsiderID)
	outsider := f.admit(outsiderID, "phone")
	first := send(t, f, left, sender, id, "receipt_visible", "known direct message")
	second := send(t, f, left, sender, id, "receipt_visible_second", "second known message")
	otherConv := f.call(left, http.MethodPost, "/api/v1/conversations/direct", sender,
		encode(t, map[string]string{"peerUserId": outsiderID}), nil)
	requireStatus(t, otherConv, http.StatusOK)
	var direct struct {
		ID string `json:"conversationId"`
	}
	must(t, json.Unmarshal(otherConv.body, &direct))
	other := send(t, f, left, sender, direct.ID, "receipt_other_conversation", "known separate direct message")

	const group = "receipt_managed_group"
	const legacy = "receipt_legacy_group"
	const legacyMessage = "receipt_legacy_message"
	createGroup(t, f, left, sender, group, []string{peerID}, true)
	groupMessage := send(t, f, left, sender, group, "receipt_group_msg", "known managed group message")
	f.sql("INSERT INTO newim.im_conversations(conversation_id,kind) VALUES($1,'group')", legacy)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2),($1,$3)", legacy, ownerID, peerID)
	f.sql("SELECT newim.persist_message($1,$2,$3,$4,'text',1,$5,$6,$7)", ownerID, "receipt_legacy_client", legacy,
		legacyMessage, time.Now().UnixMilli(), encode(t, map[string]string{"text": "legacy group message"}), "receipt_legacy_event")

	opaque := f.call(right, http.MethodGet, receiptPath(id, "missing_receipt_message"), sender, nil, nil)
	requireSyncError(t, opaque, http.StatusNotFound, "READ_NOT_FOUND")
	requireOpaque := func(token, conversation, message string) {
		t.Helper()
		reply := f.call(right, http.MethodGet, receiptPath(conversation, message), token, nil, nil)
		requireSyncError(t, reply, http.StatusNotFound, "READ_NOT_FOUND")
		if !bytes.Equal(opaque.body, reply.body) {
			t.Fatal("read receipt distinguished missing and inaccessible messages")
		}
	}
	requireOpaque(sender, "missing_receipt_conversation", first.ServerMsgID)
	requireOpaque(sender, id, other.ServerMsgID)
	requireOpaque(reader, id, first.ServerMsgID)
	requireOpaque(outsider, id, first.ServerMsgID)
	requireOpaque(sender, group, groupMessage.ServerMsgID)
	requireOpaque(sender, legacy, legacyMessage)
	requireReceipt(t, f, right, sender, id, first.ServerMsgID, false)

	f.sql("INSERT INTO newim.im_read_states(conversation_id,user_id,read_seq) VALUES($1,$2,2)", id, peerID)
	f.sql("UPDATE newim.im_conversation_members SET group_role='member',history_after_seq=1 WHERE conversation_id=$1 AND user_id=$2", id, peerID)
	requireReceipt(t, f, right, sender, id, first.ServerMsgID, false)
	requireReceipt(t, f, right, sender, id, second.ServerMsgID, true)
	f.sql("UPDATE newim.im_conversation_members SET group_role='member',history_after_seq=1 WHERE conversation_id=$1 AND user_id=$2", id, ownerID)
	requireOpaque(sender, id, first.ServerMsgID)
	requireReceipt(t, f, right, sender, id, second.ServerMsgID, true)
	f.sql("UPDATE newim.im_conversation_members SET group_role=NULL,history_after_seq=0 WHERE conversation_id=$1", id)

	f.sql("UPDATE newim.im_read_states SET read_seq=3 WHERE conversation_id=$1 AND user_id=$2", id, peerID)
	requireSyncError(t, f.call(right, http.MethodGet, receiptPath(id, first.ServerMsgID), sender, nil, nil),
		http.StatusServiceUnavailable, "READ_STORAGE_UNAVAILABLE")
	requireStoredWatermark(t, f, id, peerID, 3, nil)
	f.sql("DELETE FROM newim.im_read_states WHERE conversation_id=$1 AND user_id=$2", id, peerID)
	f.sql("UPDATE newim.im_conversations SET last_seq=1 WHERE conversation_id=$1", id)
	requireSyncError(t, f.call(right, http.MethodGet, receiptPath(id, second.ServerMsgID), sender, nil, nil),
		http.StatusServiceUnavailable, "READ_STORAGE_UNAVAILABLE")
	f.sql("UPDATE newim.im_conversations SET last_seq=2 WHERE conversation_id=$1", id)
	requireReceipt(t, f, right, sender, id, second.ServerMsgID, false)

	// A classified private conversation requires exactly its recorded active pair.
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", id, outsiderID)
	requireOpaque(sender, id, first.ServerMsgID)
	f.sql("DELETE FROM newim.im_conversation_members WHERE conversation_id=$1 AND user_id=$2", id, peerID)
	requireOpaque(sender, id, first.ServerMsgID)
	f.sql("UPDATE newim.im_conversation_members SET user_id=$2 WHERE conversation_id=$1 AND user_id=$3", id, peerID, outsiderID)
	requireReceipt(t, f, right, sender, id, first.ServerMsgID, false)
}

func testReceiptTransport(t *testing.T) {
	f := openFixture(t)
	on, off := f.watermarkGateway(nil), f.gateway("", nil)
	account := f.watermarkGateway(map[string]string{"NEWIM_READ_WATERMARK_ACCOUNT_ATTEMPTS_PER_MINUTE": "1"})
	address := f.watermarkGateway(map[string]string{"NEWIM_READ_WATERMARK_IP_ATTEMPTS_PER_MINUTE": "1"})
	id, sender, senderOther, reader := setupWatermarkDirect(t, f, on, "receipt_http", 0)
	msg := send(t, f, on, sender, id, "receipt_http_message", "authorized direct message")
	path := receiptPath(id, msg.ServerMsgID)
	requireAbsent(t, f.call(off, http.MethodGet, path, sender, nil, nil))
	requireAbsent(t, f.call(on, http.MethodGet, path+"/", sender, nil, nil))
	method := f.call(on, http.MethodPost, path, sender, nil, nil)
	requireSyncError(t, method, http.StatusMethodNotAllowed, "HTTP_METHOD_NOT_ALLOWED")
	if method.headers.Get("Allow") != "GET" {
		t.Fatal("receipt method error omitted the GET allowance")
	}
	encodedMessage := fmt.Sprintf("%%%02x%s", msg.ServerMsgID[0], msg.ServerMsgID[1:])
	encodedConversation := fmt.Sprintf("%%%02x%s", id[0], id[1:])
	for _, invalid := range []string{path + "?unused=1", path + "?", receiptPath(id, encodedMessage), receiptPath(encodedConversation, msg.ServerMsgID)} {
		requireSyncError(t, f.call(on, http.MethodGet, invalid, sender, nil, nil), http.StatusBadRequest, "READ_INVALID_INPUT")
	}
	requireSyncError(t, f.call(on, http.MethodGet, path, sender, []byte("{}"), nil), http.StatusBadRequest, "READ_INVALID_INPUT")
	requireSyncError(t, f.call(on, http.MethodGet, path, "", nil, nil), http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	requireSyncError(t, f.call(on, http.MethodGet, path, "", nil,
		http.Header{"Authorization": {"Bearer " + sender, "Bearer " + sender}}), http.StatusUnauthorized, "AUTH_INVALID_TOKEN")

	waitRateWindow()
	requireWatermark(t, getWatermark(t, f, account, sender, id), id, "0", "0", "1", false)
	requireSyncError(t, f.call(account, http.MethodGet, path, senderOther, nil, nil), http.StatusTooManyRequests, "READ_RATE_LIMITED")
	requireWatermark(t, getWatermark(t, f, account, reader, id), id, "0", "0", "1", false)
	requireReceipt(t, f, address, sender, id, msg.ServerMsgID, false)
	requireSyncError(t, f.call(address, http.MethodGet, watermarkPath(id), reader, nil, nil), http.StatusTooManyRequests, "READ_RATE_LIMITED")
	_, err := f.auth.RevokeBearer(f.ctx, sender)
	must(t, err)
	requireSyncError(t, f.call(on, http.MethodGet, path, sender, nil, nil), http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	requireReceipt(t, f, on, senderOther, id, msg.ServerMsgID, false)
	on.requireRedacted(t, sender, senderOther, reader, testDSN, "authorized direct message")
}

type receiptRecovery struct {
	Conversation string
	Sender       string
	Reader       string
	First        string
	Second       string
	Postmaster   time.Time
}

func TestReadReceiptHTTPRecovery(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(15)
	switch os.Getenv("NEWIM_SYNC_HTTP_PHASE") {
	case "prepare":
		left, right := f.watermarkGateway(nil), f.watermarkGateway(nil)
		id, sender, _, reader := setupWatermarkDirect(t, f, left, "receipt_recovery", 0)
		first := send(t, f, left, sender, id, "receipt_recovery_first", "committed before restart")
		second := send(t, f, left, sender, id, "receipt_recovery_second", "unread before restart")
		requireReceipt(t, f, right, sender, id, first.ServerMsgID, false)
		requireWatermark(t, advanceWatermark(t, f, right, reader, id, "1"), id, "1", "0", "2", true)
		requireReceipt(t, f, left, sender, id, first.ServerMsgID, true)
		requireReceipt(t, f, left, sender, id, second.ServerMsgID, false)
		state := receiptRecovery{Conversation: id, Sender: sender, Reader: reader, First: first.ServerMsgID,
			Second: second.ServerMsgID, Postmaster: watermarkPostmaster(t, f)}
		must(t, os.WriteFile(receiptRecoveryPath, encode(t, state), 0o600))
	case "restart":
		raw, err := os.ReadFile(receiptRecoveryPath)
		must(t, err)
		t.Cleanup(func() { _ = os.Remove(receiptRecoveryPath) })
		var state receiptRecovery
		must(t, json.Unmarshal(raw, &state))
		if state.Postmaster.IsZero() || !watermarkPostmaster(t, f).After(state.Postmaster) {
			t.Fatal("receipt recovery did not start a new PostgreSQL postmaster")
		}
		left, right := f.watermarkGateway(nil), f.watermarkGateway(nil)
		requireReceipt(t, f, left, state.Sender, state.Conversation, state.First, true)
		requireReceipt(t, f, right, state.Sender, state.Conversation, state.Second, false)
		requireWatermark(t, advanceWatermark(t, f, right, state.Reader, state.Conversation, "2"),
			state.Conversation, "2", "0", "2", true)
		requireReceipt(t, f, left, state.Sender, state.Conversation, state.Second, true)
	default:
		t.Fatal("NEWIM_SYNC_HTTP_PHASE must be prepare or restart")
	}
}
