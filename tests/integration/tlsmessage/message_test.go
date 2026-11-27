//go:build integration

package tlsmessage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

const (
	directPath  = "/api/v1/conversations/direct"
	sendPath    = "/api/v1/messages"
	blockPath   = "/api/v1/relationships/block"
	unblockPath = "/api/v1/relationships/unblock"
	socketPath  = "/api/v1/gateway"
)

func TestTLSMessageCheck(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(11)
	const aliceID = "tls_check_alice"
	const bobID = "tls_check_bob"
	alice := f.admit(aliceID, "mobile")
	bob := f.admit(bobID, "desktop")
	writer, receiver := f.gateway("1", nil), f.gateway("1", nil)
	conversation := createDirect(t, f, writer, alice, bobID)
	if reverse := createDirect(t, f, receiver, bob, aliceID); reverse != conversation {
		t.Fatal("both endpoints did not get the same committed conversation")
	}
	var kind, low, high string
	var members int
	must(t, f.db.QueryRow(f.ctx, `SELECT kind,direct_user_low,direct_user_high,
		(SELECT count(*) FROM newim.im_conversation_members WHERE conversation_id=$1)
		FROM newim.im_conversations WHERE conversation_id=$1`, conversation).Scan(&kind, &low, &high, &members))
	if kind != "direct" || low != aliceID || high != bobID || members != 2 {
		t.Fatal("direct conversation and verified member records did not commit together")
	}

	stream := subscribe(t, f, receiver, bob, conversation, 0)
	const clientID = "first_committed"
	const text = "hello from native TLS client"
	wire := textBody(t, clientID, conversation, text)
	first := sendWithCommitBarrier(t, f, writer, alice, conversation, wire)
	ack := expectACK(t, first)
	requireCommitted(t, f, ack, aliceID, conversation, clientID, text, 1)
	requireDelivered(t, f, stream, ack, text)
	replay := f.call(receiver, http.MethodPost, sendPath, alice, wire, nil)
	expectACK(t, replay)
	if !bytes.Equal(first.body, replay.body) || state(t, f, conversation).lastSeq != 1 {
		t.Fatal("cross-gateway same-intent retry did not return the committed receipt unchanged")
	}
	reverseText := "response over the same conversation"
	reply := expectACK(t, f.call(receiver, http.MethodPost, sendPath, bob,
		textBody(t, "peer_response", conversation, reverseText), nil))
	requireCommitted(t, f, reply, bobID, conversation, "peer_response", reverseText, 2)
	requireDelivered(t, f, stream, reply, reverseText)
	writer.requireRedacted(t, alice, bob, text, testDSN)
	receiver.requireRedacted(t, alice, bob, text, reverseText, testDSN)
}

func sendWithCommitBarrier(t *testing.T, f *fixture, gateway *process, token, conversation string, wire []byte) response {
	t.Helper()
	locker, err := pgx.Connect(f.ctx, testDSN)
	must(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = locker.Close(ctx)
	}()
	tx, err := locker.Begin(f.ctx)
	must(t, err)
	_, err = tx.Exec(f.ctx, "LOCK TABLE newim.im_outbox_events IN SHARE MODE")
	must(t, err)
	result := make(chan response, 1)
	go func() { result <- f.call(gateway, http.MethodPost, sendPath, token, wire, nil) }()
	by := time.NewTimer(800 * time.Millisecond)
	defer by.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
waiting:
	for {
		var waiting bool
		must(t, f.db.QueryRow(f.ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks
			WHERE relation='newim.im_outbox_events'::regclass AND mode='RowExclusiveLock' AND NOT granted)`).Scan(&waiting))
		if waiting {
			break waiting
		}
		select {
		case reply := <-result:
			t.Fatalf("send returned before the commit barrier, status=%d err=%v", reply.status, reply.err)
		case <-by.C:
			t.Fatal("send did not reach the database commit barrier")
		case <-tick.C:
		}
	}
	if current := state(t, f, conversation); current != (durableState{}) {
		t.Fatal("uncommitted send exposed durable state before the outbox write")
	}
	select {
	case reply := <-result:
		t.Fatalf("send returned while its transaction was blocked, status=%d err=%v", reply.status, reply.err)
	default:
	}
	must(t, tx.Commit(f.ctx))
	return <-result
}

func createDirect(t *testing.T, f *fixture, gateway *process, token, peer string) string {
	t.Helper()
	reply := f.call(gateway, http.MethodPost, directPath, token, peerBody(t, peer), nil)
	requireStatus(t, reply, http.StatusOK)
	var view map[string]string
	must(t, json.Unmarshal(reply.body, &view))
	if len(view) != 1 || view["conversationId"] == "" {
		t.Fatal("direct conversation response did not contain the committed ID")
	}
	return view["conversationId"]
}

func setBlock(t *testing.T, f *fixture, gateway *process, route, token, peer string) {
	t.Helper()
	reply := f.call(gateway, http.MethodPost, route, token, peerBody(t, peer), nil)
	requireStatus(t, reply, http.StatusNoContent)
	if len(reply.body) != 0 {
		t.Fatal("relationship response was not empty")
	}
}

func peerBody(t *testing.T, peer string) []byte {
	t.Helper()
	wire, err := json.Marshal(map[string]string{"peerUserId": peer})
	must(t, err)
	return wire
}

func textBody(t *testing.T, client, conversation, text string) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"text": text})
	must(t, err)
	wire, err := protocol.EncodeSend(protocol.Send{ProtocolVersion: 1, ClientMsgID: client, ConversationID: conversation,
		Version: 1, Type: "text", Payload: payload})
	must(t, err)
	return wire
}

func requireStatus(t *testing.T, reply response, status int) {
	t.Helper()
	if reply.err != nil || reply.status != status || !reply.verified {
		t.Fatalf("TLS message response status=%d verified=%t err=%v want=%d", reply.status, reply.verified, reply.err, status)
	}
	if reply.headers.Get("Cache-Control") != "no-store" || reply.headers.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("TLS message response omitted security headers")
	}
}

func expectACK(t *testing.T, reply response) protocol.Ack {
	t.Helper()
	requireStatus(t, reply, http.StatusOK)
	frame, err := protocol.DecodeServerFrame(reply.body)
	must(t, err)
	if frame.Ack == nil || frame.Error != nil || frame.Message != nil {
		t.Fatal("successful send did not return a persisted ACK frame")
	}
	return *frame.Ack
}

type durableState struct {
	lastSeq  int64
	latest   string
	messages int64
	outbox   int64
}

func state(t *testing.T, f *fixture, conversation string) durableState {
	t.Helper()
	var current durableState
	must(t, f.db.QueryRow(f.ctx, `SELECT c.last_seq,COALESCE(c.latest_server_msg_id,''),
		(SELECT count(*) FROM newim.im_messages AS m WHERE m.conversation_id=c.conversation_id),
		(SELECT count(*) FROM newim.im_outbox_events AS o JOIN newim.im_messages AS m
			ON o.server_msg_id=m.server_msg_id WHERE m.conversation_id=c.conversation_id)
		FROM newim.im_conversations AS c WHERE c.conversation_id=$1`, conversation).
		Scan(&current.lastSeq, &current.latest, &current.messages, &current.outbox))
	return current
}

func requireCommitted(t *testing.T, f *fixture, ack protocol.Ack, sender, conversation, client, text string, seq int64) {
	t.Helper()
	if ack.SenderID != sender || ack.ConversationID != conversation || ack.ClientMsgID != client ||
		ack.ConversationSeq != strconv.FormatInt(seq, 10) || ack.ServerMsgID == "" {
		t.Fatal("persisted acknowledgment did not match the trusted sender or client intent")
	}
	var dbTime, dbSequence int64
	var dbSender, dbClient, dbConversation, dbText string
	must(t, f.db.QueryRow(f.ctx, `SELECT m.sender_id,m.client_msg_id,m.conversation_id,m.conversation_seq,
		m.server_time_ms,convert_from(m.payload_bytes,'UTF8')::jsonb->>'text'
		FROM newim.im_messages AS m JOIN newim.im_outbox_events AS o ON o.server_msg_id=m.server_msg_id
		WHERE m.server_msg_id=$1`, ack.ServerMsgID).Scan(&dbSender, &dbClient, &dbConversation, &dbSequence, &dbTime, &dbText))
	if dbSender != sender || dbClient != client || dbConversation != conversation || dbSequence != seq || dbText != text ||
		ack.ServerTime != strconv.FormatInt(dbTime, 10) {
		t.Fatal("acknowledgment did not match the committed message and outbox")
	}
	if current := state(t, f, conversation); current != (durableState{lastSeq: seq, latest: ack.ServerMsgID, messages: seq, outbox: seq}) {
		t.Fatal("transaction did not commit one message and outbox per allocated sequence")
	}
}

func dialSocket(t *testing.T, f *fixture, gateway *process, token string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	return websocket.Dial(ctx, "wss://"+strings.TrimPrefix(gateway.url, "https://")+socketPath,
		&websocket.DialOptions{HTTPClient: f.client, HTTPHeader: http.Header{"Authorization": {"Bearer " + token}},
			Subprotocols: []string{"newim.v1"}})
}

func subscribe(t *testing.T, f *fixture, gateway *process, token, conversation string, after int64) *websocket.Conn {
	t.Helper()
	conn, reply, err := dialSocket(t, f, gateway, token)
	requireSocketStatus(t, conn, reply, err, http.StatusSwitchingProtocols)
	t.Cleanup(func() { _ = conn.CloseNow() })
	wire, err := json.Marshal(map[string]string{"conversationId": conversation, "afterSeq": strconv.FormatInt(after, 10)})
	must(t, err)
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	must(t, conn.Write(ctx, websocket.MessageText, wire))
	conn.SetReadLimit(protocol.MaxFrameBytes)
	return conn
}

func requireSocketStatus(t *testing.T, conn *websocket.Conn, reply *http.Response, err error, want int) {
	t.Helper()
	if err != nil && reply != nil && reply.Body != nil {
		defer reply.Body.Close()
	}
	if reply == nil || reply.StatusCode != want || (want == http.StatusSwitchingProtocols) != (conn != nil && err == nil) {
		status := 0
		if reply != nil {
			status = reply.StatusCode
		}
		t.Fatalf("WSS status=%d err=%v want=%d", status, err, want)
	}
	if reply.TLS == nil || len(reply.TLS.VerifiedChains) == 0 || (conn != nil && conn.Subprotocol() != "newim.v1") {
		t.Fatal("gateway did not negotiate the native protocol over verified TLS")
	}
}

func requireDelivered(t *testing.T, f *fixture, conn *websocket.Conn, ack protocol.Ack, text string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 8*time.Second)
	defer cancel()
	kind, raw, err := conn.Read(ctx)
	must(t, err)
	if kind != websocket.MessageText {
		t.Fatal("gateway did not deliver a text frame")
	}
	frame, err := protocol.DecodeServerFrame(raw)
	must(t, err)
	msg := frame.Message
	if msg == nil || msg.SenderID != ack.SenderID || msg.ClientMsgID != ack.ClientMsgID ||
		msg.ServerMsgID != ack.ServerMsgID || msg.ConversationID != ack.ConversationID ||
		msg.ConversationSeq != ack.ConversationSeq || msg.ServerTime != ack.ServerTime || msg.Version != 1 || msg.Type != "text" {
		t.Fatal("WSS delivery did not match the committed ACK")
	}
	var payload map[string]string
	must(t, json.Unmarshal(msg.Payload, &payload))
	if len(payload) != 1 || payload["text"] != text {
		t.Fatal("WSS delivery did not preserve the committed payload")
	}
}
