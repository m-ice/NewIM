//go:build integration

package group_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

const (
	directPath  = "/api/v1/conversations/direct"
	sendPath    = "/api/v1/messages"
	blockPath   = "/api/v1/relationships/block"
	unblockPath = "/api/v1/relationships/unblock"
	socketPath  = "/api/v1/gateway"
)

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

func send(t *testing.T, f *fixture, gateway *process, token, conversation, client, text string) protocol.Ack {
	t.Helper()
	return expectACK(t, f.call(gateway, http.MethodPost, sendPath, token, textBody(t, client, conversation, text), nil))
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
