//go:build integration

package accountrevocation_test

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

const socketPath = "/api/v1/gateway"

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
