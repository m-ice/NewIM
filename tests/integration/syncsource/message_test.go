//go:build integration

package syncsource_test

import (
	"encoding/json"
	"net/http"
	"testing"

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
	if reply.err != nil || reply.status != status {
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
