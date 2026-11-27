//go:build integration

package conversationsynchttp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

const sendPath = "/api/v1/messages"

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
		t.Fatalf("source fixture response status=%d verified=%t err=%v want=%d", reply.status, reply.verified, reply.err, status)
	}
	if reply.headers.Get("Cache-Control") != "no-store" || reply.headers.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("source fixture response omitted security headers")
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
