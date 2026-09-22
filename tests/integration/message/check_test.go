//go:build integration

package message_test

import (
	"encoding/json"
	"testing"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	app "github.com/m-ice/NewIM/server/message"
)

func TestMessageCheck(t *testing.T) {
	t.Run("identical-retry-returns-original", func(t *testing.T) {
		f := openFixture(t)
		user := "check_retry_user"
		conversationID := f.seedConversation("check_retry", user)
		identity := f.identity(user)
		service := f.service(nil)

		firstRequest := f.request("check_retry_client", conversationID, "hello")
		firstRequest.Payload = json.RawMessage(`{"text":"hello","n":1}`)
		firstFrame, err := f.send(service, identity, firstRequest)
		must(t, err)
		firstAck := ack(t, firstFrame)
		f.assertAtomic(conversationID, firstAck.ServerMsgID)

		f.now.Add(3000)
		secondRequest := firstRequest
		secondRequest.Payload = json.RawMessage(`{"n":1.0,"text":"hello"}`)
		secondFrame, err := f.send(service, identity, secondRequest)
		must(t, err)
		secondAck := ack(t, secondFrame)
		if secondAck.ServerMsgID != firstAck.ServerMsgID || secondAck.ConversationSeq != firstAck.ConversationSeq || secondAck.ServerTime != firstAck.ServerTime {
			t.Fatalf("retry result changed: first=%+v second=%+v", firstAck, secondAck)
		}
		if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", user, firstRequest.ClientMsgID); got != 1 {
			t.Fatalf("retry message count got %d want 1", got)
		}
		if got := f.scalarInt64("SELECT count(*) FROM newim.im_outbox_events WHERE server_msg_id=$1", firstAck.ServerMsgID); got != 1 {
			t.Fatalf("retry outbox count got %d want 1", got)
		}
		if err = protocol.Correlate(secondRequest, user, secondFrame); err != nil {
			t.Fatalf("retry ACK correlation failed: %v", err)
		}
	})

	t.Run("unequal-intent-conflict-is-permanent-and-non-mutating", func(t *testing.T) {
		f := openFixture(t)
		user := "check_conflict_user"
		conversationID := f.seedConversation("check_conflict", user)
		identity := f.identity(user)
		service := f.service(nil)

		request := f.request("check_conflict_client", conversationID, "original")
		frame, err := f.send(service, identity, request)
		must(t, err)
		original := ack(t, frame)

		conflict := request
		conflict.Payload = json.RawMessage(`{"text":"different"}`)
		if _, err = f.send(service, identity, conflict); err == nil {
			t.Fatal("unequal intent unexpectedly succeeded")
		}
		mustCode(t, err, app.SendIDConflict)
		mustDisposition(t, err, protocol.StopAutomaticRetry)
		if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", user, request.ClientMsgID); got != 1 {
			t.Fatalf("conflict changed message count to %d", got)
		}
		if got := f.scalarString("SELECT convert_from(payload_bytes,'UTF8') FROM newim.im_messages WHERE server_msg_id=$1", original.ServerMsgID); got != `{"text":"original"}` {
			t.Fatalf("conflict changed original payload: %s", got)
		}
	})

	t.Run("ack-follows-visible-commit-and-carries-exact-identity", func(t *testing.T) {
		f := openFixture(t)
		user := "check_commit_user"
		conversationID := f.seedConversation("check_commit", user)
		identity := f.identity(user)
		service := f.service(nil)
		request := f.request("check_commit_client", conversationID, "committed")

		frame, err := f.send(service, identity, request)
		must(t, err)
		result := ack(t, frame)
		if result.ClientMsgID != request.ClientMsgID || result.ConversationID != conversationID || result.SenderID != user {
			t.Fatalf("ACK identity mismatch: %+v", result)
		}
		if result.ServerMsgID == "" || result.ConversationSeq == "" || result.ServerTime == "" {
			t.Fatalf("ACK missing committed metadata: %+v", result)
		}
		if !f.scalarBool("SELECT EXISTS(SELECT 1 FROM newim.im_messages WHERE server_msg_id=$1)", result.ServerMsgID) {
			t.Fatal("ACK returned before committed message was visible")
		}
		if err = protocol.Correlate(request, user, frame); err != nil {
			t.Fatalf("ACK correlation failed: %v", err)
		}
	})
}
