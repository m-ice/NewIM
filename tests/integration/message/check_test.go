//go:build integration

package message_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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

	t.Run("duplicate-media-retry-skips-revalidation", func(t *testing.T) {
		f := openFixture(t)
		conversationID := f.seedConversation("dup_media_retry_check", "alice")
		validator := &flipMediaValidator{}
		service, err := app.NewService(f.repo, app.Config{
			IDs:            &sequenceIDs{prefix: "dup_media"},
			Clock:          app.ClockFunc(func() time.Time { return time.UnixMilli(f.now.Load()).UTC() }),
			MediaValidator: validator,
		})
		must(t, err)
		payload, err := protocol.EncodeMediaPayload(protocol.MediaMetadata{
			MediaKey: "dup_media_key", Kind: "image", ContentType: "image/jpeg",
			Size: "12", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		})
		must(t, err)
		request := protocol.Send{
			ProtocolVersion: 1, ClientMsgID: "dup_media_client", ConversationID: conversationID,
			Version: 1, Type: "media", Payload: payload,
		}
		identity := f.identity("alice")

		first, err := service.Send(context.Background(), identity, request)
		must(t, err)
		second, err := service.Send(context.Background(), identity, request)
		must(t, err)
		if first.Ack == nil || second.Ack == nil || first.Ack.ServerMsgID != second.Ack.ServerMsgID ||
			first.Ack.ConversationSeq != second.Ack.ConversationSeq {
			t.Fatalf("duplicate ACK mismatch: first=%+v second=%+v", first.Ack, second.Ack)
		}
		if validator.calls.Load() != 1 {
			t.Fatalf("media validation calls = %d, want 1", validator.calls.Load())
		}
	})

	t.Run("duplicate-retry-skips-id-generation", func(t *testing.T) {
		f := openFixture(t)
		conversationID := f.seedConversation("dup_text_retry_check", "alice")
		ids := &limitedIDs{}
		service, err := app.NewService(f.repo, app.Config{
			IDs:   ids,
			Clock: app.ClockFunc(func() time.Time { return time.UnixMilli(f.now.Load()).UTC() }),
		})
		must(t, err)
		request := f.request("dup_text_client", conversationID, "hello")
		identity := f.identity("alice")

		first, err := service.Send(context.Background(), identity, request)
		must(t, err)
		second, err := service.Send(context.Background(), identity, request)
		must(t, err)
		if first.Ack == nil || second.Ack == nil || first.Ack.ServerMsgID != second.Ack.ServerMsgID {
			t.Fatalf("duplicate ACK mismatch: first=%+v second=%+v", first.Ack, second.Ack)
		}
		if ids.calls.Load() != 2 {
			t.Fatalf("ID generation calls = %d, want 2", ids.calls.Load())
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
