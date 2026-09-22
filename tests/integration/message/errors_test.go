//go:build integration

package message_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	app "github.com/m-ice/NewIM/server/message"
)

func TestMessageErrors(t *testing.T) {
	t.Run("permanent-errors", func(t *testing.T) {
		f := openFixture(t)
		user := "errors_permanent_user"
		conversationID := f.seedConversation("errors_permanent", user)
		identity := f.identity(user)
		service := f.service(nil)

		invalid := f.request("errors_invalid", conversationID, "invalid")
		invalid.Payload = json.RawMessage(`{"text":`)
		if _, err := f.send(service, identity, invalid); err == nil {
			t.Fatal("invalid request unexpectedly succeeded")
		} else {
			mustCode(t, err, app.SendInvalidInput)
			mustDisposition(t, err, protocol.StopAutomaticRetry)
		}

		unauthorizedConversation := f.seedConversation("errors_unauthorized", "other_user")
		_, err := f.send(service, identity, f.request("errors_unauthorized", unauthorizedConversation, "denied"))
		mustCode(t, err, app.SendUnauthorized)
		mustDisposition(t, err, protocol.StopAutomaticRetry)

		_, err = f.send(service, identity, f.request("errors_missing", "errors_missing_conversation", "missing"))
		mustCode(t, err, app.SendConversationMissing)
		mustDisposition(t, err, protocol.StopAutomaticRetry)

		f.sql("UPDATE newim.im_conversations SET last_seq=9223372036854775807 WHERE conversation_id=$1", conversationID)
		_, err = f.send(service, identity, f.request("errors_exhausted", conversationID, "exhausted"))
		mustCode(t, err, app.SendSequenceExhausted)
		mustDisposition(t, err, protocol.StopAutomaticRetry)
	})

	t.Run("retryable-and-unknown-errors", func(t *testing.T) {
		f := openFixture(t)
		user := "errors_retry_user"
		conversationID := f.seedConversation("errors_retry", user)
		identity := f.identity(user)
		service := f.service(nil)

		blocker, err := f.db.Begin(ctx)
		must(t, err)
		var locked string
		err = blocker.QueryRow(ctx, "SELECT conversation_id FROM newim.im_conversations WHERE conversation_id=$1 FOR UPDATE", conversationID).Scan(&locked)
		must(t, err)
		shortCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err = service.Send(shortCtx, identity, f.request("errors_lock", conversationID, "locked"))
		cancel()
		must(t, blocker.Rollback(ctx))
		mustCode(t, err, app.SendLockUnavailable)
		mustDisposition(t, err, protocol.RetrySameIntent)
		if got := app.ProtocolCode(err); got != "SERVER_TEMPORARY_UNAVAILABLE" {
			t.Fatalf("lock protocol code got %s", got)
		}

		storageService, err := app.NewService(&errorStore{err: app.Fail(app.SendStorageUnavailable)}, app.Config{
			IDs: &sequenceIDs{prefix: "errors_storage"}, Clock: app.ClockFunc(time.Now),
		})
		must(t, err)
		_, err = storageService.Send(ctx, identity, f.request("errors_storage", conversationID, "storage"))
		mustCode(t, err, app.SendStorageUnavailable)
		mustDisposition(t, err, protocol.RetrySameIntent)

		unknownService, err := app.NewService(&errorStore{err: errors.New("unclassified")}, app.Config{
			IDs: &sequenceIDs{prefix: "errors_unknown"}, Clock: app.ClockFunc(time.Now),
		})
		must(t, err)
		_, err = unknownService.Send(ctx, identity, f.request("errors_unknown", conversationID, "unknown"))
		mustCode(t, err, app.SendUnknown)
		mustDisposition(t, err, protocol.StopAutomaticRetry)
	})
}
