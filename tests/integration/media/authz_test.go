//go:build integration

package media_test

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	mediaApp "github.com/m-ice/NewIM/server/media"
	messageApp "github.com/m-ice/NewIM/server/message"
	messageStore "github.com/m-ice/NewIM/server/storage/message"
)

type sequenceIDs struct {
	prefix string
	next   atomic.Int64
}

func (g *sequenceIDs) NewID() (string, error) {
	return fmt.Sprintf("%s_%06d", g.prefix, g.next.Add(1)), nil
}

func messageService(t *testing.T, f *fixture, validator messageApp.MediaValidator) (*messageApp.Service, *messageStore.Repository) {
	t.Helper()
	repo, err := messageStore.Open(ctx, messageStore.Config{
		DSN:              fmt.Sprintf("host=/var/run/postgresql user=newim_test dbname=%s sslmode=disable", "newim_test"),
		AllowLocalSocket: true, MaxConnections: 4, ApplicationName: "nim_media_message_test",
	})
	must(t, err)
	t.Cleanup(repo.Close)
	service, err := messageApp.NewService(repo, messageApp.Config{
		IDs: &sequenceIDs{prefix: unique("media_message_id")}, Clock: f.clock(), MediaValidator: validator,
	})
	must(t, err)
	return service, repo
}

func mediaPayload(t *testing.T, mediaKey, kind, contentType string, size int64, digest [32]byte) []byte {
	t.Helper()
	payload, err := protocol.EncodeMediaPayload(protocol.MediaMetadata{
		MediaKey: mediaKey, Kind: kind, ContentType: contentType,
		Size: fmt.Sprint(size), SHA256: fmt.Sprintf("%x", digest),
	})
	must(t, err)
	return payload
}

func sendRequest(clientID, conversationID string, payload []byte) protocol.Send {
	return protocol.Send{
		ProtocolVersion: 1, ClientMsgID: clientID, ConversationID: conversationID,
		Version: 1, Type: "media", Payload: payload,
	}
}

func assertNoMessageRows(t *testing.T, f *fixture, clientID string) {
	t.Helper()
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE client_msg_id=$1", clientID); got != 0 {
		t.Fatalf("message rows for %s got %d want 0", clientID, got)
	}
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_outbox_events o JOIN newim.im_messages m ON m.server_msg_id=o.server_msg_id WHERE m.client_msg_id=$1", clientID); got != 0 {
		t.Fatalf("outbox rows for %s got %d want 0", clientID, got)
	}
}

func TestMediaAuthz(t *testing.T) {
	f := openFixture(t)
	ownerID := unique("authz_owner")
	nonmemberID := unique("authz_nonmember")
	memberID := unique("authz_member")
	conversation := unique("authz_conversation")
	otherConversation := unique("authz_other_conversation")
	owner := f.seedIdentity(ownerID)
	nonmember := f.seedIdentity(nonmemberID)
	member := f.seedIdentity(memberID)
	f.seedConversation(conversation, ownerID, memberID)
	f.seedConversation(otherConversation, ownerID, memberID)
	objects := openLocalStore(t, mediaRoot(t, "authz"))
	signer := &recordingSigner{url: "https://private.invalid/download"}
	mediaService := newService(t, f, objects, signer, deterministicEntropy(3))
	body := content(48, 9)
	grant, err := mediaService.BeginUpload(ctx, owner, beginRequest(conversation, body, time.Minute))
	must(t, err)
	asset, err := mediaService.CompleteUpload(ctx, owner, grant.RawToken, bytes.NewReader(body))
	must(t, err)

	url, err := mediaService.ResolvePrivateDownload(ctx, owner, asset.MediaKey, 0)
	must(t, err)
	if url != signer.url || signer.Calls() != 1 {
		t.Fatalf("owner download signer calls=%d url=%q", signer.Calls(), url)
	}
	_, err = mediaService.ResolvePrivateDownload(ctx, nonmember, asset.MediaKey, 0)
	wantMediaCode(t, err, mediaApp.MediaUnauthorized)
	if signer.Calls() != 1 {
		t.Fatalf("nonmember called signer: %d", signer.Calls())
	}
	_, err = mediaService.ResolvePrivateDownload(ctx, owner, unique("missing_media"), 0)
	wantMediaCode(t, err, mediaApp.MediaNotFound)
	if signer.Calls() != 1 {
		t.Fatalf("missing asset called signer: %d", signer.Calls())
	}
	_, err = mediaService.ResolvePrivateDownload(ctx, owner, asset.MediaKey, mediaApp.MaxDownloadTTL*time.Second+time.Second)
	wantMediaCode(t, err, mediaApp.MediaInvalidInput)
	if signer.Calls() != 1 {
		t.Fatalf("invalid TTL called signer: %d", signer.Calls())
	}

	beforeGrants := f.scalarInt64("SELECT count(*) FROM newim.im_media_assets")
	_, err = mediaService.BeginUpload(ctx, nonmember, beginRequest(conversation, content(8, 1), time.Minute))
	wantMediaCode(t, err, mediaApp.MediaUnauthorized)
	if after := f.scalarInt64("SELECT count(*) FROM newim.im_media_assets"); after != beforeGrants {
		t.Fatalf("unauthorized begin changed grant count: %d -> %d", beforeGrants, after)
	}
	revoked := f.seedIdentity(unique("authz_revoked"))
	f.seedConversation(conversation, revoked.UserID())
	f.sql("UPDATE newim.im_sessions SET revoked_at=$2 WHERE session_id=$1", revoked.SessionID(), time.UnixMilli(f.now.Load()).UTC())
	_, err = mediaService.BeginUpload(ctx, revoked, beginRequest(conversation, content(8, 2), time.Minute))
	wantMediaCode(t, err, mediaApp.MediaUnauthorized)
	if after := f.scalarInt64("SELECT count(*) FROM newim.im_media_assets"); after != beforeGrants {
		t.Fatalf("revoked begin changed grant count: %d -> %d", beforeGrants, after)
	}

	msgService, _ := messageService(t, f, mediaService)
	validClient := unique("valid_media")
	validPayload := mediaPayload(t, asset.MediaKey, asset.Kind, asset.ContentType, *asset.ActualSize, asset.SHA256)
	frame, err := msgService.Send(ctx, owner, sendRequest(validClient, conversation, validPayload))
	must(t, err)
	if frame.Ack == nil || frame.Ack.ClientMsgID != validClient {
		t.Fatalf("valid media send did not ACK: %+v", frame)
	}
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE client_msg_id=$1", validClient); got != 1 {
		t.Fatalf("valid media message count got %d want 1", got)
	}

	negativePayloads := []struct {
		name    string
		payload []byte
	}{
		{"mediaKey", mediaPayload(t, unique("other_media"), asset.Kind, asset.ContentType, *asset.ActualSize, asset.SHA256)},
		{"kind", mediaPayload(t, asset.MediaKey, "video", asset.ContentType, *asset.ActualSize, asset.SHA256)},
		{"contentType", mediaPayload(t, asset.MediaKey, asset.Kind, "image/png", *asset.ActualSize, asset.SHA256)},
		{"size", mediaPayload(t, asset.MediaKey, asset.Kind, asset.ContentType, *asset.ActualSize+1, asset.SHA256)},
		{"sha256", mediaPayload(t, asset.MediaKey, asset.Kind, asset.ContentType, *asset.ActualSize, contentDigest(content(48, 10)))},
		{"owner", mediaPayload(t, asset.MediaKey, asset.Kind, asset.ContentType, *asset.ActualSize, asset.SHA256)},
	}
	for _, testCase := range negativePayloads {
		t.Run("message_"+testCase.name, func(t *testing.T) {
			client := unique("invalid_" + testCase.name)
			identity := owner
			if testCase.name == "owner" {
				identity = member
			}
			_, sendErr := msgService.Send(ctx, identity, sendRequest(client, conversation, testCase.payload))
			if messageApp.ErrorCode(sendErr) != messageApp.SendUnauthorized {
				t.Fatalf("send error got %v want %s", sendErr, messageApp.SendUnauthorized)
			}
			assertNoMessageRows(t, f, client)
		})
	}
	t.Run("message_conversation", func(t *testing.T) {
		client := unique("invalid_conversation")
		payload := mediaPayload(t, asset.MediaKey, asset.Kind, asset.ContentType, *asset.ActualSize, asset.SHA256)
		_, sendErr := msgService.Send(ctx, owner, sendRequest(client, otherConversation, payload))
		if messageApp.ErrorCode(sendErr) != messageApp.SendUnauthorized {
			t.Fatalf("send error got %v want %s", sendErr, messageApp.SendUnauthorized)
		}
		assertNoMessageRows(t, f, client)
	})
	t.Run("message_connection_binding", func(t *testing.T) {
		client := unique("invalid_connection")
		f.sql("UPDATE newim.im_media_assets SET connection_id=$2 WHERE media_key=$1", asset.MediaKey, "tampered_connection")
		payload := mediaPayload(t, asset.MediaKey, asset.Kind, asset.ContentType, *asset.ActualSize, asset.SHA256)
		_, sendErr := msgService.Send(ctx, owner, sendRequest(client, conversation, payload))
		if messageApp.ErrorCode(sendErr) != messageApp.SendUnauthorized {
			t.Fatalf("send error got %v want %s", sendErr, messageApp.SendUnauthorized)
		}
		assertNoMessageRows(t, f, client)
	})
	t.Run("message_not_ready", func(t *testing.T) {
		pendingContent := content(20, 11)
		pending, beginErr := mediaService.BeginUpload(ctx, owner, beginRequest(conversation, pendingContent, time.Minute))
		must(t, beginErr)
		client := unique("invalid_not_ready")
		payload := mediaPayload(t, pending.MediaKey, "image", "image/jpeg", int64(len(pendingContent)), contentDigest(pendingContent))
		_, sendErr := msgService.Send(ctx, owner, sendRequest(client, conversation, payload))
		if messageApp.ErrorCode(sendErr) != messageApp.SendUnauthorized {
			t.Fatalf("send error got %v want %s", sendErr, messageApp.SendUnauthorized)
		}
		assertNoMessageRows(t, f, client)
	})
}
