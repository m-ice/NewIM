package message

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/conversation"
	media "github.com/m-ice/NewIM/server/media"
)

type mediaTestStore struct {
	calls int
}

func (s *mediaTestStore) Persist(_ context.Context, _ conversation.Principal, request protocol.Send, generate func() (Generated, error)) (PersistedMessage, error) {
	s.calls++
	generated, err := generate()
	if err != nil {
		return PersistedMessage{}, err
	}
	return PersistedMessage{
		ClientMsgID: request.ClientMsgID, ConversationID: request.ConversationID, SenderID: "alice",
		ServerMsgID: generated.ServerMsgID, ConversationSeq: 1, ServerTime: generated.ServerTime,
	}, nil
}

type mediaTestIDs struct{ next int }

func (g *mediaTestIDs) NewID() (string, error) {
	g.next++
	return "id_media_000" + string(rune('0'+g.next)), nil
}

type recordingMediaValidator struct {
	calls    int
	metadata media.Metadata
	err      error
}

func (v *recordingMediaValidator) ValidateForSend(_ context.Context, _ session.ConnectionIdentity, _ string, metadata media.Metadata) error {
	v.calls++
	v.metadata = metadata
	return v.err
}

func mediaIdentity(t *testing.T) session.ConnectionIdentity {
	t.Helper()
	identity, err := session.NewConnectionIdentity("alice", "device", "session", "connection", "0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func mediaSend(t *testing.T) protocol.Send {
	t.Helper()
	digest := make([]byte, 32)
	for i := range digest {
		digest[i] = byte(i)
	}
	payload, err := protocol.EncodeMediaPayload(protocol.MediaMetadata{
		MediaKey: "media_key", Kind: "image", ContentType: "image/jpeg",
		Size: "12", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Send{
		ProtocolVersion: 1, ClientMsgID: "client_media", ConversationID: "conversation",
		Version: 1, Type: "media", Payload: payload,
	}
}

func newMediaTestService(t *testing.T, store Store, validator MediaValidator) *Service {
	t.Helper()
	service, err := NewService(store, Config{
		IDs: &mediaTestIDs{}, Clock: ClockFunc(func() time.Time { return time.Unix(1800000000, 0).UTC() }),
		MediaValidator: validator,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestMediaValidatorFailurePrecedesPersistence(t *testing.T) {
	store := &mediaTestStore{}
	validator := &recordingMediaValidator{err: media.Fail(media.MediaUnauthorized)}
	service := newMediaTestService(t, store, validator)
	_, err := service.Send(context.Background(), mediaIdentity(t), mediaSend(t))
	if ErrorCode(err) != SendUnauthorized || store.calls != 0 || validator.calls != 1 {
		t.Fatalf("error=%v store=%d validator=%d", err, store.calls, validator.calls)
	}
	if validator.metadata.MediaKey != "media_key" || validator.metadata.Size != 12 {
		t.Fatalf("validator metadata = %+v", validator.metadata)
	}
}

func TestMissingMediaValidatorRejectsMediaWithoutPersistence(t *testing.T) {
	store := &mediaTestStore{}
	service := newMediaTestService(t, store, nil)
	_, err := service.Send(context.Background(), mediaIdentity(t), mediaSend(t))
	if ErrorCode(err) != SendInvalidInput || store.calls != 0 {
		t.Fatalf("error=%v store=%d", err, store.calls)
	}
}

func TestTextDoesNotRequireMediaValidator(t *testing.T) {
	store := &mediaTestStore{}
	service := newMediaTestService(t, store, nil)
	request := protocol.Send{
		ProtocolVersion: 1, ClientMsgID: "client_text", ConversationID: "conversation",
		Version: 1, Type: "text", Payload: json.RawMessage(`{"text":"hello"}`),
	}
	frame, err := service.Send(context.Background(), mediaIdentity(t), request)
	if err != nil || frame.Ack == nil || store.calls != 1 {
		t.Fatalf("frame=%+v error=%v store=%d", frame, err, store.calls)
	}
}
