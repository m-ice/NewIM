package media

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/auth/session"
)

type fakeMediaStore struct {
	grant      Grant
	loadCalls  int
	beginCalls int
	authErr    error
	beginErr   error
}

func (s *fakeMediaStore) BeginUpload(context.Context, time.Time, session.ConnectionIdentity, string, func() (Grant, error)) error {
	s.beginCalls++
	return s.beginErr
}
func (s *fakeMediaStore) LoadGrant(context.Context, string) (Grant, error) {
	s.loadCalls++
	return s.grant, nil
}
func (s *fakeMediaStore) CompleteReady(context.Context, time.Time, session.ConnectionIdentity, Grant) (Grant, error) {
	return s.grant, nil
}
func (s *fakeMediaStore) LoadReadyAsset(context.Context, string) (Grant, error) { return s.grant, nil }
func (s *fakeMediaStore) Authorize(context.Context, time.Time, session.ConnectionIdentity, string) error {
	return s.authErr
}
func (s *fakeMediaStore) ValidateReadyForSend(context.Context, time.Time, session.ConnectionIdentity, string, Metadata) error {
	return s.authErr
}

type fakeObjects struct {
	calls int
	err   error
}

func (o *fakeObjects) PutImmutable(_ context.Context, _ string, _ io.Reader, expected ObjectInfo) (ObjectInfo, error) {
	o.calls++
	if o.err != nil {
		return ObjectInfo{}, o.err
	}
	return expected, nil
}

func testIdentity(t *testing.T, user string) session.ConnectionIdentity {
	t.Helper()
	identity, err := session.NewConnectionIdentity(user, user+"_device", user+"_session", user+"_connection", "0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func testGrant(t *testing.T, identity session.ConnectionIdentity, state State, expiry time.Time) (Grant, string) {
	t.Helper()
	grantIDBytes := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	secretBytes := bytes.Repeat([]byte{0x20}, 32)
	raw := hex.EncodeToString(grantIDBytes) + "." + base64.RawURLEncoding.EncodeToString(secretBytes)
	digest := sha256.Sum256([]byte(raw))
	grant := Grant{
		MediaKey: "media_key", OwnerUserID: identity.UserID(), DeviceID: identity.DeviceID(),
		SessionID: identity.SessionID(), ConnectionID: identity.ConnectionID(), TokenID: identity.TokenID(),
		ConversationID: "conversation", Kind: "image", ContentType: "image/jpeg", DeclaredSize: 3,
		SHA256: sha256.Sum256([]byte("abc")), GrantID: raw[:32], GrantDigest: digest,
		ExpiresAt: expiry, State: state,
	}
	if state == StateReady {
		size := int64(3)
		completed := expiry.Add(-time.Minute)
		grant.ActualSize = &size
		grant.CompletedAt = &completed
	}
	return grant, raw
}

func newTestService(t *testing.T, store Store, objects ObjectStore) *Service {
	t.Helper()
	service, err := NewService(store, Config{
		Objects: objects, Clock: ClockFunc(func() time.Time { return time.Unix(1000, 0).UTC() }),
		Entropy: bytes.NewReader(bytes.Repeat([]byte{1}, 256)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestMalformedGrantDoesNotLookupOrWrite(t *testing.T) {
	store := &fakeMediaStore{}
	objects := &fakeObjects{}
	service := newTestService(t, store, objects)
	_, err := service.CompleteUpload(context.Background(), testIdentity(t, "alice"), "bad-token", bytes.NewReader([]byte("abc")))
	if ErrorCode(err) != MediaInvalidToken || store.loadCalls != 0 || objects.calls != 0 {
		t.Fatalf("error=%v load=%d objects=%d", err, store.loadCalls, objects.calls)
	}
}

func TestExpiredPendingFailsBeforeObjectWrite(t *testing.T) {
	identity := testIdentity(t, "alice")
	grant, raw := testGrant(t, identity, StatePending, time.Unix(999, 0).UTC())
	store := &fakeMediaStore{grant: grant}
	objects := &fakeObjects{}
	service := newTestService(t, store, objects)
	_, err := service.CompleteUpload(context.Background(), identity, raw, bytes.NewReader([]byte("abc")))
	if ErrorCode(err) != MediaExpired || objects.calls != 0 {
		t.Fatalf("error=%v objects=%d", err, objects.calls)
	}
}

func TestIdentityMismatchPrecedesReadyRecoveryAndExpiry(t *testing.T) {
	owner := testIdentity(t, "alice")
	other := testIdentity(t, "bob")
	grant, raw := testGrant(t, owner, StateReady, time.Unix(999, 0).UTC())
	store := &fakeMediaStore{grant: grant}
	objects := &fakeObjects{}
	service := newTestService(t, store, objects)
	_, err := service.CompleteUpload(context.Background(), other, raw, bytes.NewReader([]byte("abc")))
	if ErrorCode(err) != MediaUnauthorized || objects.calls != 0 {
		t.Fatalf("error=%v objects=%d", err, objects.calls)
	}
}

func TestReadyReplayAfterExpirySucceedsWhenObjectMatches(t *testing.T) {
	identity := testIdentity(t, "alice")
	grant, raw := testGrant(t, identity, StateReady, time.Unix(999, 0).UTC())
	store := &fakeMediaStore{grant: grant}
	objects := &fakeObjects{}
	service := newTestService(t, store, objects)
	got, err := service.CompleteUpload(context.Background(), identity, raw, bytes.NewReader([]byte("abc")))
	if err != nil || got.State != StateReady || objects.calls != 1 {
		t.Fatalf("asset=%+v error=%v objects=%d", got, err, objects.calls)
	}
}

func TestDownloadFailureDoesNotCallSigner(t *testing.T) {
	identity := testIdentity(t, "alice")
	grant, _ := testGrant(t, identity, StateReady, time.Unix(1000, 0).UTC())
	store := &fakeMediaStore{grant: grant, authErr: errors.New("denied")}
	signer := &fakeSigner{}
	service, err := NewService(store, Config{
		Objects: &fakeObjects{}, Signer: signer, Clock: ClockFunc(func() time.Time { return time.Unix(1000, 0).UTC() }),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.ResolvePrivateDownload(context.Background(), identity, grant.MediaKey, time.Minute)
	if ErrorCode(err) != MediaStorageUnavailable || signer.calls != 0 {
		t.Fatalf("error=%v signer=%d", err, signer.calls)
	}
}

type fakeSigner struct{ calls int }

func (s *fakeSigner) Sign(context.Context, string, time.Time) (string, error) {
	s.calls++
	return "https://private.invalid/object", nil
}

func TestIntentMismatchPrecedesReadyRecovery(t *testing.T) {
	identity := testIdentity(t, "alice")
	grant, raw := testGrant(t, identity, StateReady, time.Unix(999, 0).UTC())
	store := &fakeMediaStore{grant: grant}
	objects := &fakeObjects{}
	service := newTestService(t, store, objects)
	intent := Intent{Kind: "video", ContentType: grant.ContentType, Size: grant.DeclaredSize, SHA256: grant.SHA256}
	_, err := service.CompleteUploadIntent(context.Background(), identity, raw, intent, bytes.NewReader([]byte("abc")))
	if ErrorCode(err) != MediaUnauthorized || objects.calls != 0 {
		t.Fatalf("error=%v objects=%d", err, objects.calls)
	}
}

type collisionStore struct {
	fakeMediaStore
	calls int
}

func (s *collisionStore) BeginUpload(_ context.Context, _ time.Time, _ session.ConnectionIdentity, _ string, create func() (Grant, error)) error {
	s.calls++
	if s.calls == 1 {
		return Fail(MediaGrantCollision)
	}
	_, err := create()
	return err
}

func TestBeginUploadRetriesGrantCollision(t *testing.T) {
	store := &collisionStore{}
	service := newTestService(t, store, &fakeObjects{})
	result, err := service.BeginUpload(context.Background(), testIdentity(t, "alice"), BeginUploadRequest{
		ConversationID: "conversation", Kind: "image", ContentType: "image/jpeg", Size: 3,
		SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})
	if err != nil || store.calls != 2 || result.RawToken == "" || result.MediaKey == "" {
		t.Fatalf("result=%+v error=%v calls=%d", result, err, store.calls)
	}
}
