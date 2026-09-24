package session

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"
)

type revokeCaptureStore struct {
	Store
	snapshot TokenSnapshot
	verify   func(TokenSnapshot) error
}

func (s *revokeCaptureStore) RevokeBearer(_ context.Context, _ string, verify func(TokenSnapshot) error, _ time.Time) (RevocationOutcome, error) {
	s.verify = verify
	if err := verify(s.snapshot); err != nil {
		return "", err
	}
	return RevokeOK, nil
}

type revokeNoopStore struct{ Store }

func (revokeNoopStore) RevokeBearer(context.Context, string, func(TokenSnapshot) error, time.Time) (RevocationOutcome, error) {
	return RevokeNoop, nil
}

func TestRevokeBearerUsesPersistedDigestAndObservesOutcome(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	issued, err := generateIssuedToken(rand.Reader, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	binding := SessionBinding{UserID: "user_1", DeviceID: "device_1", SessionID: "session_1"}
	digest := issued.Digest()
	snapshot, err := NewTokenSnapshot(issued.TokenID(), digest[:], binding, issued.ExpiresAt(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	store := &revokeCaptureStore{snapshot: snapshot}
	observer := &bearerCaptureObserver{}
	service, err := NewService(store, Config{Clock: ClockFunc(func() time.Time { return now }), Observer: observer})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := service.RevokeBearer(context.Background(), issued.RawToken())
	if err != nil || outcome != RevokeOK {
		t.Fatalf("outcome=%q err=%v", outcome, err)
	}
	if store.verify == nil {
		t.Fatal("store verify callback was not invoked")
	}
	tampered := TokenPrefix + issued.TokenID() + "_" + strings.Repeat("A", TokenSecretLen)
	if err := store.verify(snapshotWithDigest(t, binding, tampered, issued.ExpiresAt())); ErrorCode(err) != AuthTokenUnknown {
		t.Fatalf("tampered digest err=%v", err)
	}
	observation, ok := observer.latest("revoke_bearer")
	if !ok || observation.Code != AuthRevokeOK || observation.TokenID != issued.TokenID() {
		t.Fatalf("observation=%+v ok=%t", observation, ok)
	}
}

func TestRevokeBearerIdempotentAndErrors(t *testing.T) {
	token := TokenPrefix + strings.Repeat("a", TokenIDHexLen) + "_" + strings.Repeat("A", TokenSecretLen)
	service, err := NewService(revokeNoopStore{}, Config{
		Clock:    ClockFunc(func() time.Time { return time.Unix(1_800_000_000, 0).UTC() }),
		Observer: &bearerCaptureObserver{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := service.RevokeBearer(context.Background(), token); err != nil || outcome != RevokeNoop {
		t.Fatalf("noop outcome=%q err=%v", outcome, err)
	}
	if _, err := service.RevokeBearer(nil, token); ErrorCode(err) != AuthInvalidInput {
		t.Fatalf("nil context err=%v", err)
	}
	if _, err := service.RevokeBearer(context.Background(), "not-a-token"); ErrorCode(err) != AuthTokenMalformed {
		t.Fatalf("malformed err=%v", err)
	}
}

func snapshotWithDigest(t *testing.T, binding SessionBinding, raw string, expiresAt time.Time) TokenSnapshot {
	t.Helper()
	_, digest, err := parseRawToken(raw)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := NewTokenSnapshot(raw[len(TokenPrefix):len(TokenPrefix)+TokenIDHexLen], digest[:], binding, expiresAt, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
