package conversation

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type fakeStore struct {
	results []ReadResult
	errs    []error
	calls   []ReadRequest
}

func (f *fakeStore) Read(_ context.Context, _ string, request ReadRequest) (ReadResult, error) {
	f.calls = append(f.calls, request)
	if len(f.errs) != 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return ReadResult{}, err
		}
	}
	if len(f.results) == 0 {
		return ReadResult{}, Fail(StorageUnavailable)
	}
	result := f.results[0]
	f.results = f.results[1:]
	return result, nil
}

func testKey(fill byte) []byte {
	return bytes.Repeat([]byte{fill}, 32)
}

func testCursor() syncCursor {
	return syncCursor{
		kind:        cursorKindBootstrapPage,
		keyID:       "key1",
		account:     "user1",
		epoch:       1,
		issued:      1000,
		expires:     1900,
		limit:       100,
		fence:       10,
		afterKey:    "a",
		terminalKey: "z",
	}
}

func decodeTestCursor(t *testing.T, token string, now uint64) syncCursor {
	t.Helper()
	cursor, err := decodeCursor(map[string][]byte{"key1": testKey(1)}, token, cursorKindBootstrapPage, "user1", now)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	return cursor
}

func mustCode(t *testing.T, err error, want Code) {
	t.Helper()
	if got := ErrorCode(err); got != want {
		t.Fatalf("error code got %q want %q (err=%v)", got, want, err)
	}
}

func assertNoPageEffects(t *testing.T, page Page) {
	t.Helper()
	if len(page.Items) != 0 || page.NextCursor != "" || page.HasMore {
		t.Fatalf("error returned page effects: %+v", page)
	}
}

func TestCursorCanonicalAndAuthentication(t *testing.T) {
	keys := map[string][]byte{"key1": testKey(1)}
	cursor := testCursor()
	token, err := encodeCursor(keys, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(token, "=") {
		t.Fatal("cursor must use unpadded base64url")
	}
	if got := decodeTestCursor(t, token, 1001); got != cursor {
		t.Fatalf("decoded cursor mismatch: %#v", got)
	}

	t.Run("non-canonical", func(t *testing.T) {
		if _, err := decodeCursor(keys, token+"=", cursorKindBootstrapPage, "user1", 1001); ErrorCode(err) != InvalidCursor {
			t.Fatalf("padding accepted: %v", err)
		}
		if _, err := decodeCursor(keys, token[:len(token)-1]+"A", cursorKindBootstrapPage, "user1", 1001); ErrorCode(err) != InvalidCursor {
			t.Fatalf("tampered token accepted: %v", err)
		}
	})

	t.Run("trailing-field", func(t *testing.T) {
		raw, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil {
			t.Fatal(err)
		}
		payload := append([]byte(nil), raw[:len(raw)-cursorTagSize]...)
		payload = append(payload, 0xff)
		mac := hmac.New(sha256.New, keys["key1"])
		mac.Write(payload)
		raw = append(payload, mac.Sum(nil)...)
		extended := base64.RawURLEncoding.EncodeToString(raw)
		if _, err := decodeCursor(keys, extended, cursorKindBootstrapPage, "user1", 1001); ErrorCode(err) != InvalidCursor {
			t.Fatalf("trailing field accepted: %v", err)
		}
	})
}

func TestCursorRotationExpiryIdentityAndBounds(t *testing.T) {
	keys := map[string][]byte{
		"key1": testKey(1),
		"key2": testKey(2),
	}
	token, err := encodeCursor(keys, testCursor())
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := decodeCursor(keys, token, cursorKindBootstrapPage, "user1", 1001)
	if err != nil || rotated.keyID != "key1" {
		t.Fatalf("retained key did not decode: %v", err)
	}
	newToken, err := encodeCursor(keys, syncCursor{
		kind:        cursorKindBootstrapPage,
		keyID:       "key2",
		account:     "user1",
		epoch:       1,
		issued:      1000,
		expires:     1900,
		limit:       100,
		fence:       10,
		afterKey:    "a",
		terminalKey: "z",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeCursor(keys, newToken, cursorKindBootstrapPage, "user1", 1001); err != nil {
		t.Fatalf("rotated key did not decode: %v", err)
	}

	if _, err := decodeCursor(keys, token, cursorKindBootstrapPage, "user1", 1899); err != nil {
		t.Fatalf("cursor expired one second early: %v", err)
	}
	if _, err := decodeCursor(keys, token, cursorKindBootstrapPage, "user1", 1900); ErrorCode(err) != CursorExpired {
		t.Fatalf("expiry boundary got %v", err)
	}
	if _, err := decodeCursor(keys, token, cursorKindBootstrapPage, "user1", 999); ErrorCode(err) != InvalidCursor {
		t.Fatalf("future issuance accepted: %v", err)
	}
	if _, err := decodeCursor(keys, token, cursorKindBootstrapPage, "user2", 1001); ErrorCode(err) != InvalidCursor {
		t.Fatalf("identity replacement accepted: %v", err)
	}

	bad := testCursor()
	bad.epoch = 0
	if _, err := encodeCursor(keys, bad); ErrorCode(err) != InvalidCursor {
		t.Fatalf("zero epoch accepted: %v", err)
	}
	bad = testCursor()
	bad.limit = MaxPageItems + 1
	if _, err := encodeCursor(keys, bad); ErrorCode(err) != InvalidCursor {
		t.Fatalf("oversize limit accepted: %v", err)
	}
	bad = testCursor()
	bad.afterKey = "zz"
	if _, err := encodeCursor(keys, bad); ErrorCode(err) != InvalidCursor {
		t.Fatalf("after-key after terminal accepted: %v", err)
	}
	bad = testCursor()
	bad.expires = bad.issued
	if _, err := encodeCursor(keys, bad); ErrorCode(err) != InvalidCursor {
		t.Fatalf("non-positive lifetime accepted: %v", err)
	}
	delta := testCursor()
	delta.kind = cursorKindDeltaPage
	delta.afterKey = ""
	delta.terminalKey = ""
	delta.afterSeq = delta.fence + 1
	if _, err := encodeCursor(keys, delta); ErrorCode(err) != InvalidCursor {
		t.Fatalf("delta position beyond fence accepted: %v", err)
	}
}

func TestCursorUnknownKeyAndCoveredKeyID(t *testing.T) {
	keys := map[string][]byte{"key1": testKey(1)}
	token, err := encodeCursor(keys, testCursor())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		t.Fatal(err)
	}
	payload := append([]byte(nil), raw[:len(raw)-cursorTagSize]...)
	payload[3] = 'x'
	mac := hmac.New(sha256.New, keys["key1"])
	mac.Write(payload)
	raw = append(payload, mac.Sum(nil)...)
	changedKeyID := base64.RawURLEncoding.EncodeToString(raw)
	if _, err := decodeCursor(keys, changedKeyID, cursorKindBootstrapPage, "user1", 1001); ErrorCode(err) != InvalidCursor {
		t.Fatalf("key-id substitution accepted: %v", err)
	}

	other := map[string][]byte{"key2": testKey(2)}
	if _, err := decodeCursor(other, token, cursorKindBootstrapPage, "user1", 1001); ErrorCode(err) != InvalidCursor {
		t.Fatalf("unknown key accepted: %v", err)
	}
}

func TestBootstrapPageBoundsAndProgress(t *testing.T) {
	now := time.Unix(1000, 0)
	store := &fakeStore{}
	candidates := make([]Candidate, 0, 101)
	for i := 0; i < 101; i++ {
		id := fmt.Sprintf("c%03d", i)
		revision := uint64(i + 1)
		seq := uint64(i)
		message := "m" + id
		candidates = append(candidates, Candidate{
			Key:    id,
			Member: true,
			State: &Item{
				ConversationID:        id,
				Revision:              revision,
				Kind:                  "upsert",
				LatestConversationSeq: &seq,
				LatestServerMsgID:     &message,
			},
		})
	}
	last := candidates[len(candidates)-1].Key
	store.results = []ReadResult{{
		Epoch:              1,
		Head:               101,
		Floor:              0,
		Fence:              101,
		TerminalKey:        last,
		Candidates:         candidates,
		DirectoryExhausted: true,
	}}
	service, err := NewService(store, Config{Keys: map[string][]byte{"k": testKey(1)}, ActiveKeyID: "k", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.BeginBootstrap(context.Background(), Principal{UserID: "user1"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 100 || !page.HasMore || page.NextCursor == "" {
		t.Fatalf("unexpected first page: items=%d more=%v", len(page.Items), page.HasMore)
	}
	encoded, err := json.Marshal(page)
	if err != nil || len(encoded) > MaxResponseBytes {
		t.Fatalf("page byte bound failed: len=%d err=%v", len(encoded), err)
	}
	if len(store.calls) != 1 || store.calls[0].Kind != BootstrapRead || !store.calls[0].Start || store.calls[0].Limit != 100 {
		t.Fatalf("unexpected store request: %+v", store.calls)
	}

	store.results = []ReadResult{{
		Epoch:              1,
		Head:               101,
		Floor:              0,
		Fence:              101,
		TerminalKey:        last,
		Candidates:         candidates[100:],
		DirectoryExhausted: true,
	}}
	second, err := service.ContinueBootstrap(context.Background(), Principal{UserID: "user1"}, page.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Items) != 1 || second.HasMore || second.NextCursor == "" {
		t.Fatalf("unexpected terminal page: %+v", second)
	}
}

func TestEmptyFutureKeysAdvance(t *testing.T) {
	now := time.Unix(1000, 0)
	store := &fakeStore{results: []ReadResult{{
		Epoch:              1,
		Head:               0,
		Floor:              0,
		Fence:              0,
		TerminalKey:        "z",
		Candidates:         []Candidate{{Key: "z"}},
		DirectoryExhausted: false,
	}}}
	service, err := NewService(store, Config{Keys: map[string][]byte{"k": testKey(1)}, ActiveKeyID: "k", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.BeginBootstrap(context.Background(), Principal{UserID: "user1"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 || !page.HasMore || page.NextCursor == "" {
		t.Fatalf("empty future page did not advance: %+v", page)
	}
}

func TestErrorDoesNotReturnPageEffects(t *testing.T) {
	now := time.Unix(1000, 0)
	store := &fakeStore{errs: []error{errors.New("secret database detail")}}
	service, err := NewService(store, Config{Keys: map[string][]byte{"k": testKey(1)}, ActiveKeyID: "k", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.BeginBootstrap(context.Background(), Principal{UserID: "user1"}, 100)
	mustCode(t, err, StorageUnavailable)
	assertNoPageEffects(t, page)
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("error leaked storage detail: %v", err)
	}
}

func TestNotReadyDoesNotReturnPageEffects(t *testing.T) {
	now := time.Unix(1000, 0)
	store := &fakeStore{results: []ReadResult{{
		Epoch:              1,
		Head:               1,
		Floor:              0,
		Fence:              1,
		TerminalKey:        "c",
		Candidates:         []Candidate{{Key: "c", State: &Item{ConversationID: "c", Revision: 1, Kind: "upsert", LatestConversationSeq: uint64Ptr(0)}}},
		DirectoryExhausted: true,
	}}}
	service, err := NewService(store, Config{Keys: map[string][]byte{"k": testKey(1)}, ActiveKeyID: "k", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.BeginBootstrap(context.Background(), Principal{UserID: "user1"}, 100)
	mustCode(t, err, NotReady)
	assertNoPageEffects(t, page)
}

func uint64Ptr(value uint64) *uint64 {
	return &value
}

func TestBeginDeltaIncludesRemoveAndFreezesFence(t *testing.T) {
	now := time.Unix(1000, 0)
	checkpoint, err := encodeCursor(map[string][]byte{"k": testKey(1)}, syncCursor{
		kind:    cursorKindCheckpoint,
		keyID:   "k",
		account: "user1",
		epoch:   1,
		issued:  1000,
		expires: 2000,
		limit:   100,
		fence:   5,
	})
	if err != nil {
		t.Fatal(err)
	}
	seq0 := uint64(0)
	message := "m1"
	store := &fakeStore{results: []ReadResult{{
		Epoch: 1,
		Head:  7,
		Floor: 0,
		Fence: 7,
		Candidates: []Candidate{
			{Key: "b", Member: false, State: &Item{ConversationID: "b", Revision: 6, Kind: "remove"}},
			{Key: "a", Member: true, State: &Item{ConversationID: "a", Revision: 7, Kind: "upsert", LatestConversationSeq: &seq0, LatestServerMsgID: &message}},
		},
	}}}
	service, err := NewService(store, Config{Keys: map[string][]byte{"k": testKey(1)}, ActiveKeyID: "k", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.BeginDelta(context.Background(), Principal{UserID: "user1"}, checkpoint, 10)
	if err != nil {
		t.Fatal(err)
	}
	if page.HasMore || len(page.Items) != 2 || page.Items[0].Kind != "remove" || page.Items[1].Kind != "upsert" {
		t.Fatalf("unexpected delta page: %+v", page)
	}
	if len(store.calls) != 1 || !store.calls[0].Start || store.calls[0].Kind != DeltaRead || store.calls[0].Fence != 5 || store.calls[0].AfterSeq != 5 || store.calls[0].Limit != 10 {
		t.Fatalf("unexpected delta request: %+v", store.calls)
	}
	next, err := decodeCursor(map[string][]byte{"k": testKey(1)}, page.NextCursor, cursorKindCheckpoint, "user1", 1001)
	if err != nil {
		t.Fatal(err)
	}
	if next.fence != 7 || next.issued != 1000 {
		t.Fatalf("fence/checkpoint mismatch: %+v", next)
	}
}

func TestLimitAndCursorFailuresHaveNoEffects(t *testing.T) {
	now := time.Unix(1000, 0)
	store := &fakeStore{}
	service, err := NewService(store, Config{Keys: map[string][]byte{"k": testKey(1)}, ActiveKeyID: "k", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{-1, MaxPageItems + 1} {
		page, err := service.BeginBootstrap(context.Background(), Principal{UserID: "user1"}, limit)
		mustCode(t, err, LimitExceeded)
		assertNoPageEffects(t, page)
	}
	if len(store.calls) != 0 {
		t.Fatalf("invalid limits reached store: %+v", store.calls)
	}

	page, err := service.ContinueBootstrap(context.Background(), Principal{UserID: "user1"}, "not-a-cursor")
	mustCode(t, err, InvalidCursor)
	assertNoPageEffects(t, page)

	store.results = []ReadResult{{
		Epoch: 2,
		Head:  1,
		Floor: 0,
		Fence: 1,
	}}
	token, err := encodeCursor(map[string][]byte{"k": testKey(1)}, syncCursor{
		kind:        cursorKindBootstrapPage,
		keyID:       "k",
		account:     "user1",
		epoch:       1,
		issued:      1000,
		expires:     1900,
		limit:       100,
		fence:       1,
		afterKey:    "a",
		terminalKey: "z",
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err = service.ContinueBootstrap(context.Background(), Principal{UserID: "user1"}, token)
	mustCode(t, err, CursorExpired)
	assertNoPageEffects(t, page)
}

func TestPageEncodingRejectsOversizeItem(t *testing.T) {
	now := time.Unix(1000, 0)
	service, err := NewService(&fakeStore{}, Config{Keys: map[string][]byte{"k": testKey(1)}, ActiveKeyID: "k", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	base := syncCursor{
		kind:        cursorKindBootstrapPage,
		keyID:       "k",
		account:     "user1",
		epoch:       1,
		issued:      1000,
		expires:     1900,
		limit:       1,
		fence:       1,
		afterKey:    "a",
		terminalKey: "z",
	}
	_, err = service.renderPage(base, []itemEnvelope{{item: Item{
		ConversationID: strings.Repeat("x", 70000),
		Revision:       1,
		Kind:           "upsert",
	}}}, cursorPosition{key: "b"}, true, 1000)
	mustCode(t, err, LimitExceeded)
}
