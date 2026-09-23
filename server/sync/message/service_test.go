package message

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	session "github.com/m-ice/NewIM/server/auth/session"
)

type storeFunc func(context.Context, string, ReadRequest) (ReadResult, error)

func (f storeFunc) Read(ctx context.Context, user string, request ReadRequest) (ReadResult, error) {
	return f(ctx, user, request)
}

func identity(t *testing.T) session.ConnectionIdentity {
	t.Helper()
	value, err := session.NewConnectionIdentity("alice", "device", "session", "connection", "0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func item(seq int64) Item {
	return Item{
		ServerMsgID:     fmt.Sprintf("server_%d", seq),
		ClientMsgID:     fmt.Sprintf("client_%d", seq),
		SenderID:        "alice",
		ConversationID:  "conversation",
		ConversationSeq: seq,
		ProtocolVersion: 1,
		SchemaVersion:   1,
		MessageType:     "text",
		ServerTime:      seq,
		Payload:         []byte(`{"text":"hello"}`),
	}
}

func TestStructuralValidationPrecedesStore(t *testing.T) {
	called := false
	service, err := NewService(storeFunc(func(context.Context, string, ReadRequest) (ReadResult, error) {
		called = true
		return ReadResult{}, Fail(StorageUnavailable)
	}), Config{})
	if err != nil {
		t.Fatal(err)
	}

	page, err := service.ReadAfterSeq(context.Background(), identity(t), ReadRequest{ConversationID: "bad conversation", Limit: 100})
	if ErrorCode(err) != Forbidden || len(page.Items) != 0 || page.HasNext || page.NextAfterSeq != nil {
		t.Fatalf("malformed conversation got page=%+v err=%v", page, err)
	}
	if called {
		t.Fatal("store called for malformed conversation")
	}

	var zero session.ConnectionIdentity
	page, err = service.ReadAfterSeq(context.Background(), zero, ReadRequest{ConversationID: "conversation"})
	if ErrorCode(err) != Forbidden || len(page.Items) != 0 || page.HasNext || page.NextAfterSeq != nil {
		t.Fatalf("zero identity got page=%+v err=%v", page, err)
	}
	if called {
		t.Fatal("store called for zero identity")
	}
}

func TestAuthorizationCanPrecedeInvalidLimit(t *testing.T) {
	called := false
	service, err := NewService(storeFunc(func(_ context.Context, _ string, request ReadRequest) (ReadResult, error) {
		called = true
		if request.Limit != -1 {
			t.Fatalf("request limit got %d", request.Limit)
		}
		return ReadResult{}, Fail(Forbidden)
	}), Config{})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.ReadAfterSeq(context.Background(), identity(t), ReadRequest{ConversationID: "conversation", Limit: -1})
	if ErrorCode(err) != Forbidden || !called {
		t.Fatalf("authorization did not precede limit: page=%+v err=%v called=%v", page, err, called)
	}
}

func TestStoreErrorIsRedactedAndNeverReturnsPartialPage(t *testing.T) {
	service, err := NewService(storeFunc(func(context.Context, string, ReadRequest) (ReadResult, error) {
		return ReadResult{LatestSeq: 4, Limit: 2, Rows: []Item{item(1), item(2)}}, errors.New("password=super-secret SELECT")
	}), Config{})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.ReadAfterSeq(context.Background(), identity(t), ReadRequest{ConversationID: "conversation", Limit: 2})
	if ErrorCode(err) != StorageUnavailable || strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Items) != 0 || page.HasNext || page.NextAfterSeq != nil {
		t.Fatalf("redacted error returned page: %+v", page)
	}
	var target *Error
	if !errors.As(err, &target) || target.Unwrap() != nil {
		t.Fatalf("error is not sealed: %#v", err)
	}
}

func TestReadAfterSeqAssemblesBoundedContiguousPage(t *testing.T) {
	service, err := NewService(storeFunc(func(context.Context, string, ReadRequest) (ReadResult, error) {
		return ReadResult{LatestSeq: 4, Limit: 2, Rows: []Item{item(1), item(2), item(3)}}, nil
	}), Config{})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.ReadAfterSeq(context.Background(), identity(t), ReadRequest{ConversationID: "conversation", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.Items[0].ConversationSeq != 1 || page.Items[1].ConversationSeq != 2 || !page.HasNext || page.NextAfterSeq == nil || *page.NextAfterSeq != 2 || page.LatestSeq != 4 {
		t.Fatalf("unexpected page: %+v", page)
	}
}

func TestReadAfterSeqUsesExplicitContinuationAndReturnsTerminalPage(t *testing.T) {
	service, err := NewService(storeFunc(func(context.Context, string, ReadRequest) (ReadResult, error) {
		return ReadResult{LatestSeq: 4, Limit: 100, Rows: []Item{item(3), item(4)}}, nil
	}), Config{})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.ReadAfterSeq(context.Background(), identity(t), ReadRequest{ConversationID: "conversation", HasAfterSeq: true, AfterSeq: 2, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.HasNext || page.NextAfterSeq == nil || *page.NextAfterSeq != 4 {
		t.Fatalf("unexpected terminal page: %+v", page)
	}
}

func TestEmptyTerminalPageHasNoContinuation(t *testing.T) {
	service, err := NewService(storeFunc(func(context.Context, string, ReadRequest) (ReadResult, error) {
		return ReadResult{LatestSeq: 4, Limit: 100, Rows: nil}, nil
	}), Config{})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.ReadAfterSeq(context.Background(), identity(t), ReadRequest{ConversationID: "conversation", HasAfterSeq: true, AfterSeq: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 || page.HasNext || page.NextAfterSeq != nil || page.LatestSeq != 4 {
		t.Fatalf("unexpected empty page: %+v", page)
	}
}

func TestPageBudgetTruncatesAndKeepsContinuation(t *testing.T) {
	payload := strings.Repeat("x", 65536)
	rows := make([]Item, 0, 5)
	for seq := int64(1); seq <= 5; seq++ {
		row := item(seq)
		row.Payload = []byte(payload)
		rows = append(rows, row)
	}
	service, err := NewService(storeFunc(func(context.Context, string, ReadRequest) (ReadResult, error) {
		return ReadResult{LatestSeq: 5, Limit: 5, Rows: rows}, nil
	}), Config{})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.ReadAfterSeq(context.Background(), identity(t), ReadRequest{ConversationID: "conversation", Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 3 || !page.HasNext || page.NextAfterSeq == nil || *page.NextAfterSeq != 3 {
		t.Fatalf("unexpected byte-budget page: %+v", page)
	}
}

func TestObserverHasNoSensitiveFields(t *testing.T) {
	observer := &captureObserver{}
	service, err := NewService(storeFunc(func(context.Context, string, ReadRequest) (ReadResult, error) {
		return ReadResult{}, Fail(Forbidden)
	}), Config{Observer: observer, RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.ReadAfterSeq(context.Background(), identity(t), ReadRequest{ConversationID: "conversation"})
	if ErrorCode(err) != Forbidden {
		t.Fatalf("code got %s", ErrorCode(err))
	}
	if len(observer.values) != 1 || observer.values[0].Operation != "read_after_seq" || observer.values[0].Code != Forbidden {
		t.Fatalf("unexpected observation: %+v", observer.values)
	}
}

type captureObserver struct{ values []Observation }

func (o *captureObserver) Observe(value Observation) { o.values = append(o.values, value) }

func TestNilContextPrecedence(t *testing.T) {
	called := false
	service, err := NewService(storeFunc(func(context.Context, string, ReadRequest) (ReadResult, error) {
		called = true
		return ReadResult{}, nil
	}), Config{})
	if err != nil {
		t.Fatal(err)
	}
	var zero session.ConnectionIdentity
	page, err := service.ReadAfterSeq(nil, zero, ReadRequest{ConversationID: "conversation"})
	if ErrorCode(err) != Forbidden || len(page.Items) != 0 || called {
		t.Fatalf("malformed identity with nil context got page=%+v err=%v called=%v", page, err, called)
	}
	page, err = service.ReadAfterSeq(nil, identity(t), ReadRequest{ConversationID: "conversation"})
	if ErrorCode(err) != StorageUnavailable || len(page.Items) != 0 || called {
		t.Fatalf("valid identity with nil context got page=%+v err=%v called=%v", page, err, called)
	}
}

func TestPanickingObserverCannotEscape(t *testing.T) {
	service, err := NewService(storeFunc(func(context.Context, string, ReadRequest) (ReadResult, error) {
		return ReadResult{LatestSeq: 0, Limit: 100}, nil
	}), Config{Observer: panicObserver{}})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.ReadAfterSeq(context.Background(), identity(t), ReadRequest{ConversationID: "conversation"})
	if err != nil || page.LatestSeq != 0 {
		t.Fatalf("panicking observer changed result: page=%+v err=%v", page, err)
	}
}

type panicObserver struct{}

func (panicObserver) Observe(Observation) { panic("observer failure") }
