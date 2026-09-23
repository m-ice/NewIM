//go:build integration

package messagesync_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	session "github.com/m-ice/NewIM/server/auth/session"
	app "github.com/m-ice/NewIM/server/sync/message"
)

func TestAuthz(t *testing.T) {
	f := openFixture(t)
	conversationID := f.seedConversation("authz_member", f.user)
	otherConversation := f.seedConversation("authz_other", "other")
	if otherConversation == "" {
		t.Fatal("missing non-member conversation")
	}

	cases := []struct {
		name    string
		request app.ReadRequest
		want    app.Code
	}{
		{"nonmember-out-of-range-cursor", app.ReadRequest{ConversationID: otherConversation, HasAfterSeq: true, AfterSeq: 999, Limit: 101}, app.Forbidden},
		{"nonmember-negative-cursor", app.ReadRequest{ConversationID: otherConversation, HasAfterSeq: true, AfterSeq: -1, Limit: -1}, app.Forbidden},
		{"unknown-conversation", app.ReadRequest{ConversationID: "unknown_conversation", Limit: 101}, app.Forbidden},
		{"member-invalid-limit", app.ReadRequest{ConversationID: conversationID, Limit: 101}, app.LimitExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			page, err := f.read(tc.request)
			mustCode(t, err, tc.want)
			assertZeroPage(t, page)
		})
	}

	var zero session.ConnectionIdentity
	page, err := f.service.ReadAfterSeq(ctx, zero, app.ReadRequest{ConversationID: conversationID})
	mustCode(t, err, app.Forbidden)
	assertZeroPage(t, page)

	page, err = f.service.ReadAfterSeq(ctx, f.identity, app.ReadRequest{ConversationID: "bad conversation"})
	mustCode(t, err, app.Forbidden)
	assertZeroPage(t, page)
}

func TestDelta(t *testing.T) {
	f := openFixture(t)
	conversationID := f.seedConversation("delta_pages", f.user)
	for i := 1; i <= 5; i++ {
		f.persist(conversationID, "delta_client_"+string(rune('a'+i)), payloadOfLength(40+i))
	}

	first, err := f.read(app.ReadRequest{ConversationID: conversationID, Limit: 2})
	must(t, err)
	assertSequences(t, first, 1, 2)
	if !first.HasNext || first.NextAfterSeq == nil || *first.NextAfterSeq != 2 || first.LatestSeq != 5 {
		t.Fatalf("unexpected first page: %+v", first)
	}

	second, err := f.read(app.ReadRequest{ConversationID: conversationID, HasAfterSeq: true, AfterSeq: 2, Limit: 2})
	must(t, err)
	assertSequences(t, second, 3, 4)
	if !second.HasNext || second.NextAfterSeq == nil || *second.NextAfterSeq != 4 {
		t.Fatalf("unexpected second page: %+v", second)
	}
	repeat, err := f.read(app.ReadRequest{ConversationID: conversationID, HasAfterSeq: true, AfterSeq: 2, Limit: 2})
	must(t, err)
	assertSequences(t, repeat, 3, 4)
	if repeat.LatestSeq != second.LatestSeq || repeat.HasNext != second.HasNext || repeat.NextAfterSeq == nil || second.NextAfterSeq == nil || *repeat.NextAfterSeq != *second.NextAfterSeq {
		t.Fatalf("repeat was not deterministic: first=%+v second=%+v", second, repeat)
	}

	terminal, err := f.read(app.ReadRequest{ConversationID: conversationID, HasAfterSeq: true, AfterSeq: 4, Limit: 2})
	must(t, err)
	assertSequences(t, terminal, 5)
	if terminal.HasNext || terminal.NextAfterSeq == nil || *terminal.NextAfterSeq != 5 {
		t.Fatalf("unexpected terminal page: %+v", terminal)
	}

	empty, err := f.read(app.ReadRequest{ConversationID: conversationID, HasAfterSeq: true, AfterSeq: 5, Limit: 2})
	must(t, err)
	assertZeroPage(t, empty)
	if empty.LatestSeq != 5 {
		t.Fatalf("empty terminal latest got %d want 5", empty.LatestSeq)
	}

	all, err := f.read(app.ReadRequest{ConversationID: conversationID, Limit: 0})
	must(t, err)
	assertSequences(t, all, 1, 2, 3, 4, 5)
	if all.HasNext || all.NextAfterSeq == nil || *all.NextAfterSeq != 5 {
		t.Fatalf("unexpected default page: %+v", all)
	}

	afterZero, err := f.read(app.ReadRequest{ConversationID: conversationID, HasAfterSeq: true, AfterSeq: 0, Limit: 1})
	must(t, err)
	assertSequences(t, afterZero, 1)
	if !afterZero.HasNext || afterZero.NextAfterSeq == nil || *afterZero.NextAfterSeq != 1 {
		t.Fatalf("unexpected after-zero boundary page: %+v", afterZero)
	}
	for name, request := range map[string]app.ReadRequest{
		"min-after": {ConversationID: conversationID, HasAfterSeq: true, AfterSeq: math.MinInt64, Limit: 10},
		"max-after": {ConversationID: conversationID, HasAfterSeq: true, AfterSeq: math.MaxInt64, Limit: 10},
	} {
		t.Run(name, func(t *testing.T) {
			page, err := f.read(request)
			mustCode(t, err, app.InvalidCursor)
			assertZeroPage(t, page)
		})
	}

	reordered := f.seedConversation("delta_reordered", f.user)
	for _, seq := range []int64{3, 1, 2} {
		f.insertMessage(reordered, seq, payloadOfLength(40))
	}
	f.setHead(reordered, 3, f.messageID(reordered, 3))
	orderedPage, err := f.read(app.ReadRequest{ConversationID: reordered, Limit: 10})
	must(t, err)
	assertSequences(t, orderedPage, 1, 2, 3)

	for name, request := range map[string]app.ReadRequest{
		"negative-after":     {ConversationID: conversationID, HasAfterSeq: true, AfterSeq: -1, Limit: 10},
		"presence-conflict":  {ConversationID: conversationID, AfterSeq: 1, Limit: 10},
		"cursor-beyond-head": {ConversationID: conversationID, HasAfterSeq: true, AfterSeq: 6, Limit: 10},
	} {
		t.Run(name, func(t *testing.T) {
			page, err := f.read(request)
			mustCode(t, err, app.InvalidCursor)
			assertZeroPage(t, page)
		})
	}
	t.Run("negative-limit", func(t *testing.T) {
		page, err := f.read(app.ReadRequest{ConversationID: conversationID, Limit: -1})
		mustCode(t, err, app.LimitExceeded)
		assertZeroPage(t, page)
	})
	t.Run("cursor-error-precedes-limit", func(t *testing.T) {
		page, err := f.read(app.ReadRequest{ConversationID: conversationID, HasAfterSeq: true, AfterSeq: -1, Limit: -1})
		mustCode(t, err, app.InvalidCursor)
		assertZeroPage(t, page)
	})

	bigConversation := f.seedConversation("delta_budget", f.user)
	for i := 1; i <= 5; i++ {
		f.persist(bigConversation, "delta_big_client_"+string(rune('a'+i)), strings.Repeat("x", 65536))
	}
	budgetPage, err := f.read(app.ReadRequest{ConversationID: bigConversation, Limit: 5})
	must(t, err)
	assertSequences(t, budgetPage, 1, 2, 3)
	if !budgetPage.HasNext || budgetPage.NextAfterSeq == nil || *budgetPage.NextAfterSeq != 3 {
		t.Fatalf("unexpected budget page: %+v", budgetPage)
	}
}

func TestRedaction(t *testing.T) {
	sentinel := "payload_TOKEN_secret_DSN_SELECT_alice"
	observer := &redactionObserver{}
	service, err := app.NewService(redactionStore{err: errors.New(sentinel)}, app.Config{Observer: observer})
	must(t, err)
	_, err = service.ReadAfterSeq(context.Background(), fIdentity(t), app.ReadRequest{ConversationID: "conversation"})
	mustCode(t, err, app.StorageUnavailable)
	if strings.Contains(err.Error(), sentinel) || strings.Contains(err.Error(), "SELECT") || strings.Contains(err.Error(), "DSN") {
		t.Fatalf("sensitive text escaped error: %v", err)
	}
	var known *app.Error
	if !errors.As(err, &known) || known.Unwrap() != nil {
		t.Fatalf("error is not sealed: %#v", err)
	}
	if len(observer.values) != 1 || observer.values[0].Code != app.StorageUnavailable {
		t.Fatalf("unexpected observations: %+v", observer.values)
	}

	// Exercise a real adapter/driver error whose unsealed text would contain a
	// table name. The public error must still expose only the stable code.
	real := openFixture(t)
	conversationID := real.seedConversation("redaction_real", real.user)
	real.sql("ALTER TABLE newim.im_messages RENAME TO im_messages_hidden")
	t.Cleanup(func() {
		real.sql("ALTER TABLE newim.im_messages_hidden RENAME TO im_messages")
	})
	page, err := real.read(app.ReadRequest{ConversationID: conversationID, Limit: 10})
	mustCode(t, err, app.StorageUnavailable)
	assertZeroPage(t, page)
	if strings.Contains(err.Error(), "im_messages") || strings.Contains(err.Error(), "SELECT") || strings.Contains(err.Error(), "postgres") {
		t.Fatalf("adapter driver detail escaped sealed error: %v", err)
	}
}

func fIdentity(t *testing.T) session.ConnectionIdentity {
	t.Helper()
	value, err := session.NewConnectionIdentity("alice", "device", "session", "connection", "0123456789abcdef0123456789abcdef")
	must(t, err)
	return value
}

type redactionStore struct{ err error }

func (s redactionStore) Read(context.Context, string, app.ReadRequest) (app.ReadResult, error) {
	return app.ReadResult{}, s.err
}

type redactionObserver struct{ values []app.Observation }

func (o *redactionObserver) Observe(value app.Observation) { o.values = append(o.values, value) }
