//go:build integration

package messagesync_test

import (
	"os"
	"testing"

	app "github.com/m-ice/NewIM/server/sync/message"
)

func TestRecovery(t *testing.T) {
	f := openFixture(t)
	switch os.Getenv("NEWIM_MESSAGE_DELTA_PHASE") {
	case "", "prepare":
		prepareRecovery(t, f)
	case "restart":
		verifyRecovery(t, f)
	default:
		t.Fatalf("unknown recovery phase")
	}
}

func prepareRecovery(t *testing.T, f *fixture) {
	t.Helper()
	base := f.seedConversation("recovery_base", f.user)
	for i := 1; i <= 3; i++ {
		f.persist(base, "recovery_client_"+string(rune('a'+i)), payloadOfLength(40+i))
	}

	missingAnchor := f.seedConversation("recovery_missing_anchor", f.user)
	for _, seq := range []int64{1, 2, 4} {
		f.insertMessage(missingAnchor, seq, payloadOfLength(40))
	}
	f.setHead(missingAnchor, 4, f.messageID(missingAnchor, 4))

	gapPage := f.seedConversation("recovery_gap_page", f.user)
	for _, seq := range []int64{1, 2, 4} {
		f.insertMessage(gapPage, seq, payloadOfLength(40))
	}
	f.setHead(gapPage, 4, f.messageID(gapPage, 4))

	seqZero := f.seedConversation("recovery_seq_zero", f.user)
	f.insertMessage(seqZero, 0, payloadOfLength(40))
	f.setHead(seqZero, 0, f.messageID(seqZero, 0))

	aboveHead := f.seedConversation("recovery_above_head", f.user)
	for _, seq := range []int64{1, 3} {
		f.insertMessage(aboveHead, seq, payloadOfLength(40))
	}
	f.setHead(aboveHead, 2, f.messageID(aboveHead, 1))

	badPointer := f.seedConversation("recovery_bad_pointer", f.user)
	for _, seq := range []int64{1, 2} {
		f.insertMessage(badPointer, seq, payloadOfLength(40))
	}
	f.setHead(badPointer, 2, f.messageID(badPointer, 1))
}

func verifyRecovery(t *testing.T, f *fixture) {
	t.Helper()
	base := "recovery_base_conversation"
	page, err := f.read(app.ReadRequest{ConversationID: base, HasAfterSeq: true, AfterSeq: 1, Limit: 100})
	must(t, err)
	assertSequences(t, page, 2, 3)
	if page.LatestSeq != 3 || page.HasNext || page.NextAfterSeq == nil || *page.NextAfterSeq != 3 {
		t.Fatalf("unexpected restart page: %+v", page)
	}

	for name, tc := range map[string]struct {
		conversation string
		request      app.ReadRequest
		code         app.Code
	}{
		"missing-anchor": {conversation: "recovery_missing_anchor_conversation", request: app.ReadRequest{HasAfterSeq: true, AfterSeq: 3, Limit: 10}, code: app.InvalidCursor},
		"page-gap":       {conversation: "recovery_gap_page_conversation", request: app.ReadRequest{HasAfterSeq: true, AfterSeq: 2, Limit: 10}, code: app.StorageUnavailable},
		"seq-zero":       {conversation: "recovery_seq_zero_conversation", request: app.ReadRequest{Limit: 10}, code: app.StorageUnavailable},
		"above-head":     {conversation: "recovery_above_head_conversation", request: app.ReadRequest{Limit: 10}, code: app.StorageUnavailable},
		"bad-pointer":    {conversation: "recovery_bad_pointer_conversation", request: app.ReadRequest{Limit: 10}, code: app.StorageUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			request := tc.request
			request.ConversationID = tc.conversation
			result, err := f.read(request)
			mustCode(t, err, tc.code)
			assertZeroPage(t, result)
		})
	}

	// Revoke committed before the reader's snapshot is visible and forbidden.
	f.sql("DELETE FROM newim.im_conversation_members WHERE conversation_id=$1 AND user_id=$2", base, f.user)
	result, err := f.read(app.ReadRequest{ConversationID: base, Limit: 10})
	mustCode(t, err, app.Forbidden)
	assertZeroPage(t, result)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", base, f.user)

	// Revoke committed after snapshot acquisition does not retroactively fail
	// that read; the next request observes the committed revoke.
	done := make(chan error, 1)
	raw, rawErr := f.repo.ReadWithTestHook(ctx, f.user, app.ReadRequest{ConversationID: base, Limit: 10}, func() error {
		_, err := f.db.Exec(ctx, "DELETE FROM newim.im_conversation_members WHERE conversation_id=$1 AND user_id=$2", base, f.user)
		done <- err
		return err
	})
	must(t, rawErr)
	if len(raw.Rows) != 3 {
		t.Fatalf("snapshot read rows got %d want 3", len(raw.Rows))
	}
	must(t, <-done)
	forbiddenPage, err := f.read(app.ReadRequest{ConversationID: base, Limit: 10})
	mustCode(t, err, app.Forbidden)
	assertZeroPage(t, forbiddenPage)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", base, f.user)

	// Append committed after snapshot is absent from that snapshot and
	// visible to the next continuation.
	done = make(chan error, 1)
	raw, rawErr = f.repo.ReadWithTestHook(ctx, f.user, app.ReadRequest{ConversationID: base, HasAfterSeq: true, AfterSeq: 1, Limit: 100}, func() error {
		_, err := f.db.Exec(ctx, `SELECT newim.persist_message($1,$2,$3,$4,'text',1,1800000000999,convert_to($5,'UTF8'),$6)`,
			f.user, "recovery_snapshot_append_client", base, "recovery_snapshot_append_server", payloadOfLength(40), "recovery_snapshot_append_event")
		done <- err
		return err
	})
	if rawErr != nil {
		t.Fatal(rawErr)
	}
	if len(raw.Rows) != 2 {
		t.Fatalf("snapshot append leaked into in-flight page: %+v", raw)
	}
	must(t, <-done)
	next, err := f.read(app.ReadRequest{ConversationID: base, HasAfterSeq: true, AfterSeq: 3, Limit: 100})
	must(t, err)
	assertSequences(t, next, 4)

	// Terminating the adapter backend is a bounded storage failure, not a
	// partial page or a forged success.
	_, err = f.repo.ReadWithTestHook(ctx, f.user, app.ReadRequest{ConversationID: base, Limit: 10}, func() error {
		_, err := f.db.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='nim_message_delta_test' AND pid<>pg_backend_pid()")
		return err
	})
	mustCode(t, err, app.StorageUnavailable)
}

func (f *fixture) messageID(conversationID string, seq int64) string {
	f.t.Helper()
	return f.scalarString("SELECT server_msg_id FROM newim.im_messages WHERE conversation_id=$1 AND conversation_seq=$2", conversationID, seq)
}
