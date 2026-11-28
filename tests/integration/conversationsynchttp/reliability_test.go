//go:build integration

package conversationsynchttp_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/messagehttp"
	"github.com/m-ice/NewIM/server/push"
)

func TestTransportReliability(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(22)
	keys := writeKeys(t, "reliability", "reliability")
	original := f.accountSettingsGateway(keys)
	standby := f.accountSettingsGateway(keys)
	pair := accountPair(t, f, original, "reliable_transport")
	conversation := pair.conversation
	stream := accountHistorySocket(t, f, original, pair.owner, conversation)

	first := send(t, f, original, pair.peer, conversation, "reliable_first", "before restart")
	requireSocketMessage(t, f, stream, first)
	requireCommittedHistory(t, f, conversation, first)
	must(t, original.cmd.Process.Kill())
	select {
	case <-original.done:
		if original.waitErr == nil {
			t.Fatal("original gateway exited without interruption")
		}
	case <-f.ctx.Done():
		t.Fatal("original gateway did not exit")
	}
	readCtx, stopRead := context.WithTimeout(f.ctx, 3*time.Second)
	_, _, err := stream.Read(readCtx)
	readErr := readCtx.Err()
	stopRead()
	if err == nil || readErr != nil {
		t.Fatal("gateway exit did not close the established socket")
	}

	lostBody := textBody(t, "reliable_lost", conversation, "committed while disconnected")
	lost := discardReceipt(t, f, standby, pair.peer, lostBody)
	second := expectACK(t, lost)
	if !lost.verified || second.ConversationSeq != "2" {
		t.Fatal("discarded receipt did not come from the committed TLS send")
	}
	requireCommittedHistory(t, f, conversation, first, second)
	replacement := f.accountSettingsGateway(keys)
	resumed := accountHistorySocketFrom(t, f, replacement, pair.owner, conversation, first.ConversationSeq)
	requireSocketMessage(t, f, resumed, second)

	replay := f.call(replacement, http.MethodPost, sendPath, pair.peer, lostBody, nil)
	if got := expectACK(t, replay); !replay.verified || got != second || !bytes.Equal(lost.body, replay.body) {
		t.Fatal("same-intent retry on the replacement gateway returned a different receipt")
	}

	waiting := textBody(t, "reliable_delayed", conversation, "committed after lock release")
	block, err := f.db.Begin(f.ctx)
	must(t, err)
	defer block.Rollback(f.ctx)
	var locked string
	must(t, block.QueryRow(f.ctx,
		"SELECT conversation_id FROM newim.im_conversations WHERE conversation_id=$1 FOR UPDATE",
		conversation).Scan(&locked))
	timeout := f.call(replacement, http.MethodPost, sendPath, pair.peer, waiting, nil)
	timeout.requireError(t, http.StatusServiceUnavailable, string(messagehttp.CodeTemporaryUnavailable))
	if !timeout.verified || timeout.headers.Get("Retry-After") == "" ||
		len(timeout.headers.Values(messagehttp.TraceHeader)) != 0 {
		t.Fatal("timed-out send did not return an unactioned retry response")
	}
	must(t, block.Rollback(f.ctx))
	requireCommittedHistory(t, f, conversation, first, second)

	third := expectACK(t, f.call(replacement, http.MethodPost, sendPath, pair.peer, waiting, nil))
	requireSocketMessage(t, f, resumed, third)
	requireCommittedHistory(t, f, conversation, first, second, third)
	for _, token := range []string{pair.owner, pair.tablet} {
		floor, tail, history := accountMessageHistory(t, f, replacement, token, conversation)
		if floor != "0" || tail != "3" || len(history) != 3 {
			t.Fatal("client did not recover the full committed history")
		}
		for i, want := range []protocol.Ack{first, second, third} {
			if history[i].ServerMsgID != want.ServerMsgID || history[i].ConversationSeq != want.ConversationSeq ||
				history[i].ClientMsgID != want.ClientMsgID {
				t.Fatalf("client history differs at sequence %s", want.ConversationSeq)
			}
		}
	}
}

func requireSocketMessage(t *testing.T, f *fixture, stream *websocket.Conn, want protocol.Ack) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 4*time.Second)
	defer cancel()
	kind, wire, err := stream.Read(ctx)
	must(t, err)
	got, err := protocol.DecodeServerFrame(wire)
	must(t, err)
	if kind != websocket.MessageText || got.Message == nil || got.Message.ServerMsgID != want.ServerMsgID ||
		got.Message.ConversationSeq != want.ConversationSeq || got.Message.ClientMsgID != want.ClientMsgID ||
		got.Message.ConversationID != want.ConversationID {
		t.Fatalf("socket did not deliver committed sequence %s", want.ConversationSeq)
	}
}

func requireCommittedHistory(t *testing.T, f *fixture, conversation string, acknowledgements ...protocol.Ack) {
	t.Helper()
	wantCount := len(acknowledgements)
	var rows, sequence int
	var latest string
	must(t, f.db.QueryRow(f.ctx, `SELECT c.last_seq,c.latest_server_msg_id,
		(SELECT count(*) FROM newim.im_messages m WHERE m.conversation_id=c.conversation_id)
		FROM newim.im_conversations c WHERE c.conversation_id=$1`, conversation).Scan(&sequence, &latest, &rows))
	if rows != wantCount || sequence != wantCount || latest != acknowledgements[wantCount-1].ServerMsgID {
		t.Fatalf("committed history has %d messages and sequence %d, want %d", rows, sequence, wantCount)
	}
	for i, ack := range acknowledgements {
		var committed int
		must(t, f.db.QueryRow(f.ctx, `SELECT count(*)
			FROM newim.im_messages m JOIN newim.im_outbox_events o USING(server_msg_id)
			WHERE m.conversation_id=$1 AND m.client_msg_id=$2 AND m.server_msg_id=$3 AND m.conversation_seq=$4`,
			conversation, ack.ClientMsgID, ack.ServerMsgID, i+1).Scan(&committed))
		if ack.ConversationSeq != strconv.Itoa(i+1) || committed != 1 {
			t.Fatalf("sequence %d does not identify one committed message and outbox event", i+1)
		}
	}
}

var errReceiptDiscarded = errors.New("fixture discarded one committed send response")

type receiptDropper struct {
	transport http.RoundTripper
	committed response
}

func (d *receiptDropper) RoundTrip(request *http.Request) (*http.Response, error) {
	reply, err := d.transport.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer reply.Body.Close()
	body, err := io.ReadAll(io.LimitReader(reply.Body, protocol.MaxFrameBytes+1))
	if err != nil {
		return nil, err
	}
	d.committed = response{
		status: reply.StatusCode, headers: reply.Header, body: body,
		verified: reply.TLS != nil && len(reply.TLS.VerifiedChains) > 0,
	}
	return nil, errReceiptDiscarded
}

func discardReceipt(t *testing.T, f *fixture, gateway *process, token string, body []byte) response {
	t.Helper()
	transport := &receiptDropper{transport: f.client.Transport}
	client := *f.client
	client.Transport = transport
	sender := *f
	sender.client = &client
	if reply := sender.call(gateway, http.MethodPost, sendPath, token, body, nil); !errors.Is(reply.err, errReceiptDiscarded) {
		t.Fatal("sender did not observe a dropped response")
	}
	return transport.committed
}

func TestPushReceiverTimeout(t *testing.T) {
	f, repo, keys := pushFixture(t)
	gateway := pushGateway(f, writeKeys(t, "push", "push"), keys)
	pair := accountPair(t, f, gateway, "push_timeout")
	registerPushToken(t, f, gateway, pair.owner, push.FCM, phonePushToken)
	must(t, repo.Enable(f.ctx))
	body := textBody(t, "push_timeout_message", pair.conversation, "notification timeout preserves the message")
	ack := expectACK(t, f.call(gateway, http.MethodPost, sendPath, pair.peer, body, nil))

	var calls atomic.Int32
	cancelled := make(chan struct{})
	release := make(chan struct{})
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := io.ReadAll(io.LimitReader(r.Body, 1024))
		if err != nil || !bytes.Equal(token, []byte(phonePushToken)) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if calls.Add(1) == 1 {
			select {
			case <-r.Context().Done():
				close(cancelled)
			case <-release:
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	defer close(release)
	worker, err := push.NewWorker(repo, map[push.Channel]push.Provider{
		push.FCM: testPushProvider(func(ctx context.Context, token []byte) push.Outcome {
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, receiver.URL, bytes.NewReader(token))
			must(t, err)
			response, err := receiver.Client().Do(request)
			if errors.Is(err, context.DeadlineExceeded) && errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return push.Outcome{Result: push.TemporaryFailure}
			}
			must(t, err)
			defer response.Body.Close()
			if response.StatusCode != http.StatusNoContent {
				t.Fatalf("notification fixture status=%d", response.StatusCode)
			}
			return push.Outcome{Result: push.Accepted}
		}),
	})
	must(t, err)
	progress, err := worker.Cycle(f.ctx)
	must(t, err)
	if !progress || worker.Counts().Retry != 1 {
		t.Fatal("receiver timeout did not commit one retry")
	}
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("the first receiver request did not observe cancellation")
	}
	var event, state string
	var attempts int
	must(t, f.db.QueryRow(f.ctx, `SELECT d.event_id,d.status,d.attempts
		FROM newim.im_push_deliveries d JOIN newim.im_outbox_events o USING(event_id)
		WHERE o.server_msg_id=$1`, ack.ServerMsgID).Scan(&event, &state, &attempts))
	if state != "pending" || attempts != 1 {
		t.Fatal("the timed-out notification was not retained for retry")
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for worker.Counts().Accepted == 0 {
		select {
		case <-ticker.C:
			_, err := worker.Cycle(f.ctx)
			must(t, err)
		case <-deadline.C:
			t.Fatal("the persisted notification did not recover when the receiver resumed")
		}
	}
	must(t, f.db.QueryRow(f.ctx, "SELECT status,attempts FROM newim.im_push_deliveries WHERE event_id=$1", event).
		Scan(&state, &attempts))
	if calls.Load() != 2 || state != "accepted" || attempts != 2 {
		t.Fatal("notification retry did not converge on the original job")
	}
	pushJobs(t, f, ack.ServerMsgID, 1)
	retry := expectACK(t, f.call(gateway, http.MethodPost, sendPath, pair.peer, body, nil))
	if retry != ack {
		t.Fatal("push timeout changed the committed message receipt")
	}
	requireCommittedHistory(t, f, pair.conversation, ack)
}
