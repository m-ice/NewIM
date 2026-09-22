//go:build integration

package message_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	session "github.com/m-ice/NewIM/server/auth/session"
	conversation "github.com/m-ice/NewIM/server/conversation"
	app "github.com/m-ice/NewIM/server/message"
	store "github.com/m-ice/NewIM/server/storage/message"
)

var ctx = context.Background()
var serviceSeq atomic.Int64

type fixture struct {
	t     *testing.T
	db    *pgx.Conn
	repo  *store.Repository
	dbn   string
	now   atomic.Int64
	seqID atomic.Int64
}

func openFixture(t *testing.T) *fixture {
	t.Helper()
	database := "newim_test"
	if os.Getenv("NEWIM_MESSAGE_PHASE") == "restore" {
		database = "message_restore"
	}
	dsn := fmt.Sprintf("host=/var/run/postgresql user=newim_test dbname=%s sslmode=disable", database)
	conn, err := pgx.Connect(ctx, dsn)
	must(t, err)
	repo, err := store.Open(ctx, store.Config{
		DSN: dsn, AllowLocalSocket: true, MaxConnections: 8,
		ApplicationName: "nim_message_test",
	})
	must(t, err)
	f := &fixture{t: t, db: conn, repo: repo, dbn: database}
	f.now.Store(1_800_000_000_000)
	t.Cleanup(func() {
		repo.Close()
		_ = conn.Close(context.Background())
	})
	return f
}

func (f *fixture) seedConversation(prefix string, members ...string) string {
	f.t.Helper()
	conversationID := prefix + "_conversation"
	for _, member := range members {
		f.sql("INSERT INTO newim.im_users(user_id) VALUES($1) ON CONFLICT DO NOTHING", member)
	}
	f.sql("INSERT INTO newim.im_conversations(conversation_id) VALUES($1) ON CONFLICT DO NOTHING", conversationID)
	for _, member := range members {
		f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2) ON CONFLICT DO NOTHING", conversationID, member)
	}
	return conversationID
}

func (f *fixture) identity(userID string) session.ConnectionIdentity {
	f.t.Helper()
	identity, err := session.NewConnectionIdentity(userID, userID+"_device", userID+"_session", userID+"_connection", "0123456789abcdef0123456789abcdef")
	must(f.t, err)
	return identity
}

type sequenceIDs struct {
	prefix string
	next   atomic.Int64
}

func (g *sequenceIDs) NewID() (string, error) {
	n := g.next.Add(1)
	return fmt.Sprintf("%s_%06d", g.prefix, n), nil
}

func (f *fixture) service(observer app.Observer) *app.Service {
	f.t.Helper()
	service, err := app.NewService(f.repo, app.Config{
		IDs:      &sequenceIDs{prefix: fmt.Sprintf("generated_%d", serviceSeq.Add(1))},
		Clock:    app.ClockFunc(func() time.Time { return time.UnixMilli(f.now.Load()).UTC() }),
		Observer: observer,
	})
	must(f.t, err)
	return service
}

func (f *fixture) request(clientID, conversationID, text string) protocol.Send {
	return protocol.Send{
		ProtocolVersion: 1, ClientMsgID: clientID, ConversationID: conversationID,
		Version: 1, Type: "text", Payload: json.RawMessage(`{"text":"` + text + `"}`),
	}
}

func (f *fixture) send(service *app.Service, identity session.ConnectionIdentity, request protocol.Send) (protocol.ServerFrame, error) {
	f.t.Helper()
	return service.Send(ctx, identity, request)
}

func (f *fixture) sql(statement string, args ...any) {
	f.t.Helper()
	_, err := f.db.Exec(ctx, statement, args...)
	must(f.t, err)
}

func (f *fixture) scalarString(statement string, args ...any) string {
	f.t.Helper()
	var value string
	must(f.t, f.db.QueryRow(ctx, statement, args...).Scan(&value))
	return value
}

func (f *fixture) scalarInt64(statement string, args ...any) int64 {
	f.t.Helper()
	var value int64
	must(f.t, f.db.QueryRow(ctx, statement, args...).Scan(&value))
	return value
}

func (f *fixture) scalarBool(statement string, args ...any) bool {
	f.t.Helper()
	var value bool
	must(f.t, f.db.QueryRow(ctx, statement, args...).Scan(&value))
	return value
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("operation failed: %v", err)
	}
}

func mustCode(t *testing.T, err error, code app.Code) {
	t.Helper()
	if got := app.ErrorCode(err); got != code {
		t.Fatalf("error code got %s want %s (%v)", got, code, err)
	}
}

func mustDisposition(t *testing.T, err error, want protocol.RetryDisposition) {
	t.Helper()
	if got := app.RetryDisposition(err); got != want {
		t.Fatalf("retry disposition got %s want %s (%v)", got, want, err)
	}
}

func ack(t *testing.T, frame protocol.ServerFrame) protocol.Ack {
	t.Helper()
	if frame.Ack == nil {
		t.Fatalf("server frame is not an ACK: %+v", frame)
	}
	return *frame.Ack
}

func (f *fixture) assertAtomic(conversationID, serverMsgID string) {
	f.t.Helper()
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE conversation_id=$1 AND server_msg_id=$2", conversationID, serverMsgID); got != 1 {
		f.t.Fatalf("message count got %d want 1", got)
	}
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_outbox_events WHERE server_msg_id=$1", serverMsgID); got != 1 {
		f.t.Fatalf("outbox count got %d want 1", got)
	}
	if got := f.scalarString("SELECT last_seq::text||'|'||latest_server_msg_id FROM newim.im_conversations WHERE conversation_id=$1", conversationID); got == "0|" {
		f.t.Fatalf("conversation summary was not updated: %s", got)
	}
}

type captureObserver struct {
	mu           sync.Mutex
	observations []app.Observation
}

func (o *captureObserver) Observe(observation app.Observation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.observations = append(o.observations, observation)
}

func (o *captureObserver) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.observations)
}

type errorStore struct{ err error }

func (s *errorStore) Persist(context.Context, conversation.Principal, protocol.Send, func() (app.Generated, error)) (app.PersistedMessage, error) {
	return app.PersistedMessage{}, s.err
}

type lostResponseStore struct {
	inner app.Store
	once  atomic.Bool
}

func (s *lostResponseStore) Persist(ctx context.Context, principal conversation.Principal, request protocol.Send, generate func() (app.Generated, error)) (app.PersistedMessage, error) {
	result, err := s.inner.Persist(ctx, principal, request, generate)
	if err != nil {
		return app.PersistedMessage{}, err
	}
	if s.once.CompareAndSwap(false, true) {
		return app.PersistedMessage{}, app.Fail(app.SendStorageUnavailable)
	}
	return result, nil
}
