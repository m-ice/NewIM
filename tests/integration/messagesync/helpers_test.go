//go:build integration

package messagesync_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	session "github.com/m-ice/NewIM/server/auth/session"
	store "github.com/m-ice/NewIM/server/storage/messagesync"
	app "github.com/m-ice/NewIM/server/sync/message"
)

var ctx = context.Background()
var idSeq atomic.Int64

type fixture struct {
	t        *testing.T
	db       *pgx.Conn
	repo     *store.Repository
	service  *app.Service
	user     string
	identity session.ConnectionIdentity
}

func openFixture(t *testing.T) *fixture {
	t.Helper()
	database := os.Getenv("NEWIM_MESSAGE_DELTA_DATABASE")
	if database == "" {
		database = "newim_test"
	}
	dsn := fmt.Sprintf("host=/var/run/postgresql user=newim_test dbname=%s sslmode=disable", database)
	conn, err := pgx.Connect(ctx, dsn)
	must(t, err)
	repo, err := store.Open(ctx, store.Config{DSN: dsn, AllowLocalSocket: true, MaxConnections: 8, ApplicationName: "nim_message_delta_test"})
	must(t, err)
	service, err := app.NewService(repo, app.Config{RequestTimeout: 5_000_000_000})
	must(t, err)
	user := "delta_reader"
	identity, err := session.NewConnectionIdentity(user, user+"_device", user+"_session", user+"_connection", "0123456789abcdef0123456789abcdef")
	must(t, err)
	f := &fixture{t: t, db: conn, repo: repo, service: service, user: user, identity: identity}
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

func (f *fixture) insertMessage(conversationID string, seq int64, payload string) {
	f.t.Helper()
	n := idSeq.Add(1)
	f.sql(`INSERT INTO newim.im_messages(server_msg_id,client_msg_id,sender_id,conversation_id,conversation_seq,protocol_version,schema_version,message_type,server_time_ms,payload_bytes)
VALUES($1,$2,$3,$4,$5,1,1,'text',$6,convert_to($7,'UTF8'))`,
		fmt.Sprintf("direct_server_%d", n), fmt.Sprintf("direct_client_%d", n), f.user,
		conversationID, seq, seq, payload)
}

func (f *fixture) persist(conversationID, clientID, payload string) {
	f.t.Helper()
	n := idSeq.Add(1)
	f.sql(`SELECT newim.persist_message($1,$2,$3,$4,'text',1,$5,convert_to($6,'UTF8'),$7)`,
		f.user, clientID, conversationID, fmt.Sprintf("persisted_server_%d", n), int64(1800000000000+n),
		payload, fmt.Sprintf("persisted_event_%d", n))
}

func (f *fixture) setHead(conversationID string, head int64, serverID string) {
	f.t.Helper()
	f.sql("UPDATE newim.im_conversations SET last_seq=$2, latest_server_msg_id=$3 WHERE conversation_id=$1", conversationID, head, serverID)
}

func (f *fixture) read(request app.ReadRequest) (app.Page, error) {
	f.t.Helper()
	return f.service.ReadAfterSeq(ctx, f.identity, request)
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

func (f *fixture) explain(label, query string, args ...any) string {
	f.t.Helper()
	var raw []byte
	if err := f.db.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query, args...).Scan(&raw); err != nil {
		f.t.Fatalf("explain %s: %v", label, err)
	}
	return string(raw)
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func mustCode(t *testing.T, err error, want app.Code) {
	t.Helper()
	if got := app.ErrorCode(err); got != want {
		t.Fatalf("error code got %s want %s (%v)", got, want, err)
	}
}

func assertZeroPage(t *testing.T, page app.Page) {
	t.Helper()
	if len(page.Items) != 0 || page.HasNext || page.NextAfterSeq != nil {
		t.Fatalf("expected zero page, got %+v", page)
	}
}

func assertSequences(t *testing.T, page app.Page, want ...int64) {
	t.Helper()
	if len(page.Items) != len(want) {
		t.Fatalf("item count got %d want %d: %+v", len(page.Items), len(want), page)
	}
	for i, seq := range want {
		if page.Items[i].ConversationSeq != seq {
			t.Fatalf("item %d sequence got %d want %d", i, page.Items[i].ConversationSeq, seq)
		}
	}
}

func payloadOfLength(n int) string {
	return `{"text":"` + strings.Repeat("x", n-12) + `"}`
}
