//go:build integration

package webhook_test

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	store "github.com/m-ice/NewIM/server/storage/webhook"
	app "github.com/m-ice/NewIM/server/webhook"
)

var ctx = context.Background()
var fixtureSeq atomic.Int64

type fixture struct {
	t    *testing.T
	db   *pgx.Conn
	repo *store.Repository
	now  time.Time
	seq  atomic.Int64
}

func openFixture(t *testing.T) *fixture {
	t.Helper()
	dsn := os.Getenv("NEWIM_WEBHOOK_DSN")
	if dsn == "" {
		dsn = "host=/var/run/postgresql user=newim_test dbname=newim_test sslmode=disable"
	}
	conn, err := pgx.Connect(ctx, dsn)
	must(t, err)
	repo, err := store.Open(ctx, store.Config{DSN: dsn, AllowLocalSocket: true, MaxConnections: 8, ApplicationName: "nim_webhook_test"})
	must(t, err)
	f := &fixture{t: t, db: conn, repo: repo, now: time.Now()}
	t.Cleanup(func() {
		repo.Close()
		_ = conn.Close(context.Background())
	})
	return f
}

func (f *fixture) id(prefix string) string {
	return fmt.Sprintf("%s_%06d", prefix, f.seq.Add(1))
}

func (f *fixture) seedEvent(conversation string) string {
	f.t.Helper()
	user := f.id("webhook_user")
	client := f.id("webhook_client")
	server := f.id("webhook_server")
	event := f.id("webhook_event")
	f.sql("INSERT INTO newim.im_users(user_id) VALUES($1)", user)
	f.sql("INSERT INTO newim.im_conversations(conversation_id) VALUES($1)", conversation)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", conversation, user)
	f.sql(`SELECT newim.persist_message($1,$2,$3,$4,'text',1,$5,convert_to($6,'UTF8'),$7)`,
		user, client, conversation, server, strconvMillis(f.now), `{"text":"integration payload"}`, event)
	return event
}

func (f *fixture) insertEndpoint(activeRevision int64, url string, secret []byte) (string, []byte) {
	f.t.Helper()
	destination := f.id("webhook_destination")
	keyID := f.id("webhook_key")
	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		f.t.Fatal(err)
	}
	master := []byte("0123456789abcdef0123456789abcdef")
	block, err := aes.NewCipher(master)
	must(f.t, err)
	aead, err := cipher.NewGCM(block)
	must(f.t, err)
	material := app.SecretMaterial{DestinationID: destination, Revision: activeRevision, URL: url, KeyID: keyID, Nonce: nonce}
	ciphertext, err := app.SealSecret(aead, material, secret)
	must(f.t, err)
	tx, err := f.db.Begin(ctx)
	must(f.t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, "INSERT INTO newim.im_webhook_endpoints(destination_id,status,active_revision) VALUES($1,'active',NULL)", destination)
	must(f.t, err)
	_, err = tx.Exec(ctx, `INSERT INTO newim.im_webhook_endpoint_revisions(destination_id,revision,url,key_id,secret_nonce,secret_ciphertext)
VALUES($1,$2,$3,$4,$5,$6)`, destination, activeRevision, url, keyID, nonce, ciphertext)
	must(f.t, err)
	_, err = tx.Exec(ctx, "UPDATE newim.im_webhook_endpoints SET active_revision=$2 WHERE destination_id=$1", destination, activeRevision)
	must(f.t, err)
	must(f.t, tx.Commit(ctx))
	return destination, master
}

func (f *fixture) resolver(master []byte) app.SecretResolver {
	f.t.Helper()
	resolver, err := app.NewLocalSecretResolver(master)
	must(f.t, err)
	return resolver
}

func (f *fixture) worker(master []byte, doer app.Doer) *app.Worker {
	return f.workerWithObserver(master, doer, nil)
}

func (f *fixture) workerWithObserver(master []byte, doer app.Doer, observer app.Observer) *app.Worker {
	f.t.Helper()
	worker, err := app.NewWorker(app.Config{
		Owner: "integration_owner", BatchSize: 8, MaxConcurrent: 4, MaxPerDestination: 2, MaxAttempts: 3,
		MaxResponseBytes: 64 * 1024, LeaseTTL: 2 * time.Second, RequestTimeout: time.Second,
		BaseBackoff: 10 * time.Millisecond, MaxBackoff: time.Second, HighWater: 1000, LowWater: 100,
		MaxDestinationQueue: 1000, RatePerSecond: 100, RateBurst: 100,
		IdleDelay: 10 * time.Millisecond, Clock: app.ClockFunc(func() time.Time { return f.now }), Observer: observer,
	}, f.repo, doer, f.resolver(master))
	must(f.t, err)
	return worker
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

func strconvMillis(t time.Time) string { return fmt.Sprintf("%d", t.UnixMilli()) }

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func mustJSON(t *testing.T, raw string) []byte {
	t.Helper()
	if !json.Valid([]byte(raw)) {
		t.Fatal("invalid fixture JSON")
	}
	return []byte(raw)
}
