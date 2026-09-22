//go:build integration

package media_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/m-ice/NewIM/server/auth/session"
	app "github.com/m-ice/NewIM/server/media"
	localfs "github.com/m-ice/NewIM/server/media/localfs"
	store "github.com/m-ice/NewIM/server/storage/media"
)

var ctx = context.Background()
var fixtureSeq atomic.Int64

type fixture struct {
	t      *testing.T
	db     *pgx.Conn
	repo   *store.Repository
	now    atomic.Int64
	prefix string
}

type recordingSigner struct {
	mu    sync.Mutex
	calls int
	url   string
}

func (s *recordingSigner) Sign(context.Context, string, time.Time) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.url, nil
}

func (s *recordingSigner) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func openFixture(t *testing.T) *fixture {
	t.Helper()
	database := "newim_test"
	if os.Getenv("NEWIM_MEDIA_PHASE") == "restore" {
		database = "media_restore"
	}
	dsn := fmt.Sprintf("host=/var/run/postgresql user=newim_test dbname=%s sslmode=disable", database)
	conn, err := pgx.Connect(ctx, dsn)
	must(t, err)
	repo, err := store.Open(ctx, store.Config{
		DSN: dsn, AllowLocalSocket: true, MaxConnections: 8,
		ApplicationName: "nim_media_test",
	})
	must(t, err)
	f := &fixture{t: t, db: conn, repo: repo, prefix: fmt.Sprintf("media_%d", fixtureSeq.Add(1))}
	f.now.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	t.Cleanup(func() {
		repo.Close()
		_ = conn.Close(context.Background())
	})
	return f
}

func (f *fixture) sql(statement string, args ...any) {
	f.t.Helper()
	_, err := f.db.Exec(ctx, statement, args...)
	must(f.t, err)
}

func (f *fixture) scalarInt64(statement string, args ...any) int64 {
	f.t.Helper()
	var value int64
	must(f.t, f.db.QueryRow(ctx, statement, args...).Scan(&value))
	return value
}

func (f *fixture) scalarString(statement string, args ...any) string {
	f.t.Helper()
	var value string
	must(f.t, f.db.QueryRow(ctx, statement, args...).Scan(&value))
	return value
}

func (f *fixture) seedIdentity(user string) session.ConnectionIdentity {
	f.t.Helper()
	device := user + "_device"
	sessionID := user + "_session"
	connectionID := user + "_connection"
	digest := sha256.Sum256([]byte(user))
	tokenID := hex.EncodeToString(digest[:16])
	f.sql("INSERT INTO newim.im_users(user_id) VALUES($1) ON CONFLICT DO NOTHING", user)
	f.sql("INSERT INTO newim.im_devices(user_id,device_id) VALUES($1,$2) ON CONFLICT DO NOTHING", user, device)
	f.sql("INSERT INTO newim.im_sessions(session_id,user_id,device_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", sessionID, user, device)
	f.sql("INSERT INTO newim.im_auth_tokens(token_id,token_digest,session_id,created_at,expires_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
		tokenID, digest[:], sessionID, time.UnixMilli(f.now.Load()).Add(-time.Minute), time.UnixMilli(f.now.Load()).Add(time.Hour))
	identity, err := session.NewConnectionIdentity(user, device, sessionID, connectionID, tokenID)
	must(f.t, err)
	return identity
}

func (f *fixture) seedConversation(conversationID string, users ...string) {
	f.t.Helper()
	f.sql("INSERT INTO newim.im_conversations(conversation_id) VALUES($1) ON CONFLICT DO NOTHING", conversationID)
	for _, user := range users {
		f.sql("INSERT INTO newim.im_users(user_id) VALUES($1) ON CONFLICT DO NOTHING", user)
		f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2) ON CONFLICT DO NOTHING", conversationID, user)
	}
}

func (f *fixture) clock() app.ClockFunc {
	return app.ClockFunc(func() time.Time { return time.UnixMilli(f.now.Load()).UTC() })
}

func (f *fixture) advance(duration time.Duration) {
	f.now.Add(duration.Milliseconds())
}

func openLocalStore(t *testing.T, root string) *localfs.Store {
	t.Helper()
	objects, err := localfs.Open(root)
	must(t, err)
	t.Cleanup(func() { _ = objects.Close() })
	return objects
}

func newService(t *testing.T, f *fixture, objects app.ObjectStore, signer app.Signer, entropy []byte) *app.Service {
	t.Helper()
	service, err := app.NewService(f.repo, app.Config{
		Objects: objects, Signer: signer, Clock: f.clock(), Entropy: bytes.NewReader(entropy),
	})
	must(t, err)
	return service
}

func mediaRoot(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), name)
}

func content(size int, fill byte) []byte {
	return bytes.Repeat([]byte{fill}, size)
}

func contentDigest(value []byte) [32]byte { return sha256.Sum256(value) }

func beginRequest(conversationID string, value []byte, expiry time.Duration) app.BeginUploadRequest {
	digest := contentDigest(value)
	return app.BeginUploadRequest{
		ConversationID: conversationID, Kind: "image", ContentType: "image/jpeg",
		Size: int64(len(value)), SHA256: hex.EncodeToString(digest[:]), Expiry: expiry,
	}
}

func wantMediaCode(t *testing.T, err error, want app.Code) {
	t.Helper()
	if got := app.ErrorCode(err); got != want {
		t.Fatalf("media code got %s want %s (%v)", got, want, err)
	}
}

func unique(prefix string) string {
	return prefix + "_" + fmt.Sprint(fixtureSeq.Add(1))
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("operation failed: %v", err)
	}
}

func validIdentity(t *testing.T, f *fixture, user string) session.ConnectionIdentity {
	t.Helper()
	identity := f.seedIdentity(user)
	return identity
}

func objectInfo(value []byte) app.ObjectInfo {
	digest := contentDigest(value)
	return app.ObjectInfo{Size: int64(len(value)), SHA256: digest}
}

func assertNoMediaSideEffects(t *testing.T, f *fixture, conversationID string) {
	t.Helper()
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE conversation_id=$1", conversationID); got != 0 {
		t.Fatalf("message side effects: %d", got)
	}
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_outbox_events"); got != 0 {
		t.Fatalf("outbox side effects: %d", got)
	}
}

func assertContains(t *testing.T, value, substring string) {
	t.Helper()
	if !strings.Contains(value, substring) {
		t.Fatalf("%q does not contain %q", value, substring)
	}
}
