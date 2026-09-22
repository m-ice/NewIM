//go:build integration

package auth_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/auth/session"
	store "github.com/m-ice/NewIM/server/storage/authsession"
)

var ctx = context.Background()

type fixture struct {
	t        *testing.T
	db       *pgx.Conn
	repo     *store.Repository
	database string
	dsn      string
	now      atomic.Int64
}

func openFixture(t *testing.T) *fixture {
	t.Helper()
	database := "newim_test"
	switch os.Getenv("NEWIM_AUTH_PHASE") {
	case "restart":
		database = "newim_test"
	case "restore":
		database = "auth_restore"
	default:
		database = "newim_test"
	}
	dsn := fmt.Sprintf("host=/var/run/postgresql user=newim_test dbname=%s sslmode=disable", database)
	conn, err := pgx.Connect(ctx, dsn)
	must(t, err)
	repo, err := store.Open(ctx, store.Config{
		DSN: dsn, AllowLocalSocket: true, MaxConnections: 8,
		ApplicationName: "nim_auth_test",
	})
	must(t, err)
	f := &fixture{t: t, db: conn, repo: repo, database: database, dsn: dsn}
	f.now.Store(1_800_000_000)
	t.Cleanup(func() {
		repo.Close()
		_ = conn.Close(context.Background())
	})
	return f
}

func newBinding(prefix string) app.SessionBinding {
	return app.SessionBinding{
		UserID: prefix + "_user", DeviceID: prefix + "_device", SessionID: prefix + "_session",
	}
}

func (f *fixture) seedBinding(prefix string) app.SessionBinding {
	f.t.Helper()
	binding := newBinding(prefix)
	f.sql("INSERT INTO newim.im_users(user_id) VALUES($1) ON CONFLICT DO NOTHING", binding.UserID)
	f.sql("INSERT INTO newim.im_devices(user_id,device_id) VALUES($1,$2) ON CONFLICT DO NOTHING", binding.UserID, binding.DeviceID)
	f.sql("INSERT INTO newim.im_sessions(session_id,user_id,device_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", binding.SessionID, binding.UserID, binding.DeviceID)
	return binding
}

func (f *fixture) revokeSessionSQL(binding app.SessionBinding, at time.Time) {
	f.t.Helper()
	f.sql("UPDATE newim.im_sessions SET revoked_at=$2 WHERE session_id=$1", binding.SessionID, at)
}

func (f *fixture) service(observer app.Observer, entropy io.Reader, policy app.LoginPolicy) *app.Service {
	f.t.Helper()
	service, err := app.NewService(f.repo, app.Config{
		Clock:    app.ClockFunc(func() time.Time { return time.Unix(f.now.Load(), 0).UTC() }),
		Observer: observer, Entropy: entropy, Policy: policy,
	})
	must(f.t, err)
	return service
}

func (f *fixture) issue(service *app.Service, binding app.SessionBinding, ttl time.Duration) app.IssuedToken {
	f.t.Helper()
	token, err := service.Issue(ctx, app.IssueRequest{Binding: binding, TTL: ttl})
	must(f.t, err)
	return token
}

func (f *fixture) authenticate(service *app.Service, token app.IssuedToken, binding app.SessionBinding, connectionID string) (app.ConnectionIdentity, error) {
	f.t.Helper()
	return service.Authenticate(ctx, app.AuthenticateRequest{
		Token: token.RawToken(), Binding: binding, ConnectionID: connectionID,
	})
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

func (f *fixture) scalarBool(statement string, args ...any) bool {
	f.t.Helper()
	var value bool
	must(f.t, f.db.QueryRow(ctx, statement, args...).Scan(&value))
	return value
}

func (f *fixture) tokenDigestHex(tokenID string) string {
	f.t.Helper()
	return f.scalarString("SELECT encode(token_digest,'hex') FROM newim.im_auth_tokens WHERE token_id=$1", tokenID)
}

func (f *fixture) tokenCount() int {
	f.t.Helper()
	var count int
	must(f.t, f.db.QueryRow(ctx, "SELECT count(*) FROM newim.im_auth_tokens").Scan(&count))
	return count
}

func (f *fixture) tokenCountForSession(sessionID string) int {
	f.t.Helper()
	var count int
	must(f.t, f.db.QueryRow(ctx, "SELECT count(*) FROM newim.im_auth_tokens WHERE session_id=$1", sessionID).Scan(&count))
	return count
}

func (f *fixture) sessionRevoked(binding app.SessionBinding) bool {
	f.t.Helper()
	return f.scalarBool("SELECT revoked_at IS NOT NULL FROM newim.im_sessions WHERE session_id=$1", binding.SessionID)
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

func hashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
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

func (o *captureObserver) latest(operation string) (app.Observation, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for i := len(o.observations) - 1; i >= 0; i-- {
		if o.observations[i].Operation == operation {
			return o.observations[i], true
		}
	}
	return app.Observation{}, false
}

func (o *captureObserver) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.observations)
}

type panicObserver struct{}

func (panicObserver) Observe(app.Observation) { panic("observer failure") }

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

type repeatingReader struct {
	pattern []byte
	offset  int
}

func (r *repeatingReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.pattern[(r.offset+i)%len(r.pattern)]
	}
	r.offset = (r.offset + len(p)) % len(r.pattern)
	return len(p), nil
}
