//go:build integration

package conversationsync_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	store "github.com/m-ice/NewIM/server/storage/conversationsync"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

var ctx = context.Background()
var testKey = []byte("integration-fixed-keyring-32-bytes-minimum-0001")

const recoveryStatePath = "/tmp/nim-syn-003-recovery.json"

type fixture struct {
	t        *testing.T
	db       *pgx.Conn
	repo     *store.Repository
	database string
	dsn      string
	now      atomic.Int64
}

type recoveryState struct {
	User       string `json:"user"`
	Conv       string `json:"conv"`
	Checkpoint string `json:"checkpoint"`
	Head       uint64 `json:"head"`
	Epoch      uint64 `json:"epoch"`
	Floor      uint64 `json:"floor"`
	Revision   uint64 `json:"revision"`
}

func openFixture(t *testing.T) *fixture {
	t.Helper()
	database := "newim_test"
	if os.Getenv("NEWIM_SYNC_PHASE") == "restore" {
		database = "sync_restore"
	}
	dsn := "host=/var/run/postgresql user=newim_test dbname=" + database + " sslmode=disable"
	conn, err := pgx.Connect(ctx, dsn)
	must(t, err)
	repo, err := store.Open(ctx, store.Config{DSN: dsn, AllowLocalSocket: true, MaxConnections: 8})
	must(t, err)
	f := &fixture{t: t, db: conn, repo: repo, database: database, dsn: dsn}
	f.now.Store(1800000000)
	t.Cleanup(func() {
		repo.Close()
		_ = conn.Close(context.Background())
	})
	return f
}

func (f *fixture) service(keys map[string][]byte, active string) *app.Service {
	f.t.Helper()
	if keys == nil {
		keys = map[string][]byte{"test": testKey}
	}
	if active == "" {
		active = "test"
	}
	service, err := app.NewService(f.repo, app.Config{
		Keys:        keys,
		ActiveKeyID: active,
		Clock:       func() time.Time { return time.Unix(f.now.Load(), 0) },
	})
	must(f.t, err)
	return service
}

func (f *fixture) serviceWithObserver(keys map[string][]byte, active string, observer app.Observer) *app.Service {
	f.t.Helper()
	if keys == nil {
		keys = map[string][]byte{"test": testKey}
	}
	if active == "" {
		active = "test"
	}
	service, err := app.NewService(f.repo, app.Config{
		Keys:        keys,
		ActiveKeyID: active,
		Clock:       func() time.Time { return time.Unix(f.now.Load(), 0) },
		Observer:    observer,
	})
	must(f.t, err)
	return service
}

func (f *fixture) setNow(unix int64) { f.now.Store(unix) }

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("operation failed: %v", err)
	}
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

func (f *fixture) account(user string) {
	f.t.Helper()
	f.sql("INSERT INTO newim.im_users(user_id) VALUES($1) ON CONFLICT DO NOTHING", user)
	must(f.t, f.repo.InitializeEmptyAccount(ctx, user))
}

func (f *fixture) conversations(prefix string, count int) []string {
	f.t.Helper()
	ids := make([]string, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s%06d", prefix, i)
	}
	f.sql("INSERT INTO newim.im_conversations(conversation_id) SELECT unnest($1::text[]) ON CONFLICT DO NOTHING", ids)
	return ids
}

func (f *fixture) write(user string, ids []string, kind string) []uint64 {
	f.t.Helper()
	tx, err := f.repo.Begin(ctx)
	must(f.t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	batch, err := f.repo.PrepareBatch(ctx, tx, ids, []string{user})
	must(f.t, err)
	revisions := make([]uint64, 0, len(ids))
	for _, conversation := range ids {
		var revision uint64
		switch kind {
		case "add":
			_, err = tx.Exec(ctx, "INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2) ON CONFLICT DO NOTHING", conversation, user)
			must(f.t, err)
			revision, err = batch.RecordUpsert(ctx, conversation, user)
		case "update":
			_, err = tx.Exec(ctx, "UPDATE newim.im_conversations SET last_seq=last_seq+1 WHERE conversation_id=$1", conversation)
			must(f.t, err)
			revision, err = batch.RecordUpsert(ctx, conversation, user)
		case "remove":
			_, err = tx.Exec(ctx, "DELETE FROM newim.im_conversation_members WHERE conversation_id=$1 AND user_id=$2", conversation, user)
			must(f.t, err)
			revision, err = batch.RecordRemoval(ctx, conversation, user)
		default:
			f.t.Fatalf("unknown fixture mutation %q", kind)
		}
		must(f.t, err)
		revisions = append(revisions, revision)
	}
	must(f.t, tx.Commit(ctx))
	return revisions
}

func (f *fixture) populate(user, prefix string, count int) []string {
	f.t.Helper()
	f.account(user)
	ids := f.conversations(prefix, count)
	for start := 0; start < count; start += 100 {
		end := start + 100
		if end > count {
			end = count
		}
		f.write(user, ids[start:end], "add")
	}
	return ids
}

func principal(user string) app.Principal { return app.Principal{UserID: user} }

func normalizedLimit(limit int) int {
	if limit == 0 {
		return app.MaxPageItems
	}
	return limit
}

func checkedPage(t *testing.T, page app.Page, limit int) {
	t.Helper()
	raw, err := json.Marshal(page)
	must(t, err)
	if len(page.Items) > normalizedLimit(limit) {
		t.Fatalf("page items got %d limit %d", len(page.Items), normalizedLimit(limit))
	}
	if len(raw) > app.MaxResponseBytes {
		t.Fatalf("page bytes got %d limit %d", len(raw), app.MaxResponseBytes)
	}
	if page.NextCursor == "" {
		t.Fatal("successful page has empty cursor")
	}
	for _, item := range page.Items {
		if item.ConversationID == "" || item.Revision == 0 || (item.Kind != "upsert" && item.Kind != "remove") {
			t.Fatalf("invalid item %+v", item)
		}
	}
}

func bootstrapAll(t *testing.T, service *app.Service, user string, limit int) ([]app.Item, string) {
	t.Helper()
	page, err := service.BeginBootstrap(ctx, principal(user), limit)
	must(t, err)
	items := make([]app.Item, 0)
	seen := map[string]bool{}
	for pages := 0; ; pages++ {
		if pages > 10000 {
			t.Fatal("bootstrap failed to terminate")
		}
		checkedPage(t, page, limit)
		items = append(items, page.Items...)
		if !page.HasMore {
			return items, page.NextCursor
		}
		token := page.NextCursor
		if seen[token] {
			t.Fatal("bootstrap cursor did not advance")
		}
		seen[token] = true
		page, err = service.ContinueBootstrap(ctx, principal(user), token)
		must(t, err)
	}
}

func deltaAll(t *testing.T, service *app.Service, user, checkpoint string, limit int) ([]app.Item, string) {
	t.Helper()
	page, err := service.BeginDelta(ctx, principal(user), checkpoint, limit)
	must(t, err)
	items := make([]app.Item, 0)
	seen := map[string]bool{}
	for pages := 0; ; pages++ {
		if pages > 10000 {
			t.Fatal("delta failed to terminate")
		}
		checkedPage(t, page, limit)
		items = append(items, page.Items...)
		if !page.HasMore {
			return items, page.NextCursor
		}
		token := page.NextCursor
		if seen[token] {
			t.Fatal("delta cursor did not advance")
		}
		seen[token] = true
		page, err = service.ContinueDelta(ctx, principal(user), token)
		must(t, err)
	}
}

func wantCode(t *testing.T, page app.Page, err error, code app.Code) {
	t.Helper()
	if got := app.ErrorCode(err); got != code {
		t.Fatalf("error code got %s want %s", got, code)
	}
	if len(page.Items) != 0 || page.NextCursor != "" || page.HasMore {
		t.Fatalf("error returned page effects: %+v", page)
	}
}

func samePage(t *testing.T, left, right app.Page) {
	t.Helper()
	if !reflect.DeepEqual(left, right) {
		t.Fatal("retry changed page")
	}
}

func mergeItems(items []app.Item) map[string]app.Item {
	merged := make(map[string]app.Item)
	for _, item := range items {
		previous, ok := merged[item.ConversationID]
		if !ok || item.Revision > previous.Revision {
			merged[item.ConversationID] = item
		}
	}
	return merged
}

func (f *fixture) openConn() *pgx.Conn {
	f.t.Helper()
	conn, err := pgx.Connect(ctx, f.dsn)
	must(f.t, err)
	f.t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func async(fn func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	return done
}

func waitAsync(t *testing.T, done <-chan error, timeout time.Duration) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		t.Fatal("asynchronous operation deadline exceeded")
		return nil
	}
}

func (f *fixture) waitSQL(statement string, seconds time.Duration) {
	f.t.Helper()
	deadline := time.Now().Add(seconds)
	for time.Now().Before(deadline) {
		if f.scalarBool(statement) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	f.t.Fatalf("database barrier deadline exceeded: %s", statement)
}

func (f *fixture) waitLock(relation, mode string, granted bool) {
	f.waitLockWithArgs(relation, mode, granted)
}

func (f *fixture) waitLockWithArgs(relation, mode string, granted bool) {
	f.t.Helper()
	statement := "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation=$1::regclass AND mode=$2 AND granted=$3)"
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.scalarBool(statement, relation, mode, granted) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	f.t.Fatalf("lock barrier deadline exceeded: relation=%s mode=%s granted=%t", relation, mode, granted)
}

func (f *fixture) waitApplicationLock(application string) {
	f.t.Helper()
	statement := "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')"
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.scalarBool(statement, application) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	f.t.Fatalf("application lock wait not observed: %s", application)
}

func (f *fixture) waitBackend(application, waitEventType, waitEvent string) {
	f.t.Helper()
	statement := "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type=$2 AND wait_event=$3)"
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.scalarBool(statement, application, waitEventType, waitEvent) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	f.t.Fatalf("backend barrier deadline exceeded: %s %s/%s", application, waitEventType, waitEvent)
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
