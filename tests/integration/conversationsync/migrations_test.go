//go:build integration

package conversationsync_test

import (
	"strings"
	"testing"

	app "github.com/m-ice/NewIM/server/sync/conversation"
)

func TestSyncMigrations(t *testing.T) {
	f := openFixture(t)
	if version := f.scalarString("SHOW server_version_num"); version != "180006" {
		t.Fatalf("PostgreSQL version got %s want 180006", version)
	}

	ledger := f.scalarString("SELECT string_agg(version::text||':'||sha256,',' ORDER BY version) FROM newim_meta.migrations")
	parts := strings.Split(ledger, ",")
	if len(parts) != 4 {
		t.Fatalf("migration ledger got %q want versions 1..4", ledger)
	}
	for index, part := range parts {
		fields := strings.SplitN(part, ":", 2)
		if len(fields) != 2 || fields[0] != string(rune('1'+index)) || len(fields[1]) != 64 || strings.Trim(fields[1], "0123456789abcdef") != "" || strings.Trim(fields[1], "0") == "" {
			t.Fatalf("invalid migration ledger entry %q", part)
		}
	}

	if tables := f.scalarInt64("SELECT count(*) FROM pg_tables WHERE schemaname='newim' AND tablename IN ('im_conversation_sync_accounts','im_conversation_sync_keys','im_conversation_sync_changes')"); tables != 3 {
		t.Fatalf("sync migration table count got %d want 3", tables)
	}
	if tables := f.scalarInt64("SELECT count(*) FROM pg_tables WHERE schemaname='newim' AND tablename='im_auth_tokens'"); tables != 1 {
		t.Fatalf("additive auth table count got %d want 1", tables)
	}
	if tables := f.scalarInt64("SELECT count(*) FROM pg_tables WHERE schemaname='newim';"); tables != 15 {
		t.Fatalf("head table count got %d want 15", tables)
	}
	if triggers := f.scalarInt64("SELECT count(*) FROM pg_trigger WHERE tgrelid IN (SELECT oid FROM pg_class WHERE relnamespace='newim'::regnamespace) AND NOT tgisinternal"); triggers != 0 {
		t.Fatalf("sync migration added %d application triggers", triggers)
	}
	if functions := f.scalarInt64("SELECT count(*) FROM pg_proc WHERE pronamespace='newim'::regnamespace"); functions != 1 {
		t.Fatalf("sync migration function count got %d want existing persist_message only", functions)
	}
	if cascades := f.scalarInt64("SELECT count(*) FROM pg_constraint WHERE connamespace='newim'::regnamespace AND contype='f' AND confdeltype<>'a'"); cascades != 0 {
		t.Fatalf("sync migration introduced cascading deletes: %d", cascades)
	}

	if membership := f.scalarInt64("SELECT count(*) FROM newim.im_conversation_members WHERE user_id='migration_existing' AND conversation_id='migration_conv'"); membership != 1 {
		t.Fatalf("populated 002 membership was not preserved: %d", membership)
	}
	if backfilled := f.scalarInt64("SELECT count(*) FROM newim.im_conversation_sync_accounts WHERE user_id='migration_existing'"); backfilled != 0 {
		t.Fatalf("migration backfilled %d existing accounts", backfilled)
	}
	page, err := f.service(nil, "").BeginBootstrap(ctx, principal("migration_existing"), 100)
	wantCode(t, page, err, app.NotReady)
	if err = f.repo.InitializeEmptyAccount(ctx, "migration_existing"); app.ErrorCode(err) != app.NotReady {
		t.Fatalf("populated migration initialization got %v want %s", err, app.NotReady)
	}

	user := "migration_sync"
	conversation := "migration_sync_room"
	f.sql("INSERT INTO newim.im_users(user_id) VALUES($1)", user)
	f.sql("INSERT INTO newim.im_conversations(conversation_id) VALUES($1)", conversation)
	must(t, f.repo.InitializeEmptyAccount(ctx, user))

	tx, err := f.repo.Begin(ctx)
	must(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	batch, err := f.repo.PrepareBatch(ctx, tx, []string{conversation}, []string{user})
	must(t, err)
	_, err = tx.Exec(ctx, "INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", conversation, user)
	must(t, err)
	_, err = tx.Exec(ctx, "SELECT newim.persist_message($1,$2,$3,$4,'text',1,123,convert_to('{}','UTF8'),$5)", user, "migration_client", conversation, "migration_server", "migration_event")
	must(t, err)
	revision, err := batch.RecordUpsert(ctx, conversation, user)
	must(t, err)
	if revision != 1 {
		t.Fatalf("post-migration upsert revision got %d want 1", revision)
	}
	must(t, tx.Commit(ctx))

	items, checkpoint := bootstrapAll(t, f.service(nil, ""), user, 100)
	if len(items) != 1 || items[0].ConversationID != conversation || items[0].LatestConversationSeq == nil || *items[0].LatestConversationSeq != 1 || items[0].LatestServerMsgID == nil || *items[0].LatestServerMsgID != "migration_server" {
		t.Fatalf("post-migration projection mismatch: %+v", items)
	}
	if checkpoint == "" {
		t.Fatal("post-migration bootstrap did not return checkpoint")
	}
}
