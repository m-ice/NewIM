//go:build integration

package messagesync

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/sync/message"
)

func TestReadTransactionGuard(t *testing.T) {
	ctx := context.Background()
	database := os.Getenv("NEWIM_MESSAGE_DELTA_DATABASE")
	if database == "" {
		database = "newim_test"
	}
	dsn := fmt.Sprintf("host=/var/run/postgresql user=newim_test dbname=%s sslmode=disable", database)
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	_, err = db.Exec(ctx, "INSERT INTO newim.im_users(user_id) VALUES('guard_user') ON CONFLICT DO NOTHING; INSERT INTO newim.im_conversations(conversation_id) VALUES('guard_conversation') ON CONFLICT DO NOTHING; INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES('guard_conversation','guard_user') ON CONFLICT DO NOTHING")
	if err != nil {
		t.Fatal(err)
	}
	repo, err := Open(ctx, Config{DSN: dsn, AllowLocalSocket: true, MaxConnections: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()

	original := readTxOptions
	readTxOptions = pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite}
	_, err = repo.Read(ctx, "guard_user", app.ReadRequest{ConversationID: "guard_conversation", Limit: 10})
	if app.ErrorCode(err) != app.StorageUnavailable {
		t.Fatalf("wrong transaction mode got %v want %s", err, app.StorageUnavailable)
	}
	readTxOptions = original

	_, err = repo.Read(ctx, "guard_user", app.ReadRequest{ConversationID: "guard_conversation", Limit: 10})
	if err != nil {
		t.Fatalf("correct transaction mode failed: %v", err)
	}
}
