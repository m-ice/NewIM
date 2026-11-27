//go:build integration

package conversationsync

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

func TestReadSyncQueryPlan(t *testing.T) {
	testSourceQueryPlan(t, 17)
}

func TestAccountSettingsSourceQueryPlan(t *testing.T) {
	testSourceQueryPlan(t, 18)
}

func testSourceQueryPlan(t *testing.T, migration int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	db, err := pgx.Connect(ctx, "host=/var/run/postgresql user=newim_test dbname=newim_test sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())

	const userID = "read_plan_account"
	var version int
	if err = db.QueryRow(ctx, "SELECT max(version) FROM newim_meta.migrations").Scan(&version); err != nil || version != migration {
		t.Fatalf("migration head=%d err=%v, want %d", version, err, migration)
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	_, err = tx.Exec(ctx, `
		INSERT INTO newim.im_users(user_id) VALUES ('read_plan_account');
		INSERT INTO newim.im_conversations(conversation_id)
			SELECT 'read_plan_'||lpad(n::text,5,'0') FROM generate_series(1,20000) n;
		INSERT INTO newim.im_conversation_members(conversation_id,user_id)
			SELECT conversation_id,'read_plan_account' FROM newim.im_conversations
			WHERE conversation_id LIKE 'read_plan_%';
		INSERT INTO newim.im_messages(server_msg_id,sender_id,client_msg_id,conversation_id,conversation_seq,
			protocol_version,schema_version,message_type,server_time_ms,payload_bytes)
			SELECT 'm_'||conversation_id,'read_plan_account','c_'||conversation_id,conversation_id,1,1,1,'text',1,'{}'::bytea
			FROM newim.im_conversations WHERE conversation_id LIKE 'read_plan_%';
		UPDATE newim.im_conversations SET last_seq=1,latest_server_msg_id='m_'||conversation_id
			WHERE conversation_id LIKE 'read_plan_%';
		INSERT INTO newim.im_messages(server_msg_id,sender_id,client_msg_id,conversation_id,conversation_seq,
			protocol_version,schema_version,message_type,server_time_ms,payload_bytes)
			SELECT 'hot_m_'||n,'read_plan_account','hot_c_'||n,'read_plan_10001',n+1,1,1,'text',1,'{}'::bytea
			FROM generate_series(1,10000) n;
		UPDATE newim.im_conversations SET last_seq=10001,latest_server_msg_id='hot_m_10000'
			WHERE conversation_id='read_plan_10001';
		INSERT INTO newim.im_read_states(conversation_id,user_id,read_seq)
			SELECT conversation_id,'read_plan_account',1 FROM newim.im_conversations
			WHERE conversation_id LIKE 'read_plan_%';
		INSERT INTO newim.im_conversation_sync_accounts(user_id,last_change_seq,source_ready,read_state_epoch)
			VALUES ('read_plan_account',20000,true,1);
		INSERT INTO newim.im_conversation_sync_keys(user_id,conversation_id,first_change_seq)
			SELECT 'read_plan_account',conversation_id,row_number() OVER (ORDER BY conversation_id)
			FROM newim.im_conversations WHERE conversation_id LIKE 'read_plan_%';
		INSERT INTO newim.im_conversation_sync_changes(user_id,change_seq,conversation_id,kind,latest_seq,latest_server_msg_id)
			SELECT user_id,first_change_seq,conversation_id,'upsert',1,'m_'||conversation_id
			FROM newim.im_conversation_sync_keys WHERE user_id='read_plan_account';
		ANALYZE newim.im_conversations;
		ANALYZE newim.im_conversation_members;
		ANALYZE newim.im_messages;
		ANALYZE newim.im_read_states;
		ANALYZE newim.im_conversation_sync_accounts;
		ANALYZE newim.im_conversation_sync_keys;
		ANALYZE newim.im_conversation_sync_changes;
	`)
	if err != nil {
		t.Fatal(err)
	}
	accountSettings := migration == 18
	if accountSettings {
		_, err = tx.Exec(ctx, `
			INSERT INTO newim.im_conversation_account_settings(conversation_id,user_id,version,pinned,draft,clear_through_seq)
				SELECT conversation_id,'read_plan_account',1,true,'account draft'::bytea,
					CASE WHEN conversation_id='read_plan_10001' THEN last_seq ELSE 0 END
				FROM newim.im_conversations WHERE conversation_id LIKE 'read_plan_%';
			ANALYZE newim.im_conversation_account_settings;
		`)
		if err != nil {
			t.Fatal(err)
		}
	}

	const batchSize = 20
	const after = "read_plan_10000"
	keys, err := sourceBackfillKeys(ctx, tx, userID, after, batchSize)
	if err != nil || len(keys) != batchSize || keys[0] != "read_plan_10001" {
		t.Fatalf("backfill page length=%d err=%v", len(keys), err)
	}

	directory := readSyncExplain(t, ctx, tx, sourceBackfillKeysSQL, userID, after, batchSize)
	if directory.Kind != "Limit" || directory.Rows != batchSize {
		t.Fatalf("directory returned %s rows=%g", directory.Kind, directory.Rows)
	}
	for _, table := range []string{"im_conversation_members", "im_conversation_sync_keys"} {
		requireReadSyncIndex(t, directory, table, batchSize, 1)
	}

	projection := readSyncExplain(t, ctx, tx, sourceProjectionQuery(true, accountSettings, sourceAccountPageTargets), userID, keys)
	requireReadSyncIndex(t, projection, "im_messages", 1, batchSize, "server_msg_id")
	requireReadSyncIndex(t, projection, "im_read_states", 1, batchSize, "conversation_id", "user_id")
	requireReadSyncIndex(t, projection, "im_conversation_sync_changes", 1, batchSize)
	if accountSettings {
		requireReadSyncIndex(t, projection, "im_conversation_account_settings", 1, batchSize, "conversation_id", "user_id")
		var seq, readSeq, floor int64
		var draft []byte
		err = tx.QueryRow(ctx, `SELECT latest_seq,last_read_seq,settings_clear_through_seq,settings_draft
			FROM newim.im_conversation_sync_changes WHERE user_id=$1 AND conversation_id='read_plan_10001'
			ORDER BY change_seq DESC LIMIT 1`, userID).Scan(&seq, &readSeq, &floor, &draft)
		if err != nil || seq != 0 || readSeq != 1 || floor != 10001 || string(draft) != "account draft" {
			t.Fatalf("new hot conversation snapshot: latest=%d read=%d floor=%d draft=%q err=%v", seq, readSeq, floor, draft, err)
		}
	} else {
		legacySQL := sourceProjectionQuery(false, false, sourceAccountPageTargets)
		if strings.Contains(legacySQL, "last_read_seq") || strings.Contains(legacySQL, "im_read_states") {
			t.Fatal("legacy projection depends on read-state storage")
		}
		legacy := readSyncExplain(t, ctx, tx, legacySQL, userID, keys)
		requireReadSyncIndex(t, legacy, "im_messages", 1, batchSize, "server_msg_id")
	}

	visible := readSyncExplain(t, ctx, tx, visibleSummarySQL, "read_plan_10001", "hot_m_10000", int64(10001))
	requireReadSyncIndex(t, visible, "im_messages", 1, 1, "server_msg_id")
	var written int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM newim.im_conversation_sync_changes
		WHERE user_id=$1 AND change_seq>20000 AND last_read_seq=1`, userID).Scan(&written); err != nil || written != batchSize {
		t.Fatalf("projection wrote %d read revisions, err=%v", written, err)
	}
	if accountSettings {
		requirePersonalTombstone(t, ctx, tx, userID)
	}
}

func requirePersonalTombstone(t *testing.T, ctx context.Context, tx pgx.Tx, userID string) {
	t.Helper()
	const id = "read_plan_10001"
	scope := readScope{requireSourceReady: true, includeReadState: true, accountSettings: true}
	previous, err := latestItem(ctx, tx, userID, id, app.MaxSequence, scope)
	if err != nil || previous == nil || previous.Settings == nil {
		t.Fatalf("missing personal snapshot before deletion: %+v, err=%v", previous, err)
	}
	current, err := applyAccountSettings(ctx, tx, userID, id, &app.AccountSettingsMutation{
		Action: app.DeleteAccountConversation, ExpectedVersion: 1,
	})
	if err != nil || !current.Deleted || current.Version != 2 || current.ClearThroughSeq != 10001 || current.Pinned || current.Draft != "" {
		t.Fatalf("personal deletion: %+v, err=%v", current, err)
	}
	candidate := app.Candidate{Key: id, State: previous}
	if err = authorizeSourceSummary(ctx, tx, userID, &candidate, scope); err != nil || candidate.Member ||
		candidate.Latest == nil || candidate.Latest.Kind != "remove" || candidate.Latest.Settings != nil {
		t.Fatalf("old page after personal deletion: %+v, err=%v", candidate, err)
	}
	deletedRevision := candidate.Latest.Revision
	if err = RecordSourceWatermark(ctx, tx, userID, id, true); err != nil {
		t.Fatal(err)
	}
	latest, err := latestItem(ctx, tx, userID, id, app.MaxSequence, scope)
	if err != nil || latest == nil || latest.Revision != deletedRevision || latest.Kind != "remove" {
		t.Fatalf("watermark projection changed a personal tombstone: %+v, err=%v", latest, err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO newim.im_messages(server_msg_id,sender_id,client_msg_id,conversation_id,conversation_seq,
			protocol_version,schema_version,message_type,server_time_ms,payload_bytes)
		VALUES ('account_new_msg','read_plan_account','account_new_client','read_plan_10001',10002,1,1,'text',1,'{}'::bytea);
		UPDATE newim.im_conversations SET last_seq=10002,latest_server_msg_id='account_new_msg'
		WHERE conversation_id='read_plan_10001';
	`); err != nil {
		t.Fatal(err)
	}
	if err = RestoreAccountSettings(ctx, tx, id, 10002); err != nil {
		t.Fatal(err)
	}
	if err = RecordSourceConversation(ctx, tx, id, nil, true, true); err != nil {
		t.Fatal(err)
	}
	latest, err = latestItem(ctx, tx, userID, id, app.MaxSequence, scope)
	if err != nil || latest == nil || latest.Revision != deletedRevision+1 || latest.Kind != "upsert" ||
		latest.Settings == nil || latest.Settings.Version != 3 || latest.Settings.ClearThroughSeq != 10001 ||
		latest.Settings.Pinned || latest.Settings.Draft != "" || latest.LatestConversationSeq == nil || *latest.LatestConversationSeq != 10002 {
		t.Fatalf("personal resurrection: %+v, err=%v", latest, err)
	}
}

type readSyncPlanNode struct {
	Kind      string             `json:"Node Type"`
	Relation  string             `json:"Relation Name"`
	Index     string             `json:"Index Name"`
	IndexCond string             `json:"Index Cond"`
	Rows      float64            `json:"Actual Rows"`
	Filtered  float64            `json:"Rows Removed by Filter"`
	Loops     float64            `json:"Actual Loops"`
	Children  []readSyncPlanNode `json:"Plans"`
}

func readSyncExplain(t *testing.T, ctx context.Context, tx pgx.Tx, query string, args ...any) readSyncPlanNode {
	t.Helper()
	var raw []byte
	if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query, args...).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var result []struct {
		Plan readSyncPlanNode
	}
	if err := json.Unmarshal(raw, &result); err != nil || len(result) != 1 {
		t.Fatalf("EXPLAIN produced %d plans, err=%v", len(result), err)
	}
	t.Logf("%s", raw)
	return result[0].Plan
}

func requireReadSyncIndex(t *testing.T, plan readSyncPlanNode, table string, maxRows, maxLoops int, pointKeys ...string) {
	t.Helper()
	found := false
	var check func(readSyncPlanNode)
	check = func(node readSyncPlanNode) {
		if node.Relation == table && node.Loops != 0 && node.Kind != "ModifyTable" {
			found = true
			if !strings.HasPrefix(node.Kind, "Index") || node.Index == "" || node.IndexCond == "" ||
				node.Rows > float64(maxRows) || node.Loops > float64(maxLoops) || node.Filtered != 0 {
				t.Fatalf("%s lookup not bounded: %+v (want at most %d rows, %d loops)", table, node, maxRows, maxLoops)
			}
			for _, key := range pointKeys {
				if !strings.Contains(node.IndexCond, key) {
					t.Fatalf("%s lookup did not constrain point key %s: %s", table, key, node.IndexCond)
				}
			}
		}
		for _, child := range node.Children {
			check(child)
		}
	}
	check(plan)
	if !found {
		t.Fatalf("missing executed %s lookup", table)
	}
}
