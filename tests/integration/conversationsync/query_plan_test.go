//go:build integration

package conversationsync_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	app "github.com/m-ice/NewIM/server/sync/conversation"
)

const queryPlanPath = "/tmp/nim-syn-003-query-plans.json"

func explainJSON(t *testing.T, f *fixture, statement string, args ...any) any {
	t.Helper()
	var raw []byte
	must(t, f.db.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+statement, args...).Scan(&raw))
	var plan any
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatalf("decode explain JSON: %v", err)
	}
	return plan
}

func planJSON(t *testing.T, plan any) string {
	t.Helper()
	raw, err := json.Marshal(plan)
	must(t, err)
	return string(raw)
}

func walkPlan(value any, visit func(map[string]any)) {
	switch typed := value.(type) {
	case map[string]any:
		if nodeType, ok := typed["Node Type"].(string); ok && nodeType != "" {
			visit(typed)
		}
		for _, child := range typed {
			walkPlan(child, visit)
		}
	case []any:
		for _, child := range typed {
			walkPlan(child, visit)
		}
	}
}

func planString(t *testing.T, plan any, needle string) {
	t.Helper()
	if !strings.Contains(planJSON(t, plan), needle) {
		t.Fatalf("query plan missing %q: %s", needle, planJSON(t, plan))
	}
}

func planNoMessageScan(t *testing.T, name string, plan any) {
	t.Helper()
	encoded := planJSON(t, plan)
	if strings.Contains(encoded, "im_messages") {
		t.Fatalf("%s plan touched im_messages: %s", name, encoded)
	}
}

func planLimitAtMost(t *testing.T, name string, plan any, maximum float64) {
	t.Helper()
	found := false
	walkPlan(plan, func(node map[string]any) {
		if node["Node Type"] != "Limit" {
			return
		}
		found = true
		if rows, ok := node["Actual Rows"].(float64); ok && rows > maximum {
			t.Fatalf("%s limit returned %.0f rows maximum %.0f", name, rows, maximum)
		}
	})
	if !found {
		t.Fatalf("%s plan has no Limit node", name)
	}
}

func planIndexLoopsAtMost(t *testing.T, name string, plan any, index string, maximum float64) {
	t.Helper()
	found := false
	walkPlan(plan, func(node map[string]any) {
		if node["Index Name"] != index {
			return
		}
		found = true
		if loops, ok := node["Actual Loops"].(float64); ok && loops > maximum {
			t.Fatalf("%s index %s loops %.0f maximum %.0f", name, index, loops, maximum)
		}
	})
	if !found {
		t.Fatalf("%s plan did not use %s: %s", name, index, planJSON(t, plan))
	}
}

func TestQueryPlan(t *testing.T) {
	f := openFixture(t)
	user := "qplan_user"
	f.account(user)

	f.sql("INSERT INTO newim.im_conversations(conversation_id) SELECT 'q'||lpad(g::text,6,'0') FROM generate_series(0,100000) g")
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) SELECT conversation_id,$1 FROM newim.im_conversations WHERE conversation_id LIKE 'q%'", user)
	f.sql("INSERT INTO newim.im_conversation_sync_keys(user_id,conversation_id,first_change_seq) SELECT $1::newim.identifier,conversation_id,g FROM (SELECT conversation_id,row_number() OVER (ORDER BY conversation_id)::bigint g FROM newim.im_conversations WHERE conversation_id LIKE 'q%') rows", user)
	f.sql("INSERT INTO newim.im_conversation_sync_changes(user_id,change_seq,conversation_id,kind,latest_seq) SELECT $1::newim.identifier,first_change_seq,conversation_id,'upsert',0 FROM newim.im_conversation_sync_keys WHERE user_id=$1::newim.identifier", user)
	f.sql("UPDATE newim.im_conversation_sync_accounts SET last_change_seq=100001 WHERE user_id=$1", user)
	f.sql("ANALYZE newim.im_conversation_sync_accounts; ANALYZE newim.im_conversation_sync_keys; ANALYZE newim.im_conversation_sync_changes; ANALYZE newim.im_conversation_members")

	_, checkpoint := bootstrapAll(t, f.service(nil, ""), user, 100)
	f.sql("INSERT INTO newim.im_conversation_sync_changes(user_id,change_seq,conversation_id,kind,latest_seq) SELECT $1,g,'q000000','upsert',0 FROM generate_series(100002,200000) g", user)
	f.sql("UPDATE newim.im_conversation_sync_accounts SET last_change_seq=200000 WHERE user_id=$1", user)
	f.sql("ANALYZE newim.im_conversation_sync_accounts; ANALYZE newim.im_conversation_sync_changes")

	if conversations := f.scalarInt64("SELECT count(*) FROM newim.im_conversation_sync_keys WHERE user_id=$1", user); conversations != 100001 {
		t.Fatalf("directory rows got %d want 100001", conversations)
	}
	if hot := f.scalarInt64("SELECT count(*) FROM newim.im_conversation_sync_changes WHERE user_id=$1 AND conversation_id='q000000'", user); hot != 100000 {
		t.Fatalf("hot revisions got %d want 100000", hot)
	}
	if value := strings.TrimSpace(f.scalarString("SHOW enable_seqscan")); value != "on" {
		t.Fatalf("enable_seqscan got %q want on", value)
	}

	observer := &captureObserver{}
	service := f.serviceWithObserver(nil, "", observer)
	page, err := service.BeginBootstrap(ctx, principal(user), 100)
	must(t, err)
	checkedPage(t, page, 100)
	observation, ok := observer.latest("begin_bootstrap")
	if !ok || observation.Candidates != 200 || len(page.Items) != 100 {
		t.Fatalf("bootstrap bound observation=%+v items=%d", observation, len(page.Items))
	}
	delta, err := service.BeginDelta(ctx, principal(user), checkpoint, 100)
	must(t, err)
	checkedPage(t, delta, 100)
	observation, ok = observer.latest("begin_delta")
	if !ok || observation.Candidates != 101 || len(delta.Items) != 100 || !delta.HasMore {
		t.Fatalf("delta bound observation=%+v items=%d hasMore=%t", observation, len(delta.Items), delta.HasMore)
	}

	plans := map[string]any{}
	plans["directory"] = explainJSON(t, f, "SELECT conversation_id,first_change_seq FROM newim.im_conversation_sync_keys WHERE user_id=$1 AND conversation_id>$2 AND conversation_id<=$3 ORDER BY conversation_id LIMIT 200", user, "", "q100000")
	plans["delta"] = explainJSON(t, f, "SELECT conversation_id,change_seq,kind,latest_seq,latest_server_msg_id FROM newim.im_conversation_sync_changes WHERE user_id=$1 AND change_seq>$2 AND change_seq<=$3 ORDER BY change_seq LIMIT 101", user, int64(100001), int64(200000))
	plans["latest"] = explainJSON(t, f, "SELECT conversation_id,change_seq,kind,latest_seq,latest_server_msg_id FROM newim.im_conversation_sync_changes WHERE user_id=$1 AND conversation_id=$2 AND change_seq<=$3 ORDER BY change_seq DESC LIMIT 1", user, "q000000", int64(app.MaxSequence))
	plans["membership"] = explainJSON(t, f, "SELECT EXISTS(SELECT 1 FROM newim.im_conversation_members WHERE user_id=$1 AND conversation_id=$2)", user, "q000000")
	plans["directory-latest-lateral"] = explainJSON(t, f, `SELECT d.conversation_id,l.change_seq FROM (SELECT conversation_id FROM newim.im_conversation_sync_keys WHERE user_id=$1 AND conversation_id>$2 AND conversation_id<=$3 ORDER BY conversation_id LIMIT 200) d LEFT JOIN LATERAL (SELECT change_seq FROM newim.im_conversation_sync_changes WHERE user_id=$1 AND conversation_id=d.conversation_id AND change_seq<=$4 ORDER BY change_seq DESC LIMIT 1) l ON true`, user, "", "q100000", int64(app.MaxSequence))
	plans["delta-latest-lateral"] = explainJSON(t, f, `SELECT d.conversation_id,l.change_seq FROM (SELECT conversation_id,change_seq FROM newim.im_conversation_sync_changes WHERE user_id=$1 AND change_seq>$2 AND change_seq<=$3 ORDER BY change_seq LIMIT 101) d LEFT JOIN LATERAL (SELECT change_seq FROM newim.im_conversation_sync_changes WHERE user_id=$1 AND conversation_id=d.conversation_id AND change_seq<=$4 ORDER BY change_seq DESC LIMIT 1) l ON true`, user, int64(100001), int64(200000), int64(app.MaxSequence))

	for name, plan := range plans {
		planNoMessageScan(t, name, plan)
	}
	planString(t, plans["directory"], "im_conversation_sync_keys_pkey")
	planLimitAtMost(t, "directory", plans["directory"], 200)
	planString(t, plans["delta"], "im_conversation_sync_changes_pkey")
	planLimitAtMost(t, "delta", plans["delta"], 101)
	planString(t, plans["latest"], "im_conversation_sync_changes_version_idx")
	planLimitAtMost(t, "latest", plans["latest"], 1)
	planString(t, plans["membership"], "im_conversation_members_user_idx")
	planIndexLoopsAtMost(t, "directory-latest-lateral", plans["directory-latest-lateral"], "im_conversation_sync_changes_version_idx", 200)
	planIndexLoopsAtMost(t, "delta-latest-lateral", plans["delta-latest-lateral"], "im_conversation_sync_changes_version_idx", 101)

	raw, err := json.MarshalIndent(plans, "", "  ")
	must(t, err)
	must(t, os.WriteFile(queryPlanPath, append(raw, '\n'), 0o644))
	t.Logf("recorded query plans: %s", queryPlanPath)
}
