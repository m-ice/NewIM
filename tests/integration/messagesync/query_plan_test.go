//go:build integration

package messagesync_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	app "github.com/m-ice/NewIM/server/sync/message"
)

func TestQueryPlan(t *testing.T) {
	f := openFixture(t)
	conversationID := f.seedConversation("query_plan_target", f.user)
	otherConversation := f.seedConversation("query_plan_other", f.user)

	f.sql(`INSERT INTO newim.im_messages(server_msg_id,client_msg_id,sender_id,conversation_id,conversation_seq,protocol_version,schema_version,message_type,server_time_ms,payload_bytes)
SELECT 'qp_server_'||g,'qp_client_'||g,$1,$2,g,1,1,'text',g,convert_to('{"text":"target"}','UTF8') FROM generate_series(1,10000) g`, f.user, conversationID)
	f.sql("UPDATE newim.im_conversations SET last_seq=10000, latest_server_msg_id='qp_server_10000' WHERE conversation_id=$1", conversationID)
	f.sql(`INSERT INTO newim.im_messages(server_msg_id,client_msg_id,sender_id,conversation_id,conversation_seq,protocol_version,schema_version,message_type,server_time_ms,payload_bytes)
SELECT 'qp_other_server_'||g,'qp_other_client_'||g,$1,$2,g,1,1,'text',g,convert_to('{"text":"other"}','UTF8') FROM generate_series(1,100000) g`, f.user, otherConversation)
	f.sql("UPDATE newim.im_conversations SET last_seq=100000, latest_server_msg_id='qp_other_server_100000' WHERE conversation_id=$1", otherConversation)
	f.sql("ANALYZE newim.im_messages; ANALYZE newim.im_conversations; ANALYZE newim.im_conversation_members")

	if got := f.scalarString("SHOW enable_seqscan"); got != "on" {
		t.Fatalf("enable_seqscan got %q want on", got)
	}
	pageQuery := `SELECT server_msg_id,client_msg_id,sender_id,conversation_id,conversation_seq,protocol_version,schema_version,message_type,server_time_ms,payload_bytes FROM newim.im_messages WHERE conversation_id=$1 AND conversation_seq>$2 AND conversation_seq<=$3 ORDER BY conversation_seq LIMIT $4`
	firstQuery := `SELECT conversation_seq FROM newim.im_messages WHERE conversation_id=$1 ORDER BY conversation_seq ASC LIMIT 1`
	lastQuery := `SELECT conversation_seq FROM newim.im_messages WHERE conversation_id=$1 ORDER BY conversation_seq DESC LIMIT 1`
	anchorQuery := `SELECT EXISTS(SELECT 1 FROM newim.im_messages WHERE conversation_id=$1 AND conversation_seq=$2)`
	headQuery := `SELECT c.last_seq,c.latest_server_msg_id,m.conversation_seq FROM newim.im_conversations c JOIN newim.im_conversation_members member ON member.conversation_id=c.conversation_id AND member.user_id=$2 LEFT JOIN newim.im_messages m ON m.conversation_id=c.conversation_id AND m.server_msg_id=c.latest_server_msg_id WHERE c.conversation_id=$1`
	plans := map[string]json.RawMessage{
		"page":   json.RawMessage(f.explain("page", pageQuery, conversationID, 100, 10000, 101)),
		"first":  json.RawMessage(f.explain("first", firstQuery, conversationID)),
		"last":   json.RawMessage(f.explain("last", lastQuery, conversationID)),
		"anchor": json.RawMessage(f.explain("anchor", anchorQuery, conversationID, 5000)),
		"head":   json.RawMessage(f.explain("head", headQuery, conversationID, f.user)),
	}
	for label, plan := range plans {
		if !json.Valid(plan) {
			t.Fatalf("%s plan is not JSON", label)
		}
		assertNoMessageSeqScan(t, plan)
	}
	for _, label := range []string{"page", "first", "last", "anchor"} {
		assertUsesSequenceIndex(t, plans[label])
	}
	assertPlanRowsAtMost(t, plans["page"], 101)
	assertPlanRowsAtMost(t, plans["first"], 2)
	assertPlanRowsAtMost(t, plans["last"], 2)
	assertPlanRowsAtMost(t, plans["anchor"], 2)
	assertPlanRowsAtMost(t, plans["head"], 2)
	if err := os.WriteFile("/tmp/nim-syn-005-query-plans.json", mustJSON(t, plans), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := f.read(app.ReadRequest{ConversationID: conversationID, HasAfterSeq: true, AfterSeq: 9900, Limit: 100})
	must(t, err)
	if len(result.Items) != 100 || result.HasNext || result.NextAfterSeq == nil || *result.NextAfterSeq != 10000 {
		t.Fatalf("bounded page mismatch: %+v", result)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	must(t, err)
	return append(data, '\n')
}

func assertPlanRowsAtMost(t *testing.T, raw json.RawMessage, maximum float64) {
	t.Helper()
	seen := false
	walkPlan(t, raw, func(node map[string]any) {
		value, ok := node["Actual Rows"]
		if !ok {
			return
		}
		number, ok := value.(json.Number)
		if !ok {
			return
		}
		rows, err := number.Float64()
		if err != nil {
			t.Fatalf("invalid Actual Rows %q", number)
		}
		seen = true
		if rows > maximum {
			t.Fatalf("plan actual rows %.0f exceed %.0f: %s", rows, maximum, string(raw))
		}
	})
	if !seen {
		t.Fatalf("plan has no parsed Actual Rows: %s", string(raw))
	}
}

func assertNoMessageSeqScan(t *testing.T, raw json.RawMessage) {
	t.Helper()
	messageNodes := 0
	walkPlan(t, raw, func(node map[string]any) {
		if node["Relation Name"] != "im_messages" {
			return
		}
		messageNodes++
		if node["Node Type"] == "Seq Scan" {
			t.Fatalf("plan contains Seq Scan on im_messages: %s", string(raw))
		}
	})
	if messageNodes == 0 {
		t.Fatalf("plan does not reference im_messages: %s", string(raw))
	}
}

func assertUsesSequenceIndex(t *testing.T, raw json.RawMessage) {
	t.Helper()
	used := false
	walkPlan(t, raw, func(node map[string]any) {
		if node["Index Name"] == "im_messages_conversation_seq_key" {
			used = true
		}
	})
	if !used {
		t.Fatalf("plan does not use im_messages_conversation_seq_key: %s", string(raw))
	}
}

func walkPlan(t *testing.T, raw json.RawMessage, visit func(map[string]any)) {
	t.Helper()
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	var walk func(any)
	walk = func(node any) {
		switch current := node.(type) {
		case map[string]any:
			visit(current)
			for _, child := range current {
				walk(child)
			}
		case []any:
			for _, child := range current {
				walk(child)
			}
		}
	}
	walk(value)
}
