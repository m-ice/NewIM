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
	plans := map[string]json.RawMessage{
		"page":   json.RawMessage(f.explain("page", pageQuery, conversationID, 100, 10000, 101)),
		"first":  json.RawMessage(f.explain("first", firstQuery, conversationID)),
		"last":   json.RawMessage(f.explain("last", lastQuery, conversationID)),
		"anchor": json.RawMessage(f.explain("anchor", anchorQuery, conversationID, 5000)),
	}
	for label, plan := range plans {
		planText := string(plan)
		if !strings.Contains(planText, "im_messages_conversation_seq_key") {
			t.Fatalf("%s plan did not use im_messages_conversation_seq_key: %s", label, planText)
		}
		if strings.Contains(planText, "Seq Scan on im_messages") {
			t.Fatalf("%s plan contains a message-history sequential scan: %s", label, planText)
		}
		if !json.Valid(plan) {
			t.Fatalf("%s plan is not JSON", label)
		}
	}
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
