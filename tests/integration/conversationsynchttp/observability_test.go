//go:build integration

package conversationsynchttp_test

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/m-ice/NewIM/server/message"
	"github.com/m-ice/NewIM/server/messagehttp"
	"github.com/m-ice/NewIM/server/observability"
)

func TestMessageTrace(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(22)
	gateway := f.accountSettingsGateway(writeKeys(t, "trace", "trace"))
	pair := accountPair(t, f, gateway, "trace")
	body := textBody(t, "trace_client_message", pair.conversation, "trace private text")
	first := f.call(gateway, http.MethodPost, sendPath, pair.owner, body,
		http.Header{messagehttp.TraceHeader: {"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}})
	ack := expectACK(t, first)
	trace := traceValue(t, first)
	if trace == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatal("the server did not generate its own attempt trace")
	}
	var count int
	must(t, f.db.QueryRow(f.ctx, `SELECT count(*) FROM newim.im_messages
		WHERE sender_id=$1 AND client_msg_id=$2 AND server_msg_id=$3`,
		pair.ownerID, ack.ClientMsgID, ack.ServerMsgID).Scan(&count))
	if count != 1 {
		t.Fatal("receipt did not identify one committed message")
	}

	retry := f.call(gateway, http.MethodPost, sendPath, pair.tablet, body, nil)
	requireStatus(t, retry, http.StatusOK)
	secondTrace := traceValue(t, retry)
	if secondTrace == trace || !bytes.Equal(first.body, retry.body) {
		t.Fatal("idempotent attempts require independent trace metadata and identical ACK bytes")
	}
	for _, response := range []response{
		f.call(gateway, http.MethodPost, sendPath, "", body, nil),
		f.call(gateway, http.MethodPost, sendPath, pair.owner,
			textBody(t, ack.ClientMsgID, pair.conversation, "different intent"), nil),
	} {
		if response.err != nil || response.status < 400 || len(response.headers.Values(messagehttp.TraceHeader)) != 0 ||
			bytes.Contains(response.body, []byte(ack.ServerMsgID)) {
			t.Fatal("an unsuccessful send returned committed diagnostic metadata")
		}
	}
	gateway.requireRedacted(t, trace, secondTrace, pair.owner, pair.tablet, ack.ServerMsgID, "trace private text")
}

func traceValue(t *testing.T, reply response) string {
	t.Helper()
	values := reply.headers.Values(messagehttp.TraceHeader)
	if !reply.verified || len(values) != 1 || !message.ValidTraceID(values[0]) ||
		bytes.Contains(reply.body, []byte(values[0])) {
		t.Fatal("verified TLS response omitted its separate attempt trace")
	}
	return values[0]
}

func checkSDKReport(t *testing.T, f *fixture, gateway *process, token string, body []byte) {
	t.Helper()
	valid := f.call(gateway, "POST", observability.SDKRoute, token, body, nil)
	if valid.err != nil || !valid.verified || valid.status != http.StatusNoContent {
		t.Fatalf("SDK aggregate was rejected: status %d", valid.status)
	}
	unauthorized := f.call(gateway, "POST", observability.SDKRoute, "", body, nil)
	if unauthorized.err != nil || unauthorized.status != http.StatusUnauthorized {
		t.Fatal("SDK report did not require a current Bearer session")
	}
	for _, invalid := range [][]byte{
		[]byte(`{"version":1,"message_committed":1,"conversation":"private"}`),
		[]byte(`{"version":1,"ready":1,"ready":2}`),
		[]byte(`{"version":1,"socket_attempt":1025}`),
	} {
		rejected := f.call(gateway, "POST", observability.SDKRoute, token, invalid, nil)
		if rejected.err != nil || rejected.status != http.StatusBadRequest {
			t.Fatal("SDK accepted a report outside its fixed aggregate contract")
		}
	}
}
