//go:build integration

package tlsmessage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

func TestTLSMessageSecurity(t *testing.T) {
	t.Run("default-off-and-startup-policy", testStartupPolicy)
	t.Run("permission-block-and-receipts", testSendPermissions)
	t.Run("request-limits-have-no-side-effects", testRequestLimits)
	t.Run("address-and-account-budgets", testRateBudgets)
}

func testStartupPolicy(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(11)
	token := f.admit("tls_disabled_user", "mobile")
	gateway := f.gateway("", map[string]string{"NEWIM_USER_PROFILE": "1"})
	for _, route := range []string{directPath, sendPath, blockPath, unblockPath} {
		requireAbsent(t, f.call(gateway, http.MethodPost, route, token, []byte("{}"), nil))
	}
	requireProfileView(t, f, gateway, token, "tls_disabled_user")
	requireHandshake(t, f, gateway, token, http.StatusSwitchingProtocols)

	settings := map[string]string{
		"NEWIM_GATEWAY_ADDR": "127.0.0.1:0", "NEWIM_GATEWAY_TLS_CERT_FILE": f.tlsCert,
		"NEWIM_GATEWAY_TLS_KEY_FILE": f.tlsKey, "NEWIM_PRIVATE_MESSAGE_POLICY": "",
	}
	p := startProcess(t, os.Getenv("NEWIM_GATEWAY_BINARY"), "1", settings)
	expectFailedStartup(t, p)
	f.sql("INSERT INTO newim.im_conversations(conversation_id) VALUES($1)", "tls_unclassified_legacy")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := f.db.Exec(ctx, "DELETE FROM newim.im_conversations WHERE conversation_id=$1", "tls_unclassified_legacy")
		if err != nil {
			t.Error("unable to remove unclassified fixture")
		}
	})
	settings["NEWIM_PRIVATE_MESSAGE_POLICY"] = "1"
	p = startProcess(t, os.Getenv("NEWIM_GATEWAY_BINARY"), "1", settings)
	expectFailedStartup(t, p)
	p.requireRedacted(t, testDSN, "tls_unclassified_legacy")
}

func expectFailedStartup(t *testing.T, p *process) {
	t.Helper()
	select {
	case <-p.done:
		if p.waitErr == nil || strings.Contains(p.log.String(), "newim-gateway listening on ") {
			t.Fatal("invalid TLS message startup configuration reached readiness or exited successfully")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("invalid TLS message startup configuration did not terminate")
	}
}

func requireAbsent(t *testing.T, reply response) {
	t.Helper()
	if reply.err != nil || reply.status != http.StatusNotFound || !reply.verified {
		t.Fatalf("unmounted TLS message route status=%d err=%v", reply.status, reply.err)
	}
}

func testSendPermissions(t *testing.T) {
	f := openFixture(t)
	const aliceID = "tls_acl_alice"
	const bobID = "tls_acl_bob"
	alice := f.admit(aliceID, "mobile")
	bob := f.admit(bobID, "mobile")
	outsider := f.admit("tls_acl_outsider", "mobile")
	writer, writerOther := f.gateway("1", nil), f.gateway("1", nil)
	setBlock(t, f, writer, blockPath, bob, aliceID)
	setBlock(t, f, writerOther, blockPath, bob, aliceID)
	var blocked, reverse int
	must(t, f.db.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM newim.im_blocks WHERE user_id=$1 AND blocked_user_id=$2),
		(SELECT count(*) FROM newim.im_blocks WHERE user_id=$2 AND blocked_user_id=$1)`, bobID, aliceID).Scan(&blocked, &reverse))
	if blocked != 1 || reverse != 0 {
		t.Fatal("repeated block did not retain one directed relationship")
	}
	denied := f.call(writerOther, http.MethodPost, directPath, alice, peerBody(t, bobID), nil)
	denied.requireError(t, 403, "BLOCKED_BY_RECIPIENT")
	var directCount int
	must(t, f.db.QueryRow(f.ctx, "SELECT count(*) FROM newim.im_conversations WHERE direct_user_low=$1 AND direct_user_high=$2", aliceID, bobID).Scan(&directCount))
	if directCount != 0 {
		t.Fatal("a blocked creation wrote a conversation")
	}
	conversation := createDirect(t, f, writer, bob, aliceID)
	if existing := createDirect(t, f, writerOther, alice, bobID); existing != conversation {
		t.Fatal("block did not preserve the existing direct conversation")
	}
	reverseAck := expectACK(t, f.call(writer, http.MethodPost, sendPath, bob, textBody(t, "reverse_before_unblock", conversation, "allowed reverse"), nil))
	requireCommitted(t, f, reverseAck, bobID, conversation, "reverse_before_unblock", "allowed reverse", 1)
	setBlock(t, f, writer, unblockPath, bob, aliceID)
	setBlock(t, f, writerOther, unblockPath, bob, aliceID)
	const kept = "committed private history"
	wire := textBody(t, "kept_receipt", conversation, kept)
	first := f.call(writerOther, http.MethodPost, sendPath, alice, wire, nil)
	ack := expectACK(t, first)
	requireCommitted(t, f, ack, aliceID, conversation, "kept_receipt", kept, 2)

	setBlock(t, f, writer, blockPath, bob, aliceID)
	before := state(t, f, conversation)
	newSend := f.call(writerOther, http.MethodPost, sendPath, alice, textBody(t, "blocked_new", conversation, "fresh denied"), nil)
	newSend.requireError(t, 403, "BLOCKED_BY_RECIPIENT")
	if newSend.headers.Get("Retry-After") != "" {
		t.Fatal("permanent block returned a retry hint")
	}
	replay := f.call(writerOther, http.MethodPost, sendPath, alice, wire, nil)
	expectACK(t, replay)
	if !bytes.Equal(first.body, replay.body) {
		t.Fatal("block changed the already committed receipt")
	}
	f.call(writerOther, http.MethodPost, sendPath, alice, textBody(t, "kept_receipt", conversation, "changed intent"), nil).
		requireError(t, 409, "SEND_ID_CONFLICT")
	f.call(writer, http.MethodPost, sendPath, outsider, wire, nil).requireError(t, 403, "SEND_UNAUTHORIZED")
	stream := subscribe(t, f, writerOther, bob, conversation, 1)
	requireDelivered(t, f, stream, ack, kept)
	must(t, stream.CloseNow())

	f.sql("DELETE FROM newim.im_conversation_members WHERE conversation_id=$1 AND user_id=$2", conversation, aliceID)
	f.call(writer, http.MethodPost, sendPath, alice, wire, nil).requireError(t, 403, "SEND_UNAUTHORIZED")
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", conversation, aliceID)
	if got := state(t, f, conversation); got != before {
		t.Fatal("denied and repeated sends changed committed rows")
	}

	verifyGroupReceiptDenied(t, f, writer, alice, bob, aliceID, bobID)
	writer.requireRedacted(t, alice, bob, outsider, kept, testDSN)
	writerOther.requireRedacted(t, alice, bob, outsider, kept, testDSN)
}

func verifyGroupReceiptDenied(t *testing.T, f *fixture, gateway *process, alice, bob, aliceID, bobID string) {
	t.Helper()
	const group = "tls_acl_group"
	const body = "known history from an existing group"
	f.sql("INSERT INTO newim.im_conversations(conversation_id,kind) VALUES($1,'group')", group)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2),($1,$3)", group, aliceID, bobID)
	payload, err := json.Marshal(map[string]string{"text": body})
	must(t, err)
	now := time.Now().UnixMilli()
	// Use the existing group writer on the owned fixture so the exact prior intent has a real receipt.
	f.sql("SELECT newim.persist_message($1,$2,$3,$4,'text',1,$5,$6,$7)",
		aliceID, "group_existing", group, "known_group_server", now, payload, "known_group_event")
	before := state(t, f, group)
	if before != (durableState{lastSeq: 1, latest: "known_group_server", messages: 1, outbox: 1}) {
		t.Fatal("legacy group receipt fixture did not commit")
	}
	for _, client := range []string{"group_existing", "group_new"} {
		f.call(gateway, http.MethodPost, sendPath, alice, textBody(t, client, group, body), nil).requireError(t, 403, "SEND_UNAUTHORIZED")
	}
	if got := state(t, f, group); got != before {
		t.Fatal("TLS direct-only send changed the existing group state")
	}
	stream := subscribe(t, f, gateway, bob, group, 0)
	requireDelivered(t, f, stream, protocol.Ack{SenderID: aliceID, ClientMsgID: "group_existing", ServerMsgID: "known_group_server",
		ConversationID: group, ConversationSeq: "1", ServerTime: strconv.FormatInt(now, 10)}, body)
}

func testRequestLimits(t *testing.T) {
	f := openFixture(t)
	const aliceID = "tls_request_alice"
	const bobID = "tls_request_bob"
	alice := f.admit(aliceID, "mobile")
	f.admit(bobID, "mobile")
	gateway := f.gateway("1", nil)
	conversation := createDirect(t, f, gateway, alice, bobID)
	peer := peerBody(t, bobID)
	healthy := textBody(t, "limits_valid_shape", conversation, "known synthetic text")
	for _, route := range []string{directPath, blockPath, unblockPath, sendPath} {
		body := peer
		if route == sendPath {
			body = healthy
		}
		f.call(gateway, http.MethodPost, route, "", body, nil).requireError(t, 401, "AUTH_INVALID_TOKEN")
		allow := f.call(gateway, http.MethodGet, route, alice, nil, nil)
		allow.requireError(t, 405, "HTTP_METHOD_NOT_ALLOWED")
		if allow.headers.Get("Allow") != "POST" {
			t.Fatal("method denial omitted the allowed method")
		}
	}
	dup := http.Header{"Authorization": {"Bearer " + alice, "Bearer " + alice}}
	f.call(gateway, http.MethodPost, sendPath, "", healthy, dup).requireError(t, 401, "AUTH_INVALID_TOKEN")
	f.call(gateway, http.MethodPost, blockPath, alice, []byte(`{"peerUserId":"tls_request_bob","userId":"untrusted_actor"}`), nil).
		requireError(t, 400, "SEND_INVALID_INPUT")
	claimed := bytes.Replace(healthy, []byte(`"body":{`), []byte(`"body":{"senderId":"untrusted_actor",`), 1)
	if bytes.Equal(claimed, healthy) {
		t.Fatal("actor assertion fixture lacks a send body")
	}
	f.call(gateway, http.MethodPost, sendPath, alice, claimed, nil).requireError(t, 400, "SEND_INVALID_INPUT")
	f.call(gateway, http.MethodPost, sendPath+"?", alice, healthy, nil).requireError(t, 400, "HTTP_INVALID_QUERY")
	f.call(gateway, http.MethodPost, sendPath, alice, healthy, http.Header{"Content-Encoding": {"gzip"}}).
		requireError(t, 415, "HTTP_UNSUPPORTED_MEDIA_TYPE")
	f.call(gateway, http.MethodPost, directPath, alice, peer, http.Header{"Content-Type": {"text/plain"}}).
		requireError(t, 415, "HTTP_UNSUPPORTED_MEDIA_TYPE")
	f.call(gateway, http.MethodPost, blockPath, alice, bytes.Repeat([]byte{' '}, 513), nil).
		requireError(t, 413, "HTTP_BODY_TOO_LARGE")
	f.call(gateway, http.MethodPost, sendPath, alice, bytes.Repeat([]byte{' '}, protocol.MaxFrameBytes+1), nil).
		requireError(t, 413, "HTTP_BODY_TOO_LARGE")
	for _, alias := range []string{sendPath + "/", "/api/v1/%6dessages"} {
		requireAbsent(t, f.call(gateway, http.MethodPost, alias, alice, healthy, nil))
	}
	var directions int
	must(t, f.db.QueryRow(f.ctx, "SELECT count(*) FROM newim.im_blocks WHERE user_id=$1 OR blocked_user_id=$1", aliceID).Scan(&directions))
	if current := state(t, f, conversation); current != (durableState{}) || directions != 0 {
		t.Fatal("invalid requests changed conversation or block state")
	}
	gateway.requireRedacted(t, alice, testDSN, "untrusted_actor", "known synthetic text")
}

func testRateBudgets(t *testing.T) {
	f := openFixture(t)
	const owner = "tls_budget_owner"
	const peer = "tls_budget_peer"
	first := f.admit(owner, "mobile")
	second := f.admit(owner, "desktop")
	other := f.admit(peer, "mobile")
	accountLimited := f.gateway("1", map[string]string{
		"NEWIM_TLS_MESSAGE_ACCOUNT_ATTEMPTS_PER_MINUTE": "1", "NEWIM_USER_PROFILE": "1", "NEWIM_FRIEND_HTTP": "1",
	})
	waitRateWindow()
	created := createDirect(t, f, accountLimited, first, peer)
	f.call(accountLimited, http.MethodPost, blockPath, second, peerBody(t, peer), nil).requireError(t, 429, "HTTP_RATE_LIMITED")
	if got := createDirect(t, f, accountLimited, other, owner); got != created {
		t.Fatal("rate limit was shared across different accounts")
	}
	requireProfileView(t, f, accountLimited, first, owner)
	view := f.call(accountLimited, http.MethodPost, "/api/v1/friends/list", first, []byte("{}"), nil)
	if view.err != nil || view.status != http.StatusOK || !view.verified {
		t.Fatal("TLS message account limit changed the friend HTTP budget")
	}
	requireHandshake(t, f, accountLimited, first, http.StatusSwitchingProtocols)

	ipLimited := f.gateway("1", map[string]string{
		"NEWIM_TLS_MESSAGE_IP_ATTEMPTS_PER_MINUTE": "1", "NEWIM_GATEWAY_IP_ATTEMPTS_PER_MINUTE": "1",
	})
	waitRateWindow()
	requireHandshake(t, f, ipLimited, first, http.StatusSwitchingProtocols)
	requireHandshake(t, f, ipLimited, other, http.StatusTooManyRequests)
	reply := f.call(ipLimited, http.MethodPost, directPath, first, peerBody(t, peer), http.Header{"X-Forwarded-For": {"192.0.2.1"}})
	requireStatus(t, reply, http.StatusOK)
	f.call(ipLimited, http.MethodPost, blockPath, other, peerBody(t, owner), http.Header{"X-Forwarded-For": {"192.0.2.2"}}).
		requireError(t, 429, "HTTP_RATE_LIMITED")
	var count int
	must(t, f.db.QueryRow(f.ctx, "SELECT count(*) FROM newim.im_blocks WHERE (user_id=$1 AND blocked_user_id=$2) OR (user_id=$2 AND blocked_user_id=$1)", owner, peer).Scan(&count))
	if count != 0 {
		t.Fatal("rate-limited relationship request changed database state")
	}
}

func requireHandshake(t *testing.T, f *fixture, gateway *process, token string, want int) {
	t.Helper()
	conn, reply, err := dialSocket(t, f, gateway, token)
	if conn != nil {
		defer conn.CloseNow()
	}
	requireSocketStatus(t, conn, reply, err, want)
}

func requireProfileView(t *testing.T, f *fixture, gateway *process, token, owner string) {
	t.Helper()
	reply := f.call(gateway, http.MethodGet, "/api/v1/users/me", token, nil, nil)
	if reply.err != nil || reply.status != http.StatusOK || !reply.verified {
		t.Fatal("TLS message policy changed the existing profile route")
	}
	var view struct {
		Profile struct {
			UserID string `json:"userId"`
		} `json:"profile"`
	}
	must(t, json.Unmarshal(reply.body, &view))
	if view.Profile.UserID != owner {
		t.Fatal("existing profile route returned the wrong owner")
	}
}

func waitRateWindow() {
	remaining := time.Until(time.Now().Truncate(time.Minute).Add(time.Minute))
	if remaining < 3*time.Second {
		time.Sleep(remaining + 20*time.Millisecond)
	}
}
