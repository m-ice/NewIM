//go:build integration

package messagehttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

const (
	createDirectRoute = "/api/v1/conversations/direct"
	blockPeerRoute    = "/api/v1/relationships/block"
	unblockPeerRoute  = "/api/v1/relationships/unblock"
)

func TestPrivateMessagePolicy(t *testing.T) {
	if !t.Run("upgrade-existing-conversations", testPrivatePolicyUpgrade) {
		return
	}
	t.Run("authenticated-direction-and-receipts", testPrivatePolicyDirection)
	t.Run("concurrent-creation-and-ordering", testPrivatePolicyConcurrency)
}

func testPrivatePolicyUpgrade(t *testing.T) {
	f := openFixture(t)
	if version := f.scalarInt("SELECT max(version) FROM newim_meta.migrations"); version != 6 {
		t.Fatalf("legacy fixture version=%d want 6", version)
	}
	alice := f.seedIdentity("private_legacy_alice", seedOptions{})
	bob := f.seedIdentity("private_legacy_bob", seedOptions{})
	directID := f.seedConversation("private_legacy_direct", alice.UserID, bob.UserID)
	groupID := f.seedConversation("private_legacy_group", alice.UserID, bob.UserID)
	f.exec("INSERT INTO newim.im_blocks(user_id,blocked_user_id) VALUES($1,$2)", bob.UserID, alice.UserID)

	legacyEnv := validMessageEnv()
	legacyEnv["NEWIM_PRIVATE_MESSAGE_POLICY"] = ""
	legacy := startMessageServer(t, legacyEnv)
	legacy.waitReady(t)
	historical := textBody(t, "legacy_before_upgrade", directID, "private_legacy_preserved_text")
	first := sendPrivateMessage(t, legacy, alice, historical)
	expectPrivateACK(t, first)
	assertSendSucceeds(t, legacy, alice, groupID, "legacy_group_before_upgrade")
	legacy.stop(t)
	waitNoConnections(t, f, "newim-auth-http", "newim-message-http")

	migrationFile := os.Getenv("NEWIM_MESSAGE_HTTP_MIGRATION_SQL")
	if migrationFile == "" {
		t.Fatal("NEWIM_MESSAGE_HTTP_MIGRATION_SQL is required")
	}
	upgradeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	command := exec.CommandContext(upgradeCtx, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-d", f.dsn, "-f", migrationFile)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("upgrade legacy fixture: %v output=%q", err, output)
	}
	if version := f.scalarInt("SELECT max(version) FROM newim_meta.migrations"); version != 7 {
		t.Fatalf("upgraded fixture version=%d want 7", version)
	}
	if got := f.scalarInt("SELECT count(*) FROM newim.im_conversations WHERE kind IS NULL AND direct_user_low IS NULL AND direct_user_high IS NULL"); got != 2 {
		t.Fatalf("unclassified legacy conversations=%d want 2", got)
	}

	flagOff := startMessageServer(t, legacyEnv)
	flagOff.waitReady(t)
	assertError(t, privateRequest(t, flagOff, createDirectRoute, bob.UserID, alice), http.StatusNotFound, "HTTP_ROUTE_NOT_FOUND")
	assertSendSucceeds(t, flagOff, alice, directID, "legacy_direct_after_upgrade")
	assertSendSucceeds(t, flagOff, alice, groupID, "legacy_group_after_upgrade")
	flagOff.stop(t)
	waitNoConnections(t, f, "newim-auth-http", "newim-message-http")
	expectPrivateStartupRefused(t, privatePolicyEnv())
	waitNoConnections(t, f, "newim-auth-http", "newim-message-http")

	f.exec("UPDATE newim.im_conversations SET kind='group' WHERE conversation_id=$1", groupID)
	f.exec("UPDATE newim.im_conversations SET kind='direct',direct_user_low=$2,direct_user_high=$3 WHERE conversation_id=$1",
		directID, alice.UserID, bob.UserID)
	f.removeMembership(directID, bob.UserID)
	expectPrivateStartupRefused(t, privatePolicyEnv())
	waitNoConnections(t, f, "newim-auth-http", "newim-message-http")
	f.exec("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", directID, bob.UserID)

	messageRequired := privatePolicyEnv()
	messageRequired["NEWIM_MESSAGE_HTTP"] = "0"
	expectPrivateStartupRefused(t, messageRequired)

	server := startMessageServer(t, privatePolicyEnv())
	defer server.stop(t)
	server.waitReady(t)
	// The reviewed pair points at the existing ID, including when the peer blocks.
	if got := expectDirectConversation(t, privateRequest(t, server, createDirectRoute, bob.UserID, alice)); got != directID {
		t.Fatalf("existing direct conversation=%s want %s", got, directID)
	}
	before := privateSendState(t, f, directID)
	assertError(t, sendPrivateMessage(t, server, alice, textBody(t, "legacy_blocked_new", directID, "blocked")), http.StatusForbidden, "BLOCKED_BY_RECIPIENT")
	if got := privateSendState(t, f, directID); got != before {
		t.Fatalf("legacy blocked send changed durable state: before=%+v after=%+v", before, got)
	}
	replay := sendPrivateMessage(t, server, alice, historical)
	expectPrivateACK(t, replay)
	if !bytes.Equal(replay.body, first.body) {
		t.Fatalf("migration changed a committed ACK: before=%q after=%q", first.body, replay.body)
	}
	assertSendSucceeds(t, server, alice, groupID, "legacy_group_policy_on")

	// An unclassified row introduced after startup blocks only new sends.
	f.exec("UPDATE newim.im_conversations SET kind=NULL,direct_user_low=NULL,direct_user_high=NULL WHERE conversation_id=$1", directID)
	before = privateSendState(t, f, directID)
	assertError(t, sendPrivateMessage(t, server, alice, textBody(t, "legacy_runtime_unclassified", directID, "unclassified")),
		http.StatusServiceUnavailable, "SERVER_TEMPORARY_UNAVAILABLE")
	replay = sendPrivateMessage(t, server, alice, historical)
	expectPrivateACK(t, replay)
	if !bytes.Equal(replay.body, first.body) || privateSendState(t, f, directID) != before {
		t.Fatal("unclassified conversation did not preserve its committed receipt or sequence")
	}
	f.exec("UPDATE newim.im_conversations SET kind='direct',direct_user_low=$2,direct_user_high=$3 WHERE conversation_id=$1",
		directID, alice.UserID, bob.UserID)
	f.removeMembership(directID, bob.UserID)
	assertError(t, sendPrivateMessage(t, server, alice, textBody(t, "legacy_runtime_invalid_pair", directID, "wrong pair")),
		http.StatusServiceUnavailable, "SERVER_TEMPORARY_UNAVAILABLE")
	if got := privateSendState(t, f, directID); got != before {
		t.Fatalf("mismatched direct membership changed durable state: before=%+v after=%+v", before, got)
	}
	f.exec("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", directID, bob.UserID)
	expectPolicyNoContent(t, privateRequest(t, server, unblockPeerRoute, alice.UserID, bob))
	assertSendSucceeds(t, server, alice, directID, "legacy_reclassified_send")
}

func testPrivatePolicyDirection(t *testing.T) {
	f := openFixture(t)
	alice := f.seedIdentity("private_alice", seedOptions{})
	bob := f.seedIdentity("private_bob", seedOptions{})
	other := f.seedIdentity("private_outsider", seedOptions{})
	server := startMessageServer(t, privatePolicyEnv())
	defer server.stop(t)
	server.waitReady(t)

	for _, route := range []string{createDirectRoute, blockPeerRoute, unblockPeerRoute} {
		body := privatePeerBody(bob.UserID)
		assertError(t, doRequest(t, http.MethodPost, privateURL(server, route), body, "", nil),
			http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
		assertError(t, doRequest(t, http.MethodPost, privateURL(server, route), body, unknownToken("private_unknown"), nil),
			http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
		for _, forged := range []string{
			fmt.Sprintf(`{"peerUserId":%q,"userId":%q}`, bob.UserID, other.UserID),
			fmt.Sprintf(`{"peerUserId":%q,"peerUserId":%q}`, bob.UserID, other.UserID),
			`{"peerUserId":"private_nonexistent"}`,
			fmt.Sprintf(`{"peerUserId":%q}`, alice.UserID),
		} {
			assertError(t, doRequest(t, http.MethodPost, privateURL(server, route), []byte(forged), alice.RawToken, nil),
				http.StatusBadRequest, "SEND_INVALID_INPUT")
		}
	}
	assertNoDirectPair(t, f, alice.UserID, bob.UserID)
	assertNoDirectPair(t, f, alice.UserID, other.UserID)
	if got := f.scalarInt("SELECT count(*) FROM newim.im_blocks WHERE user_id IN ($1,$2,$3)", alice.UserID, bob.UserID, other.UserID); got != 0 {
		t.Fatalf("invalid relationship requests wrote %d rows", got)
	}

	expectPolicyNoContent(t, privateRequest(t, server, blockPeerRoute, alice.UserID, bob))
	expectPolicyNoContent(t, privateRequest(t, server, blockPeerRoute, alice.UserID, bob))
	if got := f.scalarInt("SELECT count(*) FROM newim.im_blocks WHERE user_id=$1 AND blocked_user_id=$2", bob.UserID, alice.UserID); got != 1 {
		t.Fatalf("block retries wrote %d directed rows", got)
	}
	if got := f.scalarInt("SELECT count(*) FROM newim.im_blocks WHERE user_id=$1 AND blocked_user_id=$2", alice.UserID, bob.UserID); got != 0 {
		t.Fatalf("block unexpectedly wrote reverse relationship")
	}
	conversationCount := f.scalarInt("SELECT count(*) FROM newim.im_conversations")
	memberCount := f.scalarInt("SELECT count(*) FROM newim.im_conversation_members")
	assertError(t, privateRequest(t, server, createDirectRoute, bob.UserID, alice), http.StatusForbidden, "BLOCKED_BY_RECIPIENT")
	if f.scalarInt("SELECT count(*) FROM newim.im_conversations") != conversationCount ||
		f.scalarInt("SELECT count(*) FROM newim.im_conversation_members") != memberCount {
		t.Fatal("blocked direct creation wrote a conversation or membership")
	}
	directID := expectDirectConversation(t, privateRequest(t, server, createDirectRoute, alice.UserID, bob))
	if got := expectDirectConversation(t, privateRequest(t, server, createDirectRoute, bob.UserID, alice)); got != directID {
		t.Fatalf("direct lookup while blocked=%s want %s", got, directID)
	}
	assertDirectMembers(t, f, directID, alice.UserID, bob.UserID)
	assertSendSucceeds(t, server, bob, directID, "private_reverse_allowed")

	for i, name := range []string{"private_blocked_one", "private_blocked_two"} {
		before := privateSendState(t, f, directID)
		denied := sendPrivateMessage(t, server, alice, textBody(t, name, directID, fmt.Sprint("private_rejected_text_", i)))
		assertError(t, denied, http.StatusForbidden, "BLOCKED_BY_RECIPIENT")
		if denied.headers.Get("Retry-After") != "" {
			t.Fatal("permanent block carries a retry header")
		}
		if got := privateSendState(t, f, directID); got != before {
			t.Fatalf("blocked send changed durable state: before=%+v after=%+v", before, got)
		}
	}
	assertError(t, sendPrivateMessage(t, server, other, textBody(t, "private_outsider_send", directID, "unauthorized")),
		http.StatusForbidden, "SEND_UNAUTHORIZED")

	groupID := f.seedConversation("private_two_member_group", alice.UserID, bob.UserID)
	f.exec("UPDATE newim.im_conversations SET kind='group' WHERE conversation_id=$1", groupID)
	assertSendSucceeds(t, server, alice, groupID, "private_group_allowed")

	expectPolicyNoContent(t, privateRequest(t, server, unblockPeerRoute, alice.UserID, bob))
	expectPolicyNoContent(t, privateRequest(t, server, unblockPeerRoute, alice.UserID, bob))
	committed := textBody(t, "private_committed_client", directID, "private_history_text")
	first := sendPrivateMessage(t, server, alice, committed)
	ack := expectPrivateACK(t, first)
	expectPolicyNoContent(t, privateRequest(t, server, blockPeerRoute, alice.UserID, bob))
	before := privateSendState(t, f, directID)

	replay := sendPrivateMessage(t, server, alice, committed)
	expectPrivateACK(t, replay)
	if !bytes.Equal(replay.body, first.body) {
		t.Fatalf("blocked account lost its committed ACK: before=%q after=%q", first.body, replay.body)
	}
	assertError(t, sendPrivateMessage(t, server, alice, textBody(t, "private_committed_client", directID, "different intent")),
		http.StatusConflict, "SEND_ID_CONFLICT")
	f.removeMembership(directID, alice.UserID)
	assertError(t, sendPrivateMessage(t, server, alice, committed), http.StatusForbidden, "SEND_UNAUTHORIZED")
	f.exec("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", directID, alice.UserID)
	if got := privateSendState(t, f, directID); got != before {
		t.Fatalf("replay or conflict changed durable state: before=%+v after=%+v", before, got)
	}
	if payload := f.scalarString("SELECT convert_from(payload_bytes,'UTF8') FROM newim.im_messages WHERE server_msg_id=$1", ack.ServerMsgID); payload != `{"text":"private_history_text"}` {
		t.Fatal("block did not preserve stored message history")
	}

	expectPolicyNoContent(t, privateRequest(t, server, blockPeerRoute, bob.UserID, alice))
	assertError(t, sendPrivateMessage(t, server, bob, textBody(t, "private_reverse_blocked", directID, "blocked in reverse")),
		http.StatusForbidden, "BLOCKED_BY_RECIPIENT")
	expectPolicyNoContent(t, privateRequest(t, server, unblockPeerRoute, bob.UserID, alice))
	assertSendSucceeds(t, server, bob, directID, "private_reverse_unblocked")
	assertError(t, sendPrivateMessage(t, server, alice, textBody(t, "private_block_still_active", directID, "still blocked")),
		http.StatusForbidden, "BLOCKED_BY_RECIPIENT")

	assertPrivatePolicyMetrics(t, server, 4, []string{
		alice.RawToken, bob.RawToken, other.RawToken, alice.UserID, bob.UserID, other.UserID, directID,
		ack.ServerMsgID, "private_committed_client", "private_rejected_text", "private_history_text",
	})
}

func testPrivatePolicyConcurrency(t *testing.T) {
	f := openFixture(t)
	alice := f.seedIdentity("private_race_alice", seedOptions{})
	bob := f.seedIdentity("private_race_bob", seedOptions{})
	first := startMessageServer(t, privatePolicyEnv())
	defer first.stop(t)
	first.waitReady(t)
	second := startMessageServer(t, privatePolicyEnv())
	defer second.stop(t)
	second.waitReady(t)

	const createRequests = 8
	start := make(chan struct{})
	created := make(chan httpResult, createRequests)
	for i := 0; i < createRequests; i++ {
		server, actor, peer := first, alice, bob
		if i%2 != 0 {
			server, actor, peer = second, bob, alice
		}
		go func() {
			<-start
			created <- doRequestErr(http.MethodPost, privateURL(server, createDirectRoute), privatePeerBody(peer.UserID), actor.RawToken, nil)
		}()
	}
	close(start)
	id := expectDirectConversation(t, <-created)
	for i := 1; i < createRequests; i++ {
		if got := expectDirectConversation(t, <-created); got != id {
			t.Fatalf("concurrent callers received different IDs: %s and %s", id, got)
		}
	}
	assertDirectMembers(t, f, id, alice.UserID, bob.UserID)

	// Hold back a committed send while it owns the pair lock, then queue block.
	tableLock := privateSQLTransaction(t, f)
	_, err := tableLock.Exec(ctx, "LOCK TABLE newim.im_outbox_events IN SHARE MODE")
	must(t, err)
	sentFirst := privateAsync(first, "/api/v1/messages", textBody(t, "private_send_first", id, "send wins"), alice)
	waitBackendPID(t, f, "newim-message-http", "newim.persist_message", 2*time.Second)
	blockedSecond := privateAsync(second, blockPeerRoute, privatePeerBody(alice.UserID), bob)
	waitBackendPID(t, f, "newim-message-http", "pg_advisory_xact_lock", 2*time.Second)
	must(t, tableLock.Rollback(ctx))
	firstACK := expectPrivateACK(t, <-sentFirst)
	expectPolicyNoContent(t, <-blockedSecond)
	if got := f.scalarInt("SELECT count(*) FROM newim.im_outbox_events WHERE server_msg_id=$1", firstACK.ServerMsgID); got != 1 {
		t.Fatalf("send ordered before block has outbox count=%d", got)
	}
	expectPolicyNoContent(t, privateRequest(t, second, unblockPeerRoute, alice.UserID, bob))

	// A provisional conflicting row stalls the block INSERT after it owns the
	// pair lock. Rolling back lets the HTTP block commit before the queued send.
	before := privateSendState(t, f, id)
	rowLock := privateSQLTransaction(t, f)
	_, err = rowLock.Exec(ctx, "INSERT INTO newim.im_blocks(user_id,blocked_user_id) VALUES($1,$2)", bob.UserID, alice.UserID)
	must(t, err)
	blockedFirst := privateAsync(second, blockPeerRoute, privatePeerBody(alice.UserID), bob)
	waitBackendPID(t, f, "newim-message-http", "INSERT INTO newim.im_blocks", 2*time.Second)
	sentSecond := privateAsync(first, "/api/v1/messages", textBody(t, "private_block_first", id, "block wins"), alice)
	waitBackendPID(t, f, "newim-message-http", "pg_advisory_xact_lock", 2*time.Second)
	must(t, rowLock.Rollback(ctx))
	expectPolicyNoContent(t, <-blockedFirst)
	assertError(t, <-sentSecond, http.StatusForbidden, "BLOCKED_BY_RECIPIENT")
	if got := privateSendState(t, f, id); got != before {
		t.Fatalf("send ordered after block changed durable state: before=%+v after=%+v", before, got)
	}
}

func privatePolicyEnv() map[string]string {
	env := validMessageEnv()
	env["NEWIM_PRIVATE_MESSAGE_POLICY"] = "1"
	return env
}

func expectPrivateStartupRefused(t *testing.T, env map[string]string) {
	t.Helper()
	api, ops := twoFreeAddrs(t)
	result := runServerExpectExit(t, []string{"--api-addr", api, "--ops-addr", ops}, env, 8*time.Second)
	if result.code == 0 || !strings.Contains(result.stderr, "SERVER_INVALID_MESSAGE_CONFIG") {
		t.Fatalf("invalid policy startup code=%d stderr=%q", result.code, result.stderr)
	}
	assertPortReusable(t, api)
	assertPortReusable(t, ops)
}

func privateURL(server *runningServer, route string) string { return "http://" + server.api + route }

func privatePeerBody(peer string) []byte { return []byte(fmt.Sprintf(`{"peerUserId":%q}`, peer)) }

func privateRequest(t *testing.T, server *runningServer, route, peer string, actor seedIdentity) httpResult {
	t.Helper()
	return doRequest(t, http.MethodPost, privateURL(server, route), privatePeerBody(peer), actor.RawToken, nil)
}

func sendPrivateMessage(t *testing.T, server *runningServer, actor seedIdentity, body []byte) httpResult {
	t.Helper()
	return doRequest(t, http.MethodPost, messageURL(server), body, actor.RawToken, nil)
}

func privateAsync(server *runningServer, route string, body []byte, actor seedIdentity) <-chan httpResult {
	response := make(chan httpResult, 1)
	go func() {
		response <- doRequestErr(http.MethodPost, privateURL(server, route), body, actor.RawToken, nil)
	}()
	return response
}

func expectDirectConversation(t *testing.T, result httpResult) string {
	t.Helper()
	if result.err != nil || result.status != http.StatusOK {
		t.Fatalf("direct conversation status=%d err=%v body=%q", result.status, result.err, result.body)
	}
	requireCommonHeaders(t, result.headers)
	var body struct {
		ConversationID string `json:"conversationId"`
	}
	must(t, json.Unmarshal(result.body, &body))
	if body.ConversationID == "" {
		t.Fatal("direct conversation response omitted ID")
	}
	return body.ConversationID
}

func expectPolicyNoContent(t *testing.T, result httpResult) {
	t.Helper()
	if result.err != nil || result.status != http.StatusNoContent || len(result.body) != 0 {
		t.Fatalf("relationship status=%d err=%v body=%q", result.status, result.err, result.body)
	}
	if result.headers.Get("Cache-Control") != "no-store" || result.headers.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("relationship response omitted standard security headers")
	}
}

func expectPrivateACK(t *testing.T, result httpResult) protocol.Ack {
	t.Helper()
	if result.err != nil || result.status != http.StatusOK {
		t.Fatalf("send status=%d err=%v body=%q", result.status, result.err, result.body)
	}
	requireCommonHeaders(t, result.headers)
	return decodeACK(t, result.body)
}

func assertNoDirectPair(t *testing.T, f *fixture, first, second string) {
	t.Helper()
	if count := f.scalarInt(`SELECT count(*) FROM newim.im_conversations WHERE kind='direct'
		AND direct_user_low=LEAST($1::newim.identifier,$2::newim.identifier)
		AND direct_user_high=GREATEST($1::newim.identifier,$2::newim.identifier)`, first, second); count != 0 {
		t.Fatalf("unexpected registered direct pair count=%d", count)
	}
}

func assertDirectMembers(t *testing.T, f *fixture, conversationID, left, right string) {
	t.Helper()
	var kind, low, high string
	must(t, f.db.QueryRow(ctx, `SELECT kind,direct_user_low,direct_user_high
		FROM newim.im_conversations WHERE conversation_id=$1`, conversationID).Scan(&kind, &low, &high))
	if kind != "direct" || low != left || high != right {
		t.Fatalf("direct conversation classification=%s,%s,%s", kind, low, high)
	}
	if count := f.scalarInt(`SELECT count(*) FROM newim.im_conversations
		WHERE kind='direct' AND direct_user_low=$1 AND direct_user_high=$2`, left, right); count != 1 {
		t.Fatalf("registered direct pair count=%d want 1", count)
	}
	if got := f.scalarString(`SELECT string_agg(user_id,',' ORDER BY user_id)
		FROM newim.im_conversation_members WHERE conversation_id=$1`, conversationID); got != left+","+right {
		t.Fatalf("direct conversation membership=%q", got)
	}
}

type privateDurableState struct {
	sequence int64
	head     string
	messages int64
	outbox   int64
}

func privateSendState(t *testing.T, f *fixture, conversationID string) privateDurableState {
	t.Helper()
	var state privateDurableState
	must(t, f.db.QueryRow(ctx, `SELECT last_seq,COALESCE(latest_server_msg_id,''),
		(SELECT count(*) FROM newim.im_messages WHERE conversation_id=$1),
		(SELECT count(*) FROM newim.im_outbox_events) FROM newim.im_conversations WHERE conversation_id=$1`, conversationID).
		Scan(&state.sequence, &state.head, &state.messages, &state.outbox))
	return state
}

func privateSQLTransaction(t *testing.T, f *fixture) pgx.Tx {
	t.Helper()
	conn, err := pgx.Connect(ctx, f.dsn)
	must(t, err)
	t.Cleanup(func() { _ = conn.Close(ctx) })
	tx, err := conn.Begin(ctx)
	must(t, err)
	t.Cleanup(func() { _ = tx.Rollback(ctx) })
	return tx
}

func assertPrivatePolicyMetrics(t *testing.T, server *runningServer, want int64, forbidden []string) {
	t.Helper()
	const series = `newim_message_lifecycle_errors_total{code="BLOCKED_BY_RECIPIENT",component="send"} `
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		metrics := doRequest(t, http.MethodGet, metricsURL(server), nil, "", nil)
		if metrics.status != http.StatusOK {
			t.Fatalf("metrics returned %d", metrics.status)
		}
		samples := 0
		var count int64
		for _, line := range strings.Split(string(metrics.body), "\n") {
			if value, ok := strings.CutPrefix(line, series); ok {
				samples++
				var err error
				count, err = strconv.ParseInt(value, 10, 64)
				must(t, err)
			}
		}
		if samples > 1 || count > want {
			t.Fatalf("blocked send metric has samples=%d count=%d, want 1 sample counting %d", samples, count, want)
		}
		if count < want {
			time.Sleep(25 * time.Millisecond)
			continue
		}
		for _, value := range forbidden {
			if strings.Contains(string(metrics.body), value) || strings.Contains(server.logs(), value) {
				t.Fatal("private policy exposed an identifier, credential or payload in observability")
			}
		}
		return
	}
	t.Fatal("metrics omitted bounded private-message rejection code")
}
