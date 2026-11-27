//go:build integration

package tlsmessage_test

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/friend"
)

func (f *fixture) strangerGateway(overrides map[string]string) *process {
	f.t.Helper()
	settings := map[string]string{
		"NEWIM_STRANGER_MESSAGE_POLICY": "1",
		"NEWIM_FRIEND_HTTP":             "1",
		"NEWIM_GROUP_HTTP":              "1",
	}
	for key, value := range overrides {
		settings[key] = value
	}
	return f.gateway("1", settings)
}

func (f *fixture) strangerLoopback() *process {
	f.t.Helper()
	api, err := net.Listen("tcp4", "127.0.0.1:0")
	must(f.t, err)
	defer api.Close()
	ops, err := net.Listen("tcp4", "127.0.0.1:0")
	must(f.t, err)
	defer ops.Close()
	apiAddr, opsAddr := api.Addr().String(), ops.Addr().String()
	must(f.t, api.Close())
	must(f.t, ops.Close())

	issuerFile := filepath.Join(f.t.TempDir(), "issuer-key")
	var key [32]byte
	_, err = rand.Read(key[:])
	must(f.t, err)
	must(f.t, os.WriteFile(issuerFile, []byte(hex.EncodeToString(key[:])), 0o600))
	clear(key[:])
	settings := map[string]string{
		"NEWIM_MESSAGE_HTTP":            "1",
		"NEWIM_PRIVATE_MESSAGE_POLICY":  "1",
		"NEWIM_STRANGER_MESSAGE_POLICY": "1",
		"NEWIM_AUTH_ISSUER_TOKEN_FILE":  issuerFile,
		"NEWIM_AUTH_ISSUER_TTL_SECONDS": "120",
	}
	p := startProcess(f.t, os.Getenv("NEWIM_SERVER_BINARY"), "", settings, "--api-addr", apiAddr, "--ops-addr", opsAddr)
	p.url = "http://" + apiAddr
	waitForStartup(f.t, p, func() bool {
		r := f.call(p, http.MethodGet, "/api/v1/health", "", nil, nil)
		return r.err == nil && r.status == http.StatusOK
	})
	return p
}

func callSend(f *fixture, endpoint *process, token string, wire []byte) response {
	return f.call(endpoint, http.MethodPost, sendPath, token, wire, nil)
}

func sendACK(t *testing.T, endpoint *process, reply response) protocol.Ack {
	t.Helper()
	if strings.HasPrefix(endpoint.url, "https://") {
		return expectACK(t, reply)
	}
	if reply.err != nil || reply.status != http.StatusOK || reply.verified ||
		reply.headers.Get("Cache-Control") != "no-store" || reply.headers.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("trusted loopback send status=%d err=%v", reply.status, reply.err)
	}
	frame, err := protocol.DecodeServerFrame(reply.body)
	must(t, err)
	if frame.Ack == nil || frame.Error != nil || frame.Message != nil {
		t.Fatal("trusted loopback send did not return a committed ACK")
	}
	return *frame.Ack
}

func sendFixtureJSON(t *testing.T, body any) []byte {
	t.Helper()
	wire, err := json.Marshal(body)
	must(t, err)
	return wire
}

func strangerFriendResult[T any](t *testing.T, reply response, key string) T {
	t.Helper()
	if reply.err != nil || reply.status != http.StatusOK || !reply.verified ||
		reply.headers.Get("Cache-Control") != "no-store" {
		t.Fatalf("friend API status=%d TLS verified=%t err=%v", reply.status, reply.verified, reply.err)
	}
	var envelope map[string]json.RawMessage
	must(t, json.Unmarshal(reply.body, &envelope))
	if len(envelope) != 2 || string(envelope["version"]) != "1" || envelope[key] == nil || string(envelope[key]) == "null" {
		t.Fatalf("friend API did not return a versioned %s result", key)
	}
	var result T
	must(t, json.Unmarshal(envelope[key], &result))
	return result
}

func requireStrangerLimit(t *testing.T, reply response) {
	t.Helper()
	reply.requireError(t, http.StatusForbidden, "STRANGER_MESSAGE_LIMIT")
	if reply.headers.Get("Retry-After") != "" {
		t.Fatal("waiting for the peer's reply returned a retry hint")
	}
}

func requireSameSendACK(t *testing.T, endpoint *process, reply response, original []byte) {
	t.Helper()
	sendACK(t, endpoint, reply)
	if !bytes.Equal(reply.body, original) {
		t.Fatal("same-intent replay changed the original ACK")
	}
}

func TestStrangerSendCheck(t *testing.T) {
	t.Run("first-and-peer-reply-across-composers", testStrangerFirstAndReply)
	t.Run("current-mutual-friends-and-one-way-removal", testStrangerFriends)
	t.Run("existing-messages-and-managed-group", testStrangerExistingMessages)
}

func testStrangerFirstAndReply(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(16)
	gateway, loopback := f.strangerGateway(nil), f.strangerLoopback()
	const aliceID = "stranger_first_alice"
	const bobID = "stranger_first_bob"
	alice, desktop := f.admit(aliceID, "mobile"), f.admit(aliceID, "desktop")
	bob := f.admit(bobID, "mobile")
	id := createDirect(t, f, gateway, alice, bobID)
	reverse := f.call(loopback, http.MethodPost, directPath, bob, peerBody(t, aliceID), nil)
	if reverse.err != nil || reverse.verified || reverse.status != http.StatusOK {
		t.Fatalf("trusted loopback direct open status=%d err=%v", reverse.status, reverse.err)
	}
	var opened map[string]string
	must(t, json.Unmarshal(reverse.body, &opened))
	if opened["conversationId"] != id {
		t.Fatal("both composers did not resolve the same account pair")
	}

	const firstText = "first committed private message"
	firstWire := textBody(t, "stranger_first", id, firstText)
	ws := subscribe(t, f, gateway, bob, id, 0)
	first := callSend(f, gateway, alice, firstWire)
	a := expectACK(t, first)
	requireCommitted(t, f, a, aliceID, id, "stranger_first", firstText, 1)
	requireDelivered(t, f, ws, a, firstText)
	before := state(t, f, id)

	secondWire := textBody(t, "stranger_second", id, "available after the peer replies")
	requireStrangerLimit(t, callSend(f, loopback, alice, secondWire))
	requireStrangerLimit(t, callSend(f, gateway, desktop, secondWire))
	requireSameSendACK(t, loopback, callSend(f, loopback, alice, firstWire), first.body)
	for _, endpoint := range []*process{gateway, loopback} {
		callSend(f, endpoint, alice, textBody(t, "stranger_first", id, "changed text")).
			requireError(t, http.StatusConflict, "SEND_ID_CONFLICT")
	}
	if state(t, f, id) != before {
		t.Fatal("denied sends or receipt replay changed durable state")
	}

	const replyText = "recipient reply through the trusted composer"
	reply := callSend(f, loopback, bob, textBody(t, "stranger_reply", id, replyText))
	b := sendACK(t, loopback, reply)
	requireCommitted(t, f, b, bobID, id, "stranger_reply", replyText, 2)
	requireDelivered(t, f, ws, b, replyText)
	next := expectACK(t, callSend(f, gateway, desktop, secondWire))
	requireCommitted(t, f, next, aliceID, id, "stranger_second", "available after the peer replies", 3)
	gateway.requireRedacted(t, alice, desktop, bob, firstText, testDSN)
	loopback.requireRedacted(t, alice, desktop, bob, replyText, testDSN)
}

func testStrangerFriends(t *testing.T) {
	f := openFixture(t)
	gateway, loopback := f.strangerGateway(nil), f.strangerLoopback()
	const aliceID = "stranger_friend_alice"
	const bobID = "stranger_friend_bob"
	alice, bob := f.admit(aliceID, "mobile"), f.admit(bobID, "mobile")
	id := createDirect(t, f, gateway, alice, bobID)

	one := textBody(t, "friend_first", id, "stranger starts a conversation")
	two := textBody(t, "friend_second", id, "allowed for mutual friends")
	expectACK(t, callSend(f, gateway, alice, one))
	requireStrangerLimit(t, callSend(f, loopback, alice, two))
	application := friend.ApplyRequest{RequestID: "stranger_friend_apply", PeerID: bobID, Message: "hello"}
	applied := f.call(gateway, http.MethodPost, "/api/v1/friends/applications", alice, sendFixtureJSON(t, application), nil)
	if strangerFriendResult[friend.Application](t, applied, "application").State != friend.StatePending {
		t.Fatal("the friend application did not enter the pending state")
	}
	requireStrangerLimit(t, callSend(f, gateway, alice, two))
	decision := friend.ResponseRequest{ApplicantID: aliceID, RequestID: application.RequestID, Decision: "accept"}
	accepted := f.call(gateway, http.MethodPost, "/api/v1/friends/applications/respond", bob, sendFixtureJSON(t, decision), nil)
	if strangerFriendResult[friend.Application](t, accepted, "application").State != friend.StateAccepted {
		t.Fatal("the friend application did not become mutual friendship")
	}
	b := sendACK(t, loopback, callSend(f, loopback, alice, two))
	requireCommitted(t, f, b, aliceID, id, "friend_second", "allowed for mutual friends", 2)
	a := expectACK(t, callSend(f, gateway, alice, textBody(t, "friend_third", id, "no reply needed yet")))
	requireCommitted(t, f, a, aliceID, id, "friend_third", "no reply needed yet", 3)

	removed := f.call(gateway, http.MethodPost, "/api/v1/friends/remove", bob,
		sendFixtureJSON(t, map[string]string{"peerId": aliceID}), nil)
	if !strangerFriendResult[bool](t, removed, "removed") {
		t.Fatal("friend removal did not change the current relationship")
	}
	var callerEdge, peerEdge bool
	must(t, f.db.QueryRow(f.ctx, `SELECT
		EXISTS(SELECT 1 FROM newim.im_friend_edges WHERE owner_id=$1 AND peer_id=$2),
		EXISTS(SELECT 1 FROM newim.im_friend_edges WHERE owner_id=$2 AND peer_id=$1)`, aliceID, bobID).
		Scan(&callerEdge, &peerEdge))
	if !callerEdge || peerEdge {
		t.Fatal("fixture did not preserve just the sender's outgoing friend edge")
	}
	before := state(t, f, id)
	requireStrangerLimit(t, callSend(f, gateway, alice, textBody(t, "one_way", id, "one-way friendship only")))
	requireStrangerLimit(t, callSend(f, loopback, alice, textBody(t, "one_way", id, "one-way friendship only")))
	if state(t, f, id) != before {
		t.Fatal("one-way friendship denial changed durable state")
	}
}

func testStrangerExistingMessages(t *testing.T) {
	f := openFixture(t)
	disabled := f.gateway("1", nil)
	const aliceID = "stranger_legacy_alice"
	const bobID = "stranger_legacy_bob"
	alice, bob := f.admit(aliceID, "mobile"), f.admit(bobID, "mobile")
	id := createDirect(t, f, disabled, alice, bobID)
	expectACK(t, callSend(f, disabled, alice, textBody(t, "legacy_first", id, "before enabling the policy")))
	expectACK(t, callSend(f, disabled, alice, textBody(t, "legacy_second", id, "existing sender history")))

	gateway, loopback := f.strangerGateway(nil), f.strangerLoopback()
	before := state(t, f, id)
	newWire := textBody(t, "legacy_followup", id, "waiting for the peer")
	requireStrangerLimit(t, callSend(f, loopback, alice, newWire))
	if state(t, f, id) != before {
		t.Fatal("existing sender history did not preserve the waiting limit")
	}
	for _, client := range []string{"legacy_peer_one", "legacy_peer_two"} {
		expectACK(t, callSend(f, gateway, bob, textBody(t, client, id, "existing peer history permits replies")))
	}
	next := sendACK(t, loopback, callSend(f, loopback, alice, newWire))
	requireCommitted(t, f, next, aliceID, id, "legacy_followup", "waiting for the peer", 5)

	const group = "stranger_managed_group"
	requireStatus(t, f.call(gateway, http.MethodPost, "/api/v1/groups/create", alice, sendFixtureJSON(t, map[string]any{
		"groupId": group, "profile": map[string]string{"name": "Stranger-message fixture"}, "memberIds": []string{bobID},
	}), nil), http.StatusOK)
	expectACK(t, callSend(f, gateway, alice, textBody(t, "group_first", group, "first group message")))
	second := expectACK(t, callSend(f, gateway, alice, textBody(t, "group_second", group, "second group message")))
	requireCommitted(t, f, second, aliceID, group, "group_second", "second group message", 2)
}

func TestStrangerSendSecurity(t *testing.T) {
	t.Run("block-precedence-and-replay", testStrangerBlock)
	t.Run("account-and-device-budget", testStrangerDeviceBudget)
}

func testStrangerBlock(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(16)
	gateway, loopback := f.strangerGateway(nil), f.strangerLoopback()
	const aliceID = "stranger_block_alice"
	const bobID = "stranger_block_bob"
	alice, bob := f.admit(aliceID, "mobile"), f.admit(bobID, "mobile")
	outsider := f.admit("stranger_block_outside", "mobile")
	id := createDirect(t, f, gateway, alice, bobID)
	firstWire := textBody(t, "block_prior", id, "committed before blocking")
	first := callSend(f, loopback, alice, firstWire)
	a := sendACK(t, loopback, first)
	requireCommitted(t, f, a, aliceID, id, "block_prior", "committed before blocking", 1)
	before := state(t, f, id)
	fresh := textBody(t, "block_pending", id, "uncommitted new message")
	requireStrangerLimit(t, callSend(f, gateway, alice, fresh))
	setBlock(t, f, gateway, blockPath, bob, aliceID)
	for _, endpoint := range []*process{gateway, loopback} {
		denied := callSend(f, endpoint, alice, fresh)
		denied.requireError(t, http.StatusForbidden, "BLOCKED_BY_RECIPIENT")
		if denied.headers.Get("Retry-After") != "" {
			t.Fatal("permanent block returned a retry hint")
		}
		requireSameSendACK(t, endpoint, callSend(f, endpoint, alice, firstWire), first.body)
		callSend(f, endpoint, alice, textBody(t, "block_prior", id, "changed already committed content")).
			requireError(t, http.StatusConflict, "SEND_ID_CONFLICT")
		callSend(f, endpoint, outsider, fresh).requireError(t, http.StatusForbidden, "SEND_UNAUTHORIZED")
		callSend(f, endpoint, "", fresh).requireError(t, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	}
	setBlock(t, f, gateway, unblockPath, bob, aliceID)
	requireStrangerLimit(t, callSend(f, loopback, alice, fresh))
	if state(t, f, id) != before {
		t.Fatal("blocked, unknown and repeated attempts changed durable chat state")
	}
	gateway.requireRedacted(t, alice, bob, outsider, testDSN, "uncommitted new message")
	loopback.requireRedacted(t, alice, bob, outsider, testDSN, "committed before blocking")
}

func testStrangerDeviceBudget(t *testing.T) {
	f := openFixture(t)
	unlimited := f.strangerGateway(nil)
	account := f.strangerGateway(map[string]string{
		"NEWIM_TLS_MESSAGE_ACCOUNT_ATTEMPTS_PER_MINUTE": "3",
		"NEWIM_TLS_MESSAGE_DEVICE_ATTEMPTS_PER_MINUTE":  "1",
	})
	address := f.strangerGateway(map[string]string{
		"NEWIM_TLS_MESSAGE_IP_ATTEMPTS_PER_MINUTE":     "2",
		"NEWIM_TLS_MESSAGE_DEVICE_ATTEMPTS_PER_MINUTE": "1",
	})
	const aliceID = "stranger_budget_alice"
	const bobID = "stranger_budget_bob"
	mobile, desktop := f.admit(aliceID, "mobile"), f.admit(aliceID, "desktop")
	bob := f.admit(bobID, "mobile")
	id := createDirect(t, f, unlimited, mobile, bobID)
	expectACK(t, callSend(f, unlimited, bob, textBody(t, "budget_seed", id, "permits a response")))

	waitRateWindow()
	first := expectACK(t, callSend(f, account, mobile, textBody(t, "budget_mobile", id, "mobile has one slot")))
	requireCommitted(t, f, first, aliceID, id, "budget_mobile", "mobile has one slot", 2)
	before := state(t, f, id)
	for _, client := range []string{"budget_mobile_denied_one", "budget_mobile_denied_two"} {
		callSend(f, account, mobile, textBody(t, client, id, "denied by device budget")).
			requireError(t, http.StatusTooManyRequests, "HTTP_RATE_LIMITED")
	}
	if state(t, f, id) != before {
		t.Fatal("rate-limited sends changed durable chat state")
	}
	second := expectACK(t, callSend(f, account, desktop, textBody(t, "budget_desktop", id, "other device retains its slot")))
	requireCommitted(t, f, second, aliceID, id, "budget_desktop", "other device retains its slot", 3)
	if createDirect(t, f, account, mobile, bobID) != id {
		t.Fatal("device budget changed access to relationship routes")
	}
	f.call(account, http.MethodPost, directPath, desktop, peerBody(t, bobID), nil).
		requireError(t, http.StatusTooManyRequests, "HTTP_RATE_LIMITED")
	other := expectACK(t, callSend(f, account, bob, textBody(t, "budget_other_account", id, "same device ID on another account")))
	requireCommitted(t, f, other, bobID, id, "budget_other_account", "same device ID on another account", 4)
	requireHandshake(t, f, account, mobile, http.StatusSwitchingProtocols)

	waitRateWindow()
	expectACK(t, callSend(f, address, mobile, textBody(t, "budget_ip_one", id, "uses the first address attempt")))
	before = state(t, f, id)
	callSend(f, address, mobile, textBody(t, "budget_ip_two", id, "device rejection still uses an address attempt")).
		requireError(t, http.StatusTooManyRequests, "HTTP_RATE_LIMITED")
	callSend(f, address, bob, textBody(t, "budget_ip_three", id, "address budget has been exhausted")).
		requireError(t, http.StatusTooManyRequests, "HTTP_RATE_LIMITED")
	if state(t, f, id) != before {
		t.Fatal("limited requests changed durable chat state")
	}
}

const (
	strangerRecoveryFile  = "/tmp/newim-stranger-send-recovery.json"
	strangerRecoveryAlice = "stranger_recovery_alice"
	strangerRecoveryBob   = "stranger_recovery_bob"
)

type strangerIntent struct {
	ClientID string
	Text     string
}

type strangerRecovery struct {
	Writer       string
	OtherDevice  string
	Peer         string
	Conversation string
	Winner       strangerIntent
	Loser        strangerIntent
	ACK          []byte
	Postmaster   time.Time
}

func TestStrangerSendRecovery(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(16)
	gateway, loopback := f.strangerGateway(nil), f.strangerLoopback()
	switch os.Getenv("NEWIM_TLS_MESSAGE_PHASE") {
	case "prepare":
		prepareStrangerRestart(t, f, gateway, loopback)
	case "restart":
		verifyStrangerRestart(t, f, gateway, loopback)
	default:
		t.Fatal("NEWIM_TLS_MESSAGE_PHASE must be prepare or restart")
	}
}

func prepareStrangerRestart(t *testing.T, f *fixture, gateway, loopback *process) {
	t.Helper()
	writer := f.admit(strangerRecoveryAlice, "phone")
	device := f.admit(strangerRecoveryAlice, "desktop")
	peer := f.admit(strangerRecoveryBob, "phone")
	id := createDirect(t, f, gateway, writer, strangerRecoveryBob)
	attempts := []struct {
		endpoint *process
		token    string
		intent   strangerIntent
	}{
		{gateway, writer, strangerIntent{"restart_tls", "first intent submitted through TLS"}},
		{loopback, device, strangerIntent{"restart_loopback", "first intent submitted through the trusted composer"}},
	}
	type result struct {
		index int
		reply response
	}
	start := make(chan struct{})
	results := make(chan result, len(attempts))
	for index, attempt := range attempts {
		body := textBody(t, attempt.intent.ClientID, id, attempt.intent.Text)
		go func() {
			<-start
			results <- result{index, callSend(f, attempt.endpoint, attempt.token, body)}
		}()
	}
	close(start)

	var saved strangerRecovery
	saved.Writer, saved.OtherDevice, saved.Peer, saved.Conversation = writer, device, peer, id
	for range attempts {
		got := <-results
		attempt := attempts[got.index]
		if got.reply.status == http.StatusOK {
			ack := sendACK(t, attempt.endpoint, got.reply)
			requireCommitted(t, f, ack, strangerRecoveryAlice, id, attempt.intent.ClientID, attempt.intent.Text, 1)
			saved.Winner, saved.ACK = attempt.intent, got.reply.body
		} else {
			requireStrangerLimit(t, got.reply)
			saved.Loser = attempt.intent
		}
	}
	if saved.Winner.ClientID == "" || saved.Loser.ClientID == "" {
		t.Fatal("concurrent first sends did not produce one committed ACK and one waiting rejection")
	}
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&saved.Postmaster))
	must(t, os.WriteFile(strangerRecoveryFile, sendFixtureJSON(t, saved), 0o600))
}

func verifyStrangerRestart(t *testing.T, f *fixture, gateway, loopback *process) {
	t.Helper()
	raw, err := os.ReadFile(strangerRecoveryFile)
	must(t, err)
	t.Cleanup(func() { _ = os.Remove(strangerRecoveryFile) })
	var saved strangerRecovery
	must(t, json.Unmarshal(raw, &saved))
	var started time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&started))
	if saved.Postmaster.IsZero() || !started.After(saved.Postmaster) {
		t.Fatal("restart did not start a new PostgreSQL postmaster")
	}
	id := saved.Conversation
	winner := textBody(t, saved.Winner.ClientID, id, saved.Winner.Text)
	loser := textBody(t, saved.Loser.ClientID, id, saved.Loser.Text)
	before := state(t, f, id)
	for _, endpoint := range []*process{gateway, loopback} {
		requireSameSendACK(t, endpoint, callSend(f, endpoint, saved.OtherDevice, winner), saved.ACK)
		requireStrangerLimit(t, callSend(f, endpoint, saved.Writer, loser))
	}
	if before.lastSeq != 1 || before.messages != 1 || before.outbox != 1 || state(t, f, id) != before {
		t.Fatal("first message or waiting state was lost or changed after restart")
	}

	ws := subscribe(t, f, gateway, saved.Peer, id, 0)
	winnerACK := expectACK(t, callSend(f, gateway, saved.Writer, winner))
	requireDelivered(t, f, ws, winnerACK, saved.Winner.Text)
	reply := sendACK(t, loopback, callSend(f, loopback, saved.Peer,
		textBody(t, "restart_peer_response", id, "reply after PostgreSQL restart")))
	requireCommitted(t, f, reply, strangerRecoveryBob, id, "restart_peer_response", "reply after PostgreSQL restart", 2)
	accepted := expectACK(t, callSend(f, gateway, saved.OtherDevice, loser))
	requireCommitted(t, f, accepted, strangerRecoveryAlice, id, saved.Loser.ClientID, saved.Loser.Text, 3)
	gateway.requireRedacted(t, saved.Writer, saved.OtherDevice, saved.Peer, testDSN, saved.Winner.Text)
	loopback.requireRedacted(t, saved.Writer, saved.OtherDevice, saved.Peer, testDSN, saved.Loser.Text)
}
