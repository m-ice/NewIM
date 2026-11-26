//go:build integration

package gateway_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestGatewayCheck(t *testing.T) {
	t.Run("two-process-committed-ordering-and-drain", testCommittedOrdering)
	t.Run("committed-device-eviction", testDeviceEviction)
	t.Run("verified-heartbeat-persists-activity", testHeartbeatActivity)
}

func testCommittedOrdering(t *testing.T) {
	f := openFixture(t)
	const (
		sender       = "gateway_order_sender"
		receiver     = "gateway_order_receiver"
		conversation = "gateway_order_conversation"
		firstText    = "first gateway message"
		waitingText  = "gateway message held until commit"
	)
	left := f.admit(sender, "device_a", 8)
	right := f.admit(receiver, "device_b", 8)
	f.conversation(conversation, sender, receiver)
	f.persist(nil, conversation, sender, "gateway_order_first", firstText)

	g1 := f.gateway(nil)
	g2 := f.gateway(nil)
	c1 := f.dial(g1, gatewayRoute, left.Token.RawToken(), nil, "").connected(t)
	c2 := f.dial(g2, gatewayRoute, right.Token.RawToken(), nil, "").connected(t)
	writeSubscription(t, c1, conversation, 0)
	writeSubscription(t, c2, conversation, 0)
	expectMessage(t, readFrame(c1), conversation, "gateway_order_first", sender, firstText, 1)
	expectMessage(t, readFrame(c2), conversation, "gateway_order_first", sender, firstText, 1)

	tx, err := f.db.Begin(f.ctx)
	must(t, err)
	defer func() { _ = tx.Rollback(f.ctx) }()
	f.persist(tx, conversation, sender, "gateway_order_second", waitingText)
	awaitLeft, awaitRight := readAsync(c1, ioDeadline), readAsync(c2, ioDeadline)
	// A 900 ms observation spans the 500 ms gateway polling interval.
	select {
	case <-awaitLeft:
		t.Fatal("first gateway returned before the message transaction committed")
	case <-awaitRight:
		t.Fatal("second gateway returned before the message transaction committed")
	case <-time.After(900 * time.Millisecond):
	}
	must(t, tx.Commit(f.ctx))
	expectMessage(t, <-awaitLeft, conversation, "gateway_order_second", sender, waitingText, 2)
	expectMessage(t, <-awaitRight, conversation, "gateway_order_second", sender, waitingText, 2)

	f.persist(nil, conversation, sender, "gateway_order_third", "third after commit")
	expectMessage(t, readFrame(c1), conversation, "gateway_order_third", sender, "third after commit", 3)
	expectMessage(t, readFrame(c2), conversation, "gateway_order_third", sender, "third after commit", 3)
	g1.signalDrain(t)
	expectClose(t, c1, websocket.StatusGoingAway, "server_draining")
	g1.requireExit(t)

	f.persist(nil, conversation, sender, "gateway_order_fourth", "healthy peer keeps serving")
	expectMessage(t, readFrame(c2), conversation, "gateway_order_fourth", sender, "healthy peer keeps serving", 4)
	var pending int
	must(t, f.db.QueryRow(f.ctx, `SELECT count(*) FROM newim.im_outbox_events AS e
		JOIN newim.im_messages AS m ON m.server_msg_id=e.server_msg_id
		WHERE m.conversation_id=$1 AND e.completed_at IS NULL`, conversation).Scan(&pending))
	if pending != 4 {
		t.Fatalf("gateway changed webhook outbox state: pending=%d want=4", pending)
	}
	g2.requireRedacted(t, gatewayDSN, right.Token.RawToken(), waitingText)
}

func testDeviceEviction(t *testing.T) {
	f := openFixture(t)
	const (
		sender       = "gateway_eviction_sender"
		receiver     = "gateway_eviction_receiver"
		conversation = "gateway_eviction_conversation"
	)
	f.admit(sender, "sender", 8)
	revived := f.admit(receiver, "old_device", 8)
	quiet := f.admit(receiver, "quiet_device", 8)
	f.conversation(conversation, sender, receiver)
	f.persist(nil, conversation, sender, "gateway_eviction_first", "before device eviction")
	g := f.gateway(nil)

	quietConn := f.dial(g, gatewayRoute, quiet.Token.RawToken(), nil, "").connected(t)
	writeSubscription(t, quietConn, conversation, 0)
	expectMessage(t, readFrame(quietConn), conversation, "gateway_eviction_first", sender, "before device eviction", 1)

	// Before the authorized subscription, old_device would be the victim.
	f.sql(`UPDATE newim.im_devices SET last_active_at=statement_timestamp()-interval '2 hours'
		WHERE user_id=$1 AND device_id=$2`, receiver, revived.Binding.DeviceID)
	f.sql(`UPDATE newim.im_devices SET last_active_at=statement_timestamp()-interval '1 hour'
		WHERE user_id=$1 AND device_id=$2`, receiver, quiet.Binding.DeviceID)
	revivedConn := f.dial(g, gatewayRoute, revived.Token.RawToken(), nil, "").connected(t)
	writeSubscription(t, revivedConn, conversation, 0)
	expectMessage(t, readFrame(revivedConn), conversation, "gateway_eviction_first", sender, "before device eviction", 1)
	var recent bool
	must(t, f.db.QueryRow(f.ctx, `SELECT last_active_at>statement_timestamp()-interval '2 minutes'
		FROM newim.im_devices WHERE user_id=$1 AND device_id=$2`, receiver, revived.Binding.DeviceID).Scan(&recent))
	if !recent {
		t.Fatal("authorized WSS subscription did not persist verified device activity before delivery")
	}

	next := f.admit(receiver, "new_device", 2)
	f.persist(nil, conversation, sender, "gateway_eviction_second", "after device eviction")
	expectClose(t, quietConn, websocket.StatusPolicyViolation, "device_limit")
	f.dial(g, gatewayRoute, quiet.Token.RawToken(), nil, "").rejected(t, http.StatusUnauthorized)
	expectMessage(t, readFrame(revivedConn), conversation, "gateway_eviction_second", sender, "after device eviction", 2)

	connected := f.dial(g, gatewayRoute, next.Token.RawToken(), nil, "").connected(t)
	writeSubscription(t, connected, conversation, 1)
	expectMessage(t, readFrame(connected), conversation, "gateway_eviction_second", sender, "after device eviction", 2)
	_, err := f.auth.RevokeSession(f.ctx, next.Binding)
	must(t, err)
	expectClose(t, connected, websocket.StatusPolicyViolation, "session_revoked")
	g.requireRedacted(t, gatewayDSN, quiet.Token.RawToken(), revived.Token.RawToken(), next.Token.RawToken(), "after device eviction")
}

func testHeartbeatActivity(t *testing.T) {
	f := openFixture(t)
	const conversation = "gateway_heartbeat_conversation"
	identity := f.admit("gateway_heartbeat_user", "heartbeat_device", 8)
	userID := identity.Binding.UserID
	f.conversation(conversation, userID)
	f.persist(nil, conversation, userID, "gateway_heartbeat_first", "subscribed before heartbeat")
	g := f.gateway(nil)
	receive := f.dial(g, gatewayRoute, identity.Token.RawToken(), nil, "").connected(t)
	writeSubscription(t, receive, conversation, 0)
	expectMessage(t, readFrame(receive), conversation, "gateway_heartbeat_first", userID, "subscribed before heartbeat", 1)

	// No additional message is committed until the matched Ping/Pong refreshes
	// the activity row. The running client read processes control frames.
	f.sql(`UPDATE newim.im_devices SET last_active_at=statement_timestamp()-interval '2 hours'
		WHERE user_id=$1 AND device_id=$2`, userID, identity.Binding.DeviceID)
	waitMessage := readAsync(receive, 38*time.Second)
	deadline := time.NewTimer(28 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		var recent bool
		must(t, f.db.QueryRow(f.ctx, `SELECT last_active_at>statement_timestamp()-interval '2 minutes'
			FROM newim.im_devices WHERE user_id=$1 AND device_id=$2`,
			userID, identity.Binding.DeviceID).Scan(&recent))
		if recent {
			break
		}
		select {
		case <-waitMessage:
			t.Fatal("gateway stopped before completing its heartbeat")
		case <-deadline.C:
			t.Fatal("matched heartbeat did not persist device activity")
		case <-ticker.C:
		}
	}
	f.persist(nil, conversation, userID, "gateway_heartbeat_second", "persisted after heartbeat")
	expectMessage(t, <-waitMessage, conversation, "gateway_heartbeat_second", userID, "persisted after heartbeat", 2)
}

func TestGatewaySecurity(t *testing.T) {
	t.Run("token-bound-replay-and-request-policy", testHandshakePolicy)
	t.Run("member-and-subscription-validation", testSubscriptionPolicy)
	t.Run("device-attempt-limit", testDeviceAttemptLimit)
	t.Run("direct-ip-attempt-limit", testIPAttemptLimit)
	t.Run("full-and-slow-subscriber-releases-slot", testConnectionCapacity)
	t.Run("slow-reader-releases-slot-and-keeps-healthy-peer", testSlowReader)
}

func testHandshakePolicy(t *testing.T) {
	f := openFixture(t)
	token := f.admit("gateway_replay_user", "fixture_device", 8).Token.RawToken()
	g1, g2 := f.gateway(nil), f.gateway(nil)
	f.dial(g1, gatewayRoute, "", nil, "").rejected(t, http.StatusUnauthorized)
	f.dial(g1, gatewayRoute, "invalid-fixture-token", nil, "").rejected(t, http.StatusUnauthorized)
	f.dial(g1, gatewayRoute+"?extra=1", token, nil, "").rejected(t, http.StatusBadRequest)
	f.dial(g1, gatewayRoute, token, http.Header{"Origin": {"https://newim.example.test"}}, "").rejected(t, http.StatusBadRequest)
	first := f.dial(g1, gatewayRoute, token, nil, "")
	c := first.connected(t)
	if first.key == "" {
		t.Fatal("client handshake nonce missing")
	}
	_ = c.CloseNow()
	f.dial(g2, gatewayRoute, token, nil, first.key).rejected(t, http.StatusConflict)
	_ = f.dial(g2, gatewayRoute, token, nil, "").connected(t).CloseNow()
	for _, g := range []*gatewayProcess{g1, g2} {
		g.requireRedacted(t, token, gatewayDSN, first.key, "invalid-fixture-token")
	}
}

func testSubscriptionPolicy(t *testing.T) {
	f := openFixture(t)
	const conversation = "gateway_restricted_conversation"
	member := f.admit("gateway_member_user", "fixture_device", 8)
	outsider := f.admit("gateway_outsider_user", "fixture_device", 8)
	f.conversation(conversation, member.Binding.UserID)
	g := f.gateway(nil)

	untrusted := f.dial(g, gatewayRoute, outsider.Token.RawToken(), nil, "").connected(t)
	writeSubscription(t, untrusted, conversation, 0)
	expectClose(t, untrusted, websocket.StatusPolicyViolation, "conversation_forbidden")

	invalid := f.dial(g, gatewayRoute, member.Token.RawToken(), nil, "").connected(t)
	ctx, cancel := context.WithTimeout(f.ctx, ioDeadline)
	defer cancel()
	must(t, invalid.Write(ctx, websocket.MessageText, []byte(`{"conversationId":"gateway_restricted_conversation","afterSeq":"00"}`)))
	expectClose(t, invalid, websocket.StatusPolicyViolation, "invalid_subscription")
}

func testDeviceAttemptLimit(t *testing.T) {
	f := openFixture(t)
	const user = "gateway_limited_device_user"
	token := f.admit(user, "limited_device", 8).Token.RawToken()
	fresh := f.admit(user, "other_device", 8).Token.RawToken()
	g := f.gateway(map[string]string{
		"NEWIM_GATEWAY_IP_ATTEMPTS_PER_MINUTE":     "30",
		"NEWIM_GATEWAY_DEVICE_ATTEMPTS_PER_MINUTE": "2",
	})
	withinRateWindow()
	for i := 0; i < 2; i++ {
		_ = f.dial(g, gatewayRoute, token, nil, "").connected(t).CloseNow()
	}
	f.dial(g, gatewayRoute, token, nil, "").rejected(t, http.StatusTooManyRequests)
	_ = f.dial(g, gatewayRoute, fresh, nil, "").connected(t).CloseNow()
}

func testIPAttemptLimit(t *testing.T) {
	f := openFixture(t)
	a := f.admit("gateway_ip_user_a", "fixture_device", 8).Token.RawToken()
	b := f.admit("gateway_ip_user_b", "fixture_device", 8).Token.RawToken()
	g := f.gateway(map[string]string{"NEWIM_GATEWAY_IP_ATTEMPTS_PER_MINUTE": "1"})
	withinRateWindow()
	_ = f.dial(g, gatewayRoute, a, http.Header{"X-Forwarded-For": {"192.0.2.1"}}, "").connected(t).CloseNow()
	f.dial(g, gatewayRoute, b, http.Header{"X-Forwarded-For": {"192.0.2.2"}}, "").rejected(t, http.StatusTooManyRequests)
}

func withinRateWindow() {
	remaining := time.Until(time.Now().Truncate(time.Minute).Add(time.Minute))
	if remaining < time.Second {
		time.Sleep(remaining + 10*time.Millisecond)
	}
}

func testConnectionCapacity(t *testing.T) {
	f := openFixture(t)
	token := f.admit("gateway_capacity_user", "fixture_device", 8).Token.RawToken()
	g := f.gateway(map[string]string{
		"NEWIM_GATEWAY_MAX_CONNECTIONS":      "1",
		"NEWIM_GATEWAY_SUBSCRIBE_TIMEOUT_MS": "1200",
	})
	slow := f.dial(g, gatewayRoute, token, nil, "").connected(t)
	f.dial(g, gatewayRoute, token, nil, "").rejected(t, http.StatusServiceUnavailable)
	expectClose(t, slow, websocket.StatusPolicyViolation, "subscription_timeout")
	_ = f.dial(g, gatewayRoute, token, nil, "").connected(t).CloseNow()
}

func testSlowReader(t *testing.T) {
	f := openFixture(t)
	const (
		slowConversation    = "gateway_slow_conversation"
		healthyConversation = "gateway_healthy_conversation"
	)
	sender := f.admit("gateway_slow_sender", "sender_device", 8).Binding.UserID
	slowIdentity := f.admit("gateway_slow_reader", "slow_device", 8)
	healthyIdentity := f.admit("gateway_healthy_reader", "healthy_device", 8)
	probe := f.admit("gateway_capacity_probe", "probe_device", 8).Token.RawToken()
	f.conversation(slowConversation, sender, slowIdentity.Binding.UserID)
	f.conversation(healthyConversation, sender, healthyIdentity.Binding.UserID)
	f.persist(nil, slowConversation, sender, "gateway_slow_first", "slow reader was subscribed")
	f.persist(nil, healthyConversation, sender, "gateway_healthy_first", "healthy reader was subscribed")

	// This committed backlog exceeds the local transport buffers while the
	// receiver's TCP window remains small and the receiver stops reading.
	payload, err := json.Marshal(map[string]string{"text": strings.Repeat("x", 16_000)})
	must(t, err)
	f.sql(`SELECT newim.persist_message(
		$1, 'gateway_slow_client_' || n::text, $2, 'gateway_slow_server_' || n::text,
		'text', 1, $3, $4, 'gateway_slow_event_' || n::text)
		FROM generate_series(1,512) AS n`,
		sender, slowConversation, time.Now().UnixMilli(), payload)

	g := f.gateway(map[string]string{
		"NEWIM_GATEWAY_MAX_CONNECTIONS":            "2",
		"NEWIM_GATEWAY_SUBSCRIBE_TIMEOUT_MS":       "5000",
		"NEWIM_GATEWAY_WRITE_TIMEOUT_MS":           "250",
		"NEWIM_GATEWAY_IP_ATTEMPTS_PER_MINUTE":     "4096",
		"NEWIM_GATEWAY_DEVICE_ATTEMPTS_PER_MINUTE": "4096",
	})
	slow := f.dialWithReadBuffer(g, gatewayRoute, slowIdentity.Token.RawToken(), nil, "", 1024).connected(t)
	healthy := f.dial(g, gatewayRoute, healthyIdentity.Token.RawToken(), nil, "").connected(t)
	f.dial(g, gatewayRoute, probe, nil, "").rejected(t, http.StatusServiceUnavailable)
	writeSubscription(t, slow, slowConversation, 0)
	writeSubscription(t, healthy, healthyConversation, 0)
	expectMessage(t, readFrame(slow), slowConversation, "gateway_slow_first", sender, "slow reader was subscribed", 1)
	expectMessage(t, readFrame(healthy), healthyConversation, "gateway_healthy_first", sender, "healthy reader was subscribed", 1)

	waitHealthy := readAsync(healthy, 35*time.Second)
	deadline := time.Now().Add(25 * time.Second)
	for {
		try := f.dial(g, gatewayRoute, probe, nil, "")
		if try.status() == http.StatusSwitchingProtocols {
			_ = try.connected(t).CloseNow()
			break
		}
		try.rejected(t, http.StatusServiceUnavailable)
		if time.Now().After(deadline) {
			t.Fatal("established reader stalled past the write deadline kept its slot")
		}
		time.Sleep(125 * time.Millisecond)
	}

	f.persist(nil, healthyConversation, sender, "gateway_healthy_second", "healthy reader survives a stalled peer")
	expectMessage(t, <-waitHealthy, healthyConversation, "gateway_healthy_second", sender, "healthy reader survives a stalled peer", 2)
}

func TestGatewayRecovery(t *testing.T) {
	const stateFile = "/tmp/newim-gateway-recovery-fixture.json"
	f := openFixture(t)
	switch phase := os.Getenv("NEWIM_GATEWAY_PHASE"); phase {
	case "prepare":
		prepareRecovery(t, f, stateFile)
	case "restart":
		resumeAfterRestart(t, f, stateFile)
	default:
		t.Fatalf("unexpected gateway recovery phase %q", phase)
	}
}

const (
	recoverySender       = "gateway_recovery_sender"
	recoveryReceiver     = "gateway_recovery_receiver"
	recoveryConversation = "gateway_recovery_conversation"
)

type recoveryState struct {
	Token  string
	Nonce  string
	Cursor int64
}

func prepareRecovery(t *testing.T, f *fixture, stateFile string) {
	t.Helper()
	f.admit(recoverySender, "fixture_sender_device", 8)
	token := f.admit(recoveryReceiver, "fixture_receiver_device", 8).Token.RawToken()
	f.conversation(recoveryConversation, recoverySender, recoveryReceiver)
	f.persist(nil, recoveryConversation, recoverySender, "gateway_recovery_first", "persisted before interruption")
	firstServer := f.gateway(nil)
	first := f.dial(firstServer, gatewayRoute, token, nil, "")
	c := first.connected(t)
	writeSubscription(t, c, recoveryConversation, 0)
	expectMessage(t, readFrame(c), recoveryConversation, "gateway_recovery_first", recoverySender, "persisted before interruption", 1)
	firstServer.kill()
	f.persist(nil, recoveryConversation, recoverySender, "gateway_recovery_second", "committed while offline")
	data, err := json.Marshal(recoveryState{Token: token, Nonce: first.key, Cursor: 1})
	must(t, err)
	must(t, os.WriteFile(stateFile, data, 0o600))
}

func resumeAfterRestart(t *testing.T, f *fixture, stateFile string) {
	t.Helper()
	data, err := os.ReadFile(stateFile)
	must(t, err)
	defer func() { _ = os.Remove(stateFile) }()
	var state recoveryState
	must(t, json.Unmarshal(data, &state))
	if state.Token == "" || state.Nonce == "" || state.Cursor != 1 {
		t.Fatal("recovery fixture incomplete")
	}
	restarted := f.gateway(nil)
	f.dial(restarted, gatewayRoute, state.Token, nil, state.Nonce).rejected(t, http.StatusConflict)
	c := f.dial(restarted, gatewayRoute, state.Token, nil, "").connected(t)
	writeSubscription(t, c, recoveryConversation, state.Cursor)
	expectMessage(t, readFrame(c), recoveryConversation, "gateway_recovery_second", recoverySender, "committed while offline", 2)

	f.persist(nil, recoveryConversation, recoverySender, "gateway_recovery_third", "committed after restart")
	expectMessage(t, readFrame(c), recoveryConversation, "gateway_recovery_third", recoverySender, "committed after restart", 3)
	restarted.requireRedacted(t, state.Token, state.Nonce, gatewayDSN, "committed while offline")
}
