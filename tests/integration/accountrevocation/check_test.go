//go:build integration

package accountrevocation_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/coder/websocket"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	auth "github.com/m-ice/NewIM/server/auth/session"
	authstore "github.com/m-ice/NewIM/server/storage/authsession"
)

const (
	revokePath      = "/api/v1/internal/session/revoke-account"
	admitPath       = "/api/v1/internal/session/admit"
	issuePath       = "/api/v1/internal/session/token"
	sessionPath     = "/api/v1/session"
	passwordChanged = "password_changed"
	signedOutAll    = "signed_out_all"
	accountBlocked  = "account_blocked"
)

func TestAccountRevocationCheck(t *testing.T) {
	t.Run("real-devices-legacy-bindings-and-wss", testActiveAndLegacy)
	t.Run("request-retry-with-zero-sessions", testZeroReceipt)
	t.Run("same-account-admission-order", testAdmissionOrder)
	t.Run("rollback-on-durable-receipt-commit", testCommitRollback)
}

func testActiveAndLegacy(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(13)
	const user = "revoke_live_owner"
	const other = "revoke_live_other"
	const conversation = "revoke_live_conversation"
	f.user(user)
	f.user(other)
	settings := map[string]string{"NEWIM_AUTH_MAX_DEVICES": "2", "NEWIM_AUTH_MAX_DEVICES_PER_PLATFORM": "2"}
	writer, secondary := f.server("1", settings), f.server("1", settings)
	gateway := f.gateway(nil)
	old := admit(t, f, writer, user, "old_device")
	two := admit(t, f, writer, user, "phone")
	f.sql("UPDATE newim.im_devices SET last_active_at=now()-interval '1 hour' WHERE user_id=$1 AND device_id=$2", user, old.DeviceID)
	three := admit(t, f, secondary, user, "desktop")
	extra := issue(t, f, secondary, three)
	issued, err := f.auth.AuthenticateBearer(f.ctx, three.Token)
	must(t, err)
	individuallyRevoked := f.call(secondary, http.MethodDelete, "/api/v1/session/tokens/current", three.Token, nil, nil)
	if individuallyRevoked.status != http.StatusNoContent || individuallyRevoked.err != nil || len(individuallyRevoked.body) != 0 {
		t.Fatal("session fixture could not revoke one token without revoking the whole session")
	}
	var priorTokenTime time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT revoked_at FROM newim.im_auth_tokens WHERE token_id=$1", issued.TokenID()).Scan(&priorTokenTime))
	peer := admit(t, f, secondary, other, "phone")
	before := loadEvent(t, f, old.SessionID)
	if before.reason != "device_limit" {
		t.Fatal("fixture did not produce a prior committed device-limit event")
	}
	legacyExpired := legacyBinding(t, f, user, "legacy_expired", true)
	legacyEmpty := legacyBinding(t, f, user, "legacy_empty", false)
	f.sql("UPDATE newim.im_sessions SET created_at=now()+interval '1 hour' WHERE session_id=$1", legacyEmpty.sessionID)

	f.sql("INSERT INTO newim.im_conversations(conversation_id,kind) VALUES($1,'group')", conversation)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2),($1,$3)", conversation, user, other)
	first := seedMessage(t, f, conversation, user, "before_revoke", "before account revocation", 1)
	left := subscribe(t, f, gateway, two.Token, conversation, 0)
	right := subscribe(t, f, gateway, extra, conversation, 0)
	unaffected := subscribe(t, f, gateway, peer.Token, conversation, 0)
	requireDelivered(t, f, left, first, "before account revocation")
	requireDelivered(t, f, right, first, "before account revocation")
	requireDelivered(t, f, unaffected, first, "before account revocation")

	const req = "revoke_password_changed"
	expectRevocation(t, revocation(t, f, writer, user, req, passwordChanged), 4)
	for _, token := range []string{old.Token, two.Token, three.Token, extra, legacyExpired.token} {
		assertBearer(t, f, secondary, token, http.StatusUnauthorized)
	}
	assertBearer(t, f, secondary, peer.Token, http.StatusOK)
	requireSessionClose(t, f, left)
	requireSessionClose(t, f, right)
	conn, reply, err := dialSocket(t, f, gateway, two.Token)
	requireSocketStatus(t, conn, reply, err, http.StatusUnauthorized)
	next := seedMessage(t, f, conversation, other, "after_revoke", "other account stays connected", 2)
	requireDelivered(t, f, unaffected, next, "other account stays connected")
	if after := loadEvent(t, f, old.SessionID); after != before {
		t.Fatal("account revocation changed the existing device-limit event")
	}
	for _, sessionID := range []string{two.SessionID, three.SessionID, legacyExpired.sessionID, legacyEmpty.sessionID} {
		requireDurableRevocation(t, f, user, sessionID, passwordChanged)
	}
	var samePriorTime bool
	var tokens, events int
	must(t, f.db.QueryRow(f.ctx, `SELECT revoked_at=$2,
		(SELECT count(*) FROM newim.im_auth_tokens t JOIN newim.im_sessions s USING(session_id) WHERE s.user_id=$3),
		(SELECT count(*) FROM newim.im_session_revocation_events WHERE user_id=$3)
		FROM newim.im_auth_tokens WHERE token_id=$1`, issued.TokenID(), priorTokenTime, user).Scan(&samePriorTime, &tokens, &events))
	if !samePriorTime || tokens != 5 || events != 5 {
		t.Fatal("account revocation altered previously recorded token state or durable row counts")
	}
	f.call(secondary, http.MethodPost, issuePath, f.issuerKey, issueBody(t, three), nil).
		requireError(t, http.StatusForbidden, "AUTH_SESSION_NOT_ELIGIBLE")
	if got := receipt(t, f, user, req, passwordChanged); got != 4 {
		t.Fatalf("durable receipt size=%d want 4", got)
	}
	requireUnaffected(t, f, other)
	fresh := admit(t, f, secondary, user, two.DeviceID)
	if fresh.SessionID == two.SessionID {
		t.Fatal("new device admission reused the revoked session")
	}
	expectRevocation(t, revocation(t, f, secondary, user, req, passwordChanged), 4)
	revocation(t, f, secondary, user, req, signedOutAll).requireError(t, http.StatusConflict, "AUTH_REVOCATION_CONFLICT")
	assertBearer(t, f, writer, fresh.Token, http.StatusOK)
	expectRevocation(t, revocation(t, f, writer, user, "next_signed_out_all", signedOutAll), 1)
	assertBearer(t, f, secondary, fresh.Token, http.StatusUnauthorized)
	requireDurableRevocation(t, f, user, fresh.SessionID, signedOutAll)
	requireUnaffected(t, f, other)
	writer.requireRedacted(t, user, other, req, f.issuerKey, two.Token, testDSN)
	secondary.requireRedacted(t, user, other, req, f.issuerKey, three.Token, peer.Token, testDSN)
	gateway.requireRedacted(t, user, other, two.Token, extra, peer.Token, "before account revocation", testDSN)
}

func testZeroReceipt(t *testing.T) {
	f := openFixture(t)
	const user = "revoke_empty_owner"
	f.user(user)
	server := f.server("1", nil)
	expectRevocation(t, revocation(t, f, server, user, "empty_receipt", signedOutAll), 0)
	if got := receipt(t, f, user, "empty_receipt", signedOutAll); got != 0 {
		t.Fatal("zero-session result was not recorded")
	}
	fresh := admit(t, f, server, user, "after_empty")
	expectRevocation(t, revocation(t, f, server, user, "empty_receipt", signedOutAll), 0)
	assertBearer(t, f, server, fresh.Token, http.StatusOK)
	expectRevocation(t, revocation(t, f, server, user, "block_later", accountBlocked), 1)
	requireDurableRevocation(t, f, user, fresh.SessionID, accountBlocked)
	assertBearer(t, f, server, fresh.Token, http.StatusUnauthorized)
}

type deviceReply struct {
	Version   int       `json:"version"`
	UserID    string    `json:"userId"`
	DeviceID  string    `json:"deviceId"`
	SessionID string    `json:"sessionId"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expiresAt"`
}

func admit(t *testing.T, f *fixture, server *process, user, device string) deviceReply {
	t.Helper()
	return expectAdmission(t, f.call(server, http.MethodPost, admitPath, f.issuerKey, admitBody(t, user, device), nil), user, device)
}

func admitBody(t *testing.T, user, device string) []byte {
	t.Helper()
	return encode(t, map[string]string{"userId": user, "deviceId": device, "platform": "fixture"})
}

func expectAdmission(t *testing.T, reply response, user, device string) deviceReply {
	t.Helper()
	requireAPIStatus(t, reply, http.StatusOK)
	var bound deviceReply
	decoder := json.NewDecoder(bytes.NewReader(reply.body))
	decoder.DisallowUnknownFields()
	must(t, decoder.Decode(&bound))
	if bound.Version != 1 || bound.UserID != user || bound.DeviceID != device || bound.SessionID == "" ||
		len(bound.Token) != auth.RawTokenLen || bound.ExpiresAt.IsZero() {
		t.Fatal("real device admission did not return its committed identity and token")
	}
	return bound
}

func issueBody(t *testing.T, bound deviceReply) []byte {
	t.Helper()
	return encode(t, map[string]string{"userId": bound.UserID, "deviceId": bound.DeviceID, "sessionId": bound.SessionID})
}

func issue(t *testing.T, f *fixture, server *process, bound deviceReply) string {
	t.Helper()
	reply := f.call(server, http.MethodPost, issuePath, f.issuerKey, issueBody(t, bound), nil)
	requireAPIStatus(t, reply, http.StatusOK)
	var issued struct {
		Version int    `json:"version"`
		Token   string `json:"token"`
	}
	must(t, json.Unmarshal(reply.body, &issued))
	if issued.Version != 1 || len(issued.Token) != auth.RawTokenLen || issued.Token == bound.Token {
		t.Fatal("existing session issuer did not return a new token")
	}
	return issued.Token
}

func assertBearer(t *testing.T, f *fixture, server *process, token string, status int) {
	t.Helper()
	reply := f.call(server, http.MethodGet, sessionPath, token, nil, nil)
	if status == http.StatusUnauthorized {
		reply.requireError(t, status, "AUTH_INVALID_TOKEN")
		return
	}
	requireAPIStatus(t, reply, status)
}

func revocation(t *testing.T, f *fixture, server *process, user, request, reason string) response {
	t.Helper()
	return f.call(server, http.MethodPost, revokePath, f.issuerKey, revokeBody(t, user, request, reason), nil)
}

func revokeBody(t *testing.T, user, request, reason string) []byte {
	t.Helper()
	return encode(t, map[string]string{"userId": user, "requestId": request, "reason": reason})
}

func expectRevocation(t *testing.T, reply response, count int) {
	t.Helper()
	requireAPIStatus(t, reply, http.StatusOK)
	var body map[string]json.RawMessage
	must(t, json.Unmarshal(reply.body, &body))
	if len(body) != 2 || string(body["version"]) != "1" || string(body["revokedSessions"]) != strconv.Itoa(count) {
		t.Fatalf("response did not use the versioned %d-session receipt", count)
	}
}

func requireAPIStatus(t *testing.T, reply response, status int) {
	t.Helper()
	if reply.err != nil || reply.status != status || reply.verified ||
		reply.headers.Get("Cache-Control") != "no-store" || reply.headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("loopback status=%d err=%v want=%d and noncacheable JSON", reply.status, reply.err, status)
	}
}

func encode(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	must(t, err)
	return raw
}

type revocationEvent struct {
	id     int64
	reason string
	at     time.Time
}

func loadEvent(t *testing.T, f *fixture, session string) revocationEvent {
	t.Helper()
	var e revocationEvent
	must(t, f.db.QueryRow(f.ctx, "SELECT event_id,reason,revoked_at FROM newim.im_session_revocation_events WHERE session_id=$1", session).
		Scan(&e.id, &e.reason, &e.at))
	e.at = e.at.UTC()
	return e
}

func requireDurableRevocation(t *testing.T, f *fixture, user, session, reason string) {
	t.Helper()
	var consistent bool
	must(t, f.db.QueryRow(f.ctx, `SELECT e.reason=$3 AND s.revoked_at=e.revoked_at AND s.revoked_at>=s.created_at
		AND NOT EXISTS (SELECT 1 FROM newim.im_auth_tokens t WHERE t.session_id=s.session_id AND
		(t.revoked_at IS NULL OR t.revoked_at<t.created_at))
		FROM newim.im_sessions s JOIN newim.im_session_revocation_events e ON e.session_id=s.session_id
		WHERE s.user_id=$1 AND s.session_id=$2 AND e.user_id=s.user_id AND e.device_id=s.device_id`, user, session, reason).Scan(&consistent))
	if !consistent {
		t.Fatal("session, tokens and durable reason did not commit a consistent revocation")
	}
}

func receipt(t *testing.T, f *fixture, user, request, reason string) int {
	t.Helper()
	var count int
	must(t, f.db.QueryRow(f.ctx, `SELECT revoked_sessions FROM newim.im_account_revocation_requests
		WHERE user_id=$1 AND request_id=$2 AND reason=$3`, user, request, reason).Scan(&count))
	return count
}

func requireUnaffected(t *testing.T, f *fixture, user string) {
	t.Helper()
	var affected bool
	must(t, f.db.QueryRow(f.ctx, `SELECT EXISTS (
		SELECT 1 FROM newim.im_sessions s JOIN newim.im_auth_tokens t USING (session_id)
		WHERE s.user_id=$1 AND (s.revoked_at IS NOT NULL OR t.revoked_at IS NOT NULL)
	) OR EXISTS (SELECT 1 FROM newim.im_session_revocation_events WHERE user_id=$1)
	  OR EXISTS (SELECT 1 FROM newim.im_account_revocation_requests WHERE user_id=$1)`, user).Scan(&affected))
	if affected {
		t.Fatal("account action changed the unrelated account")
	}
}

type legacySession struct{ sessionID, token string }

func legacyBinding(t *testing.T, f *fixture, user, device string, withExpiredToken bool) legacySession {
	t.Helper()
	id := device + "_session"
	f.sql("INSERT INTO newim.im_devices(user_id,device_id,created_at) VALUES($1,$2,now()-interval '2 hours')", user, device)
	f.sql("INSERT INTO newim.im_sessions(session_id,user_id,device_id,created_at) VALUES($1,$2,$3,now()-interval '2 hours')", id, user, device)
	if !withExpiredToken {
		return legacySession{sessionID: id}
	}
	store, err := authstore.Open(f.ctx, authstore.Config{DSN: testDSN, AllowLocalSocket: true, MaxConnections: 1})
	must(t, err)
	defer store.Close()
	oldTime := time.Now().Add(-time.Hour)
	service, err := auth.NewService(store, auth.Config{Clock: auth.ClockFunc(func() time.Time { return oldTime }), Observer: authObservation{}})
	must(t, err)
	token, err := service.Issue(f.ctx, auth.IssueRequest{Binding: auth.SessionBinding{UserID: user, DeviceID: device, SessionID: id}, TTL: time.Minute})
	must(t, err)
	return legacySession{sessionID: id, token: token.RawToken()}
}

func seedMessage(t *testing.T, f *fixture, conversation, sender, client, text string, seq int64) protocol.Ack {
	t.Helper()
	server := client + "_server"
	now := time.Now().UnixMilli()
	f.sql("SELECT newim.persist_message($1,$2,$3,$4,'text',1,$5,$6,$7)", sender, client, conversation, server,
		now, encode(t, map[string]string{"text": text}), client+"_event")
	return protocol.Ack{SenderID: sender, ConversationID: conversation, ClientMsgID: client, ServerMsgID: server,
		ConversationSeq: strconv.FormatInt(seq, 10), ServerTime: strconv.FormatInt(now, 10)}
}

func requireSessionClose(t *testing.T, f *fixture, conn *websocket.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 8*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	var closed websocket.CloseError
	if !errors.As(err, &closed) || closed.Code != websocket.StatusPolicyViolation || closed.Reason != "session_revoked" {
		t.Fatalf("account revocation WSS close=%v want 1008 session_revoked", err)
	}
}
