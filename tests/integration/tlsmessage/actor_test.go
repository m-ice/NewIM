//go:build integration

package tlsmessage_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/auth/login"
	"github.com/m-ice/NewIM/server/group"
	"github.com/m-ice/NewIM/server/user"
	"github.com/m-ice/NewIM/server/userhttp"
)

func (f *fixture) actorProvisioner(enabled string) (*process, string) {
	f.t.Helper()
	return f.messageLoopback(map[string]string{
		"NEWIM_ACTOR_PROVISION": enabled, "NEWIM_USER_PROFILE": "1", "NEWIM_CONVERSATION_PROJECTION": "1",
	})
}

func (f *fixture) actorGateway() *process {
	f.t.Helper()
	var key [32]byte
	_, err := rand.Read(key[:])
	must(f.t, err)
	path := filepath.Join(f.t.TempDir(), "sync-keys.json")
	must(f.t, os.WriteFile(path, sendFixtureJSON(f.t, map[string]any{
		"activeKeyId": "actor", "keys": map[string]string{"actor": hex.EncodeToString(key[:])},
	}), 0o600))
	clear(key[:])
	return f.gateway("1", map[string]string{
		"NEWIM_USER_PROFILE": "1", "NEWIM_GROUP_HTTP": "1", "NEWIM_CONVERSATION_PROJECTION": "1",
		"NEWIM_CONVERSATION_SYNC": "1", "NEWIM_CONVERSATION_SYNC_KEYS_FILE": path,
	})
}

func actorProfile(t *testing.T, f *fixture, server *process, operator, id, nickname string, typ protocol.SenderType) response {
	t.Helper()
	body := map[string]string{"userId": id, "nickname": nickname}
	if typ != "" {
		body["actorType"] = string(typ)
	}
	return f.call(server, http.MethodPost, login.UserProvisionRoute, operator, sendFixtureJSON(t, body), nil)
}

func requireActorProfile(t *testing.T, reply response, id, nickname string, verified bool) {
	t.Helper()
	if reply.err != nil || reply.status != http.StatusOK || reply.verified != verified ||
		reply.headers.Get("Cache-Control") != "no-store" {
		t.Fatalf("actor fixture profile status=%d TLS=%t err=%v", reply.status, reply.verified, reply.err)
	}
	var got struct {
		Version int          `json:"version"`
		Profile user.Profile `json:"profile"`
	}
	decoder := json.NewDecoder(bytes.NewReader(reply.body))
	decoder.DisallowUnknownFields()
	must(t, decoder.Decode(&got))
	if got.Version != 1 || got.Profile.UserID != id || got.Profile.Nickname != nickname {
		t.Fatal("actor fixture profile did not preserve the original account and profile")
	}
}

func requireActorError(t *testing.T, reply response, status int, code string, verified bool) {
	t.Helper()
	if reply.err != nil || reply.status != status || reply.verified != verified ||
		reply.headers.Get("Cache-Control") != "no-store" {
		t.Fatalf("actor fixture error status=%d TLS=%t err=%v, want %d (%s)", reply.status, reply.verified, reply.err, status, code)
	}
	var envelope struct {
		Error struct{ Code string } `json:"error"`
	}
	decoder := json.NewDecoder(bytes.NewReader(reply.body))
	decoder.DisallowUnknownFields()
	must(t, decoder.Decode(&envelope))
	if envelope.Error.Code != code {
		t.Fatalf("actor fixture error code=%q, want=%q", envelope.Error.Code, code)
	}
}

func admitActor(t *testing.T, f *fixture, server *process, operator, userID, deviceID string) string {
	t.Helper()
	reply := f.call(server, http.MethodPost, login.AdmissionRoute, operator, sendFixtureJSON(t, map[string]string{
		"userId": userID, "deviceId": deviceID, "platform": "fixture",
	}), nil)
	if reply.err != nil || reply.status != http.StatusOK || reply.verified {
		t.Fatalf("actor fixture session admission status=%d TLS=%t err=%v", reply.status, reply.verified, reply.err)
	}
	var auth struct {
		Version   int       `json:"version"`
		UserID    string    `json:"userId"`
		DeviceID  string    `json:"deviceId"`
		SessionID string    `json:"sessionId"`
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expiresAt"`
	}
	decoder := json.NewDecoder(bytes.NewReader(reply.body))
	decoder.DisallowUnknownFields()
	must(t, decoder.Decode(&auth))
	if auth.Version != 1 || auth.UserID != userID || auth.DeviceID != deviceID || auth.SessionID == "" ||
		auth.Token == "" || !auth.ExpiresAt.After(time.Now()) {
		t.Fatal("actor fixture admission did not return its live session and device")
	}
	return auth.Token
}

func TestMessageActorCheck(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(20)
	legacy, oldOperator := f.actorProvisioner("0")
	enabled, operator := f.actorProvisioner("1")
	gateway := f.actorGateway()
	const human, bot = "actor_check_human", "actor_check_bot"
	requireActorProfile(t, actorProfile(t, f, legacy, oldOperator, human, "Original person", ""), human, "Original person", false)
	requireActorProfile(t, actorProfile(t, f, enabled, operator, human, "Ignored change", protocol.SenderHuman), human, "Original person", false)
	requireActorError(t, actorProfile(t, f, enabled, operator, human, "Rejected change", protocol.SenderBot),
		http.StatusConflict, "USER_ACTOR_TYPE_CONFLICT", false)
	requireActorError(t, actorProfile(t, f, legacy, oldOperator, "actor_check_disabled", "Disabled", protocol.SenderBot),
		http.StatusBadRequest, "USER_INVALID_INPUT", false)

	first := actorProfile(t, f, enabled, operator, bot, "Original robot", protocol.SenderBot)
	requireActorProfile(t, first, bot, "Original robot", false)
	replay := actorProfile(t, f, enabled, operator, bot, "Ignored retry profile", protocol.SenderBot)
	requireActorProfile(t, replay, bot, "Original robot", false)
	if !bytes.Equal(first.body, replay.body) {
		t.Fatal("same-type actor provisioning did not preserve the response")
	}
	requireActorError(t, actorProfile(t, f, enabled, operator, bot, "Rejected actor change", protocol.SenderSystem),
		http.StatusConflict, "USER_ACTOR_TYPE_CONFLICT", false)
	requireActorProfile(t, actorProfile(t, f, legacy, oldOperator, bot, "Ignored legacy retry", ""), bot, "Original robot", false)

	normal := admitActor(t, f, legacy, oldOperator, human, "phone")
	robot := admitActor(t, f, enabled, operator, bot, "robot")
	requireActorError(t, actorProfile(t, f, enabled, normal, "actor_check_ordinary", "Account session", protocol.SenderBot),
		http.StatusUnauthorized, "AUTH_INVALID_TOKEN", false)
	requireActorError(t, actorProfile(t, f, enabled, robot, "actor_check_ordinary_bot", "Bot session", protocol.SenderBot),
		http.StatusUnauthorized, "AUTH_INVALID_TOKEN", false)
	for _, token := range []string{normal, robot} {
		attempt := sendFixtureJSON(t, map[string]string{"nickname": "Rejected patch", "actorType": "AI_AGENT"})
		requireActorError(t, f.call(gateway, http.MethodPost, userhttp.UpdateRoute, token, attempt, nil),
			http.StatusBadRequest, "USER_INVALID_INPUT", true)
	}
	requireActorProfile(t, f.call(gateway, http.MethodGet, userhttp.SelfRoute, normal, nil, nil), human, "Original person", true)
	requireActorProfile(t, f.call(gateway, http.MethodGet, userhttp.SelfRoute, robot, nil, nil), bot, "Original robot", true)

	var botType string
	var others int
	must(t, f.db.QueryRow(f.ctx, "SELECT actor_type FROM newim.im_user_actor_types WHERE user_id=$1", bot).Scan(&botType))
	must(t, f.db.QueryRow(f.ctx, `SELECT count(*) FROM newim.im_users
		WHERE user_id IN ('actor_check_disabled','actor_check_ordinary','actor_check_ordinary_bot')`).Scan(&others))
	var humanTypes int
	must(t, f.db.QueryRow(f.ctx, "SELECT count(*) FROM newim.im_user_actor_types WHERE user_id=$1", human).Scan(&humanTypes))
	if botType != string(protocol.SenderBot) || humanTypes != 0 || others != 0 {
		t.Fatal("ordinary, mismatched or disabled provisioning changed durable actor data")
	}
	enabled.requireRedacted(t, operator, oldOperator, normal, robot, testDSN)
	gateway.requireRedacted(t, operator, oldOperator, normal, robot, testDSN)
}

func TestMessageActorDelivery(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(20)
	enabled, operator := f.actorProvisioner("1")
	actors := []struct {
		id    string
		label string
		typ   protocol.SenderType
		token string
	}{
		{id: "actor_delivery_human", label: "Ordinary account"},
		{id: "actor_delivery_bot", label: "Robot account", typ: protocol.SenderBot},
		{id: "actor_delivery_system", label: "System publisher", typ: protocol.SenderSystem},
		{id: "actor_delivery_ai", label: "AI agent", typ: protocol.SenderAIAgent},
	}
	for _, actor := range actors[1:] {
		requireActorProfile(t, actorProfile(t, f, enabled, operator, actor.id, actor.label, actor.typ), actor.id, actor.label, false)
	}

	must(t, enabled.cmd.Process.Signal(syscall.SIGTERM))
	select {
	case <-enabled.done:
		must(t, enabled.waitErr)
	case <-time.After(5 * time.Second):
		t.Fatal("actor-enabled provisioning process did not stop")
	}
	disabled, legacyOperator := f.actorProvisioner("0")
	human, bot := actors[0], actors[1]
	requireActorError(t, actorProfile(t, f, disabled, legacyOperator, bot.id, bot.label, bot.typ),
		http.StatusBadRequest, "USER_INVALID_INPUT", false)
	requireActorProfile(t, actorProfile(t, f, disabled, legacyOperator, bot.id, bot.label, ""), bot.id, bot.label, false)
	requireActorProfile(t, actorProfile(t, f, disabled, legacyOperator, human.id, human.label, ""), human.id, human.label, false)
	for i := range actors {
		actors[i].token = admitActor(t, f, disabled, legacyOperator, actors[i].id, "phone")
	}
	human, bot = actors[0], actors[1]

	writer, observer := f.actorGateway(), f.actorGateway()
	direct := createDirect(t, f, writer, human.token, bot.id)
	directWire := textBody(t, "actor_direct_message", direct, "ordinary private robot message")
	directReply := callSend(f, writer, bot.token, directWire)
	directAck := expectACK(t, directReply)
	requireActorField(t, directReply.body, protocol.SenderBot)
	if directAck.SenderType != protocol.SenderBot {
		t.Fatal("private send result lost its authoritative BOT classification")
	}
	requireCommitted(t, f, directAck, bot.id, direct, "actor_direct_message", "ordinary private robot message", 1)

	const room = "actor_delivery_group"
	members := make([]string, 0, len(actors)-1)
	for _, actor := range actors[1:] {
		members = append(members, actor.id)
	}
	roomResponse := f.call(writer, http.MethodPost, "/api/v1/groups/create", human.token, sendFixtureJSON(t, group.CreateRequest{
		GroupID: room, Profile: group.Profile{Name: "Actor delivery fixture"}, JoinMode: group.ApproveApplications, MemberIDs: members,
	}), nil)
	requireStatus(t, roomResponse, http.StatusOK)
	socket := subscribe(t, f, observer, human.token, room, 0)
	type observed struct {
		raw []byte
		typ protocol.SenderType
	}
	wss := make([]observed, 0, len(actors))
	for i, actor := range actors {
		seq := int64(i + 1)
		client := "actor_group_" + strconv.FormatInt(seq, 10)
		const body = "message by a current member"
		wire := textBody(t, client, room, body)
		reply := callSend(f, writer, actor.token, wire)
		ack := expectACK(t, reply)
		if ack.SenderType != actor.typ {
			t.Fatalf("ACK sequence %d has sender type %q, want %q", seq, ack.SenderType, actor.typ)
		}
		requireActorField(t, reply.body, actor.typ)
		requireCommitted(t, f, ack, actor.id, room, client, body, seq)
		requireSameSendACK(t, observer, callSend(f, observer, actor.token, wire), reply.body)
		wss = append(wss, observed{captureActorFrame(t, f, socket, ack), actor.typ})
	}
	page := actorDelta(t, f, observer, human.token, room)
	if len(page) != len(wss) {
		t.Fatal("TLS message delta did not return the original four committed messages")
	}
	for i, raw := range page {
		requireActorField(t, raw, wss[i].typ)
		if !bytes.Equal(raw, wss[i].raw) {
			t.Fatalf("TLS message delta frame %d differed from the original WSS bytes", i+1)
		}
	}

	before := state(t, f, room)
	left := f.call(observer, http.MethodPost, "/api/v1/groups/member/action", bot.token,
		sendFixtureJSON(t, group.MemberRequest{GroupID: room, Action: group.Leave}), nil)
	requireStatus(t, left, http.StatusOK)
	callSend(f, writer, bot.token, textBody(t, "actor_after_departure", room, "must not commit after departure")).
		requireError(t, http.StatusForbidden, "SEND_UNAUTHORIZED")
	if state(t, f, room) != before {
		t.Fatal("departed BOT sent a message or changed the durable group tail")
	}
	tokens := []string{operator, legacyOperator, testDSN}
	for _, actor := range actors {
		tokens = append(tokens, actor.token)
	}
	disabled.requireRedacted(t, tokens...)
	writer.requireRedacted(t, tokens...)
	observer.requireRedacted(t, tokens...)
}

func requireActorField(t *testing.T, raw []byte, expected protocol.SenderType) {
	t.Helper()
	var frame struct {
		Body map[string]json.RawMessage `json:"body"`
	}
	must(t, json.Unmarshal(raw, &frame))
	field := frame.Body["senderType"]
	if expected.IsZero() {
		if field != nil {
			t.Fatal("legacy HUMAN reply added a senderType field")
		}
		return
	}
	if string(field) != strconv.Quote(string(expected)) {
		t.Fatal("persisted actor field is missing or was not canonically encoded")
	}
}

func captureActorFrame(t *testing.T, f *fixture, socket *websocket.Conn, ack protocol.Ack) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 8*time.Second)
	defer cancel()
	kind, raw, err := socket.Read(ctx)
	must(t, err)
	frame, err := protocol.DecodeServerFrame(raw)
	must(t, err)
	msg := frame.Message
	if kind != websocket.MessageText || msg == nil || msg.ServerMsgID != ack.ServerMsgID || msg.ClientMsgID != ack.ClientMsgID ||
		msg.SenderID != ack.SenderID || msg.SenderType != ack.SenderType || msg.ConversationID != ack.ConversationID ||
		msg.ConversationSeq != ack.ConversationSeq || msg.ServerTime != ack.ServerTime {
		t.Fatal("native WSS frame did not preserve the committed publisher and message identity")
	}
	requireActorField(t, raw, ack.SenderType)
	return raw
}

func actorDelta(t *testing.T, f *fixture, gateway *process, token, id string) []json.RawMessage {
	t.Helper()
	reply := f.call(gateway, http.MethodPost, "/api/v1/sync/messages", token, sendFixtureJSON(t, map[string]string{
		"conversationId": id, "afterSeq": "0",
	}), nil)
	requireStatus(t, reply, http.StatusOK)
	var page struct {
		HistoryAfterSeq string            `json:"historyAfterSeq"`
		LatestSeq       string            `json:"latestSeq"`
		Items           []json.RawMessage `json:"items"`
		HasNext         bool              `json:"hasNext"`
		NextAfterSeq    *string           `json:"nextAfterSeq"`
	}
	decoder := json.NewDecoder(bytes.NewReader(reply.body))
	decoder.DisallowUnknownFields()
	must(t, decoder.Decode(&page))
	if page.HistoryAfterSeq != "0" || page.LatestSeq != "4" || page.HasNext ||
		page.NextAfterSeq == nil || *page.NextAfterSeq != "4" {
		t.Fatal("actor fixture delta did not return the expected authoritative four-message window")
	}
	return page.Items
}
