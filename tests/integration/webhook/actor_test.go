//go:build integration

package webhook_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	app "github.com/m-ice/NewIM/server/webhook"
)

const legacyWebhookHuman = "webhook_actor_legacy_human"

type webhookActorMessage struct {
	sender  string
	kind    protocol.SenderType
	compact bool
}

type persistedActorWebhook struct {
	message protocol.Message
	compact bool
}

func TestWebhookActorLegacySchema(t *testing.T) {
	f := openFixture(t)
	if f.scalarInt64(`SELECT (to_regclass('newim.im_user_actor_types') IS NULL)::int`) != 1 {
		t.Fatal("the pre-020 fixture already has an actor table")
	}
	f.sql(`INSERT INTO newim.im_users(user_id) VALUES($1)`, legacyWebhookHuman)
	checkActorWebhookDeliveries(f, "legacy", []webhookActorMessage{
		{sender: legacyWebhookHuman, kind: protocol.SenderHuman},
		{sender: legacyWebhookHuman, kind: protocol.SenderHuman, compact: true},
	})
}

func TestWebhookActorDelivery(t *testing.T) {
	// The shared endpoint fixture uses fixtureSeq in its IDs. The 019 phase
	// and the 020 phase run from separate processes against the same database.
	fixtureSeq.Add(100_000)
	f := openFixture(t)
	if f.scalarInt64(`SELECT count(*) FROM newim.im_users WHERE user_id=$1`, legacyWebhookHuman) != 1 {
		t.Fatal("the migration did not preserve the legacy account")
	}
	if f.scalarInt64(`SELECT count(*) FROM newim.im_user_actor_types WHERE user_id=$1`, legacyWebhookHuman) != 0 {
		t.Fatal("the legacy human should not need a classification row")
	}
	const bot = "webhook_actor_current_bot"
	const system = "webhook_actor_current_system"
	const agent = "webhook_actor_current_agent"
	f.sql(`INSERT INTO newim.im_users(user_id) VALUES($1),($2),($3)`, bot, system, agent)
	f.sql(`INSERT INTO newim.im_user_actor_types(user_id,actor_type)
		VALUES ($1,'BOT'),($2,'SYSTEM'),($3,'AI_AGENT')`, bot, system, agent)
	checkActorWebhookDeliveries(f, "current", []webhookActorMessage{
		{sender: legacyWebhookHuman, kind: protocol.SenderHuman},
		{sender: bot, kind: protocol.SenderBot},
		{sender: system, kind: protocol.SenderSystem},
		{sender: agent, kind: protocol.SenderAIAgent, compact: true},
	})
}

func checkActorWebhookDeliveries(f *fixture, phase string, messages []webhookActorMessage) {
	t := f.t
	t.Helper()
	prefix := "webhook_actor_" + phase
	conversation := f.id(prefix + "_room")
	f.sql(`INSERT INTO newim.im_conversations(conversation_id) VALUES($1)`, conversation)
	expected := make(map[string]persistedActorWebhook, len(messages))
	for i, spec := range messages {
		f.sql(`INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)
			ON CONFLICT(conversation_id,user_id) DO NOTHING`, conversation, spec.sender)
		event, message := persistActorWebhook(f, prefix, conversation, i+1, spec)
		expected[event] = persistedActorWebhook{message: message, compact: spec.compact}
	}

	type capture struct {
		method string
		header http.Header
		body   []byte
	}
	received := make(chan capture, len(expected)+1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, protocol.WebhookMaxEnvelopeBytes+1))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		select {
		case received <- capture{method: r.Method, header: r.Header.Clone(), body: body}:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusTooManyRequests)
		}
	}))
	defer server.Close()
	secret := []byte("0123456789abcdef0123456789abcdef")
	destination, master := f.insertEndpoint(1, server.URL, secret)
	client, err := app.NewSecureClient(nil, app.Policy{AllowHTTP: true, AllowLoopback: true}, time.Second, 64*1024)
	must(t, err)
	worker := f.worker(master, client)
	defer worker.Close()
	runWorkerUntil(t, worker, 5*time.Second, func() bool {
		return f.scalarInt64(`SELECT count(*) FROM newim.im_webhook_deliveries
			WHERE destination_id=$1 AND status='delivered'`, destination) == int64(len(expected))
	}, func() string {
		return f.scalarString(`SELECT COALESCE(string_agg(status||':'||COALESCE(last_error_code,''),','),'')
			FROM newim.im_webhook_deliveries WHERE destination_id=$1`, destination)
	})
	if len(received) != len(expected) {
		t.Fatalf("got %d HTTP deliveries, want %d", len(received), len(expected))
	}
	guard, err := protocol.NewMemoryWebhookReplayGuard(len(expected))
	must(t, err)
	for len(expected) != 0 {
		item := <-received
		if item.method != http.MethodPost {
			t.Fatalf("got HTTP method %q, want POST", item.method)
		}
		signed, err := protocol.ParseWebhookHeaders(map[string][]string(item.header))
		must(t, err)
		must(t, protocol.VerifyWebhookSignature(ctx, secret, signed, item.body, f.now, guard))
		want, ok := expected[signed.EventID]
		if !ok {
			t.Fatal("received an unexpected or duplicate signed event")
		}
		delivery := f.scalarString(`SELECT delivery_id FROM newim.im_webhook_deliveries
			WHERE event_id=$1 AND destination_id=$2`, signed.EventID, destination)
		if signed.DeliveryID != delivery {
			t.Fatal("signed delivery ID differs from its persisted row")
		}
		checkActorWebhookEnvelope(t, signed.EventID, item.body, want)
		delete(expected, signed.EventID)
	}
}

func persistActorWebhook(f *fixture, prefix, conversation string, sequence int, spec webhookActorMessage) (string, protocol.Message) {
	f.t.Helper()
	const payloadPrefix = `{"text":"integration actor payload","senderType":"SYSTEM","extension":"`
	const payloadSuffix = `"}`
	message := protocol.Message{
		ProtocolVersion: 1, Version: 1, Type: "text", SenderID: spec.sender, SenderType: spec.kind,
		ClientMsgID: f.id(prefix + "_client"), ServerMsgID: f.id(prefix + "_server"),
		ConversationID: conversation, ConversationSeq: strconv.Itoa(sequence), ServerTime: strconvMillis(f.now),
		Payload: json.RawMessage(payloadPrefix + payloadSuffix),
	}
	wire, err := protocol.Encode(message)
	must(f.t, err)
	if spec.compact {
		// Fit the persisted message at the normal protocol limit. Its Webhook
		// wrapper takes additional bytes and therefore uses the compact shape.
		padding := strings.Repeat("x", protocol.MaxBytes-len(wire))
		message.Payload = json.RawMessage(payloadPrefix + padding + payloadSuffix)
		wire, err = protocol.Encode(message)
		must(f.t, err)
		if len(wire) != protocol.MaxBytes {
			f.t.Fatal("compact fixture did not fill the persisted message limit")
		}
	}
	event := f.id(prefix + "_event")
	var payload []byte
	err = f.db.QueryRow(ctx, `SELECT sender_id, client_msg_id, server_msg_id, conversation_id,
		conversation_seq::text, server_time_ms::text, payload_bytes
		FROM newim.persist_message($1,$2,$3,$4,'text',1,$5,$6,$7)`, message.SenderID, message.ClientMsgID,
		message.ConversationID, message.ServerMsgID, message.ServerTime, []byte(message.Payload), event).
		Scan(&message.SenderID, &message.ClientMsgID, &message.ServerMsgID, &message.ConversationID,
			&message.ConversationSeq, &message.ServerTime, &payload)
	must(f.t, err)
	if message.SenderID != spec.sender || message.ConversationSeq != strconv.Itoa(sequence) {
		f.t.Fatal("the message fixture did not persist the expected sender or sequence")
	}
	message.Payload = payload
	return event, message
}

func checkActorWebhookEnvelope(t *testing.T, event string, body []byte, expected persistedActorWebhook) {
	t.Helper()
	envelope, err := protocol.DecodeWebhookEnvelope(body)
	must(t, err)
	var envelopeFields map[string]json.RawMessage
	must(t, json.Unmarshal(body, &envelopeFields))
	m := expected.message
	if len(envelopeFields) != 5 || envelope.EventID != event || envelope.EventType != "message.persisted" ||
		envelope.SchemaVersion != 1 || envelope.OccurredAt != m.ServerTime {
		t.Fatal("the signed envelope changed the persisted event identity or v1 shape")
	}
	want := map[string]any{
		"senderId": m.SenderID, "clientMsgId": m.ClientMsgID, "serverMsgId": m.ServerMsgID,
		"conversationId": m.ConversationID, "conversationSeq": m.ConversationSeq, "serverTime": m.ServerTime,
		"protocolVersion": m.ProtocolVersion, "version": m.Version, "type": m.Type, "payload": m.Payload,
	}
	if m.SenderType != protocol.SenderHuman {
		want["senderType"] = m.SenderType
	}
	if expected.compact {
		digest := sha256.Sum256(m.Payload)
		want["payload"] = json.RawMessage(`{}`)
		want["payloadOmitted"] = true
		want["payloadSha256"] = hex.EncodeToString(digest[:])
		want["payloadSize"] = strconv.Itoa(len(m.Payload))
	}
	wantJSON, err := json.Marshal(want)
	must(t, err)
	var gotFields, wantFields map[string]json.RawMessage
	must(t, json.Unmarshal(wantJSON, &wantFields))
	must(t, json.Unmarshal(envelope.Payload, &gotFields))
	if len(gotFields) != len(wantFields) {
		t.Errorf("signed payload field count = %d, want %d for %s compact=%t", len(gotFields), len(wantFields), m.SenderType, expected.compact)
	}
	for field, value := range wantFields {
		if !bytes.Equal(gotFields[field], value) {
			t.Errorf("signed field %s differs from persisted message for %s compact=%t", field, m.SenderType, expected.compact)
		}
	}
}
