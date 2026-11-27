//go:build integration

package messagedeltahttp_test

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestMessageDeltaHTTPSecurity(t *testing.T) {
	t.Run("request-and-membership-boundaries", testDeltaAuthorization)
	t.Run("missing-visible-tail-fails-closed", testMissingVisibleTail)
	t.Run("conversation-sync-budget-is-shared", testSharedReadBudget)
}

func testMemberWindows(t *testing.T) {
	f := openFixture(t)
	const owner = "delta_http_window_owner"
	const user = "delta_http_window_joiner"
	f.user(owner)
	f.user(user)
	host := f.admit(owner, "phone")
	member := f.admit(user, "tablet")
	gateway := f.syncGateway(nil)
	const id = "delta_http_no_history"
	createGroup(t, f, gateway, host, id, nil, false)
	send(t, f, gateway, host, id, "window_hidden_first", "before member joined")
	join(t, f, gateway, member, id, "window_first_join")
	expectRange(t, readMessages(t, f, gateway, member, id, 0, 0), id, 1, 1, false)
	target := send(t, f, gateway, host, id, "window_visible", "after member joined")
	visible := readMessages(t, f, gateway, member, id, 0, 1)
	expectRange(t, visible, id, 1, 2, false, 2)
	if got := messageFrame(t, visible.Items[0]); got.ServerMsgID != target.ServerMsgID {
		t.Fatal("history lower bound did not begin at the first authorized message")
	}
	leave(t, f, gateway, member, id)
	send(t, f, gateway, host, id, "window_after_leave", "after departure")
	expectRange(t, readMessages(t, f, gateway, member, id, 0, 0), id, 1, 2, false, 2)
	expectRange(t, readMessages(t, f, gateway, member, id, 2, 0), id, 1, 2, false)
	requireError(t, f.call(gateway, http.MethodPost, deltaPath, member, deltaBody(t, id, 3, 0), nil), 400, "SYNC_INVALID_CURSOR")
	join(t, f, gateway, member, id, "window_rejoin")
	expectRange(t, readMessages(t, f, gateway, member, id, 0, 0), id, 3, 3, false)
	send(t, f, gateway, host, id, "window_after_rejoin", "after rejoin")
	expectRange(t, readMessages(t, f, gateway, member, id, 2, 0), id, 3, 4, false, 4)

	const empty = "delta_http_zero_departure"
	createGroup(t, f, gateway, host, empty, []string{user}, true)
	leave(t, f, gateway, member, empty)
	send(t, f, gateway, host, empty, "window_after_zero", "after zero departure")
	expectRange(t, readMessages(t, f, gateway, member, empty, 0, 0), empty, 0, 0, false)
	requireError(t, f.call(gateway, http.MethodPost, deltaPath, member, deltaBody(t, empty, 1, 0), nil), 400, "SYNC_INVALID_CURSOR")
}

func testMissingVisibleTail(t *testing.T) {
	f := openFixture(t)
	const owner = "delta_http_gap_owner"
	const reader = "delta_http_gap_reader"
	const id = "delta_http_former_visible_gap"
	f.user(owner)
	f.user(reader)
	host := f.admit(owner, "phone")
	member := f.admit(reader, "tablet")
	legacy := f.gateway("", nil)
	gateway := f.syncGateway(nil)
	createGroup(t, f, legacy, host, id, []string{reader}, true)
	seedGapMessages(t, f, owner, id, []string{"gap_item_first", "gap_item_tail"})
	leave(t, f, legacy, member, id)
	seedGapMessages(t, f, owner, id, []string{"gap_item_current"})
	expectRange(t, readMessages(t, f, gateway, member, id, 0, 1), id, 0, 2, true, 1)
	expectRange(t, readMessages(t, f, gateway, member, id, 1, 1), id, 0, 2, false, 2)
	f.sql("DELETE FROM newim.im_outbox_events WHERE server_msg_id='gap_item_tail_server'")
	f.sql("DELETE FROM newim.im_messages WHERE server_msg_id='gap_item_tail_server'")
	var physical int64
	var pointer string
	must(t, f.db.QueryRow(f.ctx, "SELECT last_seq,latest_server_msg_id FROM newim.im_conversations WHERE conversation_id=$1", id).Scan(&physical, &pointer))
	if physical != 3 || pointer != "gap_item_current_server" {
		t.Fatal("fixture did not preserve the later physical head")
	}
	for _, after := range []int64{0, 1} {
		requireError(t, f.call(gateway, http.MethodPost, deltaPath, member, deltaBody(t, id, after, 1), nil), 503, "SYNC_STORAGE_UNAVAILABLE")
	}
	gateway.requireRedacted(t, host, member, testDSN, "gap_item_tail_server", "missing tail fixture")
}

func seedGapMessages(t *testing.T, f *fixture, user, conversation string, ids []string) {
	t.Helper()
	for _, id := range ids {
		f.sql("SELECT newim.persist_message($1,$2,$3,$4,'text',1,$5,$6,$7)",
			user, id, conversation, id+"_server", time.Now().UnixMilli(), []byte(`{"text":"missing tail fixture"}`), id+"_event")
	}
}

func testDeltaAuthorization(t *testing.T) {
	f := openFixture(t)
	const owner = "delta_http_security_owner"
	const other = "delta_http_security_other"
	const id = "delta_http_security_conversation"
	f.user(owner)
	f.user(other)
	host := f.admit(owner, "device")
	outsider := f.admit(other, "device")
	enabled, disabled := f.syncGateway(nil), f.gateway("1", nil)
	createGroup(t, f, enabled, host, id, nil, true)
	send(t, f, enabled, host, id, "delta_security_send", "one valid authorized frame")
	body := deltaBody(t, id, 0, 0)
	off := f.call(disabled, http.MethodPost, deltaPath, host, body, nil)
	if off.err != nil || off.status != 404 || !off.verified {
		t.Fatal("message delta route must be absent without conversation sync enablement")
	}
	for _, credential := range []string{"", "invalid_synthetic_token", f.issuerKey} {
		requireError(t, f.call(enabled, http.MethodPost, deltaPath, credential, body, nil), 401, "AUTH_INVALID_TOKEN")
	}
	requireError(t, f.call(enabled, http.MethodPost, deltaPath, "", body, http.Header{"Authorization": {"Bearer " + host, "Bearer " + host}}), 401, "AUTH_INVALID_TOKEN")
	requireError(t, f.call(enabled, http.MethodPost, deltaPath, outsider, body, nil), 403, "SYNC_FORBIDDEN")
	requireError(t, f.call(enabled, http.MethodPost, deltaPath, host, deltaBody(t, "delta_http_absent_conversation", 0, 0), nil), 403, "SYNC_FORBIDDEN")
	deny := f.call(enabled, http.MethodGet, deltaPath, host, nil, nil)
	requireError(t, deny, 405, "HTTP_METHOD_NOT_ALLOWED")
	if deny.headers.Get("Allow") != "POST" {
		t.Fatal("method rejection did not include allowed POST")
	}
	requireError(t, f.call(enabled, http.MethodPost, deltaPath+"?", host, body, nil), 400, "SYNC_INVALID_INPUT")
	requireError(t, f.call(enabled, http.MethodPost, deltaPath, host, body, http.Header{"Content-Type": {"text/plain"}}), 415, "HTTP_UNSUPPORTED_MEDIA_TYPE")
	requireError(t, f.call(enabled, http.MethodPost, deltaPath, host, bytes.Repeat([]byte{' '}, 4097), nil), 413, "HTTP_BODY_TOO_LARGE")
	stream := io.NopCloser(bytes.NewReader(bytes.Repeat([]byte{' '}, 4097)))
	chunked, err := http.NewRequestWithContext(f.ctx, http.MethodPost, enabled.url+deltaPath, stream)
	must(t, err)
	chunked.ContentLength = -1
	chunked.Header.Set("Authorization", "Bearer "+host)
	chunked.Header.Set("Content-Type", "application/json")
	requireError(t, f.execute(chunked), 413, "HTTP_BODY_TOO_LARGE")
	for _, alias := range []string{deltaPath + "/", "/api/v1/sync/%6dessages"} {
		reply := f.call(enabled, http.MethodPost, alias, host, body, nil)
		if reply.err != nil || reply.status != 404 || !reply.verified {
			t.Fatal("delta path alias was unexpectedly mounted")
		}
	}
	for _, test := range []struct{ name, raw, code string }{
		{"missing-cursor", `{"conversationId":"delta_http_security_conversation"}`, "SYNC_INVALID_INPUT"},
		{"empty-cursor", `{"conversationId":"delta_http_security_conversation","afterSeq":""}`, "SYNC_INVALID_INPUT"},
		{"numeric-cursor", `{"conversationId":"delta_http_security_conversation","afterSeq":0}`, "SYNC_INVALID_INPUT"},
		{"null-cursor", `{"conversationId":"delta_http_security_conversation","afterSeq":null}`, "SYNC_INVALID_INPUT"},
		{"actor-field", `{"conversationId":"delta_http_security_conversation","afterSeq":"0","userId":"delta_http_security_other"}`, "SYNC_INVALID_INPUT"},
		{"duplicate-cursor", `{"conversationId":"delta_http_security_conversation","afterSeq":"0","afterSe\u0071":"1"}`, "SYNC_INVALID_INPUT"},
		{"signed-checkpoint-field", `{"conversationId":"delta_http_security_conversation","checkpoint":"signed-elsewhere","afterSeq":"0"}`, "SYNC_INVALID_INPUT"},
		{"invalid-conversation-id", `{"conversationId":"../wrong","afterSeq":"0"}`, "SYNC_INVALID_INPUT"},
		{"null-limit", `{"conversationId":"delta_http_security_conversation","afterSeq":"0","limit":null}`, "SYNC_INVALID_INPUT"},
		{"noninteger-limit", `{"conversationId":"delta_http_security_conversation","afterSeq":"0","limit":1.5}`, "SYNC_INVALID_INPUT"},
		{"negative-limit", `{"conversationId":"delta_http_security_conversation","afterSeq":"0","limit":-1}`, "SYNC_LIMIT_EXCEEDED"},
		{"large-limit", `{"conversationId":"delta_http_security_conversation","afterSeq":"0","limit":101}`, "SYNC_LIMIT_EXCEEDED"},
		{"trailing-json", `{"conversationId":"delta_http_security_conversation","afterSeq":"0"} {}`, "SYNC_INVALID_INPUT"},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireError(t, f.call(enabled, http.MethodPost, deltaPath, host, []byte(test.raw), nil), 400, test.code)
		})
	}
	for _, after := range []string{"00", "+0", "-1", "1.0", " 0", "9223372036854775808", "2"} {
		requireError(t, f.call(enabled, http.MethodPost, deltaPath, host, encode(t, map[string]string{"conversationId": id, "afterSeq": after}), nil), 400, "SYNC_INVALID_CURSOR")
	}
	expectRange(t, parsePage(t, f.call(enabled, http.MethodPost, deltaPath, host, encode(t, map[string]any{"conversationId": id, "afterSeq": "0", "limit": 0}), nil)), id, 0, 1, false, 1)
	_, err = f.auth.RevokeBearer(f.ctx, host)
	must(t, err)
	requireError(t, f.call(enabled, http.MethodPost, deltaPath, host, body, nil), 401, "AUTH_INVALID_TOKEN")
	enabled.requireRedacted(t, host, outsider, f.issuerKey, testDSN, "one valid authorized frame")
}

func testSharedReadBudget(t *testing.T) {
	f := openFixture(t)
	const owner = "delta_http_limiter_owner"
	const id = "delta_http_shared_limiter"
	f.user(owner)
	host := f.admit(owner, "first")
	same := f.admit(owner, "second")
	gateway := f.syncGateway(map[string]string{"NEWIM_CONVERSATION_SYNC_ACCOUNT_ATTEMPTS_PER_MINUTE": "1"})
	createGroup(t, f, gateway, host, id, nil, true)
	remaining := time.Until(time.Now().Truncate(time.Minute).Add(time.Minute))
	if remaining < 3*time.Second {
		time.Sleep(remaining + 20*time.Millisecond)
	}
	expectRange(t, readMessages(t, f, gateway, host, id, 0, 0), id, 0, 0, false)
	requireError(t, f.call(gateway, http.MethodPost, "/api/v1/sync/bootstrap", same, []byte("{}"), nil), 429, "SYNC_RATE_LIMITED")
}

func requireError(t *testing.T, reply response, status int, code string) {
	t.Helper()
	reply.requireError(t, status, code)
	if !reply.verified {
		t.Fatal("message delta error lacked verified TLS")
	}
}
