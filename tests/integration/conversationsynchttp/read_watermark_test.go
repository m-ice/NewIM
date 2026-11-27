//go:build integration

package conversationsynchttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const readWatermarkRecoveryFile = "/tmp/newim-read-watermark-recovery.json"

type watermarkView struct {
	ConversationID  string     `json:"conversationId"`
	LastReadSeq     string     `json:"lastReadSeq"`
	HistoryAfterSeq string     `json:"historyAfterSeq"`
	LatestSeq       string     `json:"latestSeq"`
	UpdatedAt       *time.Time `json:"updatedAt"`
}

func (f *fixture) watermarkGateway(overrides map[string]string) *process {
	f.t.Helper()
	settings := map[string]string{"NEWIM_READ_WATERMARK_HTTP": "1"}
	for key, value := range overrides {
		settings[key] = value
	}
	return f.gateway("", settings)
}

func watermarkPath(conversation string) string {
	return "/api/v1/conversations/" + conversation + "/read"
}

func watermarkBody(t *testing.T, seq string) []byte {
	t.Helper()
	return encode(t, map[string]string{"lastReadSeq": seq})
}

func getWatermark(t *testing.T, f *fixture, gateway *process, token, id string) watermarkView {
	t.Helper()
	return decodeWatermark(t, f.call(gateway, http.MethodGet, watermarkPath(id), token, nil, nil))
}

func advanceWatermark(t *testing.T, f *fixture, gateway *process, token, id, seq string) watermarkView {
	t.Helper()
	return decodeWatermark(t, f.call(gateway, http.MethodPost, watermarkPath(id), token, watermarkBody(t, seq), nil))
}

func decodeWatermark(t *testing.T, response response) watermarkView {
	t.Helper()
	requireStatus(t, response, http.StatusOK)
	if !response.verified || response.headers.Get("Content-Type") != "application/json; charset=utf-8" ||
		response.headers.Get("Cache-Control") != "no-store" || response.headers.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("read watermark response did not preserve verified TLS and the required headers")
	}
	var fields map[string]json.RawMessage
	must(t, json.Unmarshal(response.body, &fields))
	if len(fields) != 5 || fields["updatedAt"] == nil {
		t.Fatal("read watermark response did not contain its exact five fields")
	}
	var got watermarkView
	decoder := json.NewDecoder(bytes.NewReader(response.body))
	decoder.DisallowUnknownFields()
	must(t, decoder.Decode(&got))
	for _, seq := range []string{got.LastReadSeq, got.HistoryAfterSeq, got.LatestSeq} {
		n, err := strconv.ParseInt(seq, 10, 64)
		if err != nil || n < 0 || strconv.FormatInt(n, 10) != seq {
			t.Fatal("read watermark response returned a noncanonical sequence")
		}
	}
	return got
}

func requireWatermark(t *testing.T, got watermarkView, id, seq, floor, tail string, updated bool) {
	t.Helper()
	if got.ConversationID != id || got.LastReadSeq != seq || got.HistoryAfterSeq != floor ||
		got.LatestSeq != tail || (got.UpdatedAt != nil) != updated {
		t.Fatalf("watermark of %q got %q/%q/%q updated=%t, want %q/%q/%q updated=%t",
			got.ConversationID, got.LastReadSeq, got.HistoryAfterSeq, got.LatestSeq, got.UpdatedAt != nil, seq, floor, tail, updated)
	}
}

func requireSameWatermarkTime(t *testing.T, a, b watermarkView) {
	t.Helper()
	if (a.UpdatedAt == nil) != (b.UpdatedAt == nil) || (a.UpdatedAt != nil && !a.UpdatedAt.Equal(*b.UpdatedAt)) {
		t.Fatal("stale or equal request changed the last actual advancement time")
	}
}

func setupWatermarkDirect(t *testing.T, f *fixture, gateway *process, prefix string, messages int) (string, string, string, string) {
	t.Helper()
	owner, peer := prefix+"_owner", prefix+"_peer"
	loopback := f.server("", nil)
	provision(t, f, loopback, owner)
	provision(t, f, loopback, peer)
	phone, tablet, reader := f.admit(owner, "phone"), f.admit(owner, "tablet"), f.admit(peer, "phone")
	direct := f.call(gateway, http.MethodPost, "/api/v1/conversations/direct", phone, encode(t, map[string]string{"peerUserId": peer}), nil)
	requireStatus(t, direct, http.StatusOK)
	var got map[string]string
	must(t, json.Unmarshal(direct.body, &got))
	id := got["conversationId"]
	if len(got) != 1 || id == "" {
		t.Fatal("direct route did not return an established conversation")
	}
	for i := 1; i <= messages; i++ {
		ack := send(t, f, gateway, phone, id, prefix+"_msg_"+strconv.Itoa(i), "synthetic read watermark message")
		if ack.ConversationSeq != strconv.Itoa(i) {
			t.Fatal("fixture message was not committed at its expected sequence")
		}
	}
	return id, phone, tablet, reader
}

func TestReadWatermarkHTTPCheck(t *testing.T) {
	t.Run("two-gateways-and-monotonic-concurrent-writes", testWatermarkCrossGateway)
	t.Run("history-floor-and-rejoin", testWatermarkHistory)
}

func testWatermarkCrossGateway(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(15)
	left, right := f.watermarkGateway(nil), f.watermarkGateway(nil)
	const owner = "watermark_check_owner"
	id, phone, tablet, other := setupWatermarkDirect(t, f, left, "watermark_check", 5)
	requireWatermark(t, getWatermark(t, f, right, tablet, id), id, "0", "0", "5", false)
	requireWatermark(t, advanceWatermark(t, f, left, phone, id, "0"), id, "0", "0", "5", false)
	var count int
	must(t, f.db.QueryRow(f.ctx, "SELECT count(*) FROM newim.im_read_states WHERE conversation_id=$1 AND user_id=$2", id, owner).Scan(&count))
	if count != 0 {
		t.Fatal("reading or marking an implicit zero created a persistent row")
	}
	first := advanceWatermark(t, f, left, phone, id, "1")
	requireWatermark(t, first, id, "1", "0", "5", true)
	requireSameWatermarkTime(t, first, getWatermark(t, f, right, tablet, id))

	start := make(chan struct{})
	type attempt struct {
		minimum int
		reply   response
	}
	responses := make(chan attempt, 4)
	for i, seq := range []int{5, 1, 1, 3} {
		gateway, token := left, phone
		if i%2 == 0 {
			gateway, token = right, tablet
		}
		body := watermarkBody(t, strconv.Itoa(seq))
		go func() {
			<-start
			responses <- attempt{minimum: seq, reply: f.call(gateway, http.MethodPost, watermarkPath(id), token, body, nil)}
		}()
	}
	close(start)
	for range 4 {
		call := <-responses
		result := decodeWatermark(t, call.reply)
		got, _ := strconv.Atoi(result.LastReadSeq)
		if got < call.minimum || got > 5 || result.UpdatedAt == nil || result.UpdatedAt.Before(*first.UpdatedAt) {
			t.Fatal("concurrent marking returned a regressed or unauthorized sequence")
		}
	}
	final := getWatermark(t, f, left, phone, id)
	requireWatermark(t, final, id, "5", "0", "5", true)
	for _, seq := range []string{"0", "3", "5"} {
		stale := advanceWatermark(t, f, right, tablet, id, seq)
		requireWatermark(t, stale, id, "5", "0", "5", true)
		requireSameWatermarkTime(t, final, stale)
	}
	requireWatermark(t, getWatermark(t, f, right, other, id), id, "0", "0", "5", false)
	requireWatermark(t, advanceWatermark(t, f, left, other, id, "2"), id, "2", "0", "5", true)
	requireSameWatermarkTime(t, final, getWatermark(t, f, right, tablet, id))

	// A read or mark must work from the conversation tail even if message rows cannot be accessed.
	holder, err := f.db.Begin(f.ctx)
	must(t, err)
	defer holder.Rollback(f.ctx)
	_, err = holder.Exec(f.ctx, "LOCK TABLE newim.im_messages IN ACCESS EXCLUSIVE MODE")
	must(t, err)
	requireWatermark(t, getWatermark(t, f, left, phone, id), id, "5", "0", "5", true)
	requireWatermark(t, advanceWatermark(t, f, right, other, id, "4"), id, "4", "0", "5", true)
	must(t, holder.Commit(f.ctx))
	left.requireRedacted(t, phone, tablet, other, testDSN, "synthetic read watermark message")
	right.requireRedacted(t, phone, tablet, other, testDSN)
}

func testWatermarkHistory(t *testing.T) {
	f := openFixture(t)
	left, right := f.watermarkGateway(nil), f.watermarkGateway(nil)
	loopback := f.server("", nil)
	const id = "watermark_closed_history"
	const owner = "watermark_history_owner"
	const user = "watermark_history_member"
	const outsider = "watermark_history_outsider"
	for _, name := range []string{owner, user, outsider} {
		provision(t, f, loopback, name)
	}
	host, phone, guest := f.admit(owner, "phone"), f.admit(user, "phone"), f.admit(outsider, "phone")

	const legacy = "watermark_legacy_group"
	f.sql("INSERT INTO newim.im_conversations(conversation_id,kind) VALUES($1,'group')", legacy)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2),($1,$3)", legacy, owner, user)
	f.sql("SELECT newim.persist_message($1,$2,$3,$4,'text',1,$5,$6,$7)", owner, "watermark_legacy_client", legacy,
		"watermark_legacy_message", time.Now().UnixMilli(), encode(t, map[string]string{"text": "legacy history"}), "watermark_legacy_event")
	f.sql("INSERT INTO newim.im_read_states(conversation_id,user_id,read_seq) VALUES($1,$2,1)", legacy, user)
	requireWatermark(t, getWatermark(t, f, right, phone, legacy), legacy, "1", "0", "1", false)
	requireWatermark(t, advanceWatermark(t, f, left, phone, legacy, "1"), legacy, "1", "0", "1", false)
	requireStoredWatermark(t, f, legacy, user, 1, nil)

	createGroup(t, f, left, host, id, nil, false)
	for _, seq := range []string{"1", "2"} {
		send(t, f, left, host, id, id+"_"+seq, "before member joined")
	}
	join(t, f, left, phone, id, "watermark_first_join")
	requireWatermark(t, getWatermark(t, f, right, phone, id), id, "2", "2", "2", false)
	f.sql("INSERT INTO newim.im_read_states(conversation_id,user_id,read_seq) VALUES($1,$2,1)", id, user)
	stale := advanceWatermark(t, f, right, phone, id, "1")
	requireWatermark(t, stale, id, "2", "2", "2", false)
	requireStoredWatermark(t, f, id, user, 1, nil)
	send(t, f, left, host, id, id+"_3", "while member was in the group")
	requireWatermark(t, getWatermark(t, f, right, phone, id), id, "2", "2", "3", false)
	marked := advanceWatermark(t, f, right, phone, id, "3")
	requireWatermark(t, marked, id, "3", "2", "3", true)
	leave(t, f, left, phone, id)
	path := watermarkPath(id)
	departed := f.call(right, http.MethodGet, path, phone, nil, nil)
	absent := f.call(right, http.MethodGet, watermarkPath("watermark_missing"), host, nil, nil)
	outside := f.call(right, http.MethodGet, path, guest, nil, nil)
	for _, r := range []response{departed, absent, outside, f.call(right, http.MethodPost, path, phone, watermarkBody(t, "3"), nil)} {
		requireSyncError(t, r, http.StatusNotFound, "READ_NOT_FOUND")
		if !bytes.Equal(r.body, departed.body) {
			t.Fatal("read errors distinguished missing conversations, nonmembers and former members")
		}
	}

	send(t, f, left, host, id, id+"_4", "while member was absent")
	join(t, f, right, phone, id, "watermark_second_join")
	requireWatermark(t, getWatermark(t, f, left, phone, id), id, "4", "4", "4", false)
	requireWatermark(t, advanceWatermark(t, f, right, phone, id, "4"), id, "4", "4", "4", false)
	requireStoredWatermark(t, f, id, user, 3, marked.UpdatedAt)
	send(t, f, left, host, id, id+"_5", "after member rejoined")
	requireWatermark(t, advanceWatermark(t, f, right, phone, id, "5"), id, "5", "4", "5", true)

	// PostgreSQL can hold dates outside the response's RFC 3339 year range.
	badTime := time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
	f.sql("UPDATE newim.im_read_states SET updated_at=$3 WHERE conversation_id=$1 AND user_id=$2", id, user, badTime)
	requireCorruptWatermarkRefused(t, f, right, phone, id)
	requireStoredWatermark(t, f, id, user, 5, &badTime)

	// Rows from earlier writers can legally exist in SQL above the current tail.
	f.sql("UPDATE newim.im_read_states SET read_seq=6,updated_at=NULL WHERE conversation_id=$1 AND user_id=$2", id, user)
	requireCorruptWatermarkRefused(t, f, right, phone, id)
	requireStoredWatermark(t, f, id, user, 6, nil)
	f.sql("UPDATE newim.im_read_states SET read_seq=3 WHERE conversation_id=$1 AND user_id=$2", id, user)
	f.sql("UPDATE newim.im_conversation_members SET history_after_seq=7 WHERE conversation_id=$1 AND user_id=$2", id, user)
	requireCorruptWatermarkRefused(t, f, right, phone, id)
	requireStoredWatermark(t, f, id, user, 3, nil)
}

func requireCorruptWatermarkRefused(t *testing.T, f *fixture, gateway *process, token, id string) {
	t.Helper()
	requireSyncError(t, f.call(gateway, http.MethodGet, watermarkPath(id), token, nil, nil), 503, "READ_STORAGE_UNAVAILABLE")
	requireSyncError(t, f.call(gateway, http.MethodPost, watermarkPath(id), token, watermarkBody(t, "5"), nil), 503, "READ_STORAGE_UNAVAILABLE")
}

func requireStoredWatermark(t *testing.T, f *fixture, id, user string, seq int64, when *time.Time) {
	t.Helper()
	var value int64
	var updated *time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT read_seq,updated_at FROM newim.im_read_states WHERE conversation_id=$1 AND user_id=$2", id, user).
		Scan(&value, &updated))
	if value != seq {
		t.Fatalf("persisted read watermark changed to %d; expected %d", value, seq)
	}
	requireSameWatermarkTime(t, watermarkView{UpdatedAt: when}, watermarkView{UpdatedAt: updated})
}

func TestReadWatermarkHTTPSecurity(t *testing.T) {
	t.Run("strict-request-and-separate-budgets", testWatermarkHTTPPolicy)
	t.Run("locked-revocation-and-expiry", testWatermarkCredentialRaces)
}

func testWatermarkHTTPPolicy(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(15)
	gateway := f.watermarkGateway(nil)
	off := f.gateway("", nil)
	id, owner, tablet, peer := setupWatermarkDirect(t, f, gateway, "watermark_security", 1)
	path, body := watermarkPath(id), watermarkBody(t, "1")
	requireAbsent(t, f.call(off, http.MethodGet, path, owner, nil, nil))
	requireAbsent(t, f.call(off, http.MethodPost, path, owner, body, nil))
	requireAbsent(t, f.call(gateway, http.MethodGet, path+"/", owner, nil, nil))
	requireSyncError(t, f.call(gateway, http.MethodGet, path, "", nil, nil), 401, "AUTH_INVALID_TOKEN")
	requireSyncError(t, f.call(gateway, http.MethodPost, path, "", body,
		http.Header{"Authorization": {"Bearer " + owner, "Bearer " + owner}}), 401, "AUTH_INVALID_TOKEN")
	method := f.call(gateway, http.MethodPut, path, owner, body, nil)
	requireSyncError(t, method, 405, "HTTP_METHOD_NOT_ALLOWED")
	if method.headers.Get("Allow") != "GET, POST" {
		t.Fatal("wrong read HTTP Allow header")
	}

	requireSyncError(t, f.call(gateway, http.MethodGet, path+"?x=1", owner, nil, nil), 400, "READ_INVALID_INPUT")
	requireSyncError(t, f.call(gateway, http.MethodGet, path, owner, body, nil), 400, "READ_INVALID_INPUT")
	requireSyncError(t, f.call(gateway, http.MethodPost, path, owner, body, http.Header{"Content-Type": {"text/plain"}}),
		415, "HTTP_UNSUPPORTED_MEDIA_TYPE")
	requireSyncError(t, f.call(gateway, http.MethodPost, path, owner, bytes.Repeat([]byte{' '}, 4097), nil),
		413, "HTTP_BODY_TOO_LARGE")
	chunked, err := http.NewRequestWithContext(f.ctx, http.MethodPost, gateway.url+path,
		io.NopCloser(bytes.NewReader(bytes.Repeat([]byte{' '}, 4097))))
	must(t, err)
	chunked.Header.Set("Content-Type", "application/json")
	chunked.Header.Set("Authorization", "Bearer "+owner)
	requireSyncError(t, f.execute(chunked), 413, "HTTP_BODY_TOO_LARGE")
	for _, input := range []string{
		"null", "{}", `{"lastReadSeq":null}`, `{"lastReadSeq":1}`, `{"lastReadSeq":"1","userId":"other"}`,
		`{"lastReadSeq":"0","lastReadSe\u0071":"1"}`, `{"lastReadSeq":"1"} {}`,
		`{"lastReadSeq":"01"}`, `{"lastReadSeq":"+1"}`, `{"lastReadSeq":"-0"}`, `{"lastReadSeq":"-1"}`,
		`{"lastReadSeq":"9223372036854775808"}`, `{"lastReadSeq":"2"}`,
	} {
		requireSyncError(t, f.call(gateway, http.MethodPost, path, owner, []byte(input), nil), 400, "READ_INVALID_INPUT")
	}
	requireWatermark(t, getWatermark(t, f, gateway, owner, id), id, "0", "0", "1", false)

	account := f.watermarkGateway(map[string]string{"NEWIM_READ_WATERMARK_ACCOUNT_ATTEMPTS_PER_MINUTE": "1"})
	address := f.watermarkGateway(map[string]string{"NEWIM_READ_WATERMARK_IP_ATTEMPTS_PER_MINUTE": "1"})
	waitRateWindow()
	requireWatermark(t, advanceWatermark(t, f, account, owner, id, "1"), id, "1", "0", "1", true)
	requireSyncError(t, f.call(account, http.MethodGet, path, tablet, nil, nil), 429, "READ_RATE_LIMITED")
	requireWatermark(t, getWatermark(t, f, account, peer, id), id, "0", "0", "1", false)
	requireWatermark(t, getWatermark(t, f, address, owner, id), id, "1", "0", "1", true)
	requireSyncError(t, f.call(address, http.MethodGet, path, peer, nil, http.Header{"X-Forwarded-For": {"192.0.2.1"}}),
		429, "READ_RATE_LIMITED")
	send(t, f, address, owner, id, "watermark_budget_send", "message endpoint has a separate budget")
	gateway.requireRedacted(t, owner, tablet, peer, testDSN)
}

func testWatermarkCredentialRaces(t *testing.T) {
	f := openFixture(t)
	gateway := f.watermarkGateway(nil)
	const user = "watermark_credential_owner"
	id, revoking, expiring, _ := setupWatermarkDirect(t, f, gateway, "watermark_credential", 3)
	marked := advanceWatermark(t, f, gateway, revoking, id, "1")
	requireWatermark(t, marked, id, "1", "0", "3", true)

	holder, err := pgx.Connect(f.ctx, testDSN)
	must(t, err)
	t.Cleanup(func() { _ = holder.Close(context.Background()) })
	revokeID, err := f.auth.AuthenticateBearer(f.ctx, revoking)
	must(t, err)
	resume, result := f.pauseWatermarkPOST(gateway, revoking, id, "2")
	ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
	defer cancel()
	defer resume()
	lock, err := holder.Begin(ctx)
	must(t, err)
	defer lock.Rollback(f.ctx)
	_, err = lock.Exec(ctx, "SELECT session_id FROM newim.im_sessions WHERE session_id=$1 FOR UPDATE", revokeID.SessionID())
	must(t, err)
	resume()
	f.waitForWatermarkLock(holder, "newim.im_sessions", result)
	_, err = lock.Exec(ctx, "UPDATE newim.im_auth_tokens SET revoked_at=clock_timestamp() WHERE token_id=$1", revokeID.TokenID())
	must(t, err)
	must(t, lock.Commit(ctx))
	requireSyncError(t, <-result, 401, "AUTH_INVALID_TOKEN")
	requireStoredWatermark(t, f, id, user, 1, marked.UpdatedAt)

	expiryID, err := f.auth.AuthenticateBearer(f.ctx, expiring)
	must(t, err)
	lock, err = holder.Begin(f.ctx)
	must(t, err)
	defer lock.Rollback(f.ctx)
	_, err = lock.Exec(f.ctx, "SELECT read_seq FROM newim.im_read_states WHERE conversation_id=$1 AND user_id=$2 FOR UPDATE", id, user)
	must(t, err)
	// Give both initial authentication and the final transaction the same expiry.
	// This deadline fits inside the repository's row-lock timeout.
	f.sql("UPDATE newim.im_auth_tokens SET expires_at=clock_timestamp()+interval '850 milliseconds' WHERE token_id=$1", expiryID.TokenID())
	resume, result = f.pauseWatermarkPOST(gateway, expiring, id, "3")
	defer resume()
	resume()
	f.waitForWatermarkLock(holder, "newim.im_read_states", result)
	f.sql(`SELECT pg_sleep(GREATEST(EXTRACT(EPOCH FROM expires_at-clock_timestamp()),0)+0.002)
		FROM newim.im_auth_tokens WHERE token_id=$1`, expiryID.TokenID())
	must(t, lock.Commit(f.ctx))
	requireSyncError(t, <-result, 401, "AUTH_INVALID_TOKEN")
	requireStoredWatermark(t, f, id, user, 1, marked.UpdatedAt)
	fresh := f.admit(user, "new-device")
	requireWatermark(t, getWatermark(t, f, gateway, fresh, id), id, "1", "0", "3", true)
}

func (f *fixture) pauseWatermarkPOST(gateway *process, token, id, seq string) (func(), <-chan response) {
	f.t.Helper()
	raw := watermarkBody(f.t, seq)
	reader, writer := io.Pipe()
	release := make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	f.t.Cleanup(resume)
	go func() {
		<-release
		_, _ = writer.Write(raw)
		_ = writer.Close()
	}()
	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, gateway.url+watermarkPath(id), reader)
	must(f.t, err)
	req.ContentLength = int64(len(raw))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Expect", "100-continue")
	started := make(chan struct{})
	trace := &httptrace.ClientTrace{Got100Continue: func() { close(started) }}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	result := make(chan response, 1)
	go func() { result <- f.execute(req) }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	select {
	case <-started:
		return resume, result
	case r := <-result:
		f.t.Fatalf("read POST finished before authentication barrier: status=%d err=%v", r.status, r.err)
	case <-deadline.C:
		f.t.Fatal("read POST did not start reading the authenticated body")
	}
	return nil, nil
}

func (f *fixture) waitForWatermarkLock(holder *pgx.Conn, queryPart string, result <-chan response) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
	defer cancel()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	backend := int32(holder.PgConn().PID())
	for {
		var waiting bool
		must(f.t, f.db.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity WHERE application_name='newim-gateway-read'
			AND wait_event_type='Lock' AND strpos(query,$2)>0 AND $1=ANY(pg_blocking_pids(pid))
		)`, backend, queryPart).Scan(&waiting))
		if waiting {
			return
		}
		select {
		case r := <-result:
			f.t.Fatalf("read POST completed before the expected SQL row lock: status=%d err=%v", r.status, r.err)
		case <-ctx.Done():
			f.t.Fatal("read transaction did not reach the expected SQL row lock")
		case <-tick.C:
		}
	}
}

type watermarkRecovery struct {
	Conversation string
	Device       string
	UpdatedAt    time.Time
	Postmaster   time.Time
}

func TestReadWatermarkHTTPRecovery(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(15)
	switch os.Getenv("NEWIM_SYNC_HTTP_PHASE") {
	case "prepare":
		gateway := f.watermarkGateway(nil)
		id, phone, tablet, _ := setupWatermarkDirect(t, f, gateway, "watermark_recovery", 3)
		value := advanceWatermark(t, f, gateway, phone, id, "2")
		requireWatermark(t, value, id, "2", "0", "3", true)
		save := watermarkRecovery{Conversation: id, Device: tablet, UpdatedAt: *value.UpdatedAt, Postmaster: watermarkPostmaster(t, f)}
		must(t, os.WriteFile(readWatermarkRecoveryFile, encode(t, save), 0o600))
		gateway.requireRedacted(t, phone, tablet, testDSN)
	case "restart":
		raw, err := os.ReadFile(readWatermarkRecoveryFile)
		must(t, err)
		t.Cleanup(func() { _ = os.Remove(readWatermarkRecoveryFile) })
		var saved watermarkRecovery
		must(t, json.Unmarshal(raw, &saved))
		if saved.Postmaster.IsZero() || !watermarkPostmaster(t, f).After(saved.Postmaster) {
			t.Fatal("read recovery did not start a new PostgreSQL postmaster")
		}
		first, second := f.watermarkGateway(nil), f.watermarkGateway(nil)
		value := getWatermark(t, f, first, saved.Device, saved.Conversation)
		requireWatermark(t, value, saved.Conversation, "2", "0", "3", true)
		requireSameWatermarkTime(t, value, watermarkView{UpdatedAt: &saved.UpdatedAt})
		retry := advanceWatermark(t, f, second, saved.Device, saved.Conversation, "1")
		requireWatermark(t, retry, saved.Conversation, "2", "0", "3", true)
		requireSameWatermarkTime(t, retry, value)
		next := advanceWatermark(t, f, second, saved.Device, saved.Conversation, "3")
		requireWatermark(t, next, saved.Conversation, "3", "0", "3", true)
		if !next.UpdatedAt.After(saved.UpdatedAt) {
			t.Fatal("advancement after restart did not record the new update")
		}
		requireSameWatermarkTime(t, next, getWatermark(t, f, first, saved.Device, saved.Conversation))
	default:
		t.Fatal("NEWIM_SYNC_HTTP_PHASE must be prepare or restart")
	}
}

func watermarkPostmaster(t *testing.T, f *fixture) time.Time {
	t.Helper()
	var started time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&started))
	return started
}
