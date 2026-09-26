//go:build integration

package messagehttp_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

func TestMessageHTTPCheck(t *testing.T) {
	if strings.TrimSpace(os.Getenv("NEWIM_SERVER_BINARY")) == "" {
		t.Fatal("NEWIM_SERVER_BINARY is required")
	}

	t.Run("flag-off-and-startup-fail-closed", func(t *testing.T) {
		f := openFixture(t)
		body := textBody(t, "flag_off_client", "flag_off_conversation", "must not persist")

		withoutDSN := startMessageServer(t, map[string]string{
			"NEWIM_MESSAGE_HTTP": "0",
			"NEWIM_AUTH_DSN":     "",
		})
		withoutDSN.waitHealthy(t)
		result := doRequest(t, http.MethodPost, messageURL(withoutDSN), body, unknownToken("flag_off"), nil)
		assertError(t, result, http.StatusNotFound, "HTTP_ROUTE_NOT_FOUND")
		if count := activeConnections(t, f, "newim-auth-http"); count != 0 {
			t.Fatalf("flag-off no-DSN auth pool connections=%d", count)
		}
		if count := activeConnections(t, f, "newim-message-http"); count != 0 {
			t.Fatalf("flag-off no-DSN message pool connections=%d", count)
		}
		withoutDSN.stop(t)

		flagOff := startMessageServer(t, map[string]string{
			"NEWIM_MESSAGE_HTTP":            "0",
			"NEWIM_AUTH_DSN":                localDSN,
			"NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1",
		})
		flagOff.waitHealthy(t)
		waitConnectionRange(t, f, "newim-auth-http", 1, 8, 5*time.Second)
		result = doRequest(t, http.MethodPost, messageURL(flagOff), body, unknownToken("flag_off_dsn"), nil)
		assertError(t, result, http.StatusNotFound, "HTTP_ROUTE_NOT_FOUND")
		if count := activeConnections(t, f, "newim-message-http"); count != 0 {
			t.Fatalf("flag-off message pool connections=%d", count)
		}
		flagOff.stop(t)
		waitNoConnections(t, f, "newim-auth-http", "newim-message-http")

		configs := []struct {
			name string
			env  map[string]string
			api  string
		}{
			{name: "missing-dsn", env: map[string]string{"NEWIM_MESSAGE_HTTP": "1", "NEWIM_AUTH_DSN": "", "NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1"}},
			{name: "malformed-dsn", env: map[string]string{"NEWIM_MESSAGE_HTTP": "1", "NEWIM_AUTH_DSN": "postgres://user:%zz@127.0.0.1/newim", "NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1"}},
			{name: "tcp-dsn", env: map[string]string{"NEWIM_MESSAGE_HTTP": "1", "NEWIM_AUTH_DSN": "host=127.0.0.1 user=newim_test dbname=newim_test sslmode=disable", "NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1"}},
			{name: "socket-opt-in-missing", env: map[string]string{"NEWIM_MESSAGE_HTTP": "1", "NEWIM_AUTH_DSN": localDSN, "NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "0"}},
			{name: "wildcard-listener", env: map[string]string{"NEWIM_MESSAGE_HTTP": "1", "NEWIM_AUTH_DSN": localDSN, "NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1"}, api: "0.0.0.0"},
		}
		for _, config := range configs {
			t.Run(config.name, func(t *testing.T) {
				apiAddr, opsAddr := twoFreeAddrs(t)
				if config.api == "0.0.0.0" {
					_, port, err := net.SplitHostPort(apiAddr)
					must(t, err)
					apiAddr = net.JoinHostPort("0.0.0.0", port)
				}
				result := runServerExpectExit(t, []string{"--api-addr", apiAddr, "--ops-addr", opsAddr}, config.env, 8*time.Second)
				if result.code == 0 || !strings.Contains(result.stderr, "SERVER_INVALID_MESSAGE_CONFIG") {
					t.Fatalf("invalid config exit=%d stdout=%q stderr=%q", result.code, result.stdout, result.stderr)
				}
				assertPortReusable(t, apiAddr)
				assertPortReusable(t, opsAddr)
				waitNoConnections(t, f, "newim-auth-http", "newim-message-http")
			})
		}

		t.Run("occupied-api-bind", func(t *testing.T) {
			held, err := net.Listen("tcp", "127.0.0.1:0")
			must(t, err)
			defer held.Close()
			_, opsAddr := twoFreeAddrs(t)
			result := runServerExpectExit(t, []string{"--api-addr", held.Addr().String(), "--ops-addr", opsAddr}, map[string]string{
				"NEWIM_MESSAGE_HTTP": "1", "NEWIM_AUTH_DSN": localDSN, "NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1",
			}, 8*time.Second)
			if result.code == 0 || !strings.Contains(result.stderr, "SERVER_BIND_FAILED") {
				t.Fatalf("occupied bind exit=%d stdout=%q stderr=%q", result.code, result.stdout, result.stderr)
			}
			waitNoConnections(t, f, "newim-auth-http", "newim-message-http")
		})
	})

	t.Run("persist-idempotency-conflict-and-head", func(t *testing.T) {
		f := openFixture(t)
		identity := f.seedIdentity("persist", seedOptions{})
		conversationID := f.seedConversation("persist", identity.UserID)
		server := startMessageServer(t, validMessageEnv())
		defer server.stop(t)
		server.waitHealthy(t)
		waitConnectionRange(t, f, "newim-auth-http", 1, 8, 5*time.Second)
		waitConnectionRange(t, f, "newim-message-http", 1, 8, 5*time.Second)

		body := textBody(t, "persist_client", conversationID, "persist once")
		first := doRequest(t, http.MethodPost, messageURL(server), body, identity.RawToken, nil)
		if first.status != http.StatusOK {
			t.Fatalf("first status=%d body=%q", first.status, first.body)
		}
		requireCommonHeaders(t, first.headers)
		ack := decodeACK(t, first.body)
		if ack.ClientMsgID != "persist_client" || ack.ConversationID != conversationID || ack.SenderID != identity.UserID {
			t.Fatalf("ack identity mismatch: %+v", ack)
		}

		var storedConversation, storedSender, storedClient, storedType string
		var storedSeq, storedTime int64
		var storedPayload []byte
		var protocolVersion, schemaVersion int
		must(t, f.db.QueryRow(ctx, `SELECT conversation_id,sender_id,client_msg_id,conversation_seq,
			server_time_ms,protocol_version,schema_version,message_type,payload_bytes
			FROM newim.im_messages WHERE server_msg_id=$1`, ack.ServerMsgID).Scan(
			&storedConversation, &storedSender, &storedClient, &storedSeq, &storedTime,
			&protocolVersion, &schemaVersion, &storedType, &storedPayload,
		))
		if storedConversation != ack.ConversationID || storedSender != ack.SenderID || storedClient != ack.ClientMsgID ||
			fmt.Sprint(storedSeq) != ack.ConversationSeq || fmt.Sprint(storedTime) != ack.ServerTime ||
			protocolVersion != 1 || schemaVersion != 1 || storedType != "text" || string(storedPayload) != `{"text":"persist once"}` {
			t.Fatalf("stored row mismatch ack=%+v seq=%d time=%d payload=%q", ack, storedSeq, storedTime, storedPayload)
		}
		if got := f.scalarInt("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID, "persist_client"); got != 1 {
			t.Fatalf("message count=%d", got)
		}
		if got := f.scalarInt("SELECT count(*) FROM newim.im_outbox_events WHERE server_msg_id=$1", ack.ServerMsgID); got != 1 {
			t.Fatalf("outbox count=%d", got)
		}
		if got := f.scalarString("SELECT latest_server_msg_id FROM newim.im_conversations WHERE conversation_id=$1", conversationID); got != ack.ServerMsgID {
			t.Fatalf("head=%q want %q", got, ack.ServerMsgID)
		}
		if got := f.scalarInt("SELECT last_seq FROM newim.im_conversations WHERE conversation_id=$1", conversationID); fmt.Sprint(got) != ack.ConversationSeq {
			t.Fatalf("last_seq=%d ack=%s", got, ack.ConversationSeq)
		}

		second := doRequest(t, http.MethodPost, messageURL(server), body, identity.RawToken, nil)
		if second.status != http.StatusOK || !bytes.Equal(first.body, second.body) {
			t.Fatalf("idempotent replay status=%d body=%q first=%q", second.status, second.body, first.body)
		}
		if got := f.scalarInt("SELECT count(*) FROM newim.im_outbox_events WHERE server_msg_id=$1", ack.ServerMsgID); got != 1 {
			t.Fatalf("idempotent outbox count=%d", got)
		}

		conflict := doRequest(t, http.MethodPost, messageURL(server), textBody(t, "persist_client", conversationID, "different"), identity.RawToken, nil)
		assertError(t, conflict, http.StatusConflict, "SEND_ID_CONFLICT")
		if got := f.scalarInt("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID, "persist_client"); got != 1 {
			t.Fatalf("conflict mutated rows=%d", got)
		}

		concurrentBody := textBody(t, "persist_concurrent_client", conversationID, "concurrent")
		const attempts = 16
		results := make(chan httpResult, attempts)
		var wait sync.WaitGroup
		for attempt := 0; attempt < attempts; attempt++ {
			wait.Add(1)
			go func() {
				defer wait.Done()
				results <- doRequestErr(http.MethodPost, messageURL(server), concurrentBody, identity.RawToken, nil)
			}()
		}
		wait.Wait()
		close(results)
		var concurrentACK []byte
		for result := range results {
			if result.err != nil || result.status != http.StatusOK {
				t.Fatalf("concurrent status=%d err=%v body=%q", result.status, result.err, result.body)
			}
			if concurrentACK == nil {
				concurrentACK = append([]byte(nil), result.body...)
			}
			if !bytes.Equal(concurrentACK, result.body) {
				t.Fatalf("concurrent ACKs differ: first=%q current=%q", concurrentACK, result.body)
			}
		}
		if got := f.scalarInt("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID, "persist_concurrent_client"); got != 1 {
			t.Fatalf("concurrent message count=%d", got)
		}
		concurrentAck := decodeACK(t, concurrentACK)
		if got := f.scalarInt("SELECT count(*) FROM newim.im_outbox_events WHERE server_msg_id=$1", concurrentAck.ServerMsgID); got != 1 {
			t.Fatalf("concurrent outbox count=%d", got)
		}

		f.removeMembership(conversationID, identity.UserID)
		replayAfterLoss := doRequest(t, http.MethodPost, messageURL(server), body, identity.RawToken, nil)
		assertError(t, replayAfterLoss, http.StatusForbidden, "SEND_UNAUTHORIZED")
		if got := f.scalarInt("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID, "persist_client"); got != 1 {
			t.Fatalf("membership-loss replay mutated rows=%d", got)
		}
	})

	t.Run("transport-auth-and-protocol-boundaries", func(t *testing.T) {
		f := openFixture(t)
		member := f.seedIdentity("boundary_member", seedOptions{})
		nonmember := f.seedIdentity("boundary_nonmember", seedOptions{})
		expired := f.seedIdentity("boundary_expired", seedOptions{Expired: true})
		revoked := f.seedIdentity("boundary_revoked", seedOptions{Revoked: true})
		conversationID := f.seedConversation("boundary", member.UserID)
		server := startMessageServer(t, validMessageEnv())
		defer server.stop(t)
		server.waitHealthy(t)

		valid := textBody(t, "boundary_valid", conversationID, "valid")
		unknown := doRequest(t, http.MethodPost, messageURL(server), valid, unknownToken("unknown"), nil)
		assertError(t, unknown, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
		if unknown.headers.Get("WWW-Authenticate") != `Bearer realm="newim-session"` {
			t.Fatalf("unknown token challenge=%v", unknown.headers)
		}

		tampered := member.RawToken[:len(member.RawToken)-2] + "B" + member.RawToken[len(member.RawToken)-1:]
		for name, token := range map[string]string{"tampered": tampered, "expired": expired.RawToken, "revoked": revoked.RawToken, "malformed": "Bearer malformed"} {
			t.Run(name, func(t *testing.T) {
				result := doRequest(t, http.MethodPost, messageURL(server), valid, token, nil)
				assertError(t, result, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
			})
		}

		for _, test := range []struct {
			name   string
			token  string
			mutate func(*http.Request)
		}{
			{name: "missing-authorization", token: "", mutate: func(r *http.Request) { r.Header.Del("Authorization") }},
			{name: "duplicate-authorization", token: member.RawToken, mutate: func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+member.RawToken) }},
		} {
			t.Run(test.name, func(t *testing.T) {
				result := doRequest(t, http.MethodPost, messageURL(server), valid, test.token, test.mutate)
				assertError(t, result, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
			})
		}
		lowercaseBody := textBody(t, "lowercase_scheme_client", conversationID, "lowercase")
		lowercase := doRequest(t, http.MethodPost, messageURL(server), lowercaseBody, member.RawToken, func(r *http.Request) {
			r.Header.Set("Authorization", "bearer "+member.RawToken)
		})
		if lowercase.status != http.StatusOK {
			t.Fatalf("lowercase bearer status=%d body=%q", lowercase.status, lowercase.body)
		}

		nonmemberResult := doRequest(t, http.MethodPost, messageURL(server), textBody(t, "boundary_nonmember_client", conversationID, "no"), nonmember.RawToken, nil)
		assertError(t, nonmemberResult, http.StatusForbidden, "SEND_UNAUTHORIZED")
		missingConversation := doRequest(t, http.MethodPost, messageURL(server), textBody(t, "boundary_missing_client", "boundary_missing", "no"), member.RawToken, nil)
		assertError(t, missingConversation, http.StatusNotFound, "SEND_CONVERSATION_NOT_FOUND")

		t.Run("method-path-query", func(t *testing.T) {
			result := doRequest(t, http.MethodGet, messageURL(server), nil, member.RawToken, nil)
			assertError(t, result, http.StatusMethodNotAllowed, "HTTP_METHOD_NOT_ALLOWED")
			if result.headers.Get("Allow") != http.MethodPost {
				t.Fatalf("Allow=%q", result.headers.Get("Allow"))
			}
			result = doRequest(t, http.MethodPost, messageURL(server)+"/", valid, member.RawToken, nil)
			assertError(t, result, http.StatusNotFound, "HTTP_ROUTE_NOT_FOUND")
			result = doRequest(t, http.MethodPost, messageURL(server)+"?x=1", valid, member.RawToken, nil)
			assertError(t, result, http.StatusBadRequest, "HTTP_INVALID_QUERY")
			result = doRequest(t, http.MethodPost, messageURL(server)+"?", valid, member.RawToken, nil)
			assertError(t, result, http.StatusBadRequest, "HTTP_INVALID_QUERY")
			result = doRequest(t, http.MethodGet, healthURL(server), []byte("body"), "", nil)
			assertError(t, result, http.StatusBadRequest, "HTTP_BODY_NOT_ALLOWED")
		})

		t.Run("content-type-encoding-size", func(t *testing.T) {
			result := doRequest(t, http.MethodPost, messageURL(server), valid, member.RawToken, func(r *http.Request) { r.Header.Del("Content-Type") })
			assertError(t, result, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
			result = doRequest(t, http.MethodPost, messageURL(server), valid, member.RawToken, func(r *http.Request) {
				r.Header.Add("Content-Type", "application/json")
			})
			assertError(t, result, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
			result = doRequest(t, http.MethodPost, messageURL(server), valid, member.RawToken, func(r *http.Request) {
				r.Header.Set("Content-Encoding", "gzip")
			})
			assertError(t, result, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
			result = doRequest(t, http.MethodPost, messageURL(server), valid, member.RawToken, func(r *http.Request) {
				r.Header.Set("Content-Encoding", "identity")
				r.Header.Add("Content-Encoding", "identity")
			})
			assertError(t, result, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")

			result = rawFixedLengthRequest(t, server, member.RawToken, "text/plain", protocol.MaxFrameBytes+4096)
			assertError(t, result, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
			result = rawChunkedRequest(t, server, member.RawToken, "text/plain", valid)
			assertError(t, result, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
			chunkedOversized := append(append([]byte(nil), valid...), bytes.Repeat([]byte{' '}, protocol.MaxFrameBytes+4096)...)
			result = rawChunkedRequest(t, server, member.RawToken, "application/json", chunkedOversized)
			assertError(t, result, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
		})

		t.Run("protocol-shape", func(t *testing.T) {
			cases := []struct {
				name string
				body []byte
			}{
				{name: "empty", body: nil},
				{name: "malformed", body: []byte(`{"protocolVersion":`)},
				{name: "duplicate-key", body: []byte(`{"protocolVersion":1,"protocolVersion":1,"kind":"send","body":{"clientMsgId":"boundary_dup","conversationId":"` + conversationID + `","version":1,"type":"text","payload":{"text":"x"}}}`)},
				{name: "trailing-json", body: append(valid, []byte(`{}`)...)},
				{name: "outer-version", body: []byte(`{"protocolVersion":2,"kind":"send","body":{"clientMsgId":"boundary_outer","conversationId":"` + conversationID + `","version":1,"type":"text","payload":{"text":"x"}}}`)},
				{name: "inner-version", body: []byte(`{"protocolVersion":1,"kind":"send","body":{"clientMsgId":"boundary_inner","conversationId":"` + conversationID + `","version":2,"type":"text","payload":{"text":"x"}}}`)},
				{name: "unknown-type", body: []byte(`{"protocolVersion":1,"kind":"send","body":{"clientMsgId":"boundary_unknown","conversationId":"` + conversationID + `","version":1,"type":"unknown","payload":{"text":"x"}}}`)},
				{name: "media", body: []byte(`{"protocolVersion":1,"kind":"send","body":{"clientMsgId":"boundary_media","conversationId":"` + conversationID + `","version":1,"type":"media","payload":{"mediaId":"media_1"}}}`)},
				{name: "forged-sender", body: []byte(`{"protocolVersion":1,"kind":"send","senderId":"forged","body":{"clientMsgId":"boundary_sender","conversationId":"` + conversationID + `","version":1,"type":"text","payload":{"text":"x"}}}`)},
				{name: "forged-status", body: []byte(`{"protocolVersion":1,"kind":"send","body":{"clientMsgId":"boundary_status","conversationId":"` + conversationID + `","version":1,"type":"text","status":"SERVER_PERSISTED","payload":{"text":"x"}}}`)},
				{name: "too-deep", body: []byte(`{"protocolVersion":1,"kind":"send","body":{"clientMsgId":"boundary_deep","conversationId":"` + conversationID + `","version":1,"type":"text","payload":` + strings.Repeat("[", 80) + `"x"` + strings.Repeat("]", 80) + `}}`)},
			}
			for _, test := range cases {
				t.Run(test.name, func(t *testing.T) {
					body := test.body
					if body == nil {
						body = []byte{}
					}
					result := doRequest(t, http.MethodPost, messageURL(server), body, member.RawToken, nil)
					assertError(t, result, http.StatusBadRequest, "SEND_INVALID_INPUT")
				})
			}
		})

		if got := f.scalarInt("SELECT count(*) FROM newim.im_messages WHERE client_msg_id LIKE 'boundary_%'"); got != 0 {
			t.Fatalf("boundary requests persisted rows=%d", got)
		}
	})

	t.Run("backend-disconnect-recovery", func(t *testing.T) {
		f := openFixture(t)
		identity := f.seedIdentity("disconnect", seedOptions{})
		conversationID := f.seedConversation("disconnect", identity.UserID)
		server := startMessageServer(t, validMessageEnv())
		defer server.stop(t)
		server.waitHealthy(t)

		authClient := textBody(t, "disconnect_auth_client", conversationID, "auth disconnect")
		blocker, err := pgx.Connect(ctx, f.dsn)
		must(t, err)
		defer blocker.Close(context.Background())
		tx, err := blocker.Begin(ctx)
		must(t, err)
		_, err = tx.Exec(ctx, "SELECT user_id FROM newim.im_sessions WHERE session_id=$1 FOR UPDATE", identity.SessionID)
		must(t, err)
		resultCh := make(chan httpResult, 1)
		go func() {
			resultCh <- doRequestErr(http.MethodPost, messageURL(server), authClient, identity.RawToken, nil)
		}()
		pid := waitBackendPID(t, f, "newim-auth-http", "im_sessions", 8*time.Second)
		terminateBackend(t, f, pid)
		must(t, tx.Rollback(ctx))
		result := <-resultCh
		assertError(t, result, http.StatusServiceUnavailable, "AUTH_UNAVAILABLE")

		retrySuccess := false
		deadline := time.Now().Add(6 * time.Second)
		for time.Now().Before(deadline) {
			result = doRequestErr(http.MethodPost, messageURL(server), authClient, identity.RawToken, nil)
			if result.err == nil && result.status == http.StatusOK {
				retrySuccess = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !retrySuccess {
			t.Fatalf("auth recovery retry status=%d err=%v body=%q", result.status, result.err, result.body)
		}
		if got := f.scalarInt("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID, "disconnect_auth_client"); got != 1 {
			t.Fatalf("auth recovery message count=%d", got)
		}

		messageClient := textBody(t, "disconnect_message_client", conversationID, "message disconnect")
		tx, err = blocker.Begin(ctx)
		must(t, err)
		_, err = tx.Exec(ctx, "SELECT conversation_id FROM newim.im_conversations WHERE conversation_id=$1 FOR UPDATE", conversationID)
		must(t, err)
		resultCh = make(chan httpResult, 1)
		go func() {
			resultCh <- doRequestErr(http.MethodPost, messageURL(server), messageClient, identity.RawToken, nil)
		}()
		pid = waitBackendPID(t, f, "newim-message-http", "im_conversations", 8*time.Second)
		terminateBackend(t, f, pid)
		must(t, tx.Rollback(ctx))
		result = <-resultCh
		assertError(t, result, http.StatusServiceUnavailable, "SERVER_TEMPORARY_UNAVAILABLE")
		if result.headers.Get("Retry-After") != "1" {
			t.Fatalf("retry-after=%q", result.headers.Get("Retry-After"))
		}
		if got := f.scalarInt("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID, "disconnect_message_client"); got != 0 {
			t.Fatalf("failed message send persisted rows=%d", got)
		}

		retrySuccess = false
		deadline = time.Now().Add(6 * time.Second)
		for time.Now().Before(deadline) {
			result = doRequestErr(http.MethodPost, messageURL(server), messageClient, identity.RawToken, nil)
			if result.err == nil && result.status == http.StatusOK {
				retrySuccess = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !retrySuccess {
			t.Fatalf("message recovery retry status=%d err=%v body=%q", result.status, result.err, result.body)
		}
		ack := decodeACK(t, result.body)
		if got := f.scalarInt("SELECT count(*) FROM newim.im_messages WHERE server_msg_id=$1", ack.ServerMsgID); got != 1 {
			t.Fatalf("message recovery rows=%d", got)
		}
	})

	t.Run("commit-after-response-loss", func(t *testing.T) {
		f := openFixture(t)
		identity := f.seedIdentity("response_loss", seedOptions{})
		conversationID := f.seedConversation("response_loss", identity.UserID)
		upstream := startMessageServer(t, validMessageEnv())
		defer upstream.stop(t)
		upstream.waitHealthy(t)

		env := validMessageEnv()
		env["NEWIM_LOSS_UPSTREAM_API"] = "http://" + upstream.api
		env["NEWIM_LOSS_UPSTREAM_OPS"] = "http://" + upstream.ops
		wrapper := startLossWrapper(t, env)
		defer wrapper.stop(t)
		wrapper.waitHealthy(t)

		body := textBody(t, "response_loss_client", conversationID, "commit then lose response")
		result := doRequestErr(http.MethodPost, messageURL(wrapper), body, identity.RawToken, nil)
		if result.err == nil {
			t.Fatalf("loss wrapper unexpectedly returned HTTP response: status=%d body=%q", result.status, result.body)
		}

		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if f.scalarInt("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID, "response_loss_client") == 1 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if got := f.scalarInt("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID, "response_loss_client"); got != 1 {
			t.Fatalf("message count after lost response=%d", got)
		}
		if got := f.scalarInt("SELECT count(*) FROM newim.im_outbox_events WHERE server_msg_id=(SELECT server_msg_id FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2)", identity.UserID, "response_loss_client"); got != 1 {
			t.Fatalf("outbox count after lost response=%d", got)
		}

		retry := doRequest(t, http.MethodPost, messageURL(upstream), body, identity.RawToken, nil)
		ack := decodeACK(t, retry.body)
		var storedServerID string
		must(t, f.db.QueryRow(ctx, "SELECT server_msg_id FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID, "response_loss_client").Scan(&storedServerID))
		if ack.ServerMsgID != storedServerID || f.scalarInt("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID, "response_loss_client") != 1 {
			t.Fatalf("retry did not converge to committed ACK: ack=%+v stored=%s", ack, storedServerID)
		}
	})

	t.Run("sigterm-readiness-drain-and-pools", func(t *testing.T) {
		f := openFixture(t)
		identity := f.seedIdentity("shutdown", seedOptions{})
		conversationID := f.seedConversation("shutdown", identity.UserID)
		server := startMessageServer(t, validMessageEnv())
		server.waitHealthy(t)
		server.waitReady(t)
		waitConnectionRange(t, f, "newim-auth-http", 1, 8, 5*time.Second)
		waitConnectionRange(t, f, "newim-message-http", 1, 8, 5*time.Second)

		blocker, err := pgx.Connect(ctx, f.dsn)
		must(t, err)
		defer blocker.Close(context.Background())
		tx, err := blocker.Begin(ctx)
		must(t, err)
		_, err = tx.Exec(ctx, "SELECT conversation_id FROM newim.im_conversations WHERE conversation_id=$1 FOR UPDATE", conversationID)
		must(t, err)
		resultCh := make(chan httpResult, 1)
		go func() {
			resultCh <- doRequestErr(http.MethodPost, messageURL(server), textBody(t, "shutdown_client", conversationID, "drain"), identity.RawToken, nil)
		}()
		_ = waitBackendPID(t, f, "newim-message-http", "im_conversations", 8*time.Second)

		server.signalSIGTERM(t)
		waitForReadyNotReady(t, server, 3*time.Second)
		must(t, tx.Rollback(ctx))
		select {
		case result := <-resultCh:
			if result.err == nil && result.status == http.StatusOK {
				ack := decodeACK(t, result.body)
				if got := f.scalarInt("SELECT count(*) FROM newim.im_messages WHERE server_msg_id=$1", ack.ServerMsgID); got != 1 {
					t.Fatalf("drained ACK row count=%d", got)
				}
			}
		case <-time.After(8 * time.Second):
			t.Fatal("drained request did not finish")
		}
		if err := server.waitExit(t, 10*time.Second); err != nil {
			t.Fatalf("SIGTERM exit=%v stderr=%q", err, server.stderr.String())
		}
		waitNoConnections(t, f, "newim-auth-http", "newim-message-http")
	})
}

func validMessageEnv() map[string]string {
	return map[string]string{
		"NEWIM_MESSAGE_HTTP":            "1",
		"NEWIM_AUTH_DSN":                localDSN,
		"NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1",
	}
}

func unknownToken(prefix string) string {
	hash := fmt.Sprintf("%x", []byte(prefix))
	hash = (hash + strings.Repeat("0", 32))[:32]
	return "n1_" + hash + "_" + strings.Repeat("A", 43)
}
