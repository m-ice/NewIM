//go:build integration

package messagedeltahttp_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/coder/websocket"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	app "github.com/m-ice/NewIM/server/sync/message"
)

const deltaPath = "/api/v1/sync/messages"

func TestMessageDeltaHTTPCheck(t *testing.T) {
	t.Run("frames-match-native-wss", testTLSFrameParity)
	t.Run("wire-byte-and-item-bounds", testPageBounds)
	t.Run("history-lower-and-departure-upper-bounds", testMemberWindows)
}

func testTLSFrameParity(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(14)
	loopback := f.server("1", nil)
	left, right := f.syncGateway(nil), f.syncGateway(nil)
	const owner = "delta_http_parity_owner"
	const reader = "delta_http_parity_reader"
	provision(t, f, loopback, owner)
	provision(t, f, loopback, reader)
	host := f.admit(owner, "phone")
	peer := f.admit(reader, "desktop")
	reply := f.call(left, http.MethodPost, "/api/v1/conversations/direct", host, encode(t, map[string]string{"peerUserId": reader}), nil)
	requireStatus(t, reply, http.StatusOK)
	var direct struct {
		ConversationID string `json:"conversationId"`
	}
	must(t, json.Unmarshal(reply.body, &direct))
	if direct.ConversationID == "" || !reply.verified {
		t.Fatal("TLS direct source did not return a committed conversation")
	}
	const original = `{ "text" : "<em>hello & goodbye</em>", "opaque" : { "n":9007199254740993, "a":[ 1 , 2 ] } }`
	wire, err := protocol.EncodeSend(protocol.Send{ProtocolVersion: 1, ClientMsgID: "parity_source", ConversationID: direct.ConversationID,
		Version: 1, Type: "text", Payload: []byte(original)})
	must(t, err)
	ws := subscribe(t, f, right, peer, direct.ConversationID, 0)
	ack := expectACK(t, f.call(left, http.MethodPost, sendPath, host, wire, nil))
	if ack.ConversationSeq != "1" {
		t.Fatal("TLS send did not use sequence one")
	}
	raw := wssMessage(t, f, ws)
	page := readMessages(t, f, left, peer, direct.ConversationID, 0, 0)
	expectRange(t, page, direct.ConversationID, 0, 1, false, 1)
	decoded := messageFrame(t, page.Items[0])
	if !bytes.Equal(raw, page.Items[0]) || !bytes.Equal(decoded.Payload, []byte(original)) ||
		decoded.ServerMsgID != ack.ServerMsgID || decoded.SenderID != owner {
		t.Fatal("HTTP delta did not preserve the exact WSS frame and original payload bytes")
	}
	empty := readMessages(t, f, right, peer, direct.ConversationID, 1, 0)
	expectRange(t, empty, direct.ConversationID, 0, 1, false)
	left.requireRedacted(t, host, peer, testDSN, "hello & goodbye")
	right.requireRedacted(t, host, peer, testDSN)
}

type messagePage struct {
	HistoryAfterSeq string            `json:"historyAfterSeq"`
	LatestSeq       string            `json:"latestSeq"`
	Items           []json.RawMessage `json:"items"`
	HasNext         bool              `json:"hasNext"`
	NextAfterSeq    *string           `json:"nextAfterSeq"`
}

func (f *fixture) syncGateway(overrides map[string]string) *process {
	f.t.Helper()
	settings := map[string]string{"NEWIM_CONVERSATION_SYNC": "1", "NEWIM_CONVERSATION_SYNC_KEYS_FILE": testKeyFile(f.t)}
	for key, value := range overrides {
		settings[key] = value
	}
	return f.gateway("1", settings)
}

func testKeyFile(t *testing.T) string {
	t.Helper()
	secret := sha256.Sum256([]byte("synthetic message delta HTTP integration key"))
	path := filepath.Join(t.TempDir(), "synckeys.json")
	must(t, os.WriteFile(path, encode(t, map[string]any{"activeKeyId": "fixture",
		"keys": map[string]string{"fixture": hex.EncodeToString(secret[:])}}), 0o600))
	return path
}

func deltaBody(t *testing.T, conversation string, after int64, limit int) []byte {
	t.Helper()
	body := map[string]any{"conversationId": conversation, "afterSeq": strconv.FormatInt(after, 10)}
	if limit != 0 {
		body["limit"] = limit
	}
	return encode(t, body)
}

func readMessages(t *testing.T, f *fixture, gateway *process, token, conversation string, after int64, limit int) messagePage {
	t.Helper()
	return parsePage(t, f.call(gateway, http.MethodPost, deltaPath, token, deltaBody(t, conversation, after, limit), nil))
}

func parsePage(t *testing.T, reply response) messagePage {
	t.Helper()
	requireStatus(t, reply, http.StatusOK)
	if !reply.verified || reply.headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatal("message delta did not use verified TLS and JSON")
	}
	if len(reply.body) > app.MaxResponseBytes {
		t.Fatalf("message delta response exceeds byte cap: %d", len(reply.body))
	}
	var envelope map[string]json.RawMessage
	must(t, json.Unmarshal(reply.body, &envelope))
	if len(envelope) != 5 || len(envelope["nextAfterSeq"]) == 0 ||
		(string(envelope["hasNext"]) != "true" && string(envelope["hasNext"]) != "false") {
		t.Fatal("message delta did not return its exact page fields")
	}
	var page messagePage
	decoder := json.NewDecoder(bytes.NewReader(reply.body))
	decoder.DisallowUnknownFields()
	must(t, decoder.Decode(&page))
	if page.Items == nil || len(page.Items) > app.MaxPageItems {
		t.Fatal("message delta items must be a bounded array")
	}
	return page
}

func expectRange(t *testing.T, page messagePage, conversation string, lower, head int64, more bool, seqs ...int64) {
	t.Helper()
	if page.HistoryAfterSeq != strconv.FormatInt(lower, 10) || page.LatestSeq != strconv.FormatInt(head, 10) ||
		page.HasNext != more || len(page.Items) != len(seqs) {
		t.Fatalf("message range lower=%q latest=%q more=%t count=%d want %d/%d/%t/%d",
			page.HistoryAfterSeq, page.LatestSeq, page.HasNext, len(page.Items), lower, head, more, len(seqs))
	}
	for i, seq := range seqs {
		msg := messageFrame(t, page.Items[i])
		if msg.ConversationID != conversation || msg.ConversationSeq != strconv.FormatInt(seq, 10) {
			t.Fatal("message delta skipped a sequence or returned an unexpected conversation")
		}
	}
	if len(seqs) == 0 {
		if page.NextAfterSeq != nil {
			t.Fatal("empty message page must use a null next sequence")
		}
		return
	}
	if page.NextAfterSeq == nil || *page.NextAfterSeq != strconv.FormatInt(seqs[len(seqs)-1], 10) {
		t.Fatal("next sequence did not equal the last emitted message")
	}
}

func messageFrame(t *testing.T, raw []byte) protocol.Message {
	t.Helper()
	frame, err := protocol.DecodeServerFrame(raw)
	must(t, err)
	if frame.Message == nil || frame.Ack != nil || frame.Error != nil {
		t.Fatal("delta items must be native message frames")
	}
	return *frame.Message
}

func wssMessage(t *testing.T, f *fixture, ws *websocket.Conn) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 8*time.Second)
	defer cancel()
	typ, raw, err := ws.Read(ctx)
	must(t, err)
	if typ != websocket.MessageText {
		t.Fatal("WSS delivered a non-text message frame")
	}
	return raw
}
