//go:build integration

package messagedeltahttp_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	app "github.com/m-ice/NewIM/server/sync/message"
)

func testPageBounds(t *testing.T) {
	f := openFixture(t)
	const user = "delta_http_budget_owner"
	f.user(user)
	token := f.admit(user, "phone")
	gateway := f.syncGateway(nil)
	t.Run("exact-wire-cap", func(t *testing.T) { testWireCap(t, f, gateway, user, token, false) })
	t.Run("one-wire-byte-over", func(t *testing.T) { testWireCap(t, f, gateway, user, token, true) })
	t.Run("default-item-cap", func(t *testing.T) { testItemCap(t, f, gateway, user, token) })
}

func testWireCap(t *testing.T, f *fixture, gateway *process, user, token string, oneOver bool) {
	t.Helper()
	id := "delta_http_wire_exact"
	last := 5
	if oneOver {
		id = "delta_http_wire_over"
		last = 4
	}
	createGroup(t, f, gateway, token, id, nil, true)
	next := "5"
	empty := encode(t, messagePage{HistoryAfterSeq: "0", LatestSeq: "6", Items: []json.RawMessage{}, HasNext: true, NextAfterSeq: &next})
	const ordinary = 52400
	fifth := app.MaxResponseBytes - len(empty) - 4 - 4*ordinary
	if oneOver {
		fifth++
	}
	sizes := []int{ordinary, ordinary, ordinary, ordinary, fifth, 700}
	now := time.Now().UnixMilli()
	for i, size := range sizes {
		seq := strconv.Itoa(i + 1)
		msg := protocol.Message{ProtocolVersion: 1, Version: 1, ConversationID: id, ConversationSeq: seq, SenderID: user,
			ClientMsgID: id + "_client_" + seq, ServerMsgID: id + "_server_" + seq, Type: "text", ServerTime: strconv.FormatInt(now, 10)}
		payload := rawPayloadOfSize(t, msg, size)
		f.sql("SELECT newim.persist_message($1,$2,$3,$4,'text',1,$5,$6,$7)",
			user, msg.ClientMsgID, id, msg.ServerMsgID, now, payload, id+"_event_"+seq)
	}
	socket := subscribe(t, f, gateway, token, id, 0)
	frames := make([][]byte, len(sizes))
	for i, size := range sizes {
		frames[i] = wssMessage(t, f, socket)
		if len(frames[i]) != size {
			t.Fatalf("wire-size fixture frame %d: got %d want %d", i+1, len(frames[i]), size)
		}
	}
	reply := f.call(gateway, http.MethodPost, deltaPath, token, deltaBody(t, id, 0, 6), nil)
	page := parsePage(t, reply)
	expectRange(t, page, id, 0, 6, true, sequenceRange(1, last)...)
	if !oneOver && len(reply.body) != app.MaxResponseBytes {
		t.Fatalf("exact wire-budget fixture gave %d bytes, want %d", len(reply.body), app.MaxResponseBytes)
	}
	for i, raw := range page.Items {
		if !bytes.Equal(raw, frames[i]) {
			t.Fatal("byte-limited prefix was re-encoded")
		}
	}
	after, err := strconv.ParseInt(*page.NextAfterSeq, 10, 64)
	must(t, err)
	resumed := readMessages(t, f, gateway, token, id, after, 6)
	expectRange(t, resumed, id, 0, 6, false, sequenceRange(last+1, 6)...)
	for i, raw := range resumed.Items {
		if !bytes.Equal(raw, frames[last+i]) {
			t.Fatal("resumed page did not begin immediately after the actual wire prefix")
		}
	}
}

func rawPayloadOfSize(t *testing.T, msg protocol.Message, wanted int) []byte {
	t.Helper()
	const prefix = `{ "text" : "byte budget fixture", "extra" : "`
	const suffix = `" }`
	msg.Payload = []byte(prefix + suffix)
	empty, err := protocol.EncodeServerFrame(protocol.ServerFrame{Message: &msg})
	must(t, err)
	if len(empty) > wanted {
		t.Fatal("fixture size is below the message metadata")
	}
	return []byte(prefix + strings.Repeat("<", wanted-len(empty)) + suffix)
}

func testItemCap(t *testing.T, f *fixture, gateway *process, user, token string) {
	t.Helper()
	const id = "delta_http_item_count"
	createGroup(t, f, gateway, token, id, nil, true)
	f.sql(`SELECT newim.persist_message($1,'item_client_'||seq,$2,'item_server_'||seq,'text',1,$3,
		convert_to('{"text":"count fixture"}','UTF8'),'item_event_'||seq)
		FROM generate_series(1,101) AS items(seq)`, user, id, time.Now().UnixMilli())
	first := readMessages(t, f, gateway, token, id, 0, 0)
	expectRange(t, first, id, 0, 101, true, sequenceRange(1, 100)...)
	last := readMessages(t, f, gateway, token, id, 100, 0)
	expectRange(t, last, id, 0, 101, false, 101)
}

func sequenceRange(first, last int) []int64 {
	out := make([]int64, 0, last-first+1)
	for n := first; n <= last; n++ {
		out = append(out, int64(n))
	}
	return out
}
