//go:build integration

package messagedeltahttp_test

import (
	"bytes"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"
)

const (
	recoveryFile = "/tmp/newim-message-delta-http-recovery.json"
	recoveryID   = "delta_http_recovery_conversation"
)

type recoveryState struct {
	Writer          string
	Reader          string
	ReaderOther     string
	After           int64
	OfflineServerID string
	Postmaster      time.Time
}

func TestMessageDeltaHTTPRecovery(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(14)
	switch os.Getenv("NEWIM_MESSAGE_DELTA_HTTP_PHASE") {
	case "prepare":
		prepareRecovery(t, f)
	case "restart":
		resumeRecovery(t, f)
	default:
		t.Fatal("NEWIM_MESSAGE_DELTA_HTTP_PHASE must be prepare or restart")
	}
}

func prepareRecovery(t *testing.T, f *fixture) {
	t.Helper()
	const owner = "delta_http_recovery_owner"
	const peer = "delta_http_recovery_member"
	loopback := f.server("1", nil)
	gateway := f.syncGateway(nil)
	provision(t, f, loopback, owner)
	provision(t, f, loopback, peer)
	host := f.admit(owner, "phone")
	reader := f.admit(peer, "phone")
	other := f.admit(peer, "desktop")
	createGroup(t, f, gateway, host, recoveryID, []string{peer}, true)
	socket := subscribe(t, f, gateway, reader, recoveryID, 0)
	first := send(t, f, gateway, host, recoveryID, "recovery_online", "message consumed before disconnect")
	raw := wssMessage(t, f, socket)
	frame := messageFrame(t, raw)
	if frame.ServerMsgID != first.ServerMsgID || frame.ConversationSeq != "1" {
		t.Fatal("WSS did not reach the first committed message before disconnect")
	}
	after, err := strconv.ParseInt(frame.ConversationSeq, 10, 64)
	must(t, err)
	must(t, socket.CloseNow())
	second := send(t, f, loopback, host, recoveryID, "recovery_offline", "message persisted while subscriber is offline")
	if second.ConversationSeq != "2" {
		t.Fatal("loopback send did not preserve the sequence while offline")
	}
	var postmaster time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&postmaster))
	state := recoveryState{Writer: host, Reader: reader, ReaderOther: other, After: after, OfflineServerID: second.ServerMsgID, Postmaster: postmaster}
	must(t, os.WriteFile(recoveryFile, encode(t, state), 0o600))
	gateway.requireRedacted(t, host, reader, other, testDSN)
}

func resumeRecovery(t *testing.T, f *fixture) {
	t.Helper()
	raw, err := os.ReadFile(recoveryFile)
	must(t, err)
	t.Cleanup(func() { _ = os.Remove(recoveryFile) })
	var saved recoveryState
	must(t, json.Unmarshal(raw, &saved))
	if saved.Writer == "" || saved.Reader == "" || saved.ReaderOther == "" || saved.After != 1 ||
		saved.OfflineServerID == "" || saved.Postmaster.IsZero() {
		t.Fatal("message delta restart fixture is incomplete")
	}
	var postmaster time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&postmaster))
	if !postmaster.After(saved.Postmaster) {
		t.Fatal("message delta recovery did not start a new PostgreSQL postmaster")
	}

	left, right := f.syncGateway(nil), f.syncGateway(nil)
	offline := readMessages(t, f, left, saved.Reader, recoveryID, saved.After, 1)
	expectRange(t, offline, recoveryID, 0, 2, false, 2)
	if msg := messageFrame(t, offline.Items[0]); msg.ServerMsgID != saved.OfflineServerID {
		t.Fatal("restart lost the original offline message")
	}
	socket := subscribe(t, f, right, saved.ReaderOther, recoveryID, saved.After)
	if ws := wssMessage(t, f, socket); !bytes.Equal(ws, offline.Items[0]) {
		t.Fatal("resumed HTTP and WSS returned different stored frames after restart")
	}
	third := send(t, f, right, saved.Writer, recoveryID, "recovery_next_one", "first new message after restart")
	fourth := send(t, f, right, saved.Writer, recoveryID, "recovery_next_two", "second new message after restart")
	after, err := strconv.ParseInt(*offline.NextAfterSeq, 10, 64)
	must(t, err)
	start := readMessages(t, f, right, saved.Reader, recoveryID, after, 1)
	expectRange(t, start, recoveryID, 0, 4, true, 3)
	if messageFrame(t, start.Items[0]).ServerMsgID != third.ServerMsgID {
		t.Fatal("first post-restart page did not expose the next committed receipt")
	}
	after, err = strconv.ParseInt(*start.NextAfterSeq, 10, 64)
	must(t, err)
	end := readMessages(t, f, left, saved.ReaderOther, recoveryID, after, 1)
	expectRange(t, end, recoveryID, 0, 4, false, 4)
	if messageFrame(t, end.Items[0]).ServerMsgID != fourth.ServerMsgID {
		t.Fatal("second device did not resume the last committed message")
	}
	after, err = strconv.ParseInt(*end.NextAfterSeq, 10, 64)
	must(t, err)
	expectRange(t, readMessages(t, f, right, saved.Reader, recoveryID, after, 1), recoveryID, 0, 4, false)
	left.requireRedacted(t, saved.Writer, saved.Reader, saved.ReaderOther, testDSN)
	right.requireRedacted(t, saved.Writer, saved.Reader, saved.ReaderOther, testDSN)
}
