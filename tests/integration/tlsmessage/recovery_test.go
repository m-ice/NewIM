//go:build integration

package tlsmessage_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

const (
	recoveryAlice      = "tls_recovery_alice"
	recoveryBob        = "tls_recovery_bob"
	recoveryFirstText  = "read before interruption"
	recoverySecondText = "persisted while the receiver is offline"
	recoveryFile       = "/tmp/newim-tls-message-recovery.json"
)

type recoveryState struct {
	Writer        string
	Reader        string
	Conversation  string
	After         int64
	FirstReceipt  []byte
	SecondReceipt []byte
	Postmaster    time.Time
}

func TestTLSMessageRecovery(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(11)
	switch os.Getenv("NEWIM_TLS_MESSAGE_PHASE") {
	case "prepare":
		prepareRecovery(t, f)
	case "restart":
		resumeRecovery(t, f)
	default:
		t.Fatal("NEWIM_TLS_MESSAGE_PHASE must be prepare or restart")
	}
}

func prepareRecovery(t *testing.T, f *fixture) {
	t.Helper()
	writer := f.admit(recoveryAlice, "mobile")
	reader := f.admit(recoveryBob, "desktop")
	gateway := f.gateway("1", nil)
	conversation := createDirect(t, f, gateway, writer, recoveryBob)
	stream := subscribe(t, f, gateway, reader, conversation, 0)
	first := f.call(gateway, http.MethodPost, sendPath, writer, textBody(t, "recovery_first", conversation, recoveryFirstText), nil)
	ack := expectACK(t, first)
	requireCommitted(t, f, ack, recoveryAlice, conversation, "recovery_first", recoveryFirstText, 1)
	requireDelivered(t, f, stream, ack, recoveryFirstText)
	saved, err := strconv.ParseInt(ack.ConversationSeq, 10, 64)
	must(t, err)
	must(t, stream.CloseNow())

	second := f.call(gateway, http.MethodPost, sendPath, writer, textBody(t, "recovery_second", conversation, recoverySecondText), nil)
	requireCommitted(t, f, expectACK(t, second), recoveryAlice, conversation, "recovery_second", recoverySecondText, 2)
	var postmaster time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&postmaster))
	raw, err := json.Marshal(recoveryState{Writer: writer, Reader: reader, Conversation: conversation, After: saved,
		FirstReceipt: first.body, SecondReceipt: second.body, Postmaster: postmaster})
	must(t, err)
	must(t, os.WriteFile(recoveryFile, raw, 0o600))
	gateway.requireRedacted(t, writer, reader, recoveryFirstText, recoverySecondText, testDSN)
}

func resumeRecovery(t *testing.T, f *fixture) {
	t.Helper()
	raw, err := os.ReadFile(recoveryFile)
	must(t, err)
	t.Cleanup(func() { _ = os.Remove(recoveryFile) })
	var saved recoveryState
	must(t, json.Unmarshal(raw, &saved))
	if saved.Writer == "" || saved.Reader == "" || saved.Conversation == "" || saved.After != 1 ||
		len(saved.FirstReceipt) == 0 || len(saved.SecondReceipt) == 0 || saved.Postmaster.IsZero() {
		t.Fatal("durable TLS recovery fixture incomplete")
	}
	var postmaster time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&postmaster))
	if !postmaster.After(saved.Postmaster) {
		t.Fatal("recovery did not start a new PostgreSQL postmaster")
	}

	gateway := f.gateway("1", nil)
	if current := createDirect(t, f, gateway, saved.Reader, recoveryAlice); current != saved.Conversation {
		t.Fatal("restart changed the direct conversation identity")
	}
	first := f.call(gateway, http.MethodPost, sendPath, saved.Writer, textBody(t, "recovery_first", saved.Conversation, recoveryFirstText), nil)
	expectACK(t, first)
	second := f.call(gateway, http.MethodPost, sendPath, saved.Writer, textBody(t, "recovery_second", saved.Conversation, recoverySecondText), nil)
	ack := expectACK(t, second)
	if !bytes.Equal(first.body, saved.FirstReceipt) || !bytes.Equal(second.body, saved.SecondReceipt) {
		t.Fatal("restart did not preserve both original receipts for the same intent")
	}
	requireCommitted(t, f, ack, recoveryAlice, saved.Conversation, "recovery_second", recoverySecondText, 2)

	stream := subscribe(t, f, gateway, saved.Reader, saved.Conversation, saved.After)
	requireDelivered(t, f, stream, ack, recoverySecondText)
	const nextText = "new message after recovery"
	third := expectACK(t, f.call(gateway, http.MethodPost, sendPath, saved.Writer, textBody(t, "recovery_third", saved.Conversation, nextText), nil))
	requireCommitted(t, f, third, recoveryAlice, saved.Conversation, "recovery_third", nextText, 3)
	requireDelivered(t, f, stream, third, nextText)
	gateway.requireRedacted(t, saved.Writer, saved.Reader, recoveryFirstText, recoverySecondText, nextText, testDSN)
}
