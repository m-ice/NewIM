//go:build integration

package message_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	session "github.com/m-ice/NewIM/server/auth/session"
	app "github.com/m-ice/NewIM/server/message"
)

const recoveryStatePath = "/tmp/nim-srv-003-message-recovery.json"

type recoveryState struct {
	UserID       string `json:"user_id"`
	DeviceID     string `json:"device_id"`
	SessionID    string `json:"session_id"`
	ConnectionID string `json:"connection_id"`
	TokenID      string `json:"token_id"`
	Conversation string `json:"conversation_id"`
	ClientMsgID  string `json:"client_msg_id"`
	Text         string `json:"text"`
	ServerMsgID  string `json:"server_msg_id"`
	Sequence     string `json:"conversation_seq"`
	ServerTime   string `json:"server_time"`
}

func TestMessageRecovery(t *testing.T) {
	switch os.Getenv("NEWIM_MESSAGE_PHASE") {
	case "prepare":
		prepareMessageRecovery(t)
	case "restart":
		recoverMessagePhase(t, false)
	case "restore":
		recoverMessagePhase(t, true)
	default:
		t.Fatalf("unexpected message recovery phase %q", os.Getenv("NEWIM_MESSAGE_PHASE"))
	}
}

func prepareMessageRecovery(t *testing.T) {
	f := openFixture(t)
	user := "recovery_send_user"
	conversationID := f.seedConversation("recovery_send", user)
	identity := f.identity(user)
	service := f.service(nil)
	request := f.request("recovery_send_client", conversationID, "durable")
	frame, err := f.send(service, identity, request)
	must(t, err)
	result := ack(t, frame)
	f.assertAtomic(conversationID, result.ServerMsgID)

	testRollbackAndBackendTermination(t, f, service, identity)
	testLostResponseConvergence(t, f, identity)

	writeRecoveryState(t, recoveryState{
		UserID: user, DeviceID: identity.DeviceID(), SessionID: identity.SessionID(),
		ConnectionID: identity.ConnectionID(), TokenID: identity.TokenID(),
		Conversation: conversationID, ClientMsgID: request.ClientMsgID, Text: "durable",
		ServerMsgID: result.ServerMsgID, Sequence: result.ConversationSeq, ServerTime: result.ServerTime,
	})
}

func recoverMessagePhase(t *testing.T, restored bool) {
	f := openFixture(t)
	state := readRecoveryState(t)
	identity, err := session.NewConnectionIdentity(state.UserID, state.DeviceID, state.SessionID, state.ConnectionID, state.TokenID)
	must(t, err)
	service := f.service(nil)
	request := f.request(state.ClientMsgID, state.Conversation, state.Text)
	frame, err := f.send(service, identity, request)
	must(t, err)
	result := ack(t, frame)
	if result.ServerMsgID != state.ServerMsgID || result.ConversationSeq != state.Sequence || result.ServerTime != state.ServerTime {
		t.Fatalf("idempotent recovery changed result: got %+v want %+v", result, state)
	}
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", state.UserID, state.ClientMsgID); got != 1 {
		t.Fatalf("recovery message count got %d want 1", got)
	}
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_outbox_events WHERE server_msg_id=$1", state.ServerMsgID); got != 1 {
		t.Fatalf("recovery outbox count got %d want 1", got)
	}
	if restored {
		_ = os.Remove(recoveryStatePath)
	}
}

func testRollbackAndBackendTermination(t *testing.T, f *fixture, service *app.Service, identity session.ConnectionIdentity) {
	t.Helper()
	conversationID := f.seedConversation("recovery_rollback", identity.UserID())
	rollbackClient := "recovery_rollback_client"
	tx, err := f.db.Begin(ctx)
	must(t, err)
	var serverID string
	err = tx.QueryRow(ctx, `SELECT server_msg_id FROM newim.persist_message($1,$2,$3,$4,'text',1,$5,convert_to('{"text":"rollback"}','UTF8'),$6)`,
		identity.UserID(), rollbackClient, conversationID, "recovery_rollback_server", int64(f.now.Load()), "recovery_rollback_event").Scan(&serverID)
	must(t, err)
	must(t, tx.Rollback(ctx))
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID(), rollbackClient); got != 0 {
		t.Fatalf("rollback left %d messages", got)
	}
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_outbox_events WHERE event_id=$1", "recovery_rollback_event"); got != 0 {
		t.Fatalf("rollback left %d outbox rows", got)
	}

	commitFailureClient := "recovery_commit_failure_client"
	failureFunction := installMessageCommitFailureTrigger(t, f, commitFailureClient)
	defer dropMessageCommitTrigger(t, f, failureFunction)
	_, err = service.Send(ctx, identity, f.request(commitFailureClient, conversationID, "commit failure"))
	mustCode(t, err, app.SendStorageUnavailable)
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID(), commitFailureClient); got != 0 {
		t.Fatalf("commit failure left %d messages", got)
	}
	dropMessageCommitTrigger(t, f, failureFunction)

	terminationClient := "recovery_termination_client"
	blocker, err := f.db.Begin(ctx)
	must(t, err)
	var locked string
	err = blocker.QueryRow(ctx, "SELECT conversation_id FROM newim.im_conversations WHERE conversation_id=$1 FOR UPDATE", conversationID).Scan(&locked)
	must(t, err)
	done := make(chan error, 1)
	go func() {
		_, err := service.Send(context.Background(), identity, f.request(terminationClient, conversationID, "terminate"))
		done <- err
	}()
	waitForMessageLockWait(t, f)
	if _, err = f.db.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock'", "nim_message_test"); err != nil {
		t.Fatal(err)
	}
	err = <-done
	if err == nil || app.RetryDisposition(err) != protocol.RetrySameIntent {
		t.Fatalf("terminated send got %v want retryable failure", err)
	}
	must(t, blocker.Rollback(ctx))
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID(), terminationClient); got != 0 {
		t.Fatalf("termination left %d messages", got)
	}
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_outbox_events e JOIN newim.im_messages m ON e.server_msg_id=m.server_msg_id WHERE m.client_msg_id=$1", terminationClient); got != 0 {
		t.Fatalf("termination left %d outbox rows", got)
	}
}

func testLostResponseConvergence(t *testing.T, f *fixture, identity session.ConnectionIdentity) {
	t.Helper()
	conversationID := f.seedConversation("recovery_lost_response", identity.UserID())
	request := f.request("recovery_lost_client", conversationID, "lost response")
	lost := &lostResponseStore{inner: f.repo}
	lostService, err := app.NewService(lost, app.Config{
		IDs:   &sequenceIDs{prefix: fmt.Sprintf("lost_generated_%d", serviceSeq.Add(1))},
		Clock: app.ClockFunc(func() time.Time { return time.UnixMilli(f.now.Load()).UTC() }),
	})
	must(t, err)
	_, err = lostService.Send(ctx, identity, request)
	mustCode(t, err, app.SendStorageUnavailable)
	mustDisposition(t, err, protocol.RetrySameIntent)

	service := f.service(nil)
	frame, err := f.send(service, identity, request)
	must(t, err)
	result := ack(t, frame)
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", identity.UserID(), request.ClientMsgID); got != 1 {
		t.Fatalf("lost response retry created %d messages", got)
	}
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_outbox_events WHERE server_msg_id=$1", result.ServerMsgID); got != 1 {
		t.Fatalf("lost response retry created %d outbox rows", got)
	}
}

func installMessageCommitFailureTrigger(t *testing.T, f *fixture, clientMsgID string) string {
	t.Helper()
	functionName := "message_commit_failure_" + sanitizeIdentifier(clientMsgID)
	triggerName := functionName + "_trigger"
	f.sql(fmt.Sprintf("CREATE FUNCTION newim.%s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.client_msg_id = '%s'::newim.identifier THEN RAISE EXCEPTION USING ERRCODE='P0001', MESSAGE='MESSAGE_COMMIT_FAILURE'; END IF; RETURN NEW; END $$", functionName, clientMsgID))
	f.sql(fmt.Sprintf("CREATE CONSTRAINT TRIGGER %s AFTER INSERT ON newim.im_messages DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION newim.%s()", triggerName, functionName))
	return functionName
}

func dropMessageCommitTrigger(t *testing.T, f *fixture, functionName string) {
	t.Helper()
	f.sql("DROP TRIGGER IF EXISTS " + functionName + "_trigger ON newim.im_messages")
	f.sql("DROP FUNCTION IF EXISTS newim." + functionName + "()")
}

func waitForMessageLockWait(t *testing.T, f *fixture) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		err := f.db.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock'", "nim_message_test").Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("message authorization lock wait was not observed")
}

func sanitizeIdentifier(value string) string {
	return strings.NewReplacer("-", "_", ".", "_").Replace(value)
}

func writeRecoveryState(t *testing.T, state recoveryState) {
	t.Helper()
	raw, err := json.Marshal(state)
	must(t, err)
	if err = os.WriteFile(recoveryStatePath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readRecoveryState(t *testing.T) recoveryState {
	t.Helper()
	raw, err := os.ReadFile(recoveryStatePath)
	must(t, err)
	var state recoveryState
	must(t, json.Unmarshal(raw, &state))
	return state
}
