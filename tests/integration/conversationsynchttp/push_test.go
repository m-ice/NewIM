//go:build integration

package conversationsynchttp_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/push"
	pushstore "github.com/m-ice/NewIM/server/storage/push"
)

const (
	phonePushToken  = "synthetic:phone_fcm_token"
	tabletPushToken = "synthetic:tablet_fcm_token"
)

func pushFixture(t *testing.T) (*fixture, *pushstore.Repository, string) {
	t.Helper()
	f := openFixture(t)
	f.requireMigration(22)
	key := bytes.Repeat([]byte{0x73}, 32)
	keyFile := filepath.Join(t.TempDir(), "push-keys.json")
	must(t, os.WriteFile(keyFile, encode(t, map[string]any{
		"active": "fixture", "keys": map[string]string{"fixture": base64.StdEncoding.EncodeToString(key)},
	}), 0o600))
	vault, err := push.LoadVault(keyFile)
	must(t, err)
	repo, err := pushstore.Open(f.ctx, pushstore.Config{
		DSN: testDSN, AllowLocalSocket: true, ApplicationName: "newim-test-offline-push",
	}, vault)
	must(t, err)
	t.Cleanup(repo.Close)
	return f, repo, keyFile
}

func pushGateway(f *fixture, syncKey, pushKey string) *process {
	return f.syncGateway(syncKey, map[string]string{
		"NEWIM_CONVERSATION_READ_SYNC": "1", "NEWIM_CONVERSATION_ACCOUNT_SETTINGS": "1",
		"NEWIM_READ_WATERMARK_HTTP": "1", "NEWIM_PUSH_REGISTRATION_HTTP": "1",
		"NEWIM_PUSH_KEYS_FILE": pushKey, "NEWIM_GATEWAY_PUSH_PRESENCE": "1",
	})
}

func registerPushToken(t *testing.T, f *fixture, gateway *process, auth string, provider push.Channel, token string) int64 {
	t.Helper()
	reply := f.call(gateway, http.MethodPut, push.RegistrationRoute, auth,
		encode(t, map[string]string{"provider": string(provider), "token": token}), nil)
	requireStatus(t, reply, http.StatusOK)
	var result struct {
		Version int64 `json:"version,string"`
	}
	must(t, json.Unmarshal(reply.body, &result))
	if !reply.verified || result.Version < 1 || bytes.Contains(reply.body, []byte(token)) {
		t.Fatal("registration did not return a redacted version on verified TLS")
	}
	return result.Version
}

func subscriptionCount(t *testing.T, f *fixture, user, room string, expected int) {
	t.Helper()
	for tries := 0; tries < 80; tries++ {
		var count int
		err := f.db.QueryRow(f.ctx, `SELECT count(*) FROM newim.im_gateway_subscriptions
			WHERE user_id=$1 AND conversation_id=$2 AND expires_at>clock_timestamp()`, user, room).Scan(&count)
		must(t, err)
		if count == expected {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("gateway subscription leases did not converge")
}

func fanoutOne(t *testing.T, f *fixture, repo *pushstore.Repository) {
	t.Helper()
	n, err := repo.Fanout(f.ctx)
	must(t, err)
	if n != 1 {
		t.Fatalf("fanout processed %d events, want 1", n)
	}
}

func claimPush(t *testing.T, f *fixture, repo *pushstore.Repository) push.Delivery {
	t.Helper()
	job, status, err := repo.Claim(f.ctx)
	must(t, err)
	if status != push.LeaseAcquired {
		t.Fatal("no eligible push job was claimed")
	}
	return job
}

func requirePushToken(t *testing.T, f *fixture, repo *pushstore.Repository, job push.Delivery, expected string) {
	t.Helper()
	token, allowed, err := repo.Prepare(f.ctx, job)
	must(t, err)
	defer clear(token)
	if !allowed || !bytes.Equal(token, []byte(expected)) {
		t.Fatal("push used the wrong active installation token")
	}
}

func pushJobs(t *testing.T, f *fixture, msg string, expected int) {
	t.Helper()
	var count int
	must(t, f.db.QueryRow(f.ctx, `SELECT count(*) FROM newim.im_push_deliveries d
		JOIN newim.im_outbox_events o USING(event_id) WHERE o.server_msg_id=$1`, msg).Scan(&count))
	if count != expected {
		t.Fatalf("notification jobs=%d, want %d", count, expected)
	}
}

func TestPushDelivery(t *testing.T) {
	f, repo, keys := pushFixture(t)
	syncKeys := writeKeys(t, "push", "push")
	left, right := pushGateway(f, syncKeys, keys), pushGateway(f, syncKeys, keys)
	pair := accountPair(t, f, left, "push")
	registerPushToken(t, f, left, pair.owner, push.FCM, phonePushToken)
	registerPushToken(t, f, right, pair.tablet, push.FCM, tabletPushToken)

	old := send(t, f, left, pair.peer, pair.conversation, "before_push_enable", "older history")
	must(t, repo.Enable(f.ctx))
	first := accountHistorySocket(t, f, left, pair.owner, pair.conversation)
	second := accountHistorySocket(t, f, right, pair.owner, pair.conversation)
	subscriptionCount(t, f, pair.ownerID, pair.conversation, 2)
	must(t, first.CloseNow())
	subscriptionCount(t, f, pair.ownerID, pair.conversation, 1)

	online := send(t, f, left, pair.peer, pair.conversation, "push_while_online", "online device message")
	for index := 0; index < 2; index++ {
		_, data, err := second.Read(f.ctx)
		must(t, err)
		frame, err := protocol.DecodeServerFrame(data)
		must(t, err)
		if index == 1 && (frame.Message == nil || frame.Message.ServerMsgID != online.ServerMsgID) {
			t.Fatal("online device did not receive the committed message")
		}
	}
	var vendorCalls int
	worker, err := push.NewWorker(repo, map[push.Channel]push.Provider{
		push.FCM: testPushProvider(func(ctx context.Context, token []byte) push.Outcome {
			if _, bounded := ctx.Deadline(); !bounded || !bytes.Equal(token, []byte(tabletPushToken)) {
				t.Fatal("worker dispatched to the wrong installation or without a deadline")
			}
			vendorCalls++
			return push.Outcome{Result: push.Accepted}
		}),
	})
	must(t, err)
	progress, err := worker.Cycle(f.ctx)
	must(t, err)
	if !progress || vendorCalls != 1 {
		t.Fatal("worker did not dispatch a single offline notification")
	}
	pushJobs(t, f, old.ServerMsgID, 0)
	pushJobs(t, f, online.ServerMsgID, 1)
	again, err := repo.Fanout(f.ctx)
	must(t, err)
	if again != 0 {
		t.Fatal("the committed event was scheduled more than once")
	}

	must(t, second.CloseNow())
	subscriptionCount(t, f, pair.ownerID, pair.conversation, 0)
	const other = "push_other_room"
	createGroup(t, f, left, pair.owner, other, nil, true)
	otherSocket := accountHistorySocket(t, f, left, pair.owner, other)
	subscriptionCount(t, f, pair.ownerID, other, 1)
	offline := send(t, f, right, pair.peer, pair.conversation, "push_after_disconnect", "offline device message")
	fanoutOne(t, f, repo)
	pushJobs(t, f, offline.ServerMsgID, 2)
	must(t, otherSocket.CloseNow())

	// A recall after queueing is checked before any provider call.
	messageVisibility(t, f, left, pair.peer, protocol.VisibilityRequest{
		ConversationID: pair.conversation, ServerMsgID: offline.ServerMsgID,
		RequestID: "push_cancel_recalled", Action: protocol.Recall,
	}, http.StatusOK)
	for range 2 {
		queued := claimPush(t, f, repo)
		token, allowed, err := repo.Prepare(f.ctx, queued)
		must(t, err)
		if allowed || len(token) != 0 {
			t.Fatal("recalled notification remained eligible")
		}
		finishPush(t, f, repo, queued, push.Outcome{Result: push.Cancelled}, push.Cancelled)
	}
	accountState(t, f, right, http.MethodPatch, pair.tablet, pair.conversation, "settings",
		map[string]any{"expectedVersion": "0", "muted": true})
	muted := send(t, f, left, pair.peer, pair.conversation, "push_muted", "muted conversation")
	fanoutOne(t, f, repo)
	pushJobs(t, f, muted.ServerMsgID, 0)
	left.requireRedacted(t, phonePushToken, tabletPushToken)
	right.requireRedacted(t, phonePushToken, tabletPushToken)
}

func TestPushTokenRotation(t *testing.T) {
	f, repo, keys := pushFixture(t)
	gateway := pushGateway(f, writeKeys(t, "push", "push"), keys)
	pair := accountPair(t, f, gateway, "push_rotate")
	first := registerPushToken(t, f, gateway, pair.owner, push.FCM, phonePushToken)
	if retry := registerPushToken(t, f, gateway, pair.owner, push.FCM, phonePushToken); retry != first {
		t.Fatal("identical retry rotated the registration")
	}
	must(t, repo.Enable(f.ctx))
	send(t, f, gateway, pair.peer, pair.conversation, "push_before_rotation", "committed once")
	fanoutOne(t, f, repo)
	job := claimPush(t, f, repo)
	requirePushToken(t, f, repo, job, phonePushToken)

	newVersion := registerPushToken(t, f, gateway, pair.owner, push.FCM, tabletPushToken)
	if newVersion <= first {
		t.Fatal("registration version did not advance")
	}
	finishPush(t, f, repo, job, push.Outcome{Result: push.InvalidToken}, push.InvalidToken)
	reply := f.call(gateway, http.MethodDelete, push.RegistrationRoute, pair.owner,
		encode(t, map[string]string{"provider": "fcm", "version": strconv.FormatInt(first, 10)}), nil)
	requireStatus(t, reply, http.StatusNoContent)

	send(t, f, gateway, pair.peer, pair.conversation, "push_after_rotation", "new token receives this")
	fanoutOne(t, f, repo)
	next := claimPush(t, f, repo)
	if next.Version != newVersion {
		t.Fatal("late response or unregister changed the new registration")
	}
	requirePushToken(t, f, repo, next, tabletPushToken)
	finishPush(t, f, repo, next, push.Outcome{Result: push.InvalidToken}, push.InvalidToken)

	quiet := send(t, f, gateway, pair.peer, pair.conversation, "push_invalid", "removed vendor token")
	fanoutOne(t, f, repo)
	pushJobs(t, f, quiet.ServerMsgID, 0)

	const apnsToken = "01020304aabbccdd"
	apnsVersion := registerPushToken(t, f, gateway, pair.owner, push.APNs, apnsToken)
	send(t, f, gateway, pair.peer, pair.conversation, "push_apns_timestamp", "new APNs registration")
	fanoutOne(t, f, repo)
	apns := claimPush(t, f, repo)
	requirePushToken(t, f, repo, apns, apnsToken)
	finishPush(t, f, repo, apns, push.Outcome{Result: push.InvalidToken, InvalidAt: time.Now().Add(-time.Hour)}, push.InvalidToken)
	send(t, f, gateway, pair.peer, pair.conversation, "push_apns_next", "APNs registration remains usable")
	fanoutOne(t, f, repo)
	apns = claimPush(t, f, repo)
	if apns.Channel != push.APNs || apns.Version != apnsVersion {
		t.Fatal("a stale APNs timestamp invalidated a newer registration")
	}
	requirePushToken(t, f, repo, apns, apnsToken)
	finishPush(t, f, repo, apns, push.Outcome{Result: push.Accepted}, push.Accepted)
	gateway.requireRedacted(t, phonePushToken, tabletPushToken, apnsToken)
}

type testPushProvider func(context.Context, []byte) push.Outcome

func (provider testPushProvider) Send(ctx context.Context, token []byte) push.Outcome {
	return provider(ctx, token)
}

func finishPush(t *testing.T, f *fixture, repo *pushstore.Repository, job push.Delivery, outcome push.Outcome, want push.Result) {
	t.Helper()
	got, err := repo.Finish(f.ctx, job, outcome)
	must(t, err)
	if got != want {
		t.Fatalf("committed push result = %d, want %d", got, want)
	}
}

func TestPushRecovery(t *testing.T) {
	f, repo, keys := pushFixture(t)
	switch os.Getenv("NEWIM_SYNC_HTTP_PHASE") {
	case "prepare":
		process := f.server("1", map[string]string{
			"NEWIM_PUSH_WORKER": "1", "NEWIM_PUSH_ALLOW_LOCAL_SOCKET": "1",
			"NEWIM_PUSH_KEYS_FILE": keys, "NEWIM_PUSH_APNS_KEY_FILE": f.tlsKey,
			"NEWIM_PUSH_APNS_KEY_ID": "fixture", "NEWIM_PUSH_APNS_TEAM_ID": "fixture",
			"NEWIM_PUSH_APNS_TOPIC": "im.fixture",
		})
		waitForStartup(t, process, func() bool {
			var enabled bool
			err := f.db.QueryRow(f.ctx, "SELECT EXISTS(SELECT 1 FROM newim.im_push_state WHERE singleton)").Scan(&enabled)
			must(t, err)
			return enabled
		})
		must(t, process.cmd.Process.Signal(syscall.SIGTERM))
		select {
		case <-process.done:
			must(t, process.waitErr)
		case <-f.ctx.Done():
			t.Fatal("push worker did not stop with its server")
		}
		gateway := pushGateway(f, writeKeys(t, "push", "push"), keys)
		pair := accountPair(t, f, gateway, "push_recovery")
		registerPushToken(t, f, gateway, pair.owner, push.FCM, phonePushToken)
		must(t, repo.Enable(f.ctx))
		send(t, f, gateway, pair.peer, pair.conversation, "push_before_restart", "persisted before dispatch")
		fanoutOne(t, f, repo)
		job := claimPush(t, f, repo)
		requirePushToken(t, f, repo, job, phonePushToken)
		f.sql(`UPDATE newim.im_push_deliveries SET lease_until=clock_timestamp()-interval '1 second' WHERE lease_id=$1`, job.LeaseID)
	case "restart":
		must(t, repo.Enable(f.ctx))
		var expired string
		must(t, f.db.QueryRow(f.ctx, "SELECT lease_id FROM newim.im_push_deliveries WHERE status='leased'").Scan(&expired))
		job := claimPush(t, f, repo)
		if job.Attempts != 2 || job.LeaseID == expired {
			t.Fatalf("expired lease recovery returned attempt %d or reused its old lease", job.Attempts)
		}
		stale := job
		stale.LeaseID = expired
		finishPush(t, f, repo, stale, push.Outcome{Result: push.InvalidToken}, 0)
		requirePushToken(t, f, repo, job, phonePushToken)
		finishPush(t, f, repo, job, push.Outcome{Result: push.TemporaryFailure}, push.TemporaryFailure)
		job = claimPush(t, f, repo)
		if job.Attempts != 3 {
			t.Fatal("temporary failure did not keep one retryable job")
		}
		requirePushToken(t, f, repo, job, phonePushToken)
		finishPush(t, f, repo, job, push.Outcome{Result: push.Accepted}, push.Accepted)
		var messages, jobs, delivered int
		must(t, f.db.QueryRow(f.ctx, `SELECT
			(SELECT count(*) FROM newim.im_messages WHERE client_msg_id='push_before_restart'),
			(SELECT count(*) FROM newim.im_push_deliveries),
			(SELECT count(*) FROM newim.im_push_deliveries WHERE status='accepted')`).Scan(&messages, &jobs, &delivered))
		if messages != 1 || jobs != 1 || delivered != 1 {
			t.Fatal("worker recovery duplicated or rolled back the message or its notification")
		}
		f.sql(`UPDATE newim.im_push_deliveries SET status='pending', attempts=$1, available_at=clock_timestamp()
			WHERE event_id=$2`, push.MaxAttempts-1, job.EventID)
		job = claimPush(t, f, repo)
		if job.Attempts != push.MaxAttempts {
			t.Fatal("unexpected final dispatch attempt")
		}
		finishPush(t, f, repo, job, push.Outcome{Result: push.TemporaryFailure}, push.PermanentFailure)
		var state string
		must(t, f.db.QueryRow(f.ctx, "SELECT status FROM newim.im_push_deliveries WHERE event_id=$1", job.EventID).Scan(&state))
		if state != "failed" {
			t.Fatalf("exhausted notification status = %q", state)
		}
		f.sql(`UPDATE newim.im_push_deliveries SET status='pending', attempts=$1, available_at=clock_timestamp()
			WHERE event_id=$2`, push.MaxAttempts-1, job.EventID)
		job = claimPush(t, f, repo)
		f.sql(`UPDATE newim.im_push_deliveries SET lease_until=clock_timestamp()-interval '1 second'
			WHERE lease_id=$1`, job.LeaseID)
		_, status, err := repo.Claim(f.ctx)
		must(t, err)
		if status != push.LeaseExhausted {
			t.Fatalf("expired final lease result = %d", status)
		}
		must(t, f.db.QueryRow(f.ctx, "SELECT status FROM newim.im_push_deliveries WHERE event_id=$1", job.EventID).Scan(&state))
		if state != "failed" {
			t.Fatalf("expired notification status = %q", state)
		}
		_, status, err = repo.Claim(f.ctx)
		must(t, err)
		if status != push.NoDelivery {
			t.Fatalf("expired final lease processed again: %d", status)
		}
	default:
		t.Fatal("push recovery requires prepare and restart phases")
	}
}
