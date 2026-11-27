//go:build integration

package accountrevocation_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

const (
	recoveryUser         = "revoke_recovery_owner"
	recoveryOther        = "revoke_recovery_other"
	recoveryRequest      = "revoke_recovery_request"
	recoveryConversation = "revoke_recovery_conversation"
	recoveryText         = "durable message in the recovery conversation"
	recoveryStatePath    = "/tmp/newim-account-revocation-recovery.json"
)

type recoveryState struct {
	Previous       deviceReply
	Revoked        deviceReply
	Fresh          deviceReply
	Other          deviceReply
	PriorEventID   int64
	PriorEventTime time.Time
	Receipt        []byte
	Ack            protocol.Ack
	Postmaster     time.Time
}

func TestAccountRevocationRecovery(t *testing.T) {
	f := openFixture(t)
	switch os.Getenv("NEWIM_ACCOUNT_REVOCATION_PHASE") {
	case "prepare":
		prepareRecovery(t, f)
	case "restart":
		resumeRecovery(t, f)
	default:
		t.Fatal("NEWIM_ACCOUNT_REVOCATION_PHASE must be prepare or restart")
	}
}

func prepareRecovery(t *testing.T, f *fixture) {
	t.Helper()
	f.requireMigration(12)
	f.user(recoveryUser)
	f.user(recoveryOther)
	requireStartupFailure(t, f.startServer("1", nil))
	oldServer := f.server("", map[string]string{"NEWIM_AUTH_MAX_DEVICES": "1", "NEWIM_AUTH_MAX_DEVICES_PER_PLATFORM": "1"})
	previous := admit(t, f, oldServer, recoveryUser, "pre_migration_device")
	live := admit(t, f, oldServer, recoveryUser, "active_device")
	other := admit(t, f, oldServer, recoveryOther, "other_device")
	baseline := loadEvent(t, f, previous.SessionID)
	if baseline.reason != "device_limit" {
		t.Fatal("real migration fixture did not generate a legacy device eviction")
	}
	assertBearer(t, f, oldServer, live.Token, http.StatusOK)

	const ledger = "SELECT string_agg(version::text||':'||sha256,',' ORDER BY version) FROM newim_meta.migrations WHERE version<=12"
	var before, after string
	must(t, f.db.QueryRow(f.ctx, ledger).Scan(&before))
	f.upgradeRevocation()
	f.requireMigration(13)
	must(t, f.db.QueryRow(f.ctx, ledger).Scan(&after))
	if before == "" || before != after || loadEvent(t, f, previous.SessionID) != baseline {
		t.Fatal("migration changed the historical checksums or device eviction event")
	}
	assertBearer(t, f, oldServer, live.Token, http.StatusOK)
	upgraded := f.server("1", nil)
	reply := revocation(t, f, upgraded, recoveryUser, recoveryRequest, passwordChanged)
	expectRevocation(t, reply, 1)
	requireDurableRevocation(t, f, recoveryUser, live.SessionID, passwordChanged)
	assertBearer(t, f, oldServer, live.Token, http.StatusUnauthorized)
	fresh := admit(t, f, upgraded, recoveryUser, "active_device")

	f.sql("INSERT INTO newim.im_conversations(conversation_id,kind) VALUES($1,'group')", recoveryConversation)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2),($1,$3)", recoveryConversation, recoveryUser, recoveryOther)
	ack := seedMessage(t, f, recoveryConversation, recoveryUser, "account_recovery_message", recoveryText, 1)
	var postmaster time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&postmaster))
	saved := recoveryState{Previous: previous, Revoked: live, Fresh: fresh, Other: other, PriorEventID: baseline.id,
		PriorEventTime: baseline.at, Receipt: reply.body, Ack: ack, Postmaster: postmaster}
	must(t, os.WriteFile(recoveryStatePath, encode(t, saved), 0o600))
	upgraded.requireRedacted(t, f.issuerKey, recoveryUser, recoveryRequest, live.Token, fresh.Token, testDSN)
}

func resumeRecovery(t *testing.T, f *fixture) {
	t.Helper()
	f.requireMigration(13)
	data, err := os.ReadFile(recoveryStatePath)
	must(t, err)
	t.Cleanup(func() { _ = os.Remove(recoveryStatePath) })
	var saved recoveryState
	must(t, json.Unmarshal(data, &saved))
	if saved.Revoked.Token == "" || saved.Fresh.Token == "" || saved.Other.Token == "" || saved.PriorEventID <= 0 || len(saved.Receipt) == 0 {
		t.Fatal("account revocation recovery fixture incomplete")
	}
	var postmaster time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&postmaster))
	if !postmaster.After(saved.Postmaster) {
		t.Fatal("recovery did not start a new PostgreSQL postmaster")
	}
	writer := f.server("1", nil)
	gateway := f.gateway(nil)
	assertBearer(t, f, writer, saved.Previous.Token, http.StatusUnauthorized)
	assertBearer(t, f, writer, saved.Revoked.Token, http.StatusUnauthorized)
	assertBearer(t, f, writer, saved.Fresh.Token, http.StatusOK)
	assertBearer(t, f, writer, saved.Other.Token, http.StatusOK)
	requireUnaffected(t, f, recoveryOther)
	oldEvent := loadEvent(t, f, saved.Previous.SessionID)
	if oldEvent.id != saved.PriorEventID || !oldEvent.at.Equal(saved.PriorEventTime) || oldEvent.reason != "device_limit" {
		t.Fatal("restart changed the historical eviction event")
	}
	requireDurableRevocation(t, f, recoveryUser, saved.Revoked.SessionID, passwordChanged)

	repeated := revocation(t, f, writer, recoveryUser, recoveryRequest, passwordChanged)
	expectRevocation(t, repeated, 1)
	if !bytes.Equal(repeated.body, saved.Receipt) {
		t.Fatal("same action retry changed its original receipt after restart")
	}
	assertBearer(t, f, writer, saved.Fresh.Token, http.StatusOK)
	revocation(t, f, writer, recoveryUser, recoveryRequest, accountBlocked).requireError(t, http.StatusConflict, "AUTH_REVOCATION_CONFLICT")
	stale, result, err := dialSocket(t, f, gateway, saved.Revoked.Token)
	requireSocketStatus(t, stale, result, err, http.StatusUnauthorized)
	active := subscribe(t, f, gateway, saved.Fresh.Token, recoveryConversation, 0)
	requireDelivered(t, f, active, saved.Ack, recoveryText)
	expectRevocation(t, revocation(t, f, writer, recoveryUser, "revoke_after_recovery", accountBlocked), 1)
	requireSessionClose(t, f, active)
	requireDurableRevocation(t, f, recoveryUser, saved.Fresh.SessionID, accountBlocked)
	assertBearer(t, f, writer, saved.Other.Token, http.StatusOK)
	requireUnaffected(t, f, recoveryOther)
	other, reply, err := dialSocket(t, f, gateway, saved.Other.Token)
	requireSocketStatus(t, other, reply, err, http.StatusSwitchingProtocols)
	t.Cleanup(func() { _ = other.CloseNow() })
	writer.requireRedacted(t, f.issuerKey, recoveryUser, recoveryRequest, saved.Revoked.Token, saved.Fresh.Token, testDSN)
	gateway.requireRedacted(t, recoveryUser, saved.Revoked.Token, saved.Fresh.Token, recoveryText, testDSN)
}
