//go:build integration

package group_test

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/group"
)

const (
	recoveryOwner   = "grp_recovery_owner"
	recoveryMember  = "grp_recovery_member"
	recoveryLegacy  = "grp_recovery_legacy"
	recoveryManaged = "grp_recovery_managed"
	recoveryFixture = "/tmp/newim-group-recovery.json"
	firstText       = "first managed message before restart"
	secondText      = "saved while member is offline"
	legacyText      = "persisted before migration 012"
)

type recoveryState struct {
	Owner          string
	Member         string
	LegacyAsset    string
	LegacyAck      protocol.Ack
	LastSeen       int64
	ChangeRevision int64
	OfflineAck     protocol.Ack
	Postmaster     time.Time
}

func TestGroupRecovery(t *testing.T) {
	f := openFixture(t)
	switch os.Getenv("NEWIM_GROUP_PHASE") {
	case "prepare":
		prepareGroupRecovery(t, f)
	case "restart":
		resumeGroupRecovery(t, f)
	default:
		t.Fatal("NEWIM_GROUP_PHASE must be prepare or restart")
	}
}

func prepareGroupRecovery(t *testing.T, f *fixture) {
	t.Helper()
	f.requireMigration(11)
	owner := f.admit(recoveryOwner, "mobile")
	member := f.admit(recoveryMember, "desktop")
	f.sql("INSERT INTO newim.im_conversations(conversation_id,kind) VALUES($1,'group')", recoveryLegacy)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2),($1,$3)", recoveryLegacy, recoveryOwner, recoveryMember)
	now := time.Now().UnixMilli()
	const legacyServer = "legacy_recovery_message"
	const legacyClient = "legacy_recovery_client"
	f.sql("SELECT newim.persist_message($1,$2,$3,$4,'text',1,$5,$6,$7)", recoveryOwner, legacyClient, recoveryLegacy, legacyServer,
		now, encode(t, map[string]string{"text": legacyText}), "legacy_recovery_event")
	f.sql("INSERT INTO newim.im_read_states(conversation_id,user_id,read_seq) VALUES($1,$2,1)", recoveryLegacy, recoveryMember)
	media, _ := openMedia(t, f)
	memberIdentity := trustedIdentity(t, f, member)
	asset := createMediaAsset(t, f, media, memberIdentity, recoveryLegacy)
	var before, after string
	const ledger = "SELECT string_agg(version::text||':'||sha256,',' ORDER BY version) FROM newim_meta.migrations WHERE version<=11"
	must(t, f.db.QueryRow(f.ctx, ledger).Scan(&before))
	f.migrateGroups()
	f.requireMigration(12)
	must(t, f.db.QueryRow(f.ctx, ledger).Scan(&after))
	if before == "" || before != after {
		t.Fatal("migration 012 changed the existing migration ledger")
	}
	requireReferences(t, f, recoveryLegacy, recoveryMember, asset)

	legacyAck := protocol.Ack{SenderID: recoveryOwner, ClientMsgID: legacyClient, ServerMsgID: legacyServer,
		ConversationID: recoveryLegacy, ConversationSeq: "1", ServerTime: strconv.FormatInt(now, 10)}
	gateway := f.gateway("1", nil)
	legacy := subscribe(t, f, gateway, member, recoveryLegacy, 0)
	requireDelivered(t, f, legacy, legacyAck, legacyText)
	must(t, legacy.CloseNow())
	f.call(gateway, http.MethodPost, sendPath, owner, textBody(t, legacyClient, recoveryLegacy, legacyText), nil).requireError(t, 403, "SEND_UNAUTHORIZED")
	requireMessageRows(t, f, recoveryLegacy, 1)

	post[group.View](t, f, gateway, owner, createGroupPath, "view", map[string]any{
		"groupId": recoveryManaged, "profile": map[string]string{"name": "Migrated fixture"}, "joinMode": 2,
	})
	first := send(t, f, gateway, owner, recoveryManaged, "first_managed", firstText)
	join(t, f, gateway, member, recoveryManaged, "late_history_allowed", group.Join, "")
	stream := subscribe(t, f, gateway, member, recoveryManaged, 0)
	requireDelivered(t, f, stream, first, firstText)
	must(t, stream.CloseNow())
	last, err := strconv.ParseInt(first.ConversationSeq, 10, 64)
	must(t, err)
	cursor := changes(t, f, gateway, member, 0, 100).LatestRevision
	if cursor <= 0 {
		t.Fatal("managed member could not checkpoint its account revision")
	}
	second := send(t, f, gateway, owner, recoveryManaged, "saved_offline", secondText)
	left := changeMember(t, f, gateway, member, recoveryManaged, "", group.Leave, 0)
	if left.DepartedThroughSeq == nil || *left.DepartedThroughSeq != 2 {
		t.Fatal("recovery fixture did not preserve the offline message in the departing window")
	}
	send(t, f, gateway, owner, recoveryManaged, "after_member_departure", "already outside the member window")
	requireMessageRows(t, f, recoveryManaged, 3)
	var postmaster time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&postmaster))
	saved := recoveryState{Owner: owner, Member: member, LegacyAsset: asset, LegacyAck: legacyAck, LastSeen: last, ChangeRevision: cursor, OfflineAck: second, Postmaster: postmaster}
	must(t, os.WriteFile(recoveryFixture, encode(t, saved), 0o600))
	gateway.requireRedacted(t, owner, member, legacyText, firstText, secondText, testDSN)
}

func resumeGroupRecovery(t *testing.T, f *fixture) {
	t.Helper()
	f.requireMigration(12)
	raw, err := os.ReadFile(recoveryFixture)
	must(t, err)
	t.Cleanup(func() { _ = os.Remove(recoveryFixture) })
	var saved recoveryState
	must(t, json.Unmarshal(raw, &saved))
	if saved.Owner == "" || saved.Member == "" || saved.LegacyAsset == "" || saved.LastSeen != 1 || saved.ChangeRevision <= 0 || saved.OfflineAck.ConversationSeq != "2" {
		t.Fatal("group recovery fixture incomplete")
	}
	var postmaster time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&postmaster))
	if !postmaster.After(saved.Postmaster) {
		t.Fatal("group recovery did not restart the PostgreSQL postmaster")
	}
	requireReferences(t, f, recoveryLegacy, recoveryMember, saved.LegacyAsset)

	gateway := f.gateway("1", nil)
	legacy := subscribe(t, f, gateway, saved.Member, recoveryLegacy, 0)
	requireDelivered(t, f, legacy, saved.LegacyAck, legacyText)
	historic := subscribe(t, f, gateway, saved.Member, recoveryManaged, saved.LastSeen)
	requireDelivered(t, f, historic, saved.OfflineAck, secondText)
	expectRemovedChange(t, f, gateway, saved.Member, recoveryManaged, saved.ChangeRevision)
	replay := send(t, f, gateway, saved.Owner, recoveryManaged, "saved_offline", secondText)
	if replay != saved.OfflineAck {
		t.Fatal("same intent did not return committed ACK after restart")
	}
	f.call(gateway, http.MethodPost, sendPath, saved.Member, textBody(t, "member_after_recovery", recoveryManaged, "outside permission"), nil).
		requireError(t, 403, "SEND_UNAUTHORIZED")
	requireMessageRows(t, f, recoveryManaged, 3)
	newMessage := send(t, f, gateway, saved.Owner, recoveryManaged, "after_group_recovery", "new live message")
	if newMessage.ConversationSeq != "4" {
		t.Fatal("group sequence did not advance after recovery")
	}
	requireMessageRows(t, f, recoveryManaged, 4)
	reader := openMessageReader(t, f)
	expectWindow(t, f, reader, trustedIdentity(t, f, saved.Member), recoveryManaged, saved.LastSeen, 2, 2)
	expectWindow(t, f, reader, trustedIdentity(t, f, saved.Owner), recoveryManaged, 2, 4, 3, 4)
	gateway.requireRedacted(t, saved.Owner, saved.Member, legacyText, secondText, testDSN)
}
