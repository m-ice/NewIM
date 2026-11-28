//go:build integration

package conversationsynchttp_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

func (f *fixture) accountSettingsGateway(keys string) *process {
	f.t.Helper()
	return f.syncGateway(keys, map[string]string{
		"NEWIM_CONVERSATION_READ_SYNC": "1", "NEWIM_CONVERSATION_ACCOUNT_SETTINGS": "1",
		"NEWIM_READ_WATERMARK_HTTP": "1",
	})
}

func settingsPath(id, action string) string { return "/api/v1/conversations/" + id + "/" + action }

func settingsVersion(version uint64) map[string]string {
	return map[string]string{"expectedVersion": strconv.FormatUint(version, 10)}
}

func accountState(t *testing.T, f *fixture, gateway *process, method, token, id, action string, request any) app.AccountSettings {
	t.Helper()
	var raw []byte
	if request != nil {
		raw = encode(t, request)
	}
	reply := f.call(gateway, method, settingsPath(id, action), token, raw, nil)
	requireStatus(t, reply, http.StatusOK)
	var state app.AccountSettings
	must(t, json.Unmarshal(reply.body, &state))
	var fields map[string]json.RawMessage
	must(t, json.Unmarshal(reply.body, &fields))
	if !reply.verified || len(fields) != 6 || string(fields["settingsVersion"]) != strconv.Quote(strconv.FormatUint(state.Version, 10)) ||
		string(fields["clearThroughSeq"]) != strconv.Quote(strconv.FormatUint(state.ClearThroughSeq, 10)) ||
		string(fields["pinned"]) != strconv.FormatBool(state.Pinned) || string(fields["muted"]) != strconv.FormatBool(state.Muted) ||
		string(fields["deleted"]) != strconv.FormatBool(state.Deleted) || len(fields["draft"]) == 0 || fields["draft"][0] != '"' {
		t.Fatal("account settings response violated the six-field TLS contract")
	}
	return state
}

func requireAccountState(t *testing.T, got, want app.AccountSettings) {
	t.Helper()
	if got != want {
		t.Fatalf("settings version=%d floor=%d deleted=%t pinned=%t muted=%t draft bytes=%d; want %d/%d/%t/%t/%t/%d",
			got.Version, got.ClearThroughSeq, got.Deleted, got.Pinned, got.Muted, len(got.Draft),
			want.Version, want.ClearThroughSeq, want.Deleted, want.Pinned, want.Muted, len(want.Draft))
	}
}

func settingsRevision(t *testing.T, page app.Page, id, kind string, want *app.AccountSettings) app.Item {
	t.Helper()
	if len(page.Items) != 1 || page.HasMore || page.Items[0].ConversationID != id || page.Items[0].Kind != kind {
		t.Fatalf("unexpected account revision count=%d hasMore=%t; expected one %s", len(page.Items), page.HasMore, kind)
	}
	got := page.Items[0]
	if (got.Settings == nil) != (want == nil) {
		t.Fatal("account revision violated settings omission semantics")
	}
	if want != nil {
		requireAccountState(t, *got.Settings, *want)
	}
	return got
}

type accountTestPair struct {
	conversation string
	owner        string
	tablet       string
	peer         string
	ownerID      string
}

func accountPair(t *testing.T, f *fixture, gateway *process, prefix string) accountTestPair {
	t.Helper()
	server := f.readSyncServer()
	ownerID, peerID := prefix+"_owner", prefix+"_peer"
	provision(t, f, server, ownerID)
	provision(t, f, server, peerID)
	owner, tablet := f.admit(ownerID, "phone"), f.admit(ownerID, "tablet")
	peer := f.admit(peerID, "phone")
	return accountTestPair{
		conversation: openSyncDirect(t, f, gateway, owner, peerID),
		owner:        owner, tablet: tablet, peer: peer, ownerID: ownerID,
	}
}

func TestAccountSettingsHTTPCheck(t *testing.T) {
	t.Run("cross-device-settings-history-and-restoration", testAccountHistory)
	t.Run("escaped-draft-and-page-bytes", testAccountDraftPages)
}

func testAccountHistory(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(18)
	keys := writeKeys(t, "old", "old")
	left, right := f.accountSettingsGateway(keys), f.accountSettingsGateway(keys)
	off := f.readSyncGateway(keys)
	pair := accountPair(t, f, left, "account_history")
	id, owner, tablet, peer := pair.conversation, pair.owner, pair.tablet, pair.peer
	first := send(t, f, left, owner, id, "account_old_a", "owner history")
	second := send(t, f, right, peer, id, "account_old_b", "peer history")
	requireWatermark(t, advanceWatermark(t, f, right, peer, id, "1"), id, "1", "0", "2", true)
	requireReceipt(t, f, left, owner, id, first.ServerMsgID, true)
	ownerPage := readPage(t, f, left, bootstrapPath, owner, map[string]any{})
	peerPage := readPage(t, f, right, bootstrapPath, peer, map[string]any{})
	settingsRevision(t, ownerPage, id, "upsert", nil)
	settingsRevision(t, peerPage, id, "upsert", nil)
	requireAccountState(t, accountState(t, f, right, http.MethodGet, tablet, id, "settings", nil), app.AccountSettings{})

	prefs := app.AccountSettings{Version: 1, Pinned: true, Muted: true, Draft: "本地草稿"}
	patch := map[string]any{"expectedVersion": "0", "pinned": true, "muted": true, "draft": prefs.Draft}
	requireAccountState(t, accountState(t, f, left, http.MethodPatch, owner, id, "settings", patch), prefs)
	requireAccountState(t, accountState(t, f, right, http.MethodGet, tablet, id, "settings", nil), prefs)
	requireAccountState(t, accountState(t, f, right, http.MethodGet, peer, id, "settings", nil), app.AccountSettings{})
	revision := readPage(t, f, right, deltaPath, tablet, checkpoint(ownerPage.NextCursor))
	saved := settingsRevision(t, revision, id, "upsert", &prefs)
	if saved.LastReadSeq == nil || *saved.LastReadSeq != 0 {
		t.Fatal("settings update lost the account's read watermark")
	}
	noOp := map[string]any{"expectedVersion": "1", "draft": prefs.Draft}
	requireAccountState(t, accountState(t, f, right, http.MethodPatch, tablet, id, "settings", noOp), prefs)
	requireAccountQuiet(t, f, left, owner, revision.NextCursor)
	requireAccountQuiet(t, f, right, peer, peerPage.NextCursor)
	advanceWatermark(t, f, left, owner, id, "2")
	marked := readPage(t, f, right, deltaPath, tablet, checkpoint(revision.NextCursor))
	carry := settingsRevision(t, marked, id, "upsert", &prefs)
	if carry.LastReadSeq == nil || *carry.LastReadSeq != 2 {
		t.Fatal("creating a new read revision lost saved settings")
	}

	cleared := prefs
	cleared.Version, cleared.ClearThroughSeq = 2, 2
	requireAccountState(t, accountState(t, f, left, http.MethodPost, owner, id, "clear", settingsVersion(1)), cleared)
	privatePage := readPage(t, f, right, deltaPath, tablet, checkpoint(marked.NextCursor))
	got := settingsRevision(t, privatePage, id, "upsert", &cleared)
	if got.LatestConversationSeq == nil || *got.LatestConversationSeq != 0 || got.LatestServerMsgID != nil {
		t.Fatal("cleared account directory still contains a hidden summary")
	}
	floor, tail, frames := accountMessageHistory(t, f, off, owner, id)
	if floor != "2" || tail != "2" || len(frames) != 0 {
		t.Fatal("compatibility reader did not enforce personal history")
	}
	old := readPage(t, f, off, bootstrapPath, owner, map[string]any{})
	oldItem := settingsRevision(t, old, id, "upsert", nil)
	if oldItem.LatestConversationSeq == nil || *oldItem.LatestConversationSeq != 0 {
		t.Fatal("settings-off directory exposed a cleared summary")
	}
	requireWatermark(t, getWatermark(t, f, right, tablet, id), id, "2", "0", "2", true)
	requireSyncError(t, f.call(left, http.MethodGet, receiptPath(id, first.ServerMsgID), owner, nil, nil), 404, "READ_NOT_FOUND")
	requireReceipt(t, f, left, peer, id, second.ServerMsgID, true)
	_, _, peerFrames := accountMessageHistory(t, f, right, peer, id)
	if len(peerFrames) != 2 || peerFrames[0].ServerMsgID != first.ServerMsgID || peerFrames[1].ServerMsgID != second.ServerMsgID {
		t.Fatal("personal clear changed peer history")
	}

	deleted := app.AccountSettings{Version: 3, ClearThroughSeq: 2, Deleted: true}
	requireAccountState(t, accountState(t, f, right, http.MethodPost, tablet, id, "delete", settingsVersion(2)), deleted)
	tombstone := readPage(t, f, left, deltaPath, owner, checkpoint(privatePage.NextCursor))
	settingsRevision(t, tombstone, id, "remove", nil)
	quiet := readPage(t, f, off, bootstrapPath, owner, map[string]any{})
	if len(quiet.Items) != 0 || quiet.HasMore {
		t.Fatal("settings-off directory restored a deleted conversation")
	}
	requireAccountQuiet(t, f, right, peer, peerPage.NextCursor)

	ws := accountHistorySocket(t, f, off, owner, id)
	fresh := send(t, f, right, peer, id, "account_new_after_delete", "restores the conversation")
	kind, wire, err := ws.Read(f.ctx)
	must(t, err)
	frame, err := protocol.DecodeServerFrame(wire)
	must(t, err)
	if kind != websocket.MessageText || frame.Message == nil || frame.Message.ServerMsgID != fresh.ServerMsgID ||
		frame.Message.ConversationSeq != "3" {
		t.Fatal("WSS delivered hidden history instead of the newly committed message")
	}
	restored := app.AccountSettings{Version: 4, ClearThroughSeq: 2}
	requireAccountState(t, accountState(t, f, right, http.MethodGet, tablet, id, "settings", nil), restored)
	page := readPage(t, f, left, deltaPath, owner, checkpoint(tombstone.NextCursor))
	settingsRevision(t, page, id, "upsert", &restored)
	floor, tail, frames = accountMessageHistory(t, f, left, owner, id)
	if floor != "2" || tail != "3" || len(frames) != 1 || frames[0].ServerMsgID != fresh.ServerMsgID {
		t.Fatal("restored conversation did not retain the personal history boundary")
	}
	left.requireRedacted(t, owner, tablet, peer, prefs.Draft, testDSN)
}

func requireAccountQuiet(t *testing.T, f *fixture, gateway *process, token, cursor string) {
	t.Helper()
	page := readPage(t, f, gateway, deltaPath, token, checkpoint(cursor))
	if page.HasMore || len(page.Items) != 0 {
		t.Fatal("unrelated or no-op settings write appended a revision")
	}
}

func accountMessageHistory(t *testing.T, f *fixture, gateway *process, token, id string) (string, string, []protocol.Message) {
	t.Helper()
	wire := encode(t, map[string]string{"conversationId": id, "afterSeq": "0"})
	reply := f.call(gateway, http.MethodPost, "/api/v1/sync/messages", token, wire, nil)
	requireStatus(t, reply, http.StatusOK)
	var page struct {
		Floor string            `json:"historyAfterSeq"`
		Tail  string            `json:"latestSeq"`
		Items []json.RawMessage `json:"items"`
	}
	must(t, json.Unmarshal(reply.body, &page))
	if !reply.verified || page.Items == nil {
		t.Fatal("message history did not return verified, bounded TLS frames")
	}
	frames := make([]protocol.Message, len(page.Items))
	for i, raw := range page.Items {
		frame, err := protocol.DecodeServerFrame(raw)
		must(t, err)
		if frame.Message == nil || frame.Message.ConversationID != id {
			t.Fatal("message history did not return the requested conversation's frames")
		}
		frames[i] = *frame.Message
	}
	return page.Floor, page.Tail, frames
}

func accountHistorySocket(t *testing.T, f *fixture, gateway *process, token, id string) *websocket.Conn {
	t.Helper()
	return accountHistorySocketFrom(t, f, gateway, token, id, "0")
}

func accountHistorySocketFrom(t *testing.T, f *fixture, gateway *process, token, id, afterSeq string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	payload := encode(t, map[string]string{"conversationId": id, "afterSeq": afterSeq})
	conn, err := dialHistorySocket(ctx, f.client, gateway.url, token, payload)
	must(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func dialHistorySocket(ctx context.Context, client *http.Client, gatewayURL, token string, payload []byte) (*websocket.Conn, error) {
	conn, reply, err := websocket.Dial(ctx, "wss://"+strings.TrimPrefix(gatewayURL, "https://")+"/api/v1/gateway",
		&websocket.DialOptions{HTTPClient: client, HTTPHeader: http.Header{"Authorization": {"Bearer " + token}}, Subprotocols: []string{"newim.v1"}})
	if err != nil {
		return nil, err
	}
	if reply == nil || reply.StatusCode != http.StatusSwitchingProtocols || reply.TLS == nil ||
		len(reply.TLS.VerifiedChains) == 0 || conn.Subprotocol() != "newim.v1" {
		_ = conn.CloseNow()
		return nil, errors.New("account history subscription did not use verified native WSS")
	}
	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		_ = conn.CloseNow()
		return nil, err
	}
	return conn, nil
}

func testAccountDraftPages(t *testing.T) {
	f := openFixture(t)
	server := f.readSyncServer()
	keys := writeKeys(t, "old", "old")
	left, right := f.accountSettingsGateway(keys), f.accountSettingsGateway(keys)
	const user = "account_large_draft"
	provision(t, f, server, user)
	token := f.admit(user, "phone")
	draft := strings.Repeat("\x00", app.MaxAccountDraftBytes)
	wanted := app.AccountSettings{Version: 1, Draft: draft}
	ids := []string{"account_draft_a", "account_draft_b", "account_draft_c", "account_draft_d", "account_draft_e"}
	for _, id := range ids {
		createGroup(t, f, left, token, id, nil, true)
		got := accountState(t, f, right, http.MethodPatch, token, id, "settings", map[string]any{"expectedVersion": "0", "draft": draft})
		requireAccountState(t, got, wanted)
	}
	requireSyncError(t, f.call(left, http.MethodPatch, settingsPath(ids[0], "settings"), token,
		encode(t, map[string]any{"expectedVersion": "1", "draft": draft + "x"}), nil), 400, "SYNC_INVALID_INPUT")
	var request any = map[string]int{"limit": app.MaxPageItems}
	for start := 0; start < len(ids); start += 2 {
		page := readPage(t, f, left, bootstrapPath, token, request)
		end := min(len(ids), start+2)
		if len(page.Items) != end-start || page.HasMore != (end < len(ids)) {
			t.Fatal("escaped drafts exceeded or prematurely consumed the encoded page byte budget")
		}
		for i, item := range page.Items {
			if item.ConversationID != ids[start+i] || item.Settings == nil {
				t.Fatal("draft continuation omitted a conversation or its settings")
			}
			requireAccountState(t, *item.Settings, wanted)
		}
		request = continueWith(page.NextCursor)
	}
	requireAccountState(t, accountState(t, f, left, http.MethodPatch, token, ids[0], "settings",
		map[string]any{"expectedVersion": "1", "draft": ""}), app.AccountSettings{Version: 2})
}

func TestAccountSettingsHTTPSecurity(t *testing.T) {
	t.Run("authentication-versioning-and-atomicity", testAccountVersionAndRollback)
	t.Run("stale-clear-and-delete-pages", testAccountStaleHistoryPages)
}

func testAccountVersionAndRollback(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(18)
	keys := writeKeys(t, "old", "old")
	left, right := f.accountSettingsGateway(keys), f.accountSettingsGateway(keys)
	off := f.readSyncGateway(keys)
	pair := accountPair(t, f, left, "account_versions")
	id, token, tablet, peer := pair.conversation, pair.owner, pair.tablet, pair.peer
	f.user("account_versions_other")
	outsider := f.admit("account_versions_other", "phone")
	path := settingsPath(id, "settings")
	requireAbsent(t, f.call(off, http.MethodGet, path, token, nil, nil))
	requireSyncError(t, f.call(left, http.MethodGet, path, "", nil, nil), 401, "AUTH_INVALID_TOKEN")
	requireSyncError(t, f.call(left, http.MethodGet, path, outsider, nil, nil), 404, "SYNC_CONVERSATION_NOT_FOUND")
	requireSyncError(t, f.call(left, http.MethodGet, settingsPath("account_absent", "settings"), token, nil, nil), 404, "SYNC_CONVERSATION_NOT_FOUND")
	requireSyncError(t, f.call(left, http.MethodGet, path, token, []byte("{}"), nil), 400, "SYNC_INVALID_INPUT")
	requireSyncError(t, f.call(left, http.MethodPost, path, token, nil, nil), 405, "HTTP_METHOD_NOT_ALLOWED")
	for _, body := range []string{
		`{"expectedVersion":0,"pinned":true}`, `{"expectedVersion":"00","pinned":true}`,
		`{"expectedVersion":"0"}`, `{"expectedVersion":"0","pinned":null}`,
		`{"expectedVersion":"0","pinned":true,"userId":"account_versions_other"}`,
	} {
		requireSyncError(t, f.call(left, http.MethodPatch, path, token, []byte(body), nil), 400, "SYNC_INVALID_INPUT")
	}
	requireSyncError(t, f.call(left, http.MethodPatch, path, token, []byte(strings.Repeat(" ", 32769)), nil), 413, "HTTP_BODY_TOO_LARGE")
	requireSyncError(t, f.call(left, http.MethodPost, settingsPath(id, "clear"), token,
		[]byte(`{"expectedVersion":"0","draft":"mixed action"}`), nil), 400, "SYNC_INVALID_INPUT")
	ownerPage := readPage(t, f, left, bootstrapPath, token, map[string]any{})
	peerPage := readPage(t, f, right, bootstrapPath, peer, map[string]any{})
	ownerMutation := map[string]any{"expectedVersion": "0", "pinned": true}
	f.sql(`CREATE FUNCTION newim.fixture_account_settings_failure() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'fixture refused settings commit'; END;
		$$;
		CREATE CONSTRAINT TRIGGER fixture_account_settings_failure
		AFTER INSERT ON newim.im_conversation_sync_changes DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW WHEN (NEW.settings_version=1) EXECUTE FUNCTION newim.fixture_account_settings_failure()`)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := f.db.Exec(ctx, `DROP TRIGGER IF EXISTS fixture_account_settings_failure ON newim.im_conversation_sync_changes;
			DROP FUNCTION IF EXISTS newim.fixture_account_settings_failure()`); err != nil {
			t.Error("unable to remove the settings rollback fixture")
		}
	}()
	requireSyncError(t, f.call(left, http.MethodPatch, path, token, encode(t, ownerMutation), nil), 503, "SYNC_STORAGE_UNAVAILABLE")
	requireAccountState(t, accountState(t, f, right, http.MethodGet, tablet, id, "settings", nil), app.AccountSettings{})
	var durable int
	must(t, f.db.QueryRow(f.ctx, `SELECT count(*) FROM newim.im_conversation_account_settings
		WHERE conversation_id=$1 AND user_id=$2`, id, pair.ownerID).Scan(&durable))
	if durable != 0 {
		t.Fatal("failed projection commit persisted account settings")
	}
	requireAccountQuiet(t, f, right, tablet, ownerPage.NextCursor)
	f.sql("DROP TRIGGER fixture_account_settings_failure ON newim.im_conversation_sync_changes")

	start := make(chan struct{})
	results := make(chan response, 2)
	raw := encode(t, ownerMutation)
	for _, sender := range []struct {
		p     *process
		token string
	}{{left, token}, {right, tablet}} {
		go func() {
			<-start
			results <- f.call(sender.p, http.MethodPatch, path, sender.token, raw, nil)
		}()
	}
	close(start)
	var successes int
	for range 2 {
		got := <-results
		if got.status == http.StatusOK {
			requireStatus(t, got, http.StatusOK)
			successes++
		} else {
			requireSyncError(t, got, http.StatusConflict, "SYNC_ACCOUNT_STATE_CONFLICT")
		}
	}
	if successes != 1 {
		t.Fatal("concurrent same-version writes did not commit only one change")
	}
	first := app.AccountSettings{Version: 1, Pinned: true}
	requireAccountState(t, accountState(t, f, right, http.MethodGet, tablet, id, "settings", nil), first)
	requireSyncError(t, f.call(left, http.MethodPatch, path, token, raw, nil), 409, "SYNC_ACCOUNT_STATE_CONFLICT")
	update := readPage(t, f, right, deltaPath, tablet, checkpoint(ownerPage.NextCursor))
	settingsRevision(t, update, id, "upsert", &first)
	requireAccountQuiet(t, f, right, peer, peerPage.NextCursor)

	for _, name := range []string{"clear", "delete"} {
		requireSyncError(t, f.call(left, http.MethodPost, settingsPath(id, name), token, encode(t, settingsVersion(0)), nil),
			409, "SYNC_ACCOUNT_STATE_CONFLICT")
	}
	deleted := app.AccountSettings{Version: 2, Deleted: true}
	requireAccountState(t, accountState(t, f, left, http.MethodPost, token, id, "delete", settingsVersion(1)), deleted)
	remove := readPage(t, f, right, deltaPath, tablet, checkpoint(update.NextCursor))
	settingsRevision(t, remove, id, "remove", nil)
	requireAccountState(t, accountState(t, f, right, http.MethodGet, tablet, id, "settings", nil), deleted)
	requireAccountState(t, accountState(t, f, right, http.MethodPost, tablet, id, "delete", settingsVersion(2)), deleted)
	requireSyncError(t, f.call(left, http.MethodPost, settingsPath(id, "delete"), token, encode(t, settingsVersion(1)), nil),
		409, "SYNC_ACCOUNT_STATE_CONFLICT")
	requireSyncError(t, f.call(left, http.MethodPost, settingsPath(id, "clear"), token, encode(t, settingsVersion(2)), nil),
		409, "SYNC_CONVERSATION_DELETED")
	requireSyncError(t, f.call(left, http.MethodPatch, path, token, []byte(`{"expectedVersion":"2","muted":true}`), nil),
		409, "SYNC_CONVERSATION_DELETED")
	requireAccountQuiet(t, f, left, token, remove.NextCursor)
	_, err := f.auth.RevokeBearer(f.ctx, tablet)
	must(t, err)
	requireSyncError(t, f.call(right, http.MethodGet, path, tablet, nil, nil), 401, "AUTH_INVALID_TOKEN")
	left.requireRedacted(t, token, tablet, peer, outsider, testDSN, "fixture refused settings commit")
}

func testAccountStaleHistoryPages(t *testing.T) {
	f := openFixture(t)
	server := f.readSyncServer()
	keys := writeKeys(t, "old", "old")
	left, right := f.accountSettingsGateway(keys), f.accountSettingsGateway(keys)
	const owner, member = "account_pages_owner", "account_pages_member"
	provision(t, f, server, owner)
	provision(t, f, server, member)
	host, peer := f.admit(owner, "phone"), f.admit(member, "phone")
	const a, b = "account_pages_a", "account_pages_b"
	for _, id := range []string{a, b} {
		createGroup(t, f, left, host, id, []string{member}, true)
	}
	send(t, f, left, host, b, "account_pages_old", "old summary")
	first := readPage(t, f, left, bootstrapPath, host, map[string]int{"limit": 1})
	if !first.HasMore || len(first.Items) != 1 || first.Items[0].ConversationID != a {
		t.Fatal("old settings cursor fixture did not stop before the later conversation")
	}
	requireAccountState(t, accountState(t, f, right, http.MethodPost, host, b, "clear", settingsVersion(0)), app.AccountSettings{Version: 1, ClearThroughSeq: 1})
	requireSyncError(t, f.call(left, http.MethodPost, bootstrapPath, host, encode(t, continueWith(first.NextCursor)), nil), 409, "SYNC_CURSOR_EXPIRED")

	send(t, f, right, peer, b, "account_pages_next", "new visible summary")
	page := readPage(t, f, left, bootstrapPath, host, map[string]int{"limit": 1})
	if !page.HasMore || len(page.Items) != 1 || page.Items[0].ConversationID != a {
		t.Fatal("second settings cursor fixture did not stop before the later conversation")
	}
	accountState(t, f, right, http.MethodPost, host, b, "delete", settingsVersion(1))
	requireSyncError(t, f.call(left, http.MethodPost, bootstrapPath, host, encode(t, continueWith(page.NextCursor)), nil), 409, "SYNC_CURSOR_EXPIRED")
	leave(t, f, left, peer, b)
	requireSyncError(t, f.call(right, http.MethodGet, settingsPath(b, "settings"), peer, nil, nil), 404, "SYNC_CONVERSATION_NOT_FOUND")
}

const accountSettingsRecoveryFile = "/tmp/newim-account-settings-recovery.json"

type accountSettingsRecovery struct {
	Conversation string
	Owner        string
	Tablet       string
	Peer         string
	Cursor       string
	Postmaster   time.Time
}

func TestAccountSettingsHTTPRecovery(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(18)
	keys := writeKeys(t, "old", "old")
	left, right := f.accountSettingsGateway(keys), f.accountSettingsGateway(keys)
	switch os.Getenv("NEWIM_SYNC_HTTP_PHASE") {
	case "prepare":
		pair := accountPair(t, f, left, "account_recovery")
		id := pair.conversation
		send(t, f, left, pair.owner, id, "account_before_restart", "durable before restart")
		accountState(t, f, right, http.MethodPatch, pair.tablet, id, "settings", map[string]any{
			"expectedVersion": "0", "pinned": true, "draft": "saved before removal",
		})
		page := readPage(t, f, left, bootstrapPath, pair.owner, map[string]any{})
		settingsRevision(t, page, id, "upsert", &app.AccountSettings{Version: 1, Pinned: true, Draft: "saved before removal"})
		requireAccountState(t, accountState(t, f, right, http.MethodPost, pair.tablet, id, "delete", settingsVersion(1)),
			app.AccountSettings{Version: 2, ClearThroughSeq: 1, Deleted: true})
		saved := accountSettingsRecovery{Conversation: id, Owner: pair.owner, Tablet: pair.tablet, Peer: pair.peer,
			Cursor: page.NextCursor, Postmaster: watermarkPostmaster(t, f)}
		must(t, os.WriteFile(accountSettingsRecoveryFile, encode(t, saved), 0o600))
	case "restart":
		raw, err := os.ReadFile(accountSettingsRecoveryFile)
		must(t, err)
		t.Cleanup(func() { _ = os.Remove(accountSettingsRecoveryFile) })
		var saved accountSettingsRecovery
		must(t, json.Unmarshal(raw, &saved))
		if saved.Postmaster.IsZero() || !watermarkPostmaster(t, f).After(saved.Postmaster) {
			t.Fatal("account settings recovery did not start a new PostgreSQL postmaster")
		}
		id := saved.Conversation
		requireAccountState(t, accountState(t, f, right, http.MethodGet, saved.Tablet, id, "settings", nil),
			app.AccountSettings{Version: 2, ClearThroughSeq: 1, Deleted: true})
		remove := readPage(t, f, left, deltaPath, saved.Owner, checkpoint(saved.Cursor))
		settingsRevision(t, remove, id, "remove", nil)
		floor, tail, messages := accountMessageHistory(t, f, right, saved.Owner, id)
		if floor != "1" || tail != "1" || len(messages) != 0 {
			t.Fatal("personal history floor was lost after PostgreSQL restart")
		}
		_, _, messages = accountMessageHistory(t, f, right, saved.Peer, id)
		if len(messages) != 1 {
			t.Fatal("peer history was lost after PostgreSQL restart")
		}
		next := send(t, f, right, saved.Peer, id, "account_after_restart", "restored after restart")
		restored := readPage(t, f, left, deltaPath, saved.Tablet, checkpoint(remove.NextCursor))
		row := settingsRevision(t, restored, id, "upsert", &app.AccountSettings{Version: 3, ClearThroughSeq: 1})
		if row.LatestServerMsgID == nil || *row.LatestServerMsgID != next.ServerMsgID {
			t.Fatal("resumed cursor did not observe the new visible conversation summary")
		}
	default:
		t.Fatal("NEWIM_SYNC_HTTP_PHASE must be prepare or restart")
	}
}

func TestAccountSettingsHTTPWide(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(18)
	const owner = "account_settings_wide"
	f.user(owner)
	token := f.admit(owner, "phone")
	ids := make([]string, 205)
	for i := range ids {
		ids[i] = fmt.Sprintf("account_settings_wide_%04d", i)
	}
	f.sql("INSERT INTO newim.im_conversations(conversation_id,kind) SELECT id,'group' FROM unnest($1::text[]) AS ids(id)", ids)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) SELECT id,$2 FROM unnest($1::text[]) AS ids(id)", ids, owner)
	f.sql(`INSERT INTO newim.im_conversation_account_settings(conversation_id,user_id,version,pinned)
		SELECT id,$2,1,true FROM unnest($1::text[]) WITH ORDINALITY AS ids(id,n) WHERE n % 2 = 1`, ids, owner)
	runReadSyncBackfill(t, f, "1", "SYNC_SOURCE_COMPLETE", "run", "--limit", "40")
	gateway := f.accountSettingsGateway(writeKeys(t, "old", "old"))
	var request any = map[string]int{"limit": app.MaxPageItems}
	for start := 0; start < len(ids); start += app.MaxPageItems {
		page := readPage(t, f, gateway, bootstrapPath, token, request)
		end := min(len(ids), start+app.MaxPageItems)
		if len(page.Items) != end-start || page.HasMore != (end < len(ids)) {
			t.Fatal("wide account settings directory violated the page limit")
		}
		for i, item := range page.Items {
			index := start + i
			if item.ConversationID != ids[index] || (item.Settings != nil) != (index%2 == 0) {
				t.Fatal("wide directory omitted a member key or its optional settings")
			}
			if item.Settings != nil {
				requireAccountState(t, *item.Settings, app.AccountSettings{Version: 1, Pinned: true})
			}
		}
		request = continueWith(page.NextCursor)
	}
}
