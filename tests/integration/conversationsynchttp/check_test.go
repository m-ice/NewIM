//go:build integration

package conversationsynchttp_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	store "github.com/m-ice/NewIM/server/storage/conversationsync"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

const (
	bootstrapPath = "/api/v1/sync/bootstrap"
	deltaPath     = "/api/v1/sync/delta"
)

func TestSyncHTTPCheck(t *testing.T) {
	t.Run("bootstrap-and-delta-across-devices", testCrossGatewayPages)
	t.Run("bounded-page-and-continuation", testMaximumPage)
}

func testCrossGatewayPages(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(14)
	keys := writeKeys(t, "old", "old")
	left, right := f.syncGateway(keys, nil), f.syncGateway(keys, nil)
	loopback := f.server("1", nil)
	const user = "synchttp_check_owner"
	provision(t, f, loopback, user)
	phone := f.admit(user, "phone")
	tablet := f.admit(user, "tablet")
	empty := readPage(t, f, left, bootstrapPath, phone, map[string]any{})
	if len(empty.Items) != 0 || empty.HasMore {
		t.Fatal("newly provisioned account did not have a ready empty projection")
	}
	const first = "synchttp_check_a"
	const second = "synchttp_check_b"
	const third = "synchttp_check_c"
	const late = "synchttp_check_d"
	for _, id := range []string{first, second, third} {
		createGroup(t, f, left, phone, id, nil, true)
	}
	firstPage := readPage(t, f, left, bootstrapPath, phone, map[string]int{"limit": 1})
	expectItem(t, firstPage, true, first, "upsert", 1, 0, "")
	ack := send(t, f, loopback, phone, second, "sync_projection_source_message", "private payload stays outside sync")
	createGroup(t, f, right, tablet, late, nil, true)

	secondPage := readPage(t, f, right, bootstrapPath, tablet, continueWith(firstPage.NextCursor))
	expectItem(t, secondPage, true, second, "upsert", 2, 0, "")
	thirdPage := readPage(t, f, left, bootstrapPath, phone, continueWith(secondPage.NextCursor))
	expectItem(t, thirdPage, false, third, "upsert", 3, 0, "")
	deltaFirst := readPage(t, f, right, deltaPath, tablet, map[string]any{"checkpoint": thirdPage.NextCursor, "limit": 1})
	expectItem(t, deltaFirst, true, second, "upsert", 4, 1, ack.ServerMsgID)
	deltaLast := readPage(t, f, left, deltaPath, phone, continueWith(deltaFirst.NextCursor))
	expectItem(t, deltaLast, false, late, "upsert", 5, 0, "")
	quiet := readPage(t, f, right, deltaPath, tablet, map[string]string{"checkpoint": deltaLast.NextCursor})
	if len(quiet.Items) != 0 || quiet.HasMore {
		t.Fatal("final checkpoint delivered already consumed revisions")
	}
	left.requireRedacted(t, phone, tablet, firstPage.NextCursor, deltaFirst.NextCursor, fixtureKeyHex("old"), testDSN)
	right.requireRedacted(t, phone, tablet, secondPage.NextCursor, deltaLast.NextCursor, "private payload stays outside sync")
}

func testMaximumPage(t *testing.T) {
	f := openFixture(t)
	loopback := f.server("1", nil)
	gateway := f.syncGateway(writeKeys(t, "old", "old"), nil)
	const user = "synchttp_wide_owner"
	provision(t, f, loopback, user)
	token := f.admit(user, "mobile")
	ids := make([]string, app.MaxPageItems+1)
	for i := range ids {
		ids[i] = strings.Repeat("a", 124) + fmt.Sprintf("%04d", i)
	}
	f.sql("INSERT INTO newim.im_conversations(conversation_id,kind) SELECT id,'group' FROM unnest($1::text[]) AS ids(id)", ids)
	f.sql("INSERT INTO newim.im_conversation_members(conversation_id,user_id) SELECT id,$2 FROM unnest($1::text[]) AS ids(id)", ids, user)
	f.sql(`SELECT newim.persist_message($2,id,id,'b'||right(id,127),'text',1,$3,
		convert_to('{"text":"bounded response fixture"}','UTF8'),'event_'||right(id,4))
		FROM unnest($1::text[]) AS ids(id)`, ids, user, time.Now().UnixMilli())
	repository := openRepository(t, f)
	for start := 0; start < len(ids); start += app.MaxPageItems {
		chunk := ids[start:min(len(ids), start+app.MaxPageItems)]
		tx, err := repository.Begin(f.ctx)
		must(t, err)
		t.Cleanup(func() { _ = tx.Rollback(f.ctx) })
		batch, err := repository.PrepareBatch(f.ctx, tx, chunk, []string{user})
		must(t, err)
		for _, conv := range chunk {
			_, err = batch.RecordUpsert(f.ctx, conv, user)
			must(t, err)
		}
		must(t, tx.Commit(f.ctx))
	}
	first := readPage(t, f, gateway, bootstrapPath, token, map[string]int{"limit": 100})
	if len(first.Items) != 100 || !first.HasMore || first.Items[0].ConversationID != ids[0] ||
		first.Items[99].ConversationID != ids[99] || first.Items[0].LatestServerMsgID == nil || len(*first.Items[0].LatestServerMsgID) != 128 {
		t.Fatal("wide bootstrap did not return precisely 100 bounded ordered items")
	}
	last := readPage(t, f, gateway, bootstrapPath, token, continueWith(first.NextCursor))
	expectItem(t, last, false, ids[100], "upsert", 101, 1, "b"+ids[100][1:])
}

func (f *fixture) syncGateway(path string, overrides map[string]string) *process {
	f.t.Helper()
	settings := syncSettings(path)
	for key, value := range overrides {
		settings[key] = value
	}
	return f.gateway("1", settings)
}

func syncSettings(path string) map[string]string {
	return map[string]string{
		"NEWIM_CONVERSATION_SYNC":           "1",
		"NEWIM_CONVERSATION_SYNC_KEYS_FILE": path,
	}
}

func fixtureKey(id string) []byte {
	digest := sha256.Sum256([]byte("synthetic conversation sync key " + id))
	return digest[:]
}

func fixtureKeyHex(id string) string { return hex.EncodeToString(fixtureKey(id)) }

func writeKeys(t *testing.T, active string, ids ...string) string {
	t.Helper()
	keys := make(map[string]string, len(ids))
	for _, id := range ids {
		keys[id] = fixtureKeyHex(id)
	}
	path := filepath.Join(t.TempDir(), "sync-keys.json")
	data := encode(t, map[string]any{"activeKeyId": active, "keys": keys})
	must(t, os.WriteFile(path, data, 0o600))
	return path
}

func openRepository(t *testing.T, f *fixture) *store.Repository {
	t.Helper()
	repository, err := store.Open(f.ctx, store.Config{DSN: testDSN, AllowLocalSocket: true, MaxConnections: 3, RequireSourceReady: true})
	must(t, err)
	t.Cleanup(repository.Close)
	return repository
}

func continueWith(cursor string) map[string]string { return map[string]string{"cursor": cursor} }

func readPage(t *testing.T, f *fixture, gateway *process, path, token string, body any) app.Page {
	t.Helper()
	return requirePage(t, f.call(gateway, http.MethodPost, path, token, encode(t, body), nil))
}

func requirePage(t *testing.T, reply response) app.Page {
	t.Helper()
	if reply.err != nil || reply.status != http.StatusOK || !reply.verified {
		t.Fatalf("sync HTTP status=%d TLS=%t err=%v", reply.status, reply.verified, reply.err)
	}
	if reply.headers.Get("Cache-Control") != "no-store" || reply.headers.Get("X-Content-Type-Options") != "nosniff" ||
		reply.headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatal("sync HTTP response did not preserve required headers")
	}
	if len(reply.body) > app.MaxResponseBytes {
		t.Fatalf("sync HTTP response has %d bytes", len(reply.body))
	}
	var envelope map[string]json.RawMessage
	must(t, json.Unmarshal(reply.body, &envelope))
	if len(envelope) != 3 || len(envelope["items"]) == 0 || len(envelope["nextCursor"]) == 0 ||
		(string(envelope["hasMore"]) != "true" && string(envelope["hasMore"]) != "false") {
		t.Fatal("response did not serialize the existing Page directly")
	}
	var page app.Page
	decoder := json.NewDecoder(bytes.NewReader(reply.body))
	decoder.DisallowUnknownFields()
	must(t, decoder.Decode(&page))
	if page.Items == nil || len(page.Items) > app.MaxPageItems || page.NextCursor == "" || len(page.NextCursor) > app.MaxCursorBytes {
		t.Fatal("sync HTTP response does not satisfy the page/cursor limits")
	}
	return page
}

func expectItem(t *testing.T, page app.Page, more bool, id, kind string, rev, seq uint64, server string) {
	t.Helper()
	if page.HasMore != more || len(page.Items) != 1 {
		t.Fatalf("sync item count=%d hasMore=%t, want 1 and %t", len(page.Items), page.HasMore, more)
	}
	got := page.Items[0]
	if got.ConversationID != id || got.Kind != kind || got.Revision != rev {
		t.Fatalf("sync change kind=%s revision=%d, want %s and %d", got.Kind, got.Revision, kind, rev)
	}
	if kind == "remove" {
		if got.LatestConversationSeq != nil || got.LatestServerMsgID != nil {
			t.Fatal("sync tombstone carried a summary")
		}
		return
	}
	if got.LatestConversationSeq == nil || *got.LatestConversationSeq != seq ||
		(server == "" && got.LatestServerMsgID != nil) ||
		(server != "" && (got.LatestServerMsgID == nil || *got.LatestServerMsgID != server)) {
		t.Fatal("sync summary did not expose exactly the authorized head")
	}
}
