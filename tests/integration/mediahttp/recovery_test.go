//go:build integration

package mediahttp_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

const recoveryDirectory = "/tmp/newim-media-http-recovery"

type recoveryFixture struct {
	Owner        string
	Peer         string
	Conversation string
	ReadyGrant   uploadGrant
	PendingGrant uploadGrant
	Metadata     protocol.MediaMetadata
	ServerMsgID  string
	Postmaster   time.Time
}

func TestMediaHTTPRecovery(t *testing.T) {
	f := openFixture(t)
	root := filepath.Join(recoveryDirectory, "objects")
	statePath := filepath.Join(recoveryDirectory, "fixture.json")
	content := []byte("synthetic media bytes across restart \x00\xff")
	switch os.Getenv("NEWIM_MEDIA_HTTP_PHASE") {
	case "prepare":
		must(t, os.Mkdir(recoveryDirectory, 0o700))
		g := f.gateway(root, nil)
		owner := f.newAccount("media_recovery_owner")
		peer := f.newAccount("media_recovery_peer")
		id := f.direct(g, owner, "media_recovery_peer")
		intent := intentForBytes(id, content)
		ready := f.grant(g, owner, intent)
		pending := f.grant(g, owner, intent)
		meta := f.completed(g, owner, ready, intent, bytes.NewReader(content), int64(len(content)))
		payload, err := protocol.EncodeMediaPayload(meta)
		must(t, err)
		a := ack(t, f.message(g, owner, id, "media_recovery_send", "media", payload))
		f.expire(ready.MediaKey)
		state := recoveryFixture{Owner: owner, Peer: peer, Conversation: id, ReadyGrant: ready,
			PendingGrant: pending, Metadata: meta, ServerMsgID: a.ServerMsgID, Postmaster: f.postmaster()}
		must(t, os.WriteFile(statePath, encode(t, state), 0o600))
	case "restart":
		t.Cleanup(func() { _ = os.RemoveAll(recoveryDirectory) })
		raw, err := os.ReadFile(statePath)
		must(t, err)
		var state recoveryFixture
		must(t, json.Unmarshal(raw, &state))
		if state.Postmaster.IsZero() || !f.postmaster().After(state.Postmaster) {
			t.Fatal("media recovery did not start a new PostgreSQL postmaster")
		}
		left, right := f.gateway(root, nil), f.gateway(root, nil)
		f.requireDownload(right, state.Peer, state.Metadata, 0)
		resumed := f.delta(right, state.Peer, state.Conversation, "0")
		if len(resumed) != 1 {
			t.Fatal("restart lost the committed media message")
		}
		requireMediaFrame(t, resumed[0], state.ServerMsgID, state.Metadata)
		if online := f.socketFrame(f.subscribe(left, state.Peer, state.Conversation)); !bytes.Equal(online, resumed[0]) {
			t.Fatal("resumed HTTP and WSS returned different stored media frames")
		}
		if empty := f.delta(left, state.Peer, state.Conversation, "1"); len(empty) != 0 {
			t.Fatal("resume after the recovered sequence repeated the message")
		}
		intent := intentForBytes(state.Conversation, content)
		replay := f.completed(left, state.Owner, state.ReadyGrant, intent, bytes.NewReader(content), int64(len(content)))
		if replay != state.Metadata {
			t.Fatal("restart changed the ready upload's idempotent completion")
		}
		f.post(left, downloadPath, state.Peer, map[string]string{"mediaKey": state.PendingGrant.MediaKey}).
			requireError(t, http.StatusNotFound, "MEDIA_NOT_FOUND")
		pending := f.completed(right, state.Owner, state.PendingGrant, intent, bytes.NewReader(content), int64(len(content)))
		f.requireDownload(left, state.Peer, pending, 0)
	default:
		t.Fatal("NEWIM_MEDIA_HTTP_PHASE must be prepare or restart")
	}
}

func (f *fixture) postmaster() time.Time {
	f.t.Helper()
	var started time.Time
	must(f.t, f.db.QueryRow(f.ctx, "SELECT pg_postmaster_start_time()").Scan(&started))
	return started
}
