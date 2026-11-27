//go:build integration

package mediahttp_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/group"
)

func TestMediaHTTPSecurity(t *testing.T) {
	t.Run("disabled-and-strict-http", testStrictTransport)
	t.Run("private-and-immutable", testPrivateObjects)
	t.Run("revoke-or-expire-during-upload", testTransferRevalidation)
	t.Run("finalization-locks", testFinalizationLocks)
	t.Run("retry-after-commit-rejection", testCompletionRollback)
}

func testStrictTransport(t *testing.T) {
	f := openFixture(t)
	owner := f.newAccount("media_strict_owner")
	otherDevice := f.admit("media_strict_owner", "desktop")
	peer := f.newAccount("media_strict_peer")
	disabled := f.gateway("", nil)
	id := f.direct(disabled, owner, "media_strict_peer")
	payload := []byte("strict-media-fixture")
	intent := intentForBytes(id, payload)
	for _, route := range []string{grantPath, completePath, downloadPath} {
		r := f.post(disabled, route, owner, intent)
		if r.err != nil || r.status != http.StatusNotFound {
			t.Fatalf("disabled media path %s status=%d err=%v", route, r.status, r.err)
		}
	}
	ack(t, f.message(disabled, owner, id, "media_disabled_text", "text", []byte(`{"text":"still available"}`)))
	meta := protocol.MediaMetadata{MediaKey: "disabled_fixture", Kind: intent.Kind, ContentType: intent.ContentType,
		Size: intent.Size, SHA256: intent.SHA256}
	raw, err := protocol.EncodeMediaPayload(meta)
	must(t, err)
	f.message(disabled, owner, id, "media_disabled_asset", "media", raw).requireError(t, http.StatusBadRequest, "SEND_INVALID_INPUT")

	root := filepath.Join(t.TempDir(), "objects")
	enabled := f.gateway(root, nil)
	f.post(enabled, grantPath, "", intent).requireError(t, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	f.post(enabled, grantPath+"?unused=1", owner, intent).requireError(t, http.StatusBadRequest, "MEDIA_INVALID_INPUT")
	method := f.request(enabled, grantPath, owner, nil, jsonType)
	method.Method = http.MethodGet
	f.execute(method).requireError(t, http.StatusMethodNotAllowed, "HTTP_METHOD_NOT_ALLOWED")
	f.execute(f.request(enabled, grantPath, owner, bytes.NewReader(encode(t, intent)), "text/plain")).
		requireError(t, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
	rejected := []string{
		`{"conversationId":"value","ownerId":"ignored"}`,
		`{"conversationId":"first","conversationId":"second"}`,
		`{"conversationId":"value","kind":"file","contentType":"text/html","size":1,"sha256":"00"}`,
		string(encode(t, intent)) + `{}`,
	}
	for _, text := range rejected {
		f.execute(f.request(enabled, grantPath, owner, strings.NewReader(text), jsonType)).
			requireError(t, http.StatusBadRequest, "MEDIA_INVALID_INPUT")
	}
	f.execute(f.request(enabled, grantPath, owner, strings.NewReader(strings.Repeat(" ", 4097)), jsonType)).
		requireError(t, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
	badSize := intent
	badSize.Size = "0" + intent.Size
	f.post(enabled, grantPath, owner, badSize).requireError(t, http.StatusBadRequest, "MEDIA_INVALID_INPUT")
	grant := f.grant(enabled, owner, intent)
	binary := f.uploadRequest(enabled, owner, grant, bytes.NewReader(payload), int64(len(payload)))
	binary.Header.Add(uploadHeader, grant.UploadToken)
	f.execute(binary).requireError(t, http.StatusBadRequest, "MEDIA_INVALID_TOKEN")

	waitRateWindow()
	account := f.gateway(root, map[string]string{"NEWIM_MEDIA_ACCOUNT_ATTEMPTS_PER_MINUTE": "1"})
	f.grant(account, owner, intent)
	f.post(account, grantPath, otherDevice, intent).requireError(t, http.StatusTooManyRequests, "MEDIA_RATE_LIMITED")
	f.grant(account, peer, intent)
	ack(t, f.message(account, owner, id, "media_budget_text", "text", []byte(`{"text":"separate budget"}`)))
	address := f.gateway(root, map[string]string{"NEWIM_MEDIA_IP_ATTEMPTS_PER_MINUTE": "1"})
	f.grant(address, owner, intent)
	forwarded := f.request(address, grantPath, peer, bytes.NewReader(encode(t, intent)), jsonType)
	forwarded.Header.Set("X-Forwarded-For", "192.0.2.25")
	f.execute(forwarded).requireError(t, http.StatusTooManyRequests, "MEDIA_RATE_LIMITED")
}

func waitRateWindow() {
	remaining := time.Until(time.Now().Truncate(time.Minute).Add(time.Minute))
	if remaining < 5*time.Second {
		time.Sleep(remaining + 20*time.Millisecond)
	}
}

func testPrivateObjects(t *testing.T) {
	f := openFixture(t)
	root := filepath.Join(t.TempDir(), "objects")
	g := f.gateway(root, nil)
	owner := f.newAccount("media_privacy_owner")
	otherDevice := f.admit("media_privacy_owner", "desktop")
	member := f.newAccount("media_privacy_member")
	stranger := f.newAccount("media_privacy_stranger")
	const groupID = "media_privacy_group"
	const create = "/api/v1/groups/create"
	f.post(g, create, owner, map[string]any{
		"groupId": groupID, "profile": map[string]string{"name": "Media privacy fixture"},
		"memberIds": []string{"media_privacy_member"},
	}).require(t, http.StatusOK)
	content := []byte("private-attachment-fixture")
	intent := intentForBytes(groupID, content)
	f.post(g, grantPath, stranger, intent).requireError(t, http.StatusForbidden, "MEDIA_UNAUTHORIZED")
	grant := f.grant(g, owner, intent)
	download := map[string]string{"mediaKey": grant.MediaKey}
	f.post(g, downloadPath, member, download).requireError(t, http.StatusNotFound, "MEDIA_NOT_FOUND")
	f.execute(f.uploadRequest(g, otherDevice, grant, bytes.NewReader(content), int64(len(content)))).
		requireError(t, http.StatusForbidden, "MEDIA_UNAUTHORIZED")
	f.execute(f.uploadRequest(g, member, grant, bytes.NewReader(content), int64(len(content)))).
		requireError(t, http.StatusForbidden, "MEDIA_UNAUTHORIZED")

	short := content[:len(content)-1]
	f.execute(f.uploadRequest(g, owner, grant, bytes.NewReader(short), int64(len(short)))).
		requireError(t, http.StatusBadRequest, "MEDIA_INVALID_INPUT")
	wrong := bytes.Repeat([]byte{'b'}, len(content))
	f.execute(f.uploadRequest(g, owner, grant, bytes.NewReader(wrong), int64(len(wrong)))).
		requireError(t, http.StatusBadRequest, "MEDIA_INVALID_INPUT")
	f.requireAssetState(grant.MediaKey, "pending")
	f.post(g, downloadPath, member, download).requireError(t, http.StatusNotFound, "MEDIA_NOT_FOUND")

	ready := f.completed(g, owner, grant, intent, bytes.NewReader(content), int64(len(content)))
	f.requireAssetState(grant.MediaKey, "ready")
	f.requireDownload(g, member, ready, 0)
	f.post(g, downloadPath, stranger, download).requireError(t, http.StatusForbidden, "MEDIA_UNAUTHORIZED")
	f.post(g, downloadPath, "", download).requireError(t, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	f.execute(f.uploadRequest(g, owner, grant, bytes.NewReader(wrong), int64(len(wrong)))).
		requireError(t, http.StatusConflict, "MEDIA_CONFLICT")

	raw, err := protocol.EncodeMediaPayload(ready)
	must(t, err)
	f.message(g, member, groupID, "media_foreign_send", "media", raw).requireError(t, http.StatusForbidden, "SEND_UNAUTHORIZED")
	wrongConv := f.direct(g, owner, "media_privacy_member")
	f.message(g, owner, wrongConv, "media_wrong_conversation", "media", raw).
		requireError(t, http.StatusForbidden, "SEND_UNAUTHORIZED")
	altered := ready
	altered.Size = strconv.Itoa(len(content) + 1)
	badMetadata, err := protocol.EncodeMediaPayload(altered)
	must(t, err)
	f.message(g, owner, groupID, "media_wrong_metadata", "media", badMetadata).
		requireError(t, http.StatusForbidden, "SEND_UNAUTHORIZED")

	memberAction := group.MemberRequest{GroupID: groupID, UserID: "media_privacy_member", Action: group.Remove}
	f.post(g, "/api/v1/groups/member/action", owner, memberAction).require(t, http.StatusOK)
	f.post(g, downloadPath, member, download).requireError(t, http.StatusForbidden, "MEDIA_UNAUTHORIZED")
	f.expire(grant.MediaKey)
	replayed := f.completed(g, owner, grant, intent, bytes.NewReader(content), int64(len(content)))
	if ready != replayed {
		t.Fatal("ready upload retry after expiry changed committed metadata")
	}

	if f.objects == nil {
		// Replacing a committed test entry with a same-directory link must never stream its contents.
		const realName = "stored_fixture_object"
		must(t, os.Rename(filepath.Join(root, ready.MediaKey), filepath.Join(root, realName)))
		must(t, os.Symlink(realName, filepath.Join(root, ready.MediaKey)))
		f.post(g, downloadPath, owner, download).requireError(t, http.StatusServiceUnavailable, "MEDIA_STORAGE_UNAVAILABLE")
	}
	g.requireRedacted(t, owner, member, stranger, grant.UploadToken, intent.SHA256, string(content), root)
}

func (f *fixture) expire(key string) {
	f.t.Helper()
	result, err := f.db.Exec(f.ctx, "UPDATE newim.im_media_assets SET upload_expires_at=clock_timestamp() - interval '1 second' WHERE media_key=$1", key)
	must(f.t, err)
	if result.RowsAffected() != 1 {
		f.t.Fatal("fixture did not update exactly one upload expiry")
	}
}

func (f *fixture) requireAssetState(key, want string) {
	f.t.Helper()
	var state string
	must(f.t, f.db.QueryRow(f.ctx, "SELECT state FROM newim.im_media_assets WHERE media_key=$1", key).Scan(&state))
	if state != want {
		f.t.Fatalf("persisted asset state=%q want=%q", state, want)
	}
}

type pauseBody struct{ resume <-chan struct{} }

func (p pauseBody) Read([]byte) (int, error) {
	<-p.resume
	return 0, io.EOF
}

func testTransferRevalidation(t *testing.T) {
	f := openFixture(t)
	root := filepath.Join(t.TempDir(), "objects")
	g := f.gateway(root, nil)
	owner := f.newAccount("media_recheck_owner")
	uploader := f.admit("media_recheck_owner", "laptop")
	peer := f.newAccount("media_recheck_peer")
	id := f.direct(g, owner, "media_recheck_peer")
	content := bytes.Repeat([]byte("synthetic upload fixture"), 4096)
	intent := intentForBytes(id, content)
	expiring := f.grant(g, owner, intent)
	f.finishAfter(g, owner, expiring, content, func() { f.expire(expiring.MediaKey) }).
		requireError(t, http.StatusGone, "MEDIA_EXPIRED")
	revoking := f.grant(g, uploader, intent)
	f.finishAfter(g, uploader, revoking, content, func() {
		_, err := f.auth.RevokeBearer(f.ctx, uploader)
		must(t, err)
	}).requireError(t, http.StatusForbidden, "MEDIA_UNAUTHORIZED")
	f.execute(f.uploadRequest(g, uploader, revoking, bytes.NewReader(content), int64(len(content)))).
		requireError(t, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	for _, grant := range []uploadGrant{expiring, revoking} {
		f.requireAssetState(grant.MediaKey, "pending")
		f.post(g, downloadPath, peer, map[string]string{"mediaKey": grant.MediaKey}).
			requireError(t, http.StatusNotFound, "MEDIA_NOT_FOUND")
	}

	replayer := f.admit("media_recheck_owner", "tablet")
	original := f.grant(g, replayer, intent)
	metadata := f.completed(g, replayer, original, intent, bytes.NewReader(content), int64(len(content)))
	f.expire(original.MediaKey)
	f.finishAfter(g, replayer, original, content, func() {
		_, err := f.auth.RevokeBearer(f.ctx, replayer)
		must(t, err)
	}).requireError(t, http.StatusForbidden, "MEDIA_UNAUTHORIZED")
	f.requireAssetState(original.MediaKey, "ready")
	f.requireDownload(g, peer, metadata, 0)
}

func (f *fixture) finishAfter(g *gateway, token string, grant uploadGrant, content []byte, update func()) reply {
	f.t.Helper()
	resume, result := f.pauseCompletion(g, token, grant, content)
	defer resume()
	update()
	resume()
	return <-result
}

func (f *fixture) pauseCompletion(g *gateway, token string, grant uploadGrant, content []byte) (func(), <-chan reply) {
	f.t.Helper()
	body, resume := f.pausedBody(content)
	req := f.uploadRequest(g, token, grant, body, int64(len(content)))
	return resume, f.waitForBodyRead(req)
}

func (f *fixture) pausedBody(content []byte) (io.Reader, func()) {
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	f.t.Cleanup(unblock)
	split := len(content) / 2
	return io.MultiReader(bytes.NewReader(content[:split]), pauseBody{resume}, bytes.NewReader(content[split:])), unblock
}

func (f *fixture) waitForBodyRead(req *http.Request) <-chan reply {
	f.t.Helper()
	req.Header.Set("Expect", "100-continue")
	started := make(chan struct{})
	trace := &httptrace.ClientTrace{Got100Continue: func() { close(started) }}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	result := make(chan reply, 1)
	go func() { result <- f.execute(req) }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	// Go's server emits 100 Continue when the authenticated handler first reads the body.
	select {
	case <-started:
		return result
	case response := <-result:
		f.t.Fatalf("request returned before the fixture could update authorization: status=%d err=%v", response.status, response.err)
	case <-deadline.C:
		f.t.Fatal("media request did not start reading the body")
	}
	return nil
}
