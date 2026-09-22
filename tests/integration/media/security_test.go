//go:build integration

package media_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/auth/session"
	app "github.com/m-ice/NewIM/server/media"
)

func deterministicEntropy(seed byte) []byte {
	value := make([]byte, 4096)
	for i := range value {
		value[i] = byte(int(seed) + i*31)
	}
	return value
}

func assertGrantIdentityBinding(t *testing.T, f *fixture, mediaKey string, identity session.ConnectionIdentity) {
	t.Helper()
	got := f.scalarString("SELECT owner_user_id||'|'||device_id||'|'||session_id||'|'||connection_id||'|'||token_id FROM newim.im_media_assets WHERE media_key=$1", mediaKey)
	want := identity.UserID() + "|" + identity.DeviceID() + "|" + identity.SessionID() + "|" + identity.ConnectionID() + "|" + identity.TokenID()
	if got != want {
		t.Fatalf("media identity binding got %s want %s", got, want)
	}
}

func TestMediaSecurity(t *testing.T) {
	f := openFixture(t)
	user := unique("security_user")
	conversation := unique("security_conversation")
	identity := f.seedIdentity(user)
	f.seedConversation(conversation, user)
	root := mediaRoot(t, "security")
	objects := openLocalStore(t, root)
	signer := &recordingSigner{url: "https://private.invalid/download"}
	service := newService(t, f, objects, signer, deterministicEntropy(1))
	body := content(64, 7)
	grant, err := service.BeginUpload(ctx, identity, beginRequest(conversation, body, 120*time.Second))
	must(t, err)
	if len(grant.RawToken) != 76 || strings.Count(grant.RawToken, ".") != 1 {
		t.Fatalf("raw grant grammar invalid")
	}
	assertGrantIdentityBinding(t, f, grant.MediaKey, identity)
	rawDigest := sha256.Sum256([]byte(grant.RawToken))
	if got := f.scalarString("SELECT encode(upload_grant_digest,'hex') FROM newim.im_media_assets WHERE upload_grant_id=$1", grant.RawToken[:32]); got != hex.EncodeToString(rawDigest[:]) {
		t.Fatalf("persisted digest mismatch")
	}
	if got := f.scalarInt64("SELECT extract(epoch FROM upload_expires_at-to_timestamp($2::double precision))::bigint FROM newim.im_media_assets WHERE upload_grant_id=$1", grant.RawToken[:32], float64(f.now.Load())/1000); got < 119 || got > 121 {
		t.Fatalf("default upload expiry got %d want ~120", got)
	}
	asset, err := service.CompleteUpload(ctx, identity, grant.RawToken, bytes.NewReader(body))
	must(t, err)
	if asset.State != app.StateReady || asset.ActualSize == nil || *asset.ActualSize != int64(len(body)) {
		t.Fatalf("completed asset shape = %+v", asset)
	}
	assertGrantIdentityBinding(t, f, asset.MediaKey, identity)
	f.advance(2 * time.Minute)
	replayed, err := service.CompleteUpload(ctx, identity, grant.RawToken, bytes.NewReader(body))
	must(t, err)
	if replayed.MediaKey != asset.MediaKey || replayed.State != app.StateReady {
		t.Fatalf("lost-response replay changed asset: %+v", replayed)
	}
	_, err = service.CompleteUpload(ctx, identity, grant.RawToken, bytes.NewReader(content(64, 8)))
	wantMediaCode(t, err, app.MediaConflict)
	if got := f.scalarString("SELECT state||'|'||actual_size_bytes::text FROM newim.im_media_assets WHERE media_key=$1", asset.MediaKey); got != "ready|64" {
		t.Fatalf("conflicting replay changed asset: %s", got)
	}

	if err = objects.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := openLocalStore(t, root)
	restartedService := newService(t, f, restarted, signer, deterministicEntropy(2))
	restartedAsset, err := restartedService.CompleteUpload(ctx, identity, grant.RawToken, bytes.NewReader(body))
	must(t, err)
	service = newService(t, f, restarted, signer, deterministicEntropy(10))
	if restartedAsset.MediaKey != asset.MediaKey {
		t.Fatalf("restart replay returned wrong media key")
	}

	maxContent := content(16, 4)
	maxGrant, err := service.BeginUpload(ctx, identity, beginRequest(conversation, maxContent, app.MaxUploadTTL*time.Second))
	must(t, err)
	if got := f.scalarInt64("SELECT extract(epoch FROM upload_expires_at-to_timestamp($2::double precision))::bigint FROM newim.im_media_assets WHERE upload_grant_id=$1", maxGrant.RawToken[:32], float64(f.now.Load())/1000); got < 299 || got > 301 {
		t.Fatalf("maximum upload expiry got %d want ~300", got)
	}
	_, err = service.BeginUpload(ctx, identity, beginRequest(conversation, maxContent, (app.MaxUploadTTL+1)*time.Second))
	wantMediaCode(t, err, app.MediaInvalidInput)

	expiredContent := content(32, 3)
	expiredGrant, err := service.BeginUpload(ctx, identity, beginRequest(conversation, expiredContent, time.Second))
	must(t, err)
	f.advance(time.Second)
	_, err = service.CompleteUpload(ctx, identity, expiredGrant.RawToken, bytes.NewReader(expiredContent))
	wantMediaCode(t, err, app.MediaExpired)

	otherUser := unique("security_other")
	otherIdentity := f.seedIdentity(otherUser)
	f.seedConversation(conversation, otherUser)
	beforeState := f.scalarString("SELECT state||'|'||actual_size_bytes::text FROM newim.im_media_assets WHERE media_key=$1", asset.MediaKey)
	_, err = service.CompleteUpload(ctx, otherIdentity, grant.RawToken, bytes.NewReader(body))
	wantMediaCode(t, err, app.MediaUnauthorized)
	if afterState := f.scalarString("SELECT state||'|'||actual_size_bytes::text FROM newim.im_media_assets WHERE media_key=$1", asset.MediaKey); afterState != beforeState {
		t.Fatalf("cross-identity replay changed state: %s -> %s", beforeState, afterState)
	}

	revokedUser := unique("security_revoked_user")
	revokedIdentity := f.seedIdentity(revokedUser)
	f.seedConversation(conversation, revokedUser)
	revokedContent := content(24, 5)
	revokedGrant, err := service.BeginUpload(ctx, revokedIdentity, beginRequest(conversation, revokedContent, 2*time.Minute))
	must(t, err)
	f.sql("UPDATE newim.im_sessions SET revoked_at=$2 WHERE session_id=$1", revokedIdentity.SessionID(), time.UnixMilli(f.now.Load()).UTC())
	_, err = service.CompleteUpload(ctx, revokedIdentity, revokedGrant.RawToken, bytes.NewReader(revokedContent))
	wantMediaCode(t, err, app.MediaUnauthorized)
	if got := f.scalarString("SELECT state FROM newim.im_media_assets WHERE media_key=$1", revokedGrant.MediaKey); got != "pending" {
		t.Fatalf("revoked completion changed state to %s", got)
	}

	readyRevokedUser := unique("security_ready_revoked_user")
	readyRevokedIdentity := f.seedIdentity(readyRevokedUser)
	f.seedConversation(conversation, readyRevokedUser)
	readyRevokedContent := content(28, 6)
	readyRevokedGrant, err := service.BeginUpload(ctx, readyRevokedIdentity, beginRequest(conversation, readyRevokedContent, 2*time.Minute))
	must(t, err)
	readyRevokedAsset, err := service.CompleteUpload(ctx, readyRevokedIdentity, readyRevokedGrant.RawToken, bytes.NewReader(readyRevokedContent))
	must(t, err)
	f.advance(2 * time.Minute)
	f.sql("UPDATE newim.im_sessions SET revoked_at=$2 WHERE session_id=$1", readyRevokedIdentity.SessionID(), time.UnixMilli(f.now.Load()).UTC())
	beforeReadyState := f.scalarString("SELECT state||'|'||actual_size_bytes::text FROM newim.im_media_assets WHERE media_key=$1", readyRevokedAsset.MediaKey)
	_, err = service.CompleteUpload(ctx, readyRevokedIdentity, readyRevokedGrant.RawToken, bytes.NewReader(readyRevokedContent))
	wantMediaCode(t, err, app.MediaUnauthorized)
	if afterReadyState := f.scalarString("SELECT state||'|'||actual_size_bytes::text FROM newim.im_media_assets WHERE media_key=$1", readyRevokedAsset.MediaKey); afterReadyState != beforeReadyState {
		t.Fatalf("revoked ready replay changed state: %s -> %s", beforeReadyState, afterReadyState)
	}

}

func TestMediaProcessRestart(t *testing.T) {
	if os.Getenv("NEWIM_MEDIA_RESTART_CHILD") == "1" {
		runMediaRestartPhase(t, os.Getenv("NEWIM_MEDIA_RESTART_PHASE"))
		return
	}
	root := "/tmp/newim-media-process-restart"
	for _, phase := range []string{"prepare", "restart"} {
		command := exec.Command(os.Args[0], "-test.run=^TestMediaProcessRestart$", "-test.count=1")
		command.Env = append(os.Environ(),
			"NEWIM_MEDIA_RESTART_CHILD=1",
			"NEWIM_MEDIA_RESTART_PHASE="+phase,
			"NEWIM_MEDIA_RESTART_ROOT="+root,
		)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("restart phase %s failed: %v\n%s", phase, err, output)
		}
	}
}

func runMediaRestartPhase(t *testing.T, phase string) {
	f := openFixture(t)
	user := "media_restart_user"
	conversation := "media_restart_conversation"
	identity := f.seedIdentity(user)
	f.seedConversation(conversation, user)
	entropy := deterministicEntropy(0x40)
	rawToken := rawTokenFromEntropy(entropy)
	body := content(40, 0x55)
	root := os.Getenv("NEWIM_MEDIA_RESTART_ROOT")
	if root == "" {
		t.Fatal("missing restart root")
	}
	objects := openLocalStore(t, root)
	signer := &recordingSigner{url: "https://private.invalid/restart"}
	service := newService(t, f, objects, signer, entropy)
	switch phase {
	case "prepare":
		grant, err := service.BeginUpload(ctx, identity, beginRequest(conversation, body, time.Minute))
		must(t, err)
		if grant.RawToken != rawToken {
			t.Fatal("deterministic restart token mismatch")
		}
		asset, err := service.CompleteUpload(ctx, identity, grant.RawToken, bytes.NewReader(body))
		must(t, err)
		if asset.State != app.StateReady {
			t.Fatalf("restart asset state = %s", asset.State)
		}
	case "restart":
		asset, err := service.CompleteUpload(ctx, identity, rawToken, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("restart replay failed: %v", err)
		}
		if asset.State != app.StateReady {
			t.Fatalf("restart replay state = %s", asset.State)
		}
		url, err := service.ResolvePrivateDownload(ctx, identity, asset.MediaKey, time.Minute)
		if err != nil || url != signer.url || signer.Calls() != 1 {
			t.Fatalf("restart download url=%q calls=%d err=%v", url, signer.Calls(), err)
		}
	default:
		t.Fatalf("unknown restart phase %q", phase)
	}
}

func rawTokenFromEntropy(entropy []byte) string {
	if len(entropy) < 64 {
		panic("insufficient test entropy")
	}
	grantID := hex.EncodeToString(entropy[:16])
	secret := base64.RawURLEncoding.EncodeToString(entropy[16:48])
	return grantID + "." + secret
}
