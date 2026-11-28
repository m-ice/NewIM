//go:build integration

package conversationsynchttp_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/m-ice/NewIM/server/push"
	pushstore "github.com/m-ice/NewIM/server/storage/push"
)

func TestPushKitGateway(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(23)
	keyFile := filepath.Join(t.TempDir(), "push-keys.json")
	key := bytes.Repeat([]byte{0x4b}, 32)
	must(t, os.WriteFile(keyFile, encode(t, map[string]any{
		"active": "fixture", "keys": map[string]string{"fixture": base64.StdEncoding.EncodeToString(key)},
	}), 0o600))
	vault, err := push.LoadVault(keyFile)
	must(t, err)
	repo, err := pushstore.Open(f.ctx, pushstore.Config{
		DSN: testDSN, AllowLocalSocket: true, ApplicationName: "newim-pushkit-integration",
	}, vault)
	must(t, err)
	t.Cleanup(repo.Close)

	gateway := pushGateway(f, writeKeys(t, "pushkit", "pushkit"), keyFile)
	pair := accountPair(t, f, gateway, "pushkit")
	const original = "synthetic:pushkit_old"
	const current = "synthetic:pushkit_new"
	anonymous := f.call(gateway, http.MethodPut, push.RegistrationRoute, "", encode(t, map[string]string{
		"provider": string(push.PushKit), "token": original,
	}), nil)
	requireStatus(t, anonymous, http.StatusUnauthorized)

	old := registerPushToken(t, f, gateway, pair.owner, push.PushKit, original)
	if retry := registerPushToken(t, f, gateway, pair.owner, push.PushKit, original); retry != old {
		t.Fatal("unchanged token renewed its registration version")
	}
	must(t, repo.Enable(f.ctx))
	send(t, f, gateway, pair.peer, pair.conversation, "pushkit_before_rotation", "committed before rotation")
	fanoutOne(t, f, repo)
	job := claimPush(t, f, repo)
	if job.Channel != push.PushKit || job.Version != old {
		t.Fatal("queued delivery selected the wrong Push Kit registration")
	}
	requirePushToken(t, f, repo, job, original)
	fresh := registerPushToken(t, f, gateway, pair.owner, push.PushKit, current)
	if fresh <= old {
		t.Fatal("Push Kit rotation did not advance the version")
	}
	finishPush(t, f, repo, job, push.Outcome{Result: push.InvalidToken}, push.InvalidToken)

	unregister := func(version int64) {
		t.Helper()
		reply := f.call(gateway, http.MethodDelete, push.RegistrationRoute, pair.owner,
			encode(t, map[string]string{"provider": string(push.PushKit), "version": strconv.FormatInt(version, 10)}), nil)
		requireStatus(t, reply, http.StatusNoContent)
	}
	unregister(old)

	next := send(t, f, gateway, pair.peer, pair.conversation, "pushkit_after_rotation", "committed after rotation")
	calls := 0
	worker, err := push.NewWorker(repo, map[push.Channel]push.Provider{
		push.PushKit: testPushProvider(func(ctx context.Context, token []byte) push.Outcome {
			if _, bounded := ctx.Deadline(); !bounded || !bytes.Equal(token, []byte(current)) {
				t.Fatal("a stale or unbounded Push Kit delivery was dispatched")
			}
			calls++
			return push.Outcome{Result: push.Accepted}
		}),
	})
	must(t, err)
	processed, err := worker.Cycle(f.ctx)
	must(t, err)
	if !processed || calls != 1 || worker.Counts().Accepted != 1 {
		t.Fatal("Push Kit worker did not complete its versioned delivery")
	}
	pushJobs(t, f, next.ServerMsgID, 1)

	unregister(fresh)
	after := send(t, f, gateway, pair.peer, pair.conversation, "pushkit_after_unregister", "committed after unregister")
	fanoutOne(t, f, repo)
	pushJobs(t, f, after.ServerMsgID, 0)
	gateway.requireRedacted(t, original, current, pair.owner, pair.peer)
}
