//go:build integration

package media_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/auth/session"
	app "github.com/m-ice/NewIM/server/media"
)

type failCompleteStore struct{ app.Store }

func (s failCompleteStore) CompleteReady(context.Context, time.Time, session.ConnectionIdentity, app.Grant) (app.Grant, error) {
	return app.Grant{}, app.Fail(app.MediaStorageUnavailable)
}

func TestMediaAmbiguousCommitLeavesPendingAndRecovers(t *testing.T) {
	f := openFixture(t)
	user := unique("ambiguous_user")
	conversation := unique("ambiguous_conversation")
	identity := f.seedIdentity(user)
	f.seedConversation(conversation, user)
	root := mediaRoot(t, "ambiguous")
	objects := openLocalStore(t, root)
	body := content(36, 12)
	service, err := app.NewService(failCompleteStore{Store: f.repo}, app.Config{
		Objects: objects, Signer: &recordingSigner{}, Clock: f.clock(), Entropy: bytes.NewReader(deterministicEntropy(21)),
	})
	must(t, err)
	grant, err := service.BeginUpload(ctx, identity, beginRequest(conversation, body, time.Minute))
	must(t, err)
	_, err = service.CompleteUpload(ctx, identity, grant.RawToken, bytes.NewReader(body))
	wantMediaCode(t, err, app.MediaStorageUnavailable)
	if got := f.scalarString("SELECT state FROM newim.im_media_assets WHERE media_key=$1", grant.MediaKey); got != "pending" {
		t.Fatalf("ambiguous commit state got %s want pending", got)
	}
	if _, err = os.Stat(filepath.Join(root, grant.MediaKey)); err != nil {
		t.Fatalf("orphan object was not durable: %v", err)
	}
	recovery := newService(t, f, objects, &recordingSigner{}, deterministicEntropy(22))
	asset, err := recovery.CompleteUpload(ctx, identity, grant.RawToken, bytes.NewReader(body))
	must(t, err)
	if asset.State != app.StateReady {
		t.Fatalf("recovery state = %s", asset.State)
	}
}
