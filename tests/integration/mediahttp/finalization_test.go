//go:build integration

package mediahttp_test

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func testFinalizationLocks(t *testing.T) {
	f := openFixture(t)
	g := f.gateway(filepath.Join(t.TempDir(), "objects"), nil)
	owner := f.newAccount("media_finalization_owner")
	peer := f.newAccount("media_finalization_peer")
	conversation := f.direct(g, owner, "media_finalization_peer")
	content := []byte("synthetic finalization fixture")
	intent := intentForBytes(conversation, content)

	holder, err := pgx.Connect(f.ctx, testDSN)
	must(t, err)
	t.Cleanup(func() { _ = holder.Close(context.Background()) })

	expiring := f.grant(g, owner, intent)
	resume, result := f.pauseCompletion(g, owner, expiring, content)
	defer resume()
	asset, err := holder.Begin(f.ctx)
	must(t, err)
	defer asset.Rollback(f.ctx)
	_, err = asset.Exec(f.ctx, "SELECT media_key FROM newim.im_media_assets WHERE media_key=$1 FOR UPDATE", expiring.MediaKey)
	must(t, err)
	resume()
	f.waitForMediaLock(asset, "FROM newim.im_media_assets", result)
	// Move expiry to after lock acquisition began, but before finalization can continue.
	_, err = asset.Exec(f.ctx, "UPDATE newim.im_media_assets SET upload_expires_at=clock_timestamp() WHERE media_key=$1", expiring.MediaKey)
	must(t, err)
	must(t, asset.Commit(f.ctx))
	(<-result).requireError(t, http.StatusGone, "MEDIA_EXPIRED")
	f.requireAssetState(expiring.MediaKey, "pending")
	f.post(g, downloadPath, peer, map[string]string{"mediaKey": expiring.MediaKey}).
		requireError(t, http.StatusNotFound, "MEDIA_NOT_FOUND")

	token, err := f.auth.AuthenticateBearer(f.ctx, owner)
	must(t, err)
	revoked := f.grant(g, owner, intent)
	resume, result = f.pauseCompletion(g, owner, revoked, content)
	defer resume()
	session, err := holder.Begin(f.ctx)
	must(t, err)
	defer session.Rollback(f.ctx)
	_, err = session.Exec(f.ctx, "SELECT session_id FROM newim.im_sessions WHERE session_id=$1 FOR UPDATE", token.SessionID())
	must(t, err)
	resume()
	f.waitForMediaLock(session, "FROM newim.im_sessions", result)
	// Update only the token while holding its session lock, as token revocation does.
	_, err = session.Exec(f.ctx, "UPDATE newim.im_auth_tokens SET revoked_at=clock_timestamp() WHERE token_id=$1", token.TokenID())
	must(t, err)
	must(t, session.Commit(f.ctx))
	(<-result).requireError(t, http.StatusForbidden, "MEDIA_UNAUTHORIZED")
	f.requireAssetState(revoked.MediaKey, "pending")
	f.post(g, downloadPath, peer, map[string]string{"mediaKey": revoked.MediaKey}).
		requireError(t, http.StatusNotFound, "MEDIA_NOT_FOUND")

	f.requireFreshGrantBearer(g, holder, intent)
}

func (f *fixture) requireFreshGrantBearer(g *gateway, holder *pgx.Conn, intent grantRequest) {
	f.t.Helper()
	uploader := f.admit("media_finalization_owner", "other")
	bearer, err := f.auth.AuthenticateBearer(f.ctx, uploader)
	must(f.t, err)

	raw := encode(f.t, intent)
	body, resume := f.pausedBody(raw)
	defer resume()
	req := f.request(g, grantPath, uploader, body, jsonType)
	req.ContentLength = int64(len(raw))
	result := f.waitForBodyRead(req)

	tx, err := holder.Begin(f.ctx)
	must(f.t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, "SELECT session_id FROM newim.im_sessions WHERE session_id=$1 FOR UPDATE", bearer.SessionID())
	must(f.t, err)
	resume()
	f.waitForMediaLock(tx, "FROM newim.im_sessions", result)
	_, err = tx.Exec(f.ctx, "UPDATE newim.im_auth_tokens SET expires_at=clock_timestamp() WHERE token_id=$1", bearer.TokenID())
	must(f.t, err)
	must(f.t, tx.Commit(f.ctx))
	(<-result).requireError(f.t, http.StatusForbidden, "MEDIA_UNAUTHORIZED")

	var count int
	must(f.t, f.db.QueryRow(f.ctx, "SELECT count(*) FROM newim.im_media_assets WHERE token_id=$1", bearer.TokenID()).Scan(&count))
	if count != 0 {
		f.t.Fatal("grant issuance committed an asset using the expired bearer")
	}
}

func (f *fixture) waitForMediaLock(holder pgx.Tx, lockQuery string, result <-chan reply) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 4*time.Second)
	defer cancel()
	blocker := int32(holder.Conn().PgConn().PID())
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		err := f.db.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE application_name='newim-gateway-media'
			  AND wait_event_type='Lock' AND strpos(query,$2) > 0
			  AND $1 = ANY(pg_blocking_pids(pid))
		)`, blocker, lockQuery).Scan(&waiting)
		must(f.t, err)
		if waiting {
			return
		}
		select {
		case response := <-result:
			f.t.Fatalf("request returned before reaching the expected media lock: status=%d err=%v", response.status, response.err)
		case <-ctx.Done():
			f.t.Fatal("media transaction did not wait for the fixture's row lock")
		case <-ticker.C:
		}
	}
}

func testCompletionRollback(t *testing.T) {
	f := openFixture(t)
	root := filepath.Join(t.TempDir(), "objects")
	g := f.gateway(root, nil)
	owner := f.newAccount("media_rollback_owner")
	peer := f.newAccount("media_rollback_peer")
	conversation := f.direct(g, owner, "media_rollback_peer")
	content := []byte("synthetic immutable object to adopt after rollback")
	intent := intentForBytes(conversation, content)
	grant := f.grant(g, owner, intent)

	_, err := f.db.Exec(f.ctx, `CREATE FUNCTION newim.fixture_media_commit_failure() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'fixture rejected metadata commit';
		END;
		$$;
		CREATE CONSTRAINT TRIGGER fixture_media_commit_failure
		AFTER UPDATE ON newim.im_media_assets DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW WHEN (OLD.state='pending' AND NEW.state='ready')
		EXECUTE FUNCTION newim.fixture_media_commit_failure();`)
	must(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := f.db.Exec(ctx, `DROP TRIGGER IF EXISTS fixture_media_commit_failure ON newim.im_media_assets;
			DROP FUNCTION IF EXISTS newim.fixture_media_commit_failure()`)
		if err != nil {
			t.Error(err)
		}
	})
	f.execute(f.uploadRequest(g, owner, grant, bytes.NewReader(content), int64(len(content)))).
		requireError(t, http.StatusServiceUnavailable, "MEDIA_STORAGE_UNAVAILABLE")
	f.requireAssetState(grant.MediaKey, "pending")
	f.post(g, downloadPath, peer, map[string]string{"mediaKey": grant.MediaKey}).
		requireError(t, http.StatusNotFound, "MEDIA_NOT_FOUND")

	path := filepath.Join(root, grant.MediaKey)
	original, err := os.Stat(path)
	must(t, err)
	raw, err := os.ReadFile(path)
	must(t, err)
	if !bytes.Equal(raw, content) {
		t.Fatal("failed commit changed the published object")
	}
	_, err = f.db.Exec(f.ctx, "DROP TRIGGER fixture_media_commit_failure ON newim.im_media_assets")
	must(t, err)
	metadata := f.completed(g, owner, grant, intent, bytes.NewReader(content), int64(len(content)))
	recovered, err := os.Stat(path)
	must(t, err)
	if !os.SameFile(original, recovered) {
		t.Fatal("retry replaced the immutable file left by the failed commit")
	}
	f.requireDownload(g, peer, metadata, 0)
	g.requireRedacted(t, "fixture rejected metadata commit", grant.UploadToken, root, intent.SHA256)
}
