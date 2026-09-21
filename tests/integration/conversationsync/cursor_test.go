//go:build integration

package conversationsync_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	app "github.com/m-ice/NewIM/server/sync/conversation"
)

func mutateToken(token string) string {
	if token == "" {
		return "x"
	}
	replacement := byte('A')
	if token[len(token)-1] == 'A' {
		replacement = 'B'
	}
	return token[:len(token)-1] + string(replacement)
}

func TestCursorExpiry(t *testing.T) {
	f := openFixture(t)

	t.Run("mac-kind-identity-and-bounds", func(t *testing.T) {
		user := "cursor_auth"
		f.populate(user, "cursor_auth", 2)
		service := f.service(nil, "")
		page, err := service.BeginBootstrap(ctx, principal(user), 1)
		must(t, err)
		pageToken := page.NextCursor

		page, err = service.ContinueBootstrap(ctx, principal("someone_else"), pageToken)
		wantCode(t, page, err, app.InvalidCursor)
		page, err = service.ContinueBootstrap(ctx, principal(user), mutateToken(pageToken))
		wantCode(t, page, err, app.InvalidCursor)
		page, err = service.ContinueBootstrap(ctx, principal(user), strings.Repeat("a", app.MaxCursorBytes+1))
		wantCode(t, page, err, app.InvalidCursor)
		page, err = service.BeginDelta(ctx, principal(user), pageToken, 100)
		wantCode(t, page, err, app.InvalidCursor)

		_, checkpoint := bootstrapAll(t, service, user, 100)
		page, err = service.ContinueBootstrap(ctx, principal(user), checkpoint)
		wantCode(t, page, err, app.InvalidCursor)
	})

	t.Run("rotation-retention-unknown-key-and-restart", func(t *testing.T) {
		user := "cursor_rotation"
		f.populate(user, "cursor_rotation", 3)
		secondKey := []byte("integration-fixed-keyring-32-bytes-minimum-0002")
		oldOnly := f.service(map[string][]byte{"old": testKey}, "old")
		page, err := oldOnly.BeginBootstrap(ctx, principal(user), 1)
		must(t, err)
		oldToken := page.NextCursor

		rotated := f.service(map[string][]byte{"old": testKey, "new": secondKey}, "new")
		continued, err := rotated.ContinueBootstrap(ctx, principal(user), oldToken)
		must(t, err)
		checkedPage(t, continued, 1)
		restarted := f.service(map[string][]byte{"old": testKey, "new": secondKey}, "new")
		replayed, err := restarted.ContinueBootstrap(ctx, principal(user), oldToken)
		must(t, err)
		samePage(t, continued, replayed)

		newOnly := f.service(map[string][]byte{"new": secondKey}, "new")
		page, err = newOnly.ContinueBootstrap(ctx, principal(user), oldToken)
		wantCode(t, page, err, app.InvalidCursor)

		fresh, err := rotated.BeginBootstrap(ctx, principal(user), 1)
		must(t, err)
		page, err = oldOnly.ContinueBootstrap(ctx, principal(user), fresh.NextCursor)
		wantCode(t, page, err, app.InvalidCursor)
	})

	t.Run("page-expiry-boundary", func(t *testing.T) {
		user := "cursor_page_expiry"
		f.populate(user, "cursor_page_expiry", 2)
		start := int64(1800002000)
		f.setNow(start)
		service := f.service(nil, "")
		page, err := service.BeginBootstrap(ctx, principal(user), 1)
		must(t, err)
		token := page.NextCursor

		f.setNow(start + int64((15*time.Minute-time.Second)/time.Second))
		page, err = service.ContinueBootstrap(ctx, principal(user), token)
		must(t, err)
		checkedPage(t, page, 1)

		f.setNow(start + int64((15*time.Minute)/time.Second))
		page, err = service.ContinueBootstrap(ctx, principal(user), token)
		wantCode(t, page, err, app.CursorExpired)
	})

	t.Run("checkpoint-expiry-boundary", func(t *testing.T) {
		user := "cursor_checkpoint_expiry"
		f.populate(user, "cursor_checkpoint_expiry", 1)
		start := int64(1800003000)
		f.setNow(start)
		service := f.service(nil, "")
		_, checkpoint := bootstrapAll(t, service, user, 100)

		f.setNow(start + int64((24*time.Hour-time.Second)/time.Second))
		page, err := service.BeginDelta(ctx, principal(user), checkpoint, 100)
		must(t, err)
		checkedPage(t, page, 100)

		f.setNow(start + int64((24*time.Hour)/time.Second))
		page, err = service.BeginDelta(ctx, principal(user), checkpoint, 100)
		wantCode(t, page, err, app.CursorExpired)
	})

	t.Run("epoch-floor-and-noop-boundary", func(t *testing.T) {
		user := "cursor_floor"
		f.populate(user, "cursor_floor", 3)
		service := f.service(nil, "")
		page, err := service.BeginBootstrap(ctx, principal(user), 1)
		must(t, err)
		token := page.NextCursor

		must(t, f.repo.AdvanceFloor(ctx, user, 0))
		page, err = service.ContinueBootstrap(ctx, principal(user), token)
		must(t, err)
		checkedPage(t, page, 1)

		must(t, f.repo.AdvanceFloor(ctx, user, 1))
		page, err = service.ContinueBootstrap(ctx, principal(user), token)
		wantCode(t, page, err, app.CursorExpired)

		_, checkpoint := bootstrapAll(t, service, user, 100)
		must(t, f.repo.AdvanceFloor(ctx, user, 1))
		page, err = service.BeginDelta(ctx, principal(user), checkpoint, 100)
		must(t, err)
		checkedPage(t, page, 100)

		must(t, f.repo.AdvanceFloor(ctx, user, 3))
		page, err = service.BeginDelta(ctx, principal(user), checkpoint, 100)
		wantCode(t, page, err, app.CursorExpired)

		row := f.scalarString("SELECT epoch||'|'||last_change_seq||'|'||min_valid_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", user)
		if row != "3|3|3" {
			t.Fatalf("epoch/head/floor got %s want 3|3|3", row)
		}
	})

	t.Run("epoch-exhaustion-does-not-change-state", func(t *testing.T) {
		user := "cursor_epoch_exhaustion"
		f.populate(user, user, 1)
		f.sql("UPDATE newim.im_conversation_sync_accounts SET epoch=$2 WHERE user_id=$1", user, int64(app.MaxSequence))
		before := f.scalarString("SELECT epoch||'|'||last_change_seq||'|'||min_valid_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", user)
		wantCode(t, app.Page{}, f.repo.AdvanceFloor(ctx, user, 1), app.SequenceExhausted)
		after := f.scalarString("SELECT epoch||'|'||last_change_seq||'|'||min_valid_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", user)
		if after != before {
			t.Fatalf("epoch exhaustion changed state from %s to %s", before, after)
		}
	})

	t.Run("invalid-limit-has-no-effects", func(t *testing.T) {
		user := "cursor_limit"
		f.populate(user, fmt.Sprintf("%s", user), 1)
		_, checkpoint := bootstrapAll(t, f.service(nil, ""), user, 100)
		for _, limit := range []int{-1, app.MaxPageItems + 1} {
			page, err := f.service(nil, "").BeginDelta(ctx, principal(user), checkpoint, limit)
			wantCode(t, page, err, app.LimitExceeded)
		}
	})
}
