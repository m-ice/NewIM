//go:build integration

package conversationsync_test

import (
	"testing"

	app "github.com/m-ice/NewIM/server/sync/conversation"
)

func TestDeltaTombstones(t *testing.T) {
	f := openFixture(t)

	t.Run("candidate-and-item-bounds", func(t *testing.T) {
		user := "delta_bounds"
		f.account(user)
		conversation := f.conversations("delta_bounds", 1)[0]
		f.write(user, []string{conversation}, "add")
		_, checkpoint := bootstrapAll(t, f.service(nil, ""), user, 100)
		for i := 0; i < 101; i++ {
			f.write(user, []string{conversation}, "update")
		}
		observer := &captureObserver{}
		service := f.serviceWithObserver(nil, "", observer)
		page, err := service.BeginDelta(ctx, principal(user), checkpoint, 100)
		must(t, err)
		checkedPage(t, page, 100)
		observation, ok := observer.latest("begin_delta")
		if !ok || observation.Candidates != app.MaxPageItems+1 {
			t.Fatalf("delta candidates got %+v want %d", observation, app.MaxPageItems+1)
		}
		if len(page.Items) != 100 || !page.HasMore {
			t.Fatalf("first delta page got %d items hasMore=%t", len(page.Items), page.HasMore)
		}
		page, err = service.ContinueDelta(ctx, principal(user), page.NextCursor)
		must(t, err)
		checkedPage(t, page, 100)
		if len(page.Items) != 1 || page.HasMore {
			t.Fatalf("terminal delta page got %d items hasMore=%t", len(page.Items), page.HasMore)
		}
	})

	t.Run("frozen-fences-and-normal-revocation", func(t *testing.T) {
		user := "delta_fence"
		f.account(user)
		conversation := f.conversations("delta_fence", 1)[0]
		f.write(user, []string{conversation}, "add")
		f.write(user, []string{conversation}, "update")
		_, checkpoint := bootstrapAll(t, f.service(nil, ""), user, 100)
		for i := 0; i < 3; i++ {
			f.write(user, []string{conversation}, "update")
		}

		service := f.service(nil, "")
		page, err := service.BeginDelta(ctx, principal(user), checkpoint, 1)
		must(t, err)
		checkedPage(t, page, 1)
		if len(page.Items) != 1 || page.Items[0].Revision != 3 || !page.HasMore {
			t.Fatalf("frozen H2=5 page mismatch: %+v", page)
		}
		frozen := page.NextCursor

		f.write(user, []string{conversation}, "remove")
		page, err = service.ContinueDelta(ctx, principal(user), frozen)
		wantCode(t, page, err, app.CursorExpired)

		candidates, err := f.repo.Read(ctx, user, app.ReadRequest{
			Kind: app.DeltaRead, Start: false, Epoch: 1, Fence: 5, AfterSeq: 3, Limit: 100,
		})
		must(t, err)
		if len(candidates.Candidates) != 2 {
			t.Fatalf("frozen adapter candidates got %d want 2", len(candidates.Candidates))
		}
		for _, candidate := range candidates.Candidates {
			if candidate.State == nil || candidate.State.Kind != "upsert" || candidate.Latest == nil || candidate.Latest.Kind != "remove" || candidate.Latest.Revision != 6 {
				t.Fatalf("candidate did not retain bounded upsert/remove proof: %+v", candidate)
			}
		}

		h6Candidates, err := f.repo.Read(ctx, user, app.ReadRequest{
			Kind: app.DeltaRead, Start: true, Epoch: 1, Fence: 2, AfterSeq: 2, Limit: 100,
		})
		must(t, err)
		if h6Candidates.Fence != 6 || len(h6Candidates.Candidates) != 4 {
			t.Fatalf("H2=6 candidate round got fence=%d candidates=%d", h6Candidates.Fence, len(h6Candidates.Candidates))
		}
		if h6Candidates.Candidates[2].State.Revision != 5 || h6Candidates.Candidates[3].State.Kind != "remove" || h6Candidates.Candidates[3].State.Revision != 6 {
			t.Fatalf("upsert5/remove6 not enumerable in adapter order: %+v", h6Candidates.Candidates)
		}

		page, err = service.BeginDelta(ctx, principal(user), checkpoint, 100)
		wantCode(t, page, err, app.CursorExpired)
		items, recovered := bootstrapAll(t, service, user, 100)
		if len(items) != 0 || recovered == "" {
			t.Fatalf("fresh bootstrap did not recover projected remove: items=%d checkpoint=%q", len(items), recovered)
		}
	})

	t.Run("remove-recreate-reordered-convergence", func(t *testing.T) {
		user := "delta_recreate"
		f.account(user)
		conversation := f.conversations("delta_recreate", 1)[0]
		f.write(user, []string{conversation}, "add")
		_, checkpoint := bootstrapAll(t, f.service(nil, ""), user, 100)
		f.write(user, []string{conversation}, "remove")
		f.write(user, []string{conversation}, "add")

		service := f.service(nil, "")
		first, err := service.BeginDelta(ctx, principal(user), checkpoint, 1)
		must(t, err)
		checkedPage(t, first, 1)
		second, err := service.ContinueDelta(ctx, principal(user), first.NextCursor)
		must(t, err)
		checkedPage(t, second, 1)
		if len(first.Items) != 1 || first.Items[0].Kind != "remove" || len(second.Items) != 1 || second.Items[0].Kind != "upsert" {
			t.Fatalf("remove/recreate was compressed: first=%+v second=%+v", first, second)
		}
		ordered := append(append([]app.Item{}, first.Items...), second.Items...)
		reversed := append(append([]app.Item{}, second.Items...), first.Items...)
		if got := mergeItems(ordered)[conversation]; got.Kind != "upsert" || got.Revision != 3 {
			t.Fatalf("ordered merge mismatch: %+v", got)
		}
		if got := mergeItems(reversed)[conversation]; got.Kind != "upsert" || got.Revision != 3 {
			t.Fatalf("reordered merge mismatch: %+v", got)
		}

		tx, err := f.repo.Begin(ctx)
		must(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		batch, err := f.repo.PrepareBatch(ctx, tx, []string{conversation}, []string{user})
		must(t, err)
		duplicate, err := batch.RecordUpsert(ctx, conversation, user)
		must(t, err)
		if duplicate != 3 {
			t.Fatalf("duplicate upsert allocated revision %d want 3", duplicate)
		}
		must(t, tx.Commit(ctx))
		if head := f.scalarInt64("SELECT last_change_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", user); head != 3 {
			t.Fatalf("duplicate upsert changed head to %d", head)
		}
	})

	t.Run("duplicate-remove-and-account-isolation", func(t *testing.T) {
		userA := "delta_a"
		f.account(userA)
		conversationA := f.conversations("delta_a", 1)[0]
		f.write(userA, []string{conversationA}, "add")
		_, checkpointA := bootstrapAll(t, f.service(nil, ""), userA, 100)

		userB := "delta_b"
		f.account(userB)
		conversationB := f.conversations("delta_b", 1)[0]
		f.write(userB, []string{conversationB}, "add")
		itemsB, _ := bootstrapAll(t, f.service(nil, ""), userB, 100)
		if len(itemsB) != 1 || itemsB[0].ConversationID != conversationB {
			t.Fatalf("account B observed another account: %+v", itemsB)
		}
		page, err := f.service(nil, "").BeginDelta(ctx, principal(userB), checkpointA, 100)
		wantCode(t, page, err, app.InvalidCursor)

		f.write(userB, []string{conversationB}, "remove")
		tx, err := f.repo.Begin(ctx)
		must(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		batch, err := f.repo.PrepareBatch(ctx, tx, []string{conversationB}, []string{userB})
		must(t, err)
		revision, err := batch.RecordRemoval(ctx, conversationB, userB)
		must(t, err)
		if revision != 2 {
			t.Fatalf("duplicate remove allocated revision %d want 2", revision)
		}
		must(t, tx.Commit(ctx))
		if head := f.scalarInt64("SELECT last_change_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", userB); head != 2 {
			t.Fatalf("duplicate remove changed head to %d", head)
		}
	})
}
