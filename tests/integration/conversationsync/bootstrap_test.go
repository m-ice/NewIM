//go:build integration

package conversationsync_test

import (
	"fmt"
	"testing"

	app "github.com/m-ice/NewIM/server/sync/conversation"
)

func TestBootstrap(t *testing.T) {
	f := openFixture(t)

	for _, count := range []int{0, 1, 101, 100001} {
		count := count
		t.Run(fmt.Sprintf("population-%d", count), func(t *testing.T) {
			user := fmt.Sprintf("boot%d", count)
			prefix := fmt.Sprintf("b%06d", count)
			f.populate(user, prefix, count)
			items, checkpoint := bootstrapAll(t, f.service(nil, ""), user, 100)
			if len(items) != count {
				t.Fatalf("bootstrap items got %d want %d", len(items), count)
			}
			if checkpoint == "" {
				t.Fatal("terminal bootstrap did not return a checkpoint")
			}
		})
	}

	t.Run("directory-item-and-byte-bounds", func(t *testing.T) {
		user := "bounds"
		f.populate(user, "bounds", 401)
		observer := &captureObserver{}
		service := f.serviceWithObserver(nil, "", observer)
		page, err := service.BeginBootstrap(ctx, principal(user), 100)
		must(t, err)
		checkedPage(t, page, 100)
		observation, ok := observer.latest("begin_bootstrap")
		if !ok || observation.Candidates != app.MaxDirectoryCandidates {
			t.Fatalf("directory candidate bound got %+v want %d", observation, app.MaxDirectoryCandidates)
		}
		total := len(page.Items)
		seen := map[string]bool{}
		for page.HasMore {
			if seen[page.NextCursor] {
				t.Fatal("bootstrap cursor repeated")
			}
			seen[page.NextCursor] = true
			page, err = service.ContinueBootstrap(ctx, principal(user), page.NextCursor)
			must(t, err)
			checkedPage(t, page, 100)
			observation, ok = observer.latest("continue_bootstrap")
			if !ok || observation.Candidates < 1 || observation.Candidates > app.MaxDirectoryCandidates {
				t.Fatalf("continued directory candidate bound got %+v", observation)
			}
			total += len(page.Items)
		}
		if total != 401 {
			t.Fatalf("bounded bootstrap got %d items want 401", total)
		}
	})

	t.Run("empty-remove-and-future-key-pages", func(t *testing.T) {
		user := "removed"
		ids := f.populate(user, "removed", 401)
		for start := 0; start < len(ids); start += 100 {
			end := start + 100
			if end > len(ids) {
				end = len(ids)
			}
			f.write(user, ids[start:end], "remove")
		}
		page, err := f.service(nil, "").BeginBootstrap(ctx, principal(user), 100)
		must(t, err)
		if len(page.Items) != 0 || !page.HasMore || page.NextCursor == "" {
			t.Fatalf("remove page should be empty with continuation: %+v", page)
		}
		items, _ := bootstrapAll(t, f.service(nil, ""), user, 100)
		if len(items) != 0 {
			t.Fatalf("removed bootstrap disclosed %d items", len(items))
		}

		futureUser := "future"
		f.populate(futureUser, "future", 100)
		futureConversation := f.conversations("zzzz_future", 1)[0]
		f.sql("INSERT INTO newim.im_conversation_sync_keys(user_id,conversation_id,first_change_seq) VALUES($1,$2,101)", futureUser, futureConversation)
		futurePage, err := f.service(nil, "").BeginBootstrap(ctx, principal(futureUser), 100)
		must(t, err)
		if len(futurePage.Items) != 100 || !futurePage.HasMore {
			t.Fatalf("future key did not consume a bounded first page: %+v", futurePage)
		}
		futurePage, err = f.service(nil, "").ContinueBootstrap(ctx, principal(futureUser), futurePage.NextCursor)
		must(t, err)
		checkedPage(t, futurePage, 100)
		if len(futurePage.Items) != 0 || futurePage.HasMore {
			t.Fatalf("future-key terminal page should be empty and complete: %+v", futurePage)
		}
	})

	t.Run("stable-fence-and-subsequent-delta", func(t *testing.T) {
		user := "snapshot"
		ids := f.populate(user, "snap", 101)
		service := f.service(nil, "")
		page, err := service.BeginBootstrap(ctx, principal(user), 1)
		must(t, err)
		checkedPage(t, page, 1)
		token := page.NextCursor
		f.write(user, []string{ids[100]}, "update")
		added := f.conversations("snapnew", 1)
		f.write(user, added, "add")

		items := append([]app.Item{}, page.Items...)
		for page.HasMore {
			page, err = service.ContinueBootstrap(ctx, principal(user), page.NextCursor)
			must(t, err)
			checkedPage(t, page, 1)
			items = append(items, page.Items...)
		}
		checkpoint := page.NextCursor
		if len(items) != 101 {
			t.Fatalf("old fence returned %d items want 101", len(items))
		}
		for _, item := range items {
			if item.ConversationID == ids[100] && (item.LatestConversationSeq == nil || *item.LatestConversationSeq != 0) {
				t.Fatalf("bootstrap fence changed for %s: %+v", ids[100], item)
			}
			if item.ConversationID == added[0] {
				t.Fatalf("new key above H leaked into bootstrap: %+v", item)
			}
		}
		freshItems, _ := bootstrapAll(t, service, user, 100)
		if len(freshItems) != 102 {
			t.Fatalf("fresh bootstrap got %d items want 102", len(freshItems))
		}
		deltaItems, _ := deltaAll(t, service, user, checkpoint, 100)
		if len(deltaItems) != 2 {
			t.Fatalf("subsequent delta got %d items want 2", len(deltaItems))
		}
		want := map[string]bool{ids[100]: false, added[0]: false}
		for _, item := range deltaItems {
			if _, ok := want[item.ConversationID]; !ok {
				t.Fatalf("unexpected delta item %+v", item)
			}
			want[item.ConversationID] = true
		}
		for conversation, found := range want {
			if !found {
				t.Fatalf("delta lost %s", conversation)
			}
		}
		again, err := service.ContinueBootstrap(ctx, principal(user), token)
		must(t, err)
		sameAgain, err := service.ContinueBootstrap(ctx, principal(user), token)
		must(t, err)
		samePage(t, again, sameAgain)
	})

	t.Run("projected-and-unprojected-deep-revocation", func(t *testing.T) {
		for _, atomic := range []bool{false, true} {
			user := fmt.Sprintf("deep%t", atomic)
			ids := f.populate(user, user, 202)
			service := f.service(nil, "")
			page, err := service.BeginBootstrap(ctx, principal(user), 100)
			must(t, err)
			token := page.NextCursor
			if atomic {
				f.write(user, ids[200:201], "remove")
			} else {
				f.sql("DELETE FROM newim.im_conversation_members WHERE user_id=$1 AND conversation_id=$2", user, ids[200])
			}
			page, err = service.ContinueBootstrap(ctx, principal(user), token)
			must(t, err)
			if len(page.Items) != 100 {
				t.Fatalf("second page got %d items want 100", len(page.Items))
			}
			page, err = service.ContinueBootstrap(ctx, principal(user), page.NextCursor)
			if atomic {
				wantCode(t, page, err, app.CursorExpired)
				items, _ := bootstrapAll(t, service, user, 100)
				if len(items) != 201 {
					t.Fatalf("fresh bootstrap after revoke got %d items want 201", len(items))
				}
			} else {
				wantCode(t, page, err, app.NotReady)
			}
		}
	})

	t.Run("terminal-page-replay-after-revoke", func(t *testing.T) {
		user := "terminal"
		ids := f.populate(user, "terminal", 2)
		service := f.service(nil, "")
		page, err := service.BeginBootstrap(ctx, principal(user), 1)
		must(t, err)
		token := page.NextCursor
		page, err = service.ContinueBootstrap(ctx, principal(user), token)
		must(t, err)
		if page.HasMore {
			t.Fatal("expected terminal bootstrap page")
		}
		f.write(user, ids[1:], "remove")
		page, err = service.ContinueBootstrap(ctx, principal(user), token)
		wantCode(t, page, err, app.CursorExpired)
	})

	t.Run("invalid-limits", func(t *testing.T) {
		user := "limits"
		f.populate(user, "limits", 1)
		for _, limit := range []int{-1, app.MaxPageItems + 1} {
			page, err := f.service(nil, "").BeginBootstrap(ctx, principal(user), limit)
			wantCode(t, page, err, app.LimitExceeded)
		}
	})
}
