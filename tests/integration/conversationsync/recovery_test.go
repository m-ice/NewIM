//go:build integration

package conversationsync_test

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	app "github.com/m-ice/NewIM/server/sync/conversation"
)

func TestIsolationRecovery(t *testing.T) {
	switch os.Getenv("NEWIM_SYNC_PHASE") {
	case "prepare":
		recoveryPrepare(t)
	case "restart":
		recoveryRestart(t)
	case "restore":
		recoveryRestore(t)
	default:
		t.Fatal("NEWIM_SYNC_PHASE must be prepare, restart or restore")
	}
}

func writeRecoveryState(t *testing.T, state recoveryState) {
	t.Helper()
	raw, err := json.Marshal(state)
	must(t, err)
	must(t, os.WriteFile(recoveryStatePath, append(raw, '\n'), 0o600))
}

func readRecoveryState(t *testing.T) recoveryState {
	t.Helper()
	raw, err := os.ReadFile(recoveryStatePath)
	must(t, err)
	var state recoveryState
	must(t, json.Unmarshal(raw, &state))
	return state
}

func recoveryPrepare(t *testing.T) {
	t.Run("late-commit-rollback-backend-death-and-max", func(t *testing.T) {
		f := openFixture(t)
		user := "recovery_writer"
		f.account(user)
		conversation := f.conversations("recovery_writer", 1)[0]
		f.write(user, []string{conversation}, "add")

		first, err := f.repo.Begin(ctx)
		must(t, err)
		_, err = first.Exec(ctx, "SET LOCAL application_name='nim_sync_late_1'")
		must(t, err)
		batch, err := f.repo.PrepareBatch(ctx, first, []string{conversation}, []string{user})
		must(t, err)
		_, err = first.Exec(ctx, "UPDATE newim.im_conversations SET last_seq=last_seq+1 WHERE conversation_id=$1", conversation)
		must(t, err)
		revision1, err := batch.RecordUpsert(ctx, conversation, user)
		must(t, err)
		if revision1 != 2 {
			t.Fatalf("first writer revision got %d want 2", revision1)
		}

		second := async(func() error {
			work, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			tx, err := f.repo.Begin(work)
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			if _, err = tx.Exec(work, "SET LOCAL application_name='nim_sync_late_2'"); err != nil {
				return err
			}
			batch, err := f.repo.PrepareBatch(work, tx, []string{conversation}, []string{user})
			if err != nil {
				return err
			}
			if _, err = tx.Exec(work, "UPDATE newim.im_conversations SET last_seq=last_seq+1 WHERE conversation_id=$1", conversation); err != nil {
				return err
			}
			if _, err = batch.RecordUpsert(work, conversation, user); err != nil {
				return err
			}
			return tx.Commit(work)
		})
		f.waitApplicationLock("nim_sync_late_2")
		must(t, first.Commit(ctx))
		must(t, waitAsync(t, second, 8*time.Second))
		if head := f.scalarInt64("SELECT last_change_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", user); head != 3 {
			t.Fatalf("late writer gap/head got %d want 3", head)
		}

		rollback, err := f.repo.Begin(ctx)
		must(t, err)
		batch, err = f.repo.PrepareBatch(ctx, rollback, []string{conversation}, []string{user})
		must(t, err)
		_, err = rollback.Exec(ctx, "UPDATE newim.im_conversations SET last_seq=last_seq+1 WHERE conversation_id=$1", conversation)
		must(t, err)
		revision, err := batch.RecordUpsert(ctx, conversation, user)
		must(t, err)
		if revision != 4 {
			t.Fatalf("rollback trial revision got %d want 4", revision)
		}
		must(t, rollback.Rollback(ctx))
		if head := f.scalarInt64("SELECT last_change_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", user); head != 3 {
			t.Fatalf("rollback changed head to %d", head)
		}
		if last := f.scalarInt64("SELECT last_seq FROM newim.im_conversations WHERE conversation_id=$1", conversation); last != 2 {
			t.Fatalf("rollback changed source summary to %d", last)
		}

		victim := f.openConn()
		victimTx, err := victim.Begin(ctx)
		must(t, err)
		_, err = victimTx.Exec(ctx, "SET LOCAL application_name='nim_sync_victim'")
		must(t, err)
		batch, err = f.repo.PrepareBatch(ctx, victimTx, []string{conversation}, []string{user})
		must(t, err)
		_, err = victimTx.Exec(ctx, "UPDATE newim.im_conversations SET last_seq=last_seq+1 WHERE conversation_id=$1", conversation)
		must(t, err)
		if _, err = batch.RecordUpsert(ctx, conversation, user); err != nil {
			t.Fatal(err)
		}
		var pid int32
		must(t, victimTx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))
		f.sql("SELECT pg_terminate_backend($1)", pid)
		if err = victimTx.Commit(ctx); err == nil {
			t.Fatal("terminated backend commit unexpectedly succeeded")
		}
		if head := f.scalarInt64("SELECT last_change_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", user); head != 3 {
			t.Fatalf("backend death changed head to %d", head)
		}
		if last := f.scalarInt64("SELECT last_seq FROM newim.im_conversations WHERE conversation_id=$1", conversation); last != 2 {
			t.Fatalf("backend death changed source summary to %d", last)
		}
		if count := f.scalarInt64("SELECT count(*) FROM newim.im_conversation_sync_changes WHERE user_id=$1", user); count != 3 {
			t.Fatalf("backend death changed revision count to %d", count)
		}

		f.sql("UPDATE newim.im_conversation_sync_accounts SET last_change_seq=$2,min_valid_seq=$2 WHERE user_id=$1", user, int64(app.MaxSequence))
		maxTx, err := f.repo.Begin(ctx)
		must(t, err)
		batch, err = f.repo.PrepareBatch(ctx, maxTx, []string{conversation}, []string{user})
		must(t, err)
		_, err = maxTx.Exec(ctx, "UPDATE newim.im_conversations SET last_seq=last_seq+1 WHERE conversation_id=$1", conversation)
		must(t, err)
		_, err = batch.RecordUpsert(ctx, conversation, user)
		if app.ErrorCode(err) != app.SequenceExhausted {
			t.Fatalf("MAX head got %v want %s", err, app.SequenceExhausted)
		}
		must(t, maxTx.Rollback(ctx))
		if last := f.scalarInt64("SELECT last_seq FROM newim.im_conversations WHERE conversation_id=$1", conversation); last != 2 {
			t.Fatalf("MAX rejection changed source summary to %d", last)
		}
	})

	t.Run("initialization-batch-table-gates", func(t *testing.T) {
		f := openFixture(t)
		user := "recovery_init_batch"
		f.account(user)
		conversation := f.conversations("recovery_init_batch", 1)[0]
		held, err := f.repo.Begin(ctx)
		must(t, err)
		_, err = held.Exec(ctx, "SET LOCAL application_name='nim_sync_batch_hold'")
		must(t, err)
		batch, err := f.repo.PrepareBatch(ctx, held, []string{conversation}, []string{user})
		must(t, err)
		_, err = held.Exec(ctx, "INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", conversation, user)
		must(t, err)
		revision, err := batch.RecordUpsert(ctx, conversation, user)
		must(t, err)
		if revision != 1 {
			t.Fatalf("held batch revision got %d want 1", revision)
		}

		timeout, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		err = f.repo.InitializeEmptyAccount(timeout, user)
		cancel()
		if app.ErrorCode(err) != app.StorageUnavailable {
			t.Fatalf("initialization timeout got %v want %s", err, app.StorageUnavailable)
		}
		if head := f.scalarInt64("SELECT last_change_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", user); head != 0 {
			t.Fatalf("uncommitted holder changed committed head to %d", head)
		}

		initialized := async(func() error { return f.repo.InitializeEmptyAccount(context.Background(), user) })
		f.waitLock("newim.im_conversation_members", "ShareLock", false)
		must(t, held.Commit(ctx))
		if err = waitAsync(t, initialized, 8*time.Second); app.ErrorCode(err) != app.NotReady {
			t.Fatalf("populated initialization got %v want %s", err, app.NotReady)
		}
		if head := f.scalarInt64("SELECT last_change_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", user); head != 1 {
			t.Fatalf("initialization refusal changed head to %d", head)
		}
	})

	t.Run("initialization-holds-share-before-batch", func(t *testing.T) {
		f := openFixture(t)
		user := "recovery_init_share"
		f.sql("INSERT INTO newim.im_users(user_id) VALUES($1)", user)
		conversation := f.conversations("recovery_init_share", 1)[0]
		holdConn := f.openConn()
		holdTx, err := holdConn.Begin(ctx)
		must(t, err)
		_, err = holdTx.Exec(ctx, "SELECT user_id FROM newim.im_users WHERE user_id=$1 FOR UPDATE", user)
		must(t, err)

		initialized := async(func() error { return f.repo.InitializeEmptyAccount(context.Background(), user) })
		f.waitLock("newim.im_conversation_members", "ShareLock", true)

		short, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		timeoutTx, err := f.repo.Begin(short)
		if err == nil {
			_, err = f.repo.PrepareBatch(short, timeoutTx, []string{conversation}, []string{user})
			if err != nil {
				_ = timeoutTx.Rollback(context.Background())
			}
		}
		cancel()
		if app.ErrorCode(err) != app.StorageUnavailable {
			t.Fatalf("batch timeout got %v want %s", err, app.StorageUnavailable)
		}
		time.Sleep(50 * time.Millisecond)
		if f.scalarBool("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='newim.im_conversation_members'::regclass AND mode='RowExclusiveLock' AND NOT granted)") {
			t.Fatal("timed-out batch retained a waiting table lock")
		}

		batchDone := async(func() error {
			work, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			tx, err := f.repo.Begin(work)
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			if _, err = tx.Exec(work, "SET LOCAL application_name='nim_sync_batch_after_init'"); err != nil {
				return err
			}
			batch, err := f.repo.PrepareBatch(work, tx, []string{conversation}, []string{user})
			if err != nil {
				return err
			}
			if _, err = tx.Exec(work, "INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", conversation, user); err != nil {
				return err
			}
			if _, err = batch.RecordUpsert(work, conversation, user); err != nil {
				return err
			}
			return tx.Commit(work)
		})
		f.waitLock("newim.im_conversation_members", "RowExclusiveLock", false)
		must(t, holdTx.Rollback(ctx))
		must(t, waitAsync(t, initialized, 8*time.Second))
		must(t, waitAsync(t, batchDone, 8*time.Second))
		if row := f.scalarString("SELECT epoch||'|'||last_change_seq||'|'||min_valid_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", user); row != "1|1|0" {
			t.Fatalf("initialization/batch final account row got %s", row)
		}
	})

	t.Run("concurrent-independent-and-ordered-batches", func(t *testing.T) {
		f := openFixture(t)
		userA := "recovery_parallel_a"
		userB := "recovery_parallel_b"
		f.account(userA)
		f.account(userB)
		conversationA := f.conversations("recovery_parallel_a", 1)[0]
		conversationB := f.conversations("recovery_parallel_b", 1)[0]
		prepared := make(chan error, 2)
		released := make(chan struct{}, 2)
		var wg sync.WaitGroup
		for _, work := range []struct {
			user, conversation, application string
		}{
			{userA, conversationA, "nim_sync_parallel_a"},
			{userB, conversationB, "nim_sync_parallel_b"},
		} {
			wg.Add(1)
			go func(work struct{ user, conversation, application string }) {
				defer wg.Done()
				tx, err := f.repo.Begin(context.Background())
				if err != nil {
					prepared <- err
					return
				}
				if _, err = tx.Exec(context.Background(), "SET LOCAL application_name='"+work.application+"'"); err != nil {
					_ = tx.Rollback(context.Background())
					prepared <- err
					return
				}
				batch, err := f.repo.PrepareBatch(context.Background(), tx, []string{work.conversation}, []string{work.user})
				if err != nil {
					_ = tx.Rollback(context.Background())
					prepared <- err
					return
				}
				prepared <- nil
				<-released
				if _, err = tx.Exec(context.Background(), "INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", work.conversation, work.user); err == nil {
					_, err = batch.RecordUpsert(context.Background(), work.conversation, work.user)
				}
				if err == nil {
					err = tx.Commit(context.Background())
				} else {
					_ = tx.Rollback(context.Background())
				}
				prepared <- err
			}(work)
		}
		must(t, waitAsync(t, prepared, 5*time.Second))
		must(t, waitAsync(t, prepared, 5*time.Second))
		released <- struct{}{}
		released <- struct{}{}
		must(t, waitAsync(t, prepared, 5*time.Second))
		must(t, waitAsync(t, prepared, 5*time.Second))
		wg.Wait()

		user := "recovery_order"
		f.account(user)
		ids := f.conversations("recovery_order", 2)
		const workers = 8
		errors := make(chan error, workers)
		var orderWG sync.WaitGroup
		for i := 0; i < workers; i++ {
			orderWG.Add(1)
			go func() {
				defer orderWG.Done()
				tx, err := f.repo.Begin(context.Background())
				if err != nil {
					errors <- err
					return
				}
				batch, err := f.repo.PrepareBatch(context.Background(), tx, []string{ids[1], ids[0]}, []string{user})
				if err != nil {
					_ = tx.Rollback(context.Background())
					errors <- err
					return
				}
				for _, conversation := range ids {
					if _, err = tx.Exec(context.Background(), "INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2) ON CONFLICT DO NOTHING", conversation, user); err != nil {
						_ = tx.Rollback(context.Background())
						errors <- err
						return
					}
					if _, err = tx.Exec(context.Background(), "UPDATE newim.im_conversations SET last_seq=last_seq+1 WHERE conversation_id=$1", conversation); err != nil {
						_ = tx.Rollback(context.Background())
						errors <- err
						return
					}
					if _, err = batch.RecordUpsert(context.Background(), conversation, user); err != nil {
						_ = tx.Rollback(context.Background())
						errors <- err
						return
					}
				}
				if err = tx.Commit(context.Background()); err != nil {
					errors <- err
				}
			}()
		}
		orderWG.Wait()
		close(errors)
		for err := range errors {
			must(t, err)
		}
		if head := f.scalarInt64("SELECT last_change_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", user); head != workers*2 {
			t.Fatalf("ordered stress head got %d want %d", head, workers*2)
		}
	})

	t.Run("account-isolation", func(t *testing.T) {
		f := openFixture(t)
		userA := "recovery_isolated_a"
		userB := "recovery_isolated_b"
		f.populate(userA, "recovery_isolated_a", 2)
		f.account(userB)
		items, checkpoint := bootstrapAll(t, f.service(nil, ""), userB, 100)
		if len(items) != 0 || checkpoint == "" {
			t.Fatalf("account B observed account A items: %+v", items)
		}
	})

	f := openFixture(t)
	user := "recovery_state"
	f.account(user)
	conversation := f.conversations("recovery_state_room", 1)[0]
	f.write(user, []string{conversation}, "add")
	f.write(user, []string{conversation}, "update")
	f.write(user, []string{conversation}, "update")
	must(t, f.repo.AdvanceFloor(ctx, user, 2))
	items, checkpoint := bootstrapAll(t, f.service(nil, ""), user, 100)
	if len(items) != 1 || items[0].Revision != 3 {
		t.Fatalf("recovery seed did not retain latest projection: %+v", items)
	}
	writeRecoveryState(t, recoveryState{User: user, Conv: conversation, Checkpoint: checkpoint, Head: 3, Epoch: 2, Floor: 2, Revision: 3})
	t.Logf("prepared populated projection at head=3 epoch=2 floor=2")
}

func recoveryRestart(t *testing.T) {
	f := openFixture(t)
	state := readRecoveryState(t)
	row := f.scalarString("SELECT epoch||'|'||last_change_seq||'|'||min_valid_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", state.User)
	wantRow := "2|3|2"
	if row != wantRow {
		t.Fatalf("restart account row got %s want %s", row, wantRow)
	}
	f.write(state.User, []string{state.Conv}, "update")
	items, checkpoint := deltaAll(t, f.service(nil, ""), state.User, state.Checkpoint, 100)
	if len(items) != 1 || items[0].Revision != 4 {
		t.Fatalf("cursor continuation after crash got %+v", items)
	}
	state.Checkpoint = checkpoint
	state.Head = 4
	state.Revision = 4
	writeRecoveryState(t, state)
	t.Logf("continued persisted cursor after backend/container restart at revision=4")
}

func recoveryRestore(t *testing.T) {
	f := openFixture(t)
	state := readRecoveryState(t)
	row := f.scalarString("SELECT epoch||'|'||last_change_seq||'|'||min_valid_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", state.User)
	wantRow := "2|4|2"
	if row != wantRow {
		t.Fatalf("restore account row got %s want %s", row, wantRow)
	}
	if count := f.scalarInt64("SELECT count(*) FROM newim.im_conversation_sync_changes WHERE user_id=$1", state.User); count != 4 {
		t.Fatalf("restored revision count got %d want 4", count)
	}
	f.write(state.User, []string{state.Conv}, "update")
	items, checkpoint := deltaAll(t, f.service(nil, ""), state.User, state.Checkpoint, 100)
	if len(items) != 1 || items[0].Revision != 5 {
		t.Fatalf("cursor continuation after logical restore got %+v", items)
	}
	if checkpoint == "" {
		t.Fatal("restored delta did not issue a checkpoint")
	}
	t.Logf("continued restored cursor in new database/process at revision=5")
}
