package conversationsync

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

const sourceBackfillLock int64 = 0x4e494d53594e4301

type sourceBackfillState struct {
	epoch int64
	ready bool
	after string
}

// BackfillStep reconciles one account page and commits its progress.
// BackfillStep 核对账号的一页会话并提交回填进度。
func (r *Repository) BackfillStep(ctx context.Context, userID string, limit int) (bool, error) {
	if ctx == nil || r == nil || !r.requireSourceReady {
		return false, app.Fail(app.StorageUnavailable)
	}
	if !identifier(userID) {
		return false, app.Fail(app.Forbidden)
	}
	if limit < 1 || limit > app.MaxPageItems {
		return false, app.Fail(app.LimitExceeded)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	if err = LockSourceMembership(ctx, tx); err != nil {
		return false, err
	}
	if err = userExists(ctx, tx, userID); err != nil {
		return false, err
	}
	initial, err := readSourceBackfillState(ctx, tx, userID, false)
	if err != nil {
		return false, err
	}
	var keys []string
	var more bool
	if !initial.ready {
		keys, err = sourceBackfillKeys(ctx, tx, userID, initial.after, limit+1)
		if err != nil {
			return false, err
		}
		more = len(keys) > limit
		keys = keys[:min(len(keys), limit)]
		if err = lockBackfillConversations(ctx, tx, keys); err != nil {
			return false, err
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO newim.im_conversation_sync_accounts(user_id)
		VALUES ($1) ON CONFLICT (user_id) DO NOTHING`, userID); err != nil {
		return false, fixed(err)
	}
	current, err := readSourceBackfillState(ctx, tx, userID, true)
	if err != nil {
		return false, err
	}
	if current.ready || current != initial {
		return current.ready, fixed(tx.Commit(ctx))
	}
	if len(keys) > 0 {
		if err = appendSourceProjection(ctx, tx, sourceAccountPageTargets, userID, keys); err != nil {
			return false, err
		}
	}

	var after *string
	if more {
		after = &keys[len(keys)-1]
	}
	ready := after == nil
	tag, err := tx.Exec(ctx, `UPDATE newim.im_conversation_sync_accounts
		SET source_ready=$2,source_after_key=$3 WHERE user_id=$1`, userID, ready, after)
	if err != nil || tag.RowsAffected() != 1 {
		return false, app.Fail(app.StorageUnavailable)
	}
	if err = tx.Commit(ctx); err != nil {
		return false, fixed(err)
	}
	return ready, nil
}

func readSourceBackfillState(ctx context.Context, tx pgx.Tx, userID string, lock bool) (sourceBackfillState, error) {
	query := `SELECT epoch,source_ready,COALESCE(source_after_key::text,'')
		FROM newim.im_conversation_sync_accounts WHERE user_id=$1`
	if lock {
		query += " FOR UPDATE"
	}
	var state sourceBackfillState
	err := tx.QueryRow(ctx, query, userID).Scan(&state.epoch, &state.ready, &state.after)
	if !lock && errors.Is(err, pgx.ErrNoRows) {
		return sourceBackfillState{epoch: 1}, nil
	}
	if err != nil || state.epoch < 1 || (state.after != "" && !identifier(state.after)) ||
		(state.ready && state.after != "") {
		return sourceBackfillState{}, app.Fail(app.StorageUnavailable)
	}
	return state, nil
}

func sourceBackfillKeys(ctx context.Context, tx pgx.Tx, userID, after string, limit int) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT conversation_id FROM (
		(SELECT conversation_id FROM newim.im_conversation_members
		 WHERE user_id=$1 AND departed_at IS NULL AND conversation_id>$2::text
		 ORDER BY conversation_id LIMIT $3)
		UNION
		(SELECT conversation_id FROM newim.im_conversation_sync_keys
		 WHERE user_id=$1 AND conversation_id>$2::text
		 ORDER BY conversation_id LIMIT $3)
	) AS directory ORDER BY conversation_id LIMIT $3`, userID, after, limit)
	if err != nil {
		return nil, fixed(err)
	}
	keys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	return keys, fixed(err)
}

func lockBackfillConversations(ctx context.Context, tx pgx.Tx, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT conversation_id FROM newim.im_conversations
		WHERE conversation_id=ANY($1::text[]) ORDER BY conversation_id FOR UPDATE`, keys)
	if err != nil {
		return fixed(err)
	}
	locked, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || !slices.Equal(keys, locked) {
		return app.Fail(app.StorageUnavailable)
	}
	return nil
}

// NextSourceAccounts examines at most one directory page, including ready accounts.
// NextSourceAccounts 每次检查一页账号，游标也会越过已就绪账号。
func (r *Repository) NextSourceAccounts(ctx context.Context, after string) (next string, pending []string, err error) {
	if ctx == nil || r == nil || r.pool == nil || !r.requireSourceReady {
		return "", nil, app.Fail(app.StorageUnavailable)
	}
	if after != "" && !identifier(after) {
		return "", nil, app.Fail(app.InvalidCursor)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	rows, err := r.pool.Query(ctx, `SELECT page.user_id,COALESCE((
		SELECT a.source_ready FROM newim.im_conversation_sync_accounts a WHERE a.user_id=page.user_id
	), false)
	FROM (SELECT user_id FROM newim.im_users WHERE user_id>$1::text ORDER BY user_id LIMIT $2) page
	ORDER BY page.user_id`, after, app.MaxPageItems)
	if err != nil {
		return "", nil, fixed(err)
	}
	defer rows.Close()
	for rows.Next() {
		var ready bool
		if err = rows.Scan(&next, &ready); err != nil {
			return "", nil, fixed(err)
		}
		if !ready {
			pending = append(pending, next)
		}
	}
	if err = rows.Err(); err != nil {
		return "", nil, fixed(err)
	}
	return next, pending, nil
}

// RunSourceBackfill completes the account directory under a lock shared with reset.
// RunSourceBackfill 在与重置互斥的锁下遍历全部账号。
func (r *Repository) RunSourceBackfill(ctx context.Context, limit int) (err error) {
	if ctx == nil || r == nil || r.pool == nil || !r.requireSourceReady || r.pool.Config().MaxConns < 2 {
		return app.Fail(app.StorageUnavailable)
	}
	if limit < 1 || limit > app.MaxPageItems {
		return app.Fail(app.LimitExceeded)
	}
	lockCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	conn, err := r.pool.Acquire(lockCtx)
	if err != nil {
		return fixed(err)
	}
	var locked bool
	err = conn.QueryRow(lockCtx, "SELECT pg_catalog.pg_try_advisory_lock($1)", sourceBackfillLock).Scan(&locked)
	if err != nil {
		_ = conn.Hijack().Close(lockCtx)
		return fixed(err)
	}
	if !locked {
		conn.Release()
		return app.Fail(app.NotReady)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var unlocked bool
		unlockErr := conn.QueryRow(unlockCtx, "SELECT pg_catalog.pg_advisory_unlock($1)", sourceBackfillLock).Scan(&unlocked)
		if unlockErr != nil || !unlocked {
			_ = conn.Hijack().Close(unlockCtx)
			if err == nil {
				err = app.Fail(app.StorageUnavailable)
			}
			return
		}
		conn.Release()
	}()

	var after string
	for {
		next, pending, err := r.NextSourceAccounts(ctx, after)
		if err != nil || next == "" {
			return err
		}
		for _, userID := range pending {
			for {
				ready, err := r.BackfillStep(ctx, userID, limit)
				if err != nil {
					return err
				}
				if ready {
					break
				}
			}
		}
		after = next
	}
}

// ResetSourceAccount invalidates an account's cursors before rebuilding it.
// ResetSourceAccount 在重建前推进账号代际，使旧游标失效。
func (r *Repository) ResetSourceAccount(ctx context.Context, userID string) error {
	if ctx == nil || r == nil || !r.requireSourceReady {
		return app.Fail(app.StorageUnavailable)
	}
	if !identifier(userID) {
		return app.Fail(app.Forbidden)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)
	var locked bool
	if err = tx.QueryRow(ctx, "SELECT pg_catalog.pg_try_advisory_xact_lock($1)", sourceBackfillLock).Scan(&locked); err != nil {
		return fixed(err)
	}
	if !locked {
		return app.Fail(app.NotReady)
	}
	if err = userExists(ctx, tx, userID); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO newim.im_conversation_sync_accounts AS account(user_id)
		VALUES ($1) ON CONFLICT (user_id) DO UPDATE SET epoch=account.epoch+1,
		min_valid_seq=account.last_change_seq,source_ready=false,source_after_key=NULL
		WHERE account.epoch<9223372036854775807`, userID)
	if err != nil {
		return fixed(err)
	}
	if tag.RowsAffected() != 1 {
		return app.Fail(app.SequenceExhausted)
	}
	return fixed(tx.Commit(ctx))
}
