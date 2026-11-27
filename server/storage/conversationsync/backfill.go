package conversationsync

import (
	"context"
	"errors"
	"slices"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

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

// NextSourceAccount finds the next account still needing a backfill.
// NextSourceAccount 按账号键继续查找尚未就绪的账号。
func (r *Repository) NextSourceAccount(ctx context.Context, after string) (string, error) {
	if ctx == nil || r == nil || !r.requireSourceReady {
		return "", app.Fail(app.StorageUnavailable)
	}
	if after != "" && !identifier(after) {
		return "", app.Fail(app.InvalidCursor)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	var user string
	err := r.pool.QueryRow(ctx, `SELECT u.user_id FROM newim.im_users u
		LEFT JOIN newim.im_conversation_sync_accounts a USING (user_id)
		WHERE u.user_id>$1::text AND a.source_ready IS DISTINCT FROM true
		ORDER BY u.user_id LIMIT 1`, after).Scan(&user)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return user, fixed(err)
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
