package conversationsync

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

// InitializeEmptyAccount only onboards empty accounts; it never resets history.
// InitializeEmptyAccount 只初始化空账户，绝不重置已有历史。
func (r *Repository) InitializeEmptyAccount(ctx context.Context, user string) error {
	if !identifier(user) {
		return app.Fail(app.Forbidden)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "LOCK TABLE newim.im_conversation_members IN SHARE MODE"); err != nil {
		return fixed(err)
	}
	var id string
	err = tx.QueryRow(ctx, "SELECT user_id FROM newim.im_users WHERE user_id=$1 FOR UPDATE", user).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.Forbidden)
	}
	if err != nil {
		return fixed(err)
	}
	var populated bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM newim.im_conversation_members WHERE user_id=$1)
 OR EXISTS(SELECT 1 FROM newim.im_conversation_sync_keys WHERE user_id=$1)
 OR EXISTS(SELECT 1 FROM newim.im_conversation_sync_changes WHERE user_id=$1)`, user).Scan(&populated)
	if err != nil {
		return fixed(err)
	}
	if populated {
		return app.Fail(app.NotReady)
	}
	var epoch, head, floor int64
	err = tx.QueryRow(ctx, "SELECT epoch,last_change_seq,min_valid_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1 FOR UPDATE", user).Scan(&epoch, &head, &floor)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(ctx, "INSERT INTO newim.im_conversation_sync_accounts(user_id) VALUES($1)", user)
	} else if err == nil && (epoch != 1 || head != 0 || floor != 0) {
		return app.Fail(app.NotReady)
	}
	if err != nil {
		return fixed(err)
	}
	return fixed(tx.Commit(ctx))
}

// AdvanceFloor invalidates old cursors without deleting projection history.
// AdvanceFloor 使旧游标过期，但不删除任何投影历史。
func (r *Repository) AdvanceFloor(ctx context.Context, user string, floor uint64) error {
	if !identifier(user) {
		return app.Fail(app.Forbidden)
	}
	if floor > app.MaxSequence {
		return app.Fail(app.LimitExceeded)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err = userExists(ctx, tx, user); err != nil {
		return err
	}
	var epoch, head, current int64
	err = tx.QueryRow(ctx, "SELECT epoch,last_change_seq,min_valid_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1 FOR UPDATE", user).Scan(&epoch, &head, &current)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.NotReady)
	}
	if err != nil {
		return fixed(err)
	}
	if floor < uint64(current) || floor > uint64(head) {
		return app.Fail(app.LimitExceeded)
	}
	if floor > uint64(current) {
		if uint64(epoch) == app.MaxSequence {
			return app.Fail(app.SequenceExhausted)
		}
		_, err = tx.Exec(ctx, "UPDATE newim.im_conversation_sync_accounts SET min_valid_seq=$2,epoch=epoch+1 WHERE user_id=$1", user, int64(floor))
		if err != nil {
			return fixed(err)
		}
	}
	return fixed(tx.Commit(ctx))
}
