package friend

import (
	"context"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/friend"
)

func ensureEdge(ctx context.Context, tx pgx.Tx, owner, peer string) error {
	inserted, err := tx.Exec(ctx, `INSERT INTO newim.im_friend_edges(owner_id,peer_id)
		VALUES ($1,$2) ON CONFLICT (owner_id,peer_id) DO NOTHING`, owner, peer)
	if err != nil {
		return app.StorageUnavailable
	}
	if inserted.RowsAffected() == 0 {
		return nil
	}
	edge, err := scanEdge(tx.QueryRow(ctx, edgeQuery+" AND e.peer_id=$2", owner, peer))
	if err != nil {
		return app.StorageUnavailable
	}
	return appendChange(ctx, tx, owner, peer, app.FriendUpserted, edge)
}

// Remark updates only the authenticated owner's edge.
// Remark 只更新当前已核验账号的好友备注。
func (r *Repository) Remark(ctx context.Context, actor, peer, remark string) (app.Edge, error) {
	if ctx == nil || !validPair(actor, peer) || !validText(remark) {
		return app.Edge{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return app.Edge{}, err
	}
	defer rollback(tx)
	if err = lockPair(ctx, tx, actor, peer); err != nil {
		return app.Edge{}, err
	}
	edge, err := scanEdge(tx.QueryRow(ctx, edgeQuery+" AND e.peer_id=$2", actor, peer))
	if err != nil {
		return app.Edge{}, storageError(err)
	}
	if edge.Remark != remark {
		if err = lockHeads(ctx, tx, actor); err != nil {
			return app.Edge{}, err
		}
		if _, err = tx.Exec(ctx, `UPDATE newim.im_friend_edges SET remark=$3 WHERE owner_id=$1 AND peer_id=$2`,
			actor, peer, remark); err != nil {
			return app.Edge{}, app.StorageUnavailable
		}
		edge.Remark = remark
		if err = appendChange(ctx, tx, actor, peer, app.FriendUpserted, edge); err != nil {
			return app.Edge{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return app.Edge{}, app.StorageUnavailable
	}
	return edge, nil
}

// Remove deletes only the authenticated owner's edge, if present.
// Remove 仅移除当前已核验账号自己的好友记录。
func (r *Repository) Remove(ctx context.Context, actor, peer string) (bool, error) {
	if ctx == nil || !validPair(actor, peer) {
		return false, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	if err = lockPair(ctx, tx, actor, peer); err != nil {
		return false, err
	}
	var present bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM newim.im_friend_edges WHERE owner_id=$1 AND peer_id=$2)`,
		actor, peer).Scan(&present); err != nil {
		return false, app.StorageUnavailable
	}
	if present {
		if err = lockHeads(ctx, tx, actor); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `DELETE FROM newim.im_friend_edges WHERE owner_id=$1 AND peer_id=$2`,
			actor, peer); err != nil {
			return false, app.StorageUnavailable
		}
		if err = appendChange(ctx, tx, actor, peer, app.FriendRemoved, nil); err != nil {
			return false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, app.StorageUnavailable
	}
	return present, nil
}
