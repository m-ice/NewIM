package friend

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/friend"
	"github.com/m-ice/NewIM/server/user"
)

// Apply stores a new attempt once, keyed by the authenticated applicant.
// Apply 按已核验申请人和申请标识保存一次申请。
func (r *Repository) Apply(ctx context.Context, actor string, request app.ApplyRequest) (app.Application, error) {
	if ctx == nil || !validPair(actor, request.PeerID) || !user.ValidID(request.RequestID) || !validText(request.Message) {
		return app.Application{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return app.Application{}, err
	}
	defer rollback(tx)
	if err = lockPair(ctx, tx, actor, request.PeerID); err != nil {
		if err == app.NotFound {
			// An existing key cannot be reassigned even to a missing recipient.
			_, loadErr := applicationByKey(ctx, tx, actor, request.RequestID)
			if loadErr == nil {
				return app.Application{}, app.Conflict
			}
			if !errors.Is(loadErr, pgx.ErrNoRows) {
				return app.Application{}, app.StorageUnavailable
			}
		}
		return app.Application{}, err
	}
	// The applicant's head also serializes a request ID reused across two pairs.
	if err = lockHeads(ctx, tx, actor, request.PeerID); err != nil {
		return app.Application{}, err
	}
	saved, err := applicationByKey(ctx, tx, actor, request.RequestID)
	if err == nil {
		if saved.RecipientID != request.PeerID || saved.Message != request.Message {
			return app.Application{}, app.Conflict
		}
		return commitApplication(ctx, tx, saved)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return app.Application{}, app.StorageUnavailable
	}
	var pending, mutual bool
	err = tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM newim.im_friend_applications WHERE applicant_id=$1 AND recipient_id=$2 AND state='pending'
	), EXISTS (
		SELECT 1 FROM newim.im_friend_edges WHERE owner_id=$1 AND peer_id=$2
	) AND EXISTS (
		SELECT 1 FROM newim.im_friend_edges WHERE owner_id=$2 AND peer_id=$1
	)`, actor, request.PeerID).Scan(&pending, &mutual)
	if err != nil {
		return app.Application{}, app.StorageUnavailable
	}
	if pending {
		return app.Application{}, app.Pending
	}
	if mutual {
		return app.Application{}, app.Conflict
	}
	saved, err = scanApplication(tx.QueryRow(ctx, `INSERT INTO newim.im_friend_applications
		(applicant_id, request_id, recipient_id, message, state) VALUES ($1,$2,$3,$4,'pending') RETURNING `+
		applicationFields, actor, request.RequestID, request.PeerID, request.Message))
	if err != nil {
		return app.Application{}, app.StorageUnavailable
	}
	if err = recordApplication(ctx, tx, saved); err != nil {
		return app.Application{}, err
	}
	return commitApplication(ctx, tx, saved)
}

// Respond resolves an application belonging to the authenticated recipient.
// Respond 仅处理当前已核验收件人收到的申请。
func (r *Repository) Respond(ctx context.Context, actor string, request app.ResponseRequest) (app.Application, error) {
	if ctx == nil || !validPair(actor, request.ApplicantID) || !user.ValidID(request.RequestID) {
		return app.Application{}, app.InvalidInput
	}
	var state app.State
	switch request.Decision {
	case "accept":
		state = app.StateAccepted
	case "reject":
		state = app.StateRejected
	default:
		return app.Application{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return app.Application{}, err
	}
	defer rollback(tx)
	if err = lockPair(ctx, tx, request.ApplicantID, actor); err != nil {
		return app.Application{}, err
	}
	saved, err := scanApplication(tx.QueryRow(ctx, "SELECT "+applicationFields+`
		FROM newim.im_friend_applications WHERE applicant_id=$1 AND request_id=$2 AND recipient_id=$3`,
		request.ApplicantID, request.RequestID, actor))
	if err != nil {
		return app.Application{}, storageError(err)
	}
	if saved.State == state {
		return commitApplication(ctx, tx, saved)
	}
	if saved.State != app.StatePending {
		return app.Application{}, app.Conflict
	}
	if err = lockHeads(ctx, tx, request.ApplicantID, actor); err != nil {
		return app.Application{}, err
	}
	saved, err = scanApplication(tx.QueryRow(ctx, `UPDATE newim.im_friend_applications SET state=$4
		WHERE applicant_id=$1 AND request_id=$2 AND recipient_id=$3 AND state='pending' RETURNING `+
		applicationFields, request.ApplicantID, request.RequestID, actor, state))
	if err != nil {
		return app.Application{}, app.StorageUnavailable
	}
	if err = recordApplication(ctx, tx, saved); err != nil {
		return app.Application{}, err
	}
	if state == app.StateAccepted {
		if err = acceptPair(ctx, tx, saved.ApplicantID, saved.RecipientID); err != nil {
			return app.Application{}, err
		}
	}
	return commitApplication(ctx, tx, saved)
}

func acceptPair(ctx context.Context, tx pgx.Tx, applicant, recipient string) error {
	other, err := scanApplication(tx.QueryRow(ctx, `UPDATE newim.im_friend_applications SET state='accepted'
		WHERE applicant_id=$1 AND recipient_id=$2 AND state='pending' RETURNING `+applicationFields, recipient, applicant))
	switch {
	case err == nil:
		if err = recordApplication(ctx, tx, other); err != nil {
			return err
		}
	case !errors.Is(err, pgx.ErrNoRows):
		return app.StorageUnavailable
	}
	if err = ensureEdge(ctx, tx, applicant, recipient); err != nil {
		return err
	}
	return ensureEdge(ctx, tx, recipient, applicant)
}

func applicationByKey(ctx context.Context, tx pgx.Tx, applicant, requestID string) (app.Application, error) {
	return scanApplication(tx.QueryRow(ctx, "SELECT "+applicationFields+`
		FROM newim.im_friend_applications WHERE applicant_id=$1 AND request_id=$2`, applicant, requestID))
}

func recordApplication(ctx context.Context, tx pgx.Tx, saved app.Application) error {
	if err := appendChange(ctx, tx, saved.ApplicantID, saved.RecipientID, app.ApplicationChanged, saved); err != nil {
		return err
	}
	return appendChange(ctx, tx, saved.RecipientID, saved.ApplicantID, app.ApplicationChanged, saved)
}

func commitApplication(ctx context.Context, tx pgx.Tx, saved app.Application) (app.Application, error) {
	if err := tx.Commit(ctx); err != nil {
		return app.Application{}, app.StorageUnavailable
	}
	return saved, nil
}
