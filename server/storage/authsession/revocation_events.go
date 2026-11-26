package authsession

import (
	"context"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/auth/session"
)

// MaxRevocationEvents is the maximum size of one account replay page.
const MaxRevocationEvents = 100

// ListRevocationEvents returns one page of committed events for a trusted
// account. A cursor from one account must not be used for another account.
func (r *Repository) ListRevocationEvents(ctx context.Context, userID string, afterEventID int64, limit int) ([]app.SessionRevocationEvent, error) {
	if ctx == nil || !identifier(userID) || afterEventID < 0 || limit < 1 || limit > MaxRevocationEvents {
		return nil, app.Fail(app.AuthInvalidInput)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	rows, err := tx.Query(ctx, `SELECT event_id, user_id, device_id, session_id, revoked_at, reason
		FROM newim.im_session_revocation_events
		WHERE user_id=$1 AND event_id>$2 ORDER BY event_id LIMIT $3`, userID, afterEventID, limit)
	if err != nil {
		return nil, fixed(err)
	}
	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (app.SessionRevocationEvent, error) {
		var event app.SessionRevocationEvent
		err := row.Scan(&event.EventID, &event.Binding.UserID, &event.Binding.DeviceID,
			&event.Binding.SessionID, &event.RevokedAt, &event.Reason)
		return event, err
	})
	if err != nil {
		return nil, fixed(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, app.Fail(app.AuthStorageUnavailable)
	}
	return events, nil
}
