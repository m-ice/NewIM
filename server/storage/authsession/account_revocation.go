package authsession

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/auth/session"
)

// CheckAccountRevocationSchema checks readiness before the trusted route is bound.
func (r *Repository) CheckAccountRevocationSchema(ctx context.Context) error {
	if ctx == nil {
		return app.Fail(app.AuthInvalidInput)
	}
	if r == nil || !r.admissionRequired {
		return app.Fail(app.AuthStorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, `SELECT user_id, request_id, reason, revoked_sessions, recorded_at
		FROM newim.im_account_revocation_requests LIMIT 0`); err != nil {
		return fixed(err)
	}
	if _, err = tx.Exec(ctx, `SELECT user_id, device_id, session_id, revoked_at, reason
		FROM newim.im_session_revocation_events LIMIT 0`); err != nil {
		return fixed(err)
	}
	return fixed(tx.Commit(ctx))
}

// RevokeAccount serializes with device admission and commits revocation and its receipt together.
func (r *Repository) RevokeAccount(ctx context.Context, request app.AccountRevocationRequest, now time.Time) (app.AccountRevocationResult, error) {
	if ctx == nil || !request.Valid() || now.IsZero() {
		return app.AccountRevocationResult{}, app.Fail(app.AuthInvalidInput)
	}
	if r == nil || !r.admissionRequired {
		return app.AccountRevocationResult{}, app.Fail(app.AuthStorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return app.AccountRevocationResult{}, err
	}
	defer rollback(tx)
	if err = lockAccount(ctx, tx, request.UserID); err != nil {
		return app.AccountRevocationResult{}, err
	}

	var storedReason string
	var result app.AccountRevocationResult
	err = tx.QueryRow(ctx, `SELECT reason, revoked_sessions FROM newim.im_account_revocation_requests
		WHERE user_id=$1 AND request_id=$2`, request.UserID, request.RequestID).
		Scan(&storedReason, &result.RevokedSessions)
	switch {
	case err == nil:
		if storedReason != request.Reason {
			return app.AccountRevocationResult{}, app.Fail(app.AuthRevocationConflict)
		}
		if err = tx.Commit(ctx); err != nil {
			return app.AccountRevocationResult{}, fixed(err)
		}
		return result, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return app.AccountRevocationResult{}, fixed(err)
	}

	rows, err := tx.Query(ctx, `SELECT session_id FROM newim.im_sessions
		WHERE user_id=$1 AND revoked_at IS NULL ORDER BY session_id FOR UPDATE`, request.UserID)
	if err != nil {
		return app.AccountRevocationResult{}, fixed(err)
	}
	sessions, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return app.AccountRevocationResult{}, fixed(err)
	}
	result.RevokedSessions, err = revokeLockedSessions(ctx, tx, sessions, now, request.Reason)
	if err != nil {
		return app.AccountRevocationResult{}, err
	}
	if result.RevokedSessions != int64(len(sessions)) {
		return app.AccountRevocationResult{}, app.Fail(app.AuthStorageUnavailable)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO newim.im_account_revocation_requests
		(user_id, request_id, reason, revoked_sessions, recorded_at) VALUES ($1,$2,$3,$4,$5)`,
		request.UserID, request.RequestID, request.Reason, result.RevokedSessions, now); err != nil {
		return app.AccountRevocationResult{}, fixed(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return app.AccountRevocationResult{}, fixed(err)
	}
	return result, nil
}
