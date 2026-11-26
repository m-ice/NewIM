package authsession

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/auth/session"
)

// MaxGatewayHandshakePrune bounds the number of expired rows locked per call.
// MaxGatewayHandshakePrune 限制单次清理锁定的过期行数。
const MaxGatewayHandshakePrune = 100

// CheckGatewaySchema verifies that the guarded database has the replay storage.
// CheckGatewaySchema 在受校验的数据库上确认握手防重放存储已迁移。
func (r *Repository) CheckGatewaySchema(ctx context.Context) error {
	if ctx == nil {
		return app.Fail(app.AuthInvalidInput)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SELECT token_id, nonce_hash, expires_at FROM newim.im_gateway_handshakes LIMIT 0"); err != nil {
		return fixed(err)
	}
	return fixed(tx.Commit(ctx))
}

// ReserveGatewayHandshake stores a nonce hash for a proof-verified token.
// ReserveGatewayHandshake 为已验证的令牌记录随机值摘要，重复请求返回 false。
func (r *Repository) ReserveGatewayHandshake(ctx context.Context, tokenID string, nonceHash [32]byte, now, expiresAt time.Time) (bool, error) {
	if ctx == nil || !lowerHexTokenID(tokenID) || now.IsZero() || expiresAt.IsZero() {
		return false, app.Fail(app.AuthInvalidInput)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	binding, expiry, err := gatewayTokenBinding(ctx, tx, tokenID)
	if err != nil {
		return false, err
	}
	if !expiry.After(now) {
		return false, app.Fail(app.AuthTokenExpired)
	}
	if !expiry.Equal(expiresAt) {
		return false, app.Fail(app.AuthForbidden)
	}
	if r.admissionRequired {
		if err = verifyAdmittedBinding(ctx, tx, binding); err != nil {
			return false, err
		}
	}
	inserted, err := tx.Exec(ctx, `INSERT INTO newim.im_gateway_handshakes (token_id, nonce_hash, expires_at)
		VALUES ($1,$2,$3) ON CONFLICT (token_id, nonce_hash) DO NOTHING`, tokenID, nonceHash[:], expiry)
	if err != nil {
		return false, fixed(err)
	}
	// Recheck after possible unique-key waiting, against the clock that
	// authorizes pruning. A nonce deleted at expiry cannot be reserved again.
	var live bool
	if err = tx.QueryRow(ctx, "SELECT $1::timestamptz > clock_timestamp()", expiry).Scan(&live); err != nil {
		return false, fixed(err)
	}
	if !live {
		return false, app.Fail(app.AuthTokenExpired)
	}
	if err = tx.Commit(ctx); err != nil {
		return false, fixed(err)
	}
	return inserted.RowsAffected() == 1, nil
}

func gatewayTokenBinding(ctx context.Context, tx pgx.Tx, tokenID string) (app.SessionBinding, time.Time, error) {
	var binding app.SessionBinding
	var sessionRevokedAt, tokenRevokedAt *time.Time
	var expiry time.Time
	err := tx.QueryRow(ctx, "SELECT session_id FROM newim.im_auth_tokens WHERE token_id=$1", tokenID).Scan(&binding.SessionID)
	if err != nil {
		return app.SessionBinding{}, time.Time{}, gatewayTokenError(err)
	}
	err = tx.QueryRow(ctx, `SELECT user_id, device_id, revoked_at
		FROM newim.im_sessions WHERE session_id=$1 FOR SHARE`, binding.SessionID).
		Scan(&binding.UserID, &binding.DeviceID, &sessionRevokedAt)
	if err != nil {
		return app.SessionBinding{}, time.Time{}, gatewayTokenError(err)
	}
	err = tx.QueryRow(ctx, `SELECT expires_at, revoked_at FROM newim.im_auth_tokens
		WHERE token_id=$1 AND session_id=$2 FOR SHARE`, tokenID, binding.SessionID).
		Scan(&expiry, &tokenRevokedAt)
	if err != nil {
		return app.SessionBinding{}, time.Time{}, gatewayTokenError(err)
	}
	if tokenRevokedAt != nil {
		return app.SessionBinding{}, time.Time{}, app.Fail(app.AuthTokenRevoked)
	}
	if sessionRevokedAt != nil {
		return app.SessionBinding{}, time.Time{}, app.Fail(app.AuthSessionRevoked)
	}
	return binding, expiry, nil
}

func gatewayTokenError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.AuthTokenUnknown)
	}
	return fixed(err)
}

// PruneGatewayHandshakes deletes at most limit expired handshake records.
// PruneGatewayHandshakes 每次最多删除 limit 条已过期握手记录。
func (r *Repository) PruneGatewayHandshakes(ctx context.Context, now time.Time, limit int) (int64, error) {
	if ctx == nil || now.IsZero() || limit < 1 || limit > MaxGatewayHandshakePrune {
		return 0, app.Fail(app.AuthInvalidInput)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	deleted, err := tx.Exec(ctx, `WITH expired AS (
		SELECT token_id, nonce_hash FROM newim.im_gateway_handshakes
		WHERE expires_at <= LEAST($1::timestamptz, statement_timestamp())
		ORDER BY expires_at, token_id, nonce_hash LIMIT $2 FOR UPDATE SKIP LOCKED
	)
	DELETE FROM newim.im_gateway_handshakes AS handshake USING expired
	WHERE handshake.token_id=expired.token_id AND handshake.nonce_hash=expired.nonce_hash`, now, limit)
	if err != nil {
		return 0, fixed(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, fixed(err)
	}
	return deleted.RowsAffected(), nil
}
