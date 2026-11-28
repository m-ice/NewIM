package push

import (
	"context"
	"crypto/subtle"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/m-ice/NewIM/server/auth/session"
	app "github.com/m-ice/NewIM/server/push"
	"github.com/m-ice/NewIM/server/storage/authsession"
)

func (r *Repository) Register(ctx context.Context, bearer session.BearerSession, channel app.Channel, token []byte) (int64, error) {
	if ctx == nil || !app.ValidToken(channel, token) {
		return 0, app.ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, app.ErrStorage
	}
	defer rollback(tx)
	expiry, err := lockRegistration(ctx, tx, bearer)
	if err != nil {
		return 0, err
	}

	binding := app.TokenBinding{
		UserID: bearer.UserID(), DeviceID: bearer.DeviceID(), SessionID: bearer.SessionID(), Channel: channel,
	}
	current, err := r.currentRegistration(ctx, tx, binding, token)
	if err != nil {
		return 0, err
	}
	if current == 0 {
		current, err = r.replaceRegistration(ctx, tx, binding, token)
		if err != nil {
			return 0, err
		}
	}
	if err := authsession.CheckLockedBearerExpiry(ctx, tx, expiry); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, app.ErrStorage
	}
	return current, nil
}

func lockRegistration(ctx context.Context, tx pgx.Tx, bearer session.BearerSession) (time.Time, error) {
	expiry, err := authsession.LockVerifiedBearer(ctx, tx, bearer)
	if err != nil {
		return time.Time{}, err
	}
	var device string
	err = tx.QueryRow(ctx, `SELECT device_id FROM newim.im_devices
		WHERE user_id=$1 AND device_id=$2 FOR UPDATE`, bearer.UserID(), bearer.DeviceID()).Scan(&device)
	if err != nil {
		return time.Time{}, app.ErrStorage
	}
	return expiry, nil
}

func (r *Repository) currentRegistration(ctx context.Context, tx pgx.Tx, binding app.TokenBinding, raw []byte) (int64, error) {
	var sealed app.SealedToken
	err := tx.QueryRow(ctx, `SELECT registration_version,key_id,token_nonce,token_ciphertext FROM newim.im_push_tokens
		WHERE user_id=$1 AND device_id=$2 AND provider=$3 AND session_id=$4 AND invalidated_at IS NULL`,
		binding.UserID, binding.DeviceID, binding.Channel, binding.SessionID).
		Scan(&binding.Version, &sealed.KeyID, &sealed.Nonce, &sealed.Ciphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, app.ErrStorage
	}
	if sealed.KeyID != r.vault.ActiveKey() {
		return 0, nil
	}
	token, err := r.vault.Open(binding, sealed)
	defer clear(token)
	if err != nil || subtle.ConstantTimeCompare(token, raw) != 1 {
		return 0, nil
	}
	return binding.Version, nil
}

func (r *Repository) replaceRegistration(ctx context.Context, tx pgx.Tx, binding app.TokenBinding, raw []byte) (int64, error) {
	if err := tx.QueryRow(ctx, `SELECT nextval('newim.im_push_registration_version')`).Scan(&binding.Version); err != nil {
		return 0, app.ErrStorage
	}
	sealed, err := r.vault.Seal(binding, raw)
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx, `UPDATE newim.im_push_tokens SET invalidated_at=GREATEST(registered_at,clock_timestamp())
		WHERE user_id=$1 AND device_id=$2 AND provider<>$3 AND registration_version IS NOT NULL AND invalidated_at IS NULL`,
		binding.UserID, binding.DeviceID, binding.Channel)
	if err != nil {
		return 0, app.ErrStorage
	}
	_, err = tx.Exec(ctx, `INSERT INTO newim.im_push_tokens
		(user_id,device_id,provider,session_id,registration_version,key_id,token_nonce,token_ciphertext,registered_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,clock_timestamp())
		ON CONFLICT (user_id,device_id,provider) DO UPDATE SET
		  session_id=EXCLUDED.session_id,registration_version=EXCLUDED.registration_version,
		  key_id=EXCLUDED.key_id,token_nonce=EXCLUDED.token_nonce,token_ciphertext=EXCLUDED.token_ciphertext,
		  registered_at=EXCLUDED.registered_at,invalidated_at=NULL`,
		binding.UserID, binding.DeviceID, binding.Channel, binding.SessionID, binding.Version,
		sealed.KeyID, sealed.Nonce, sealed.Ciphertext)
	if err != nil {
		return 0, app.ErrStorage
	}
	return binding.Version, nil
}

func (r *Repository) Unregister(ctx context.Context, bearer session.BearerSession, channel app.Channel, version int64) error {
	if ctx == nil || !channel.Valid() || version < 1 {
		return app.ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return app.ErrStorage
	}
	defer rollback(tx)
	expiry, err := lockRegistration(ctx, tx, bearer)
	if err != nil {
		return err
	}
	if err = invalidate(ctx, tx, app.TokenBinding{
		UserID: bearer.UserID(), DeviceID: bearer.DeviceID(), SessionID: bearer.SessionID(),
		Channel: channel, Version: version,
	}, nil); err != nil {
		return err
	}
	if err = authsession.CheckLockedBearerExpiry(ctx, tx, expiry); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return app.ErrStorage
	}
	return nil
}

func invalidate(ctx context.Context, tx pgx.Tx, binding app.TokenBinding, invalidAt *time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE newim.im_push_tokens
		SET invalidated_at=GREATEST(registered_at,clock_timestamp())
		WHERE user_id=$1 AND device_id=$2 AND session_id=$3 AND provider=$4
		  AND registration_version=$5 AND invalidated_at IS NULL
		  AND ($6::timestamptz IS NULL OR date_trunc('milliseconds',registered_at)<=$6)`,
		binding.UserID, binding.DeviceID, binding.SessionID, binding.Channel, binding.Version, invalidAt)
	if err != nil {
		return app.ErrStorage
	}
	return nil
}
