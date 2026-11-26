package authsession

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/auth/session"
)

// MarkGatewayActivity records a verified admitted device's most recent activity.
// MarkGatewayActivity 更新已核验且仍准入设备的最近活动时间。
func (r *Repository) MarkGatewayActivity(ctx context.Context, identity app.ConnectionIdentity) error {
	if ctx == nil || identity == (app.ConnectionIdentity{}) {
		return app.Fail(app.AuthInvalidInput)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)
	// Admission chooses eviction candidates under the same account lock.
	if err = lockAccount(ctx, tx, identity.UserID()); err != nil {
		return err
	}
	binding, expiry, err := gatewayTokenBinding(ctx, tx, identity.TokenID())
	if err != nil {
		return err
	}
	if binding.UserID != identity.UserID() || binding.DeviceID != identity.DeviceID() || binding.SessionID != identity.SessionID() {
		return app.Fail(app.AuthForbidden)
	}
	if err = verifyAdmittedBinding(ctx, tx, binding); err != nil {
		return err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return fixed(err)
	}
	if !expiry.After(now) {
		return app.Fail(app.AuthTokenExpired)
	}
	if err = refreshDeviceActivity(ctx, tx, binding.UserID, binding.DeviceID, now); err != nil {
		return err
	}
	return fixed(tx.Commit(ctx))
}
