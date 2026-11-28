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

// CheckGatewaySubscriptionSchema requires the offline-push migration when tracking is enabled.
func (r *Repository) CheckGatewaySubscriptionSchema(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	_, err := r.pool.Exec(ctx, `SELECT connection_id, conversation_id, expires_at
		FROM newim.im_gateway_subscriptions LIMIT 0`)
	return fixed(err)
}

// RenewGatewaySubscription records presence only while the verified token and membership are live.
func (r *Repository) RenewGatewaySubscription(ctx context.Context, identity app.ConnectionIdentity, conversationID string, ttl time.Duration) error {
	if ctx == nil || identity == (app.ConnectionIdentity{}) || !identifier(conversationID) ||
		ttl < time.Second || ttl > time.Minute {
		return app.Fail(app.AuthInvalidInput)
	}
	inserted, err := r.pool.Exec(ctx, `INSERT INTO newim.im_gateway_subscriptions AS subscription
		(connection_id,user_id,device_id,session_id,token_id,conversation_id,refreshed_at,expires_at)
		SELECT $1,live.user_id,live.device_id,live.session_id,token.token_id,member.conversation_id,
		       statement_timestamp(),LEAST(token.expires_at,statement_timestamp()+$7*interval '1 millisecond')
		FROM newim.im_sessions live
		JOIN newim.im_auth_tokens token ON token.session_id=live.session_id AND token.token_id=$2
		JOIN newim.im_conversation_members member ON member.user_id=live.user_id AND member.conversation_id=$3
		WHERE live.user_id=$4 AND live.device_id=$5 AND live.session_id=$6
		  AND live.admitted_at IS NOT NULL AND live.revoked_at IS NULL
		  AND token.revoked_at IS NULL AND token.expires_at>statement_timestamp()
		  AND member.departed_at IS NULL
		ON CONFLICT (connection_id) DO UPDATE SET
		  refreshed_at=EXCLUDED.refreshed_at,expires_at=EXCLUDED.expires_at
		WHERE (subscription.user_id,subscription.device_id,subscription.session_id,subscription.token_id,subscription.conversation_id)
		    = (EXCLUDED.user_id,EXCLUDED.device_id,EXCLUDED.session_id,EXCLUDED.token_id,EXCLUDED.conversation_id)`,
		identity.ConnectionID(), identity.TokenID(), conversationID,
		identity.UserID(), identity.DeviceID(), identity.SessionID(), ttl.Milliseconds(),
	)
	if err != nil {
		return fixed(err)
	}
	if inserted.RowsAffected() != 1 {
		return app.Fail(app.AuthForbidden)
	}
	return nil
}

// ReleaseGatewaySubscription leaves any newer connection for the same device intact.
func (r *Repository) ReleaseGatewaySubscription(ctx context.Context, identity app.ConnectionIdentity) error {
	if ctx == nil || identity == (app.ConnectionIdentity{}) {
		return app.Fail(app.AuthInvalidInput)
	}
	_, err := r.pool.Exec(ctx, `DELETE FROM newim.im_gateway_subscriptions
		WHERE connection_id=$1 AND user_id=$2 AND device_id=$3 AND session_id=$4 AND token_id=$5`,
		identity.ConnectionID(), identity.UserID(), identity.DeviceID(), identity.SessionID(), identity.TokenID())
	return fixed(err)
}
