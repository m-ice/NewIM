package push

import (
	"context"
	"crypto/rand"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/push"
)

func (r *Repository) Enable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	_, err := r.pool.Exec(ctx, `INSERT INTO newim.im_push_state(singleton,enabled_from)
		VALUES (true,clock_timestamp()) ON CONFLICT DO NOTHING`)
	return storageError(err)
}

// Both fan-out and attempts use the current authorization and notification preference.
const audience = `
	JOIN newim.im_conversations conversation ON conversation.conversation_id=msg.conversation_id
	JOIN newim.im_conversation_members member ON member.conversation_id=msg.conversation_id
	LEFT JOIN newim.im_groups room ON room.group_id=conversation.conversation_id
	LEFT JOIN newim.im_conversation_account_settings settings
	  ON settings.conversation_id=member.conversation_id AND settings.user_id=member.user_id
	LEFT JOIN newim.im_read_states read_state
	  ON read_state.conversation_id=member.conversation_id AND read_state.user_id=member.user_id
	JOIN newim.im_push_tokens token ON token.user_id=member.user_id
	JOIN newim.im_sessions live ON live.session_id=token.session_id
		 AND live.user_id=token.user_id AND live.device_id=token.device_id
`

const canNotify = `
	member.user_id<>msg.sender_id AND member.departed_at IS NULL
	AND (conversation.kind='direct' OR (conversation.kind='group' AND room.group_id IS NOT NULL AND room.dissolved_at IS NULL))
	AND token.registration_version IS NOT NULL AND token.invalidated_at IS NULL
	AND live.admitted_at IS NOT NULL AND live.revoked_at IS NULL
	AND NOT COALESCE(settings.muted,false) AND settings.deleted_through_seq IS NULL
	AND msg.conversation_seq>GREATEST(member.history_after_seq,COALESCE(read_state.read_seq,0),COALESCE(settings.clear_through_seq,0))
	AND NOT EXISTS (
		SELECT 1 FROM newim.im_message_visibility_events visibility
		WHERE visibility.conversation_id=msg.conversation_id AND visibility.target_seq=msg.conversation_seq
		  AND (visibility.audience_user_id IS NULL OR visibility.audience_user_id=member.user_id)
	)
	AND NOT EXISTS (
		SELECT 1 FROM newim.im_gateway_subscriptions online
		JOIN newim.im_auth_tokens bearer ON bearer.token_id=online.token_id AND bearer.session_id=online.session_id
		WHERE online.conversation_id=msg.conversation_id AND online.user_id=token.user_id
		  AND online.device_id=token.device_id AND online.session_id=token.session_id
		  AND online.expires_at>statement_timestamp() AND bearer.revoked_at IS NULL
		  AND bearer.expires_at>statement_timestamp()
	)
`

func (r *Repository) Fanout(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	done, err := r.pool.Exec(ctx, `WITH pending AS MATERIALIZED (
		SELECT o.event_id,o.server_msg_id FROM newim.im_outbox_events o
		JOIN newim.im_push_state state ON state.singleton
		WHERE o.push_fanout_at IS NULL AND o.created_at>=state.enabled_from
		ORDER BY o.created_at,o.event_id LIMIT 1 FOR UPDATE OF o SKIP LOCKED
	), inserted AS (
		INSERT INTO newim.im_push_deliveries(event_id,user_id,device_id,session_id,provider,registration_version)
		SELECT pending.event_id,token.user_id,token.device_id,token.session_id,token.provider,token.registration_version
		FROM pending JOIN newim.im_messages msg ON msg.server_msg_id=pending.server_msg_id`+audience+`
		WHERE `+canNotify+`
		ON CONFLICT (event_id,user_id,device_id) DO NOTHING
	)
	UPDATE newim.im_outbox_events o SET push_fanout_at=clock_timestamp()
	FROM pending WHERE o.event_id=pending.event_id`)
	return done.RowsAffected(), storageError(err)
}

func (r *Repository) Claim(ctx context.Context) (app.Delivery, app.ClaimStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	var attempt app.Delivery
	var leaseID *string
	// Expired leases with exhausted attempt counts are closed by the same statement.
	err := r.pool.QueryRow(ctx, `WITH expired AS MATERIALIZED (
		SELECT event_id,user_id,device_id FROM newim.im_push_deliveries
		WHERE status='leased' AND lease_until<=statement_timestamp()
		ORDER BY lease_until,event_id,user_id,device_id LIMIT 1 FOR UPDATE SKIP LOCKED
	), queued AS MATERIALIZED (
		SELECT event_id,user_id,device_id FROM newim.im_push_deliveries
		WHERE status='pending' AND available_at<=statement_timestamp() AND NOT EXISTS (SELECT 1 FROM expired)
		ORDER BY available_at,event_id,user_id,device_id LIMIT 1 FOR UPDATE SKIP LOCKED
	), next_job AS (
		SELECT * FROM expired UNION ALL SELECT * FROM queued
	), claimed AS (
		UPDATE newim.im_push_deliveries d SET
		  status=CASE WHEN attempts<$1 THEN 'leased' ELSE 'failed' END,
		  lease_id=CASE WHEN attempts<$1 THEN $2::newim.identifier ELSE NULL END,
		  lease_until=CASE WHEN attempts<$1 THEN statement_timestamp()+interval '30 seconds' ELSE NULL END,
		  attempts=LEAST(attempts+1,$1)
		FROM next_job job WHERE (d.event_id,d.user_id,d.device_id)=(job.event_id,job.user_id,job.device_id)
		RETURNING d.event_id,d.user_id,d.device_id,d.session_id,d.provider,d.registration_version,d.lease_id,d.attempts
	)
	SELECT * FROM claimed`, app.MaxAttempts, rand.Text()).
		Scan(&attempt.EventID, &attempt.UserID, &attempt.DeviceID, &attempt.SessionID,
			&attempt.Channel, &attempt.Version, &leaseID, &attempt.Attempts)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return app.Delivery{}, app.NoDelivery, nil
	case err != nil:
		return app.Delivery{}, app.NoDelivery, app.ErrStorage
	case leaseID == nil:
		return app.Delivery{}, app.LeaseExhausted, nil
	default:
		attempt.LeaseID = *leaseID
		return attempt, app.LeaseAcquired, nil
	}
}

func (r *Repository) Prepare(ctx context.Context, job app.Delivery) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	var sealed app.SealedToken
	err := r.pool.QueryRow(ctx, `SELECT token.key_id,token.token_nonce,token.token_ciphertext
		FROM newim.im_push_deliveries job
		JOIN newim.im_outbox_events o ON o.event_id=job.event_id
		JOIN newim.im_messages msg ON msg.server_msg_id=o.server_msg_id`+audience+`
		WHERE (job.event_id,job.user_id,job.device_id,job.lease_id)=($1,$2,$3,$4)
		  AND job.status='leased' AND job.lease_until>clock_timestamp()
		  AND (token.user_id,token.device_id,token.session_id,token.provider,token.registration_version)
		     =(job.user_id,job.device_id,job.session_id,job.provider,job.registration_version)
		  AND `+canNotify, job.EventID, job.UserID, job.DeviceID, job.LeaseID).
		Scan(&sealed.KeyID, &sealed.Nonce, &sealed.Ciphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, app.ErrStorage
	}
	raw, err := r.vault.Open(job.TokenBinding, sealed)
	return raw, err == nil, err
}

func (r *Repository) Finish(ctx context.Context, job app.Delivery, outcome app.Outcome) (app.Result, error) {
	result := outcome.Result
	if result == app.TemporaryFailure && job.Attempts >= app.MaxAttempts {
		result = app.PermanentFailure
	}
	status := "failed"
	switch result {
	case app.Accepted:
		status = "accepted"
	case app.Cancelled, app.InvalidToken:
		status = "cancelled"
	case app.TemporaryFailure:
		status = "pending"
	case app.PermanentFailure:
	default:
		return 0, app.ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, app.ErrStorage
	}
	defer rollback(tx)
	wait := min(max(outcome.RetryAfter, 0), 24*time.Hour)
	changed, err := tx.Exec(ctx, `UPDATE newim.im_push_deliveries
		SET status=$5,lease_id=NULL,lease_until=NULL,
		  available_at=clock_timestamp()+$6*interval '1 millisecond'
		WHERE (event_id,user_id,device_id,lease_id)=($1,$2,$3,$4)
		  AND status='leased' AND lease_until>clock_timestamp()`,
		job.EventID, job.UserID, job.DeviceID, job.LeaseID, status, wait.Milliseconds())
	if err != nil {
		return 0, app.ErrStorage
	}
	if changed.RowsAffected() == 0 {
		return 0, nil
	}
	if result == app.InvalidToken {
		var invalidAt *time.Time
		if !outcome.InvalidAt.IsZero() {
			invalidAt = &outcome.InvalidAt
		}
		if err := invalidate(ctx, tx, job.TokenBinding, invalidAt); err != nil {
			return 0, err
		}
	}
	if err := storageError(tx.Commit(ctx)); err != nil {
		return 0, err
	}
	return result, nil
}

func (r *Repository) PrunePresence(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	_, err := r.pool.Exec(ctx, `WITH stale AS (
		SELECT connection_id FROM newim.im_gateway_subscriptions
		WHERE expires_at<=statement_timestamp()
		ORDER BY expires_at,connection_id LIMIT 100 FOR UPDATE SKIP LOCKED
	)
	DELETE FROM newim.im_gateway_subscriptions live USING stale WHERE live.connection_id=stale.connection_id`)
	return storageError(err)
}

func storageError(err error) error {
	if err != nil {
		return app.ErrStorage
	}
	return nil
}
