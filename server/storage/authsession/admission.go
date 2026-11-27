package authsession

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/m-ice/NewIM/server/auth/session"
)

func verifyAdmissionReadiness(ctx context.Context, pool *pgxpool.Pool) error {
	var missing, eventsReady bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM newim.im_sessions AS s
		JOIN newim.im_devices AS d ON d.user_id=s.user_id AND d.device_id=s.device_id
		WHERE s.revoked_at IS NULL AND (
			(s.admitted_at IS NOT NULL AND (d.platform IS NULL OR d.last_active_at IS NULL))
			OR (s.admitted_at IS NULL AND EXISTS (
				SELECT 1 FROM newim.im_auth_tokens AS t
				WHERE t.session_id=s.session_id AND t.revoked_at IS NULL AND t.expires_at>statement_timestamp()
			))
		)
	), to_regclass('newim.im_session_revocation_events') IS NOT NULL`).Scan(&missing, &eventsReady)
	if err != nil || missing || !eventsReady {
		return app.Fail(app.AuthStorageUnavailable)
	}
	return nil
}

func lockAccount(ctx context.Context, tx pgx.Tx, userID string) error {
	var locked string
	err := tx.QueryRow(ctx, `SELECT user_id FROM newim.im_users WHERE user_id=$1 FOR UPDATE`, userID).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.AuthForbidden)
	}
	return fixed(err)
}

func verifyAdmittedBinding(ctx context.Context, tx pgx.Tx, binding app.SessionBinding) error {
	var admitted, classified bool
	err := tx.QueryRow(ctx, `SELECT s.admitted_at IS NOT NULL,
		d.platform IS NOT NULL AND d.last_active_at IS NOT NULL
		FROM newim.im_sessions AS s
		JOIN newim.im_devices AS d ON d.user_id=s.user_id AND d.device_id=s.device_id
		WHERE s.session_id=$1`, binding.SessionID).Scan(&admitted, &classified)
	if err != nil {
		return app.Fail(app.AuthStorageUnavailable)
	}
	if !admitted {
		return app.Fail(app.AuthForbidden)
	}
	if !classified {
		return app.Fail(app.AuthStorageUnavailable)
	}
	return nil
}

func refreshDeviceActivity(ctx context.Context, tx pgx.Tx, userID, deviceID string, now time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE newim.im_devices SET last_active_at=GREATEST(last_active_at,$3)
		WHERE user_id=$1 AND device_id=$2`, userID, deviceID, now)
	return fixed(err)
}

// Admit commits the device, session, token and any evictions in one account transaction.
func (r *Repository) Admit(ctx context.Context, request app.AdmissionRequest, generate func() (app.IssuedToken, error)) (app.AdmissionResult, error) {
	if ctx == nil || !request.Valid() || generate == nil {
		return app.AdmissionResult{}, app.Fail(app.AuthInvalidInput)
	}
	if r == nil || !r.admissionRequired {
		return app.AdmissionResult{}, app.Fail(app.AuthStorageUnavailable)
	}
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return app.AdmissionResult{}, err
	}
	defer rollback(tx)
	if err = lockAccount(ctx, tx, request.UserID); err != nil {
		return app.AdmissionResult{}, err
	}
	if err = checkDevicePlatform(ctx, tx, request); err != nil {
		return app.AdmissionResult{}, err
	}
	devices, err := activeDevices(ctx, tx, request.UserID)
	if err != nil {
		return app.AdmissionResult{}, err
	}
	victims, err := chooseVictims(devices, request)
	if err != nil {
		return app.AdmissionResult{}, err
	}
	current, sessions, err := lockAdmissionSessions(ctx, tx, request, victims)
	if err != nil {
		return app.AdmissionResult{}, err
	}
	issued, err := generate()
	if err != nil {
		return app.AdmissionResult{}, fixed(err)
	}
	now := issued.IssuedAt()
	if _, err = revokeLockedSessions(ctx, tx, sessions, now, app.RevocationReasonDeviceLimit); err != nil {
		return app.AdmissionResult{}, err
	}
	current, err = admitDeviceSession(ctx, tx, request, current, now)
	if err != nil {
		return app.AdmissionResult{}, err
	}
	if err = persistIssuedToken(ctx, tx, current, issued); err != nil {
		return app.AdmissionResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return app.AdmissionResult{}, app.Fail(app.AuthStorageUnavailable)
	}
	return app.AdmissionResult{
		Binding: app.SessionBinding{UserID: request.UserID, DeviceID: request.DeviceID, SessionID: current},
		Token:   issued,
	}, nil
}

func checkDevicePlatform(ctx context.Context, tx pgx.Tx, request app.AdmissionRequest) error {
	var platform *string
	err := tx.QueryRow(ctx, `SELECT platform FROM newim.im_devices
		WHERE user_id=$1 AND device_id=$2`, request.UserID, request.DeviceID).Scan(&platform)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fixed(err)
	}
	if platform != nil && *platform != request.Platform {
		return app.Fail(app.AuthForbidden)
	}
	return nil
}

type activeDevice struct {
	id, platform string
}

func activeDevices(ctx context.Context, tx pgx.Tx, userID string) ([]activeDevice, error) {
	rows, err := tx.Query(ctx, `SELECT d.device_id, d.platform, d.last_active_at
		FROM newim.im_devices AS d
		WHERE d.user_id=$1 AND EXISTS (
			SELECT 1 FROM newim.im_sessions AS s
			WHERE s.user_id=d.user_id AND s.device_id=d.device_id
				AND s.revoked_at IS NULL AND s.admitted_at IS NOT NULL
		)
		ORDER BY d.last_active_at, d.device_id`, userID)
	if err != nil {
		return nil, fixed(err)
	}
	defer rows.Close()
	var devices []activeDevice
	for rows.Next() {
		var device activeDevice
		var activity time.Time
		if err = rows.Scan(&device.id, &device.platform, &activity); err != nil {
			return nil, fixed(err)
		}
		devices = append(devices, device)
	}
	return devices, fixed(rows.Err())
}

func chooseVictims(devices []activeDevice, request app.AdmissionRequest) ([]string, error) {
	accountCount := 1
	platformCounts := map[string]int{request.Platform: 1}
	for _, device := range devices {
		if device.id == request.DeviceID {
			continue
		}
		accountCount++
		platformCounts[device.platform]++
	}
	var victims []string
	evicted := make(map[string]struct{})
	for _, device := range devices {
		if device.id == request.DeviceID || platformCounts[device.platform] <= request.Limits.Platform {
			continue
		}
		victims = append(victims, device.id)
		evicted[device.id] = struct{}{}
		platformCounts[device.platform]--
		accountCount--
	}
	for _, device := range devices {
		if accountCount <= request.Limits.Account {
			break
		}
		if _, selected := evicted[device.id]; device.id == request.DeviceID || selected {
			continue
		}
		victims = append(victims, device.id)
		accountCount--
		platformCounts[device.platform]--
	}
	if accountCount > request.Limits.Account {
		return nil, app.Fail(app.AuthDeviceLimitExceeded)
	}
	for _, count := range platformCounts {
		if count > request.Limits.Platform {
			return nil, app.Fail(app.AuthDeviceLimitExceeded)
		}
	}
	return victims, nil
}

func lockAdmissionSessions(ctx context.Context, tx pgx.Tx, request app.AdmissionRequest, victims []string) (string, []string, error) {
	selected := append([]string{request.DeviceID}, victims...)
	rows, err := tx.Query(ctx, `SELECT session_id, device_id FROM newim.im_sessions
		WHERE user_id=$1 AND device_id=ANY($2::text[]) AND revoked_at IS NULL AND admitted_at IS NOT NULL
		ORDER BY session_id FOR UPDATE`, request.UserID, selected)
	if err != nil {
		return "", nil, fixed(err)
	}
	defer rows.Close()
	var current string
	var victimsSessions []string
	for rows.Next() {
		var sessionID, deviceID string
		if err = rows.Scan(&sessionID, &deviceID); err != nil {
			return "", nil, fixed(err)
		}
		if deviceID == request.DeviceID {
			if current == "" {
				current = sessionID
			}
			continue
		}
		victimsSessions = append(victimsSessions, sessionID)
	}
	return current, victimsSessions, fixed(rows.Err())
}

// revokeLockedSessions records events for the caller's already locked sessions.
func revokeLockedSessions(ctx context.Context, tx pgx.Tx, sessions []string, now time.Time, reason string) (int64, error) {
	if len(sessions) == 0 {
		return 0, nil
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM newim.im_auth_tokens
		WHERE session_id=ANY($1::text[]) AND revoked_at IS NULL
		ORDER BY session_id, token_id FOR UPDATE`, sessions); err != nil {
		return 0, fixed(err)
	}
	// Preserve the original timestamp for existing device-limit evictions.
	clampSessionTime := reason != app.RevocationReasonDeviceLimit
	tag, err := tx.Exec(ctx, `WITH revoked AS (
		UPDATE newim.im_sessions
		SET revoked_at=CASE WHEN $3 THEN GREATEST(created_at,$2) ELSE $2 END
		WHERE session_id=ANY($1::text[]) AND revoked_at IS NULL
		RETURNING user_id, device_id, session_id, revoked_at
	)
	INSERT INTO newim.im_session_revocation_events (user_id, device_id, session_id, revoked_at, reason)
	SELECT user_id, device_id, session_id, revoked_at, $4 FROM revoked ORDER BY session_id`,
		sessions, now, clampSessionTime, reason)
	if err != nil {
		return 0, fixed(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE newim.im_auth_tokens SET revoked_at=GREATEST(created_at,$2)
		WHERE session_id=ANY($1::text[]) AND revoked_at IS NULL`, sessions, now); err != nil {
		return 0, fixed(err)
	}
	return tag.RowsAffected(), nil
}

func admitDeviceSession(ctx context.Context, tx pgx.Tx, request app.AdmissionRequest, current string, now time.Time) (string, error) {
	if _, err := tx.Exec(ctx, `INSERT INTO newim.im_devices (user_id, device_id, platform, last_active_at)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (user_id, device_id) DO UPDATE
		SET platform=COALESCE(newim.im_devices.platform,EXCLUDED.platform),
			last_active_at=GREATEST(newim.im_devices.last_active_at,EXCLUDED.last_active_at)`,
		request.UserID, request.DeviceID, request.Platform, now); err != nil {
		return "", fixed(err)
	}
	if current != "" {
		return current, nil
	}
	err := tx.QueryRow(ctx, `INSERT INTO newim.im_sessions (session_id, user_id, device_id, admitted_at)
		VALUES (pg_catalog.gen_random_uuid()::text,$1,$2,$3) RETURNING session_id`,
		request.UserID, request.DeviceID, now).Scan(&current)
	return current, fixed(err)
}
