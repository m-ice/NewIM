// Package read stores account read watermarks and resolves receipts in PostgreSQL.
package read

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m-ice/NewIM/server/auth/session"
	app "github.com/m-ice/NewIM/server/read"
	"github.com/m-ice/NewIM/server/storage/authsession"
	"github.com/m-ice/NewIM/server/storage/membership"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
	"github.com/m-ice/NewIM/server/user"
)

const requestTimeout = 5 * time.Second

type Config struct {
	DSN              string
	AllowLocalSocket bool
	ApplicationName  string
	Guard            *postgresidentity.Guard
}

type Repository struct {
	pool *pgxpool.Pool
}

var _ app.Store = (*Repository)(nil)

// Open verifies the database transport and the watermark schema.
func Open(ctx context.Context, config Config) (*Repository, error) {
	if ctx == nil || config.DSN == "" {
		return nil, app.Fail(app.StorageUnavailable)
	}
	if config.ApplicationName == "" {
		config.ApplicationName = "newim-read"
	}
	if !user.ValidID(config.ApplicationName) {
		return nil, app.Fail(app.StorageUnavailable)
	}
	settings, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	if !validTransport(settings, config.AllowLocalSocket) {
		return nil, app.Fail(app.StorageUnavailable)
	}
	settings.MaxConns = 8
	settings.MinConns = 0
	settings.MinIdleConns = 0
	conn := settings.ConnConfig
	conn.ConnectTimeout = 2 * time.Second
	conn.DialFunc = (&net.Dialer{Timeout: conn.ConnectTimeout}).DialContext
	conn.MaxProtocolMessageBodyLen = 1 << 20
	conn.RuntimeParams["application_name"] = config.ApplicationName
	conn.RuntimeParams["statement_timeout"] = "3000"
	conn.RuntimeParams["lock_timeout"] = "1000"
	conn.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	conn.RuntimeParams["transaction_timeout"] = "5000"
	if guard := config.Guard; guard != nil {
		settings.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			if guard.AfterConnect(ctx, conn) != nil {
				return app.Fail(app.StorageUnavailable)
			}
			return nil
		}
		settings.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
			if guard.Verify(ctx, conn) != nil {
				return false, app.Fail(app.StorageUnavailable)
			}
			return true, nil
		}
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, settings)
	if err != nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	r := &Repository{pool: pool}
	if err = r.checkSchema(ctx); err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

func validTransport(settings *pgxpool.Config, allowLocalSocket bool) bool {
	conn := settings.ConnConfig
	if strings.HasPrefix(conn.Host, "/") {
		return allowLocalSocket && len(conn.Fallbacks) == 0
	}
	if conn.Host == "" || conn.TLSConfig == nil || conn.TLSConfig.InsecureSkipVerify || conn.TLSConfig.ServerName != conn.Host {
		return false
	}
	for _, fallback := range conn.Fallbacks {
		tls := fallback.TLSConfig
		if fallback.Host == "" || strings.HasPrefix(fallback.Host, "/") ||
			tls == nil || tls.InsecureSkipVerify || tls.ServerName != fallback.Host {
			return false
		}
	}
	return true
}

func (r *Repository) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

func (r *Repository) checkSchema(ctx context.Context) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return app.Fail(app.StorageUnavailable)
	}
	defer rollback(tx)
	advanced, err := membership.Advanced(ctx, tx)
	if err != nil || !advanced {
		return app.Fail(app.StorageUnavailable)
	}
	var ready bool
	err = tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM pg_catalog.pg_attribute
		WHERE attrelid='newim.im_read_states'::regclass AND attname='updated_at'
		  AND atttypid='pg_catalog.timestamptz'::regtype AND NOT attnotnull AND NOT attisdropped
	)`).Scan(&ready)
	if err != nil || !ready {
		return app.Fail(app.StorageUnavailable)
	}
	if _, err = tx.Exec(ctx, "SELECT conversation_id,user_id,read_seq,updated_at FROM newim.im_read_states LIMIT 0"); err != nil {
		return app.Fail(app.StorageUnavailable)
	}
	if err = tx.Commit(ctx); err != nil {
		return app.Fail(app.StorageUnavailable)
	}
	return nil
}

func (r *Repository) Get(ctx context.Context, bearer session.BearerSession, id string) (app.State, error) {
	return r.watermark(ctx, bearer, id, nil)
}

func (r *Repository) Advance(ctx context.Context, bearer session.BearerSession, id string, seq int64) (app.State, error) {
	if seq < 0 {
		return app.State{}, app.Fail(app.InvalidInput)
	}
	return r.watermark(ctx, bearer, id, &seq)
}

func (r *Repository) watermark(ctx context.Context, bearer session.BearerSession, id string, seq *int64) (app.State, error) {
	if !app.ValidConversationID(id) {
		return app.State{}, app.Fail(app.InvalidInput)
	}
	return withVerifiedBearer(ctx, r, bearer, func(ctx context.Context, tx pgx.Tx) (app.State, error) {
		return applyWatermark(ctx, tx, id, bearer.UserID(), seq)
	})
}

func withVerifiedBearer[T any](ctx context.Context, r *Repository, bearer session.BearerSession, operation func(context.Context, pgx.Tx) (T, error)) (T, error) {
	var zero T
	if ctx == nil || r == nil || r.pool == nil {
		return zero, app.Fail(app.StorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return zero, app.Fail(app.StorageUnavailable)
	}
	defer rollback(tx)
	expiry, err := authsession.LockVerifiedBearer(ctx, tx, bearer)
	if err != nil {
		return zero, authError(err)
	}

	result, operationErr := operation(ctx, tx)
	// Only a live bearer may receive a protected operation result.
	if expiryErr := authsession.CheckLockedBearerExpiry(ctx, tx, expiry); expiryErr != nil {
		return zero, authError(expiryErr)
	}
	if operationErr != nil {
		return zero, operationErr
	}
	if err = tx.Commit(ctx); err != nil {
		return zero, app.Fail(app.StorageUnavailable)
	}
	return result, nil
}

func applyWatermark(ctx context.Context, tx pgx.Tx, id, userID string, seq *int64) (app.State, error) {
	state, err := lockVisibleConversation(ctx, tx, id, userID)
	if err != nil {
		return app.State{}, err
	}
	state, err = loadWatermark(ctx, tx, state, userID)
	if err != nil {
		return app.State{}, err
	}
	if seq != nil {
		if *seq > state.LatestSeq {
			return app.State{}, app.Fail(app.InvalidInput)
		}
		if *seq > state.LastReadSeq {
			return advanceWatermark(ctx, tx, state, userID, *seq)
		}
	}
	return state, nil
}

func lockVisibleConversation(ctx context.Context, tx pgx.Tx, id, userID string) (app.State, error) {
	state := app.State{ConversationID: id}
	err := tx.QueryRow(ctx, `SELECT c.last_seq FROM newim.im_conversations AS c
		WHERE c.conversation_id=$1 AND EXISTS (
			SELECT 1 FROM newim.im_conversation_members AS m
			WHERE m.conversation_id=c.conversation_id AND m.user_id=$2 AND m.departed_at IS NULL
		) FOR SHARE OF c`, id, userID).Scan(&state.LatestSeq)
	if err != nil {
		return app.State{}, visibilityError(err)
	}
	// Read the current member row in a fresh statement after any conversation lock wait.
	err = tx.QueryRow(ctx, `SELECT history_after_seq FROM newim.im_conversation_members
		WHERE conversation_id=$1 AND user_id=$2 AND departed_at IS NULL FOR SHARE`, id, userID).
		Scan(&state.HistoryAfterSeq)
	if err != nil {
		return app.State{}, visibilityError(err)
	}
	if state.HistoryAfterSeq < 0 || state.HistoryAfterSeq > state.LatestSeq {
		return app.State{}, app.Fail(app.StorageUnavailable)
	}
	return state, nil
}

func loadWatermark(ctx context.Context, tx pgx.Tx, state app.State, userID string) (app.State, error) {
	var stored int64
	var updatedAt *time.Time
	err := tx.QueryRow(ctx, `SELECT read_seq,updated_at FROM newim.im_read_states
		WHERE conversation_id=$1 AND user_id=$2`, state.ConversationID, userID).Scan(&stored, &updatedAt)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return app.State{}, app.Fail(app.StorageUnavailable)
	}
	if stored < 0 || stored > state.LatestSeq {
		return app.State{}, app.Fail(app.StorageUnavailable)
	}
	state.LastReadSeq = max(stored, state.HistoryAfterSeq)
	state.UpdatedAt = nil
	if updatedAt != nil && stored >= state.HistoryAfterSeq {
		at := updatedAt.UTC()
		state.UpdatedAt = &at
	}
	return state, nil
}

func advanceWatermark(ctx context.Context, tx pgx.Tx, state app.State, userID string, seq int64) (app.State, error) {
	tag, err := tx.Exec(ctx, `INSERT INTO newim.im_read_states AS current (conversation_id,user_id,read_seq)
		VALUES ($1,$2,$3) ON CONFLICT (conversation_id,user_id) DO UPDATE SET read_seq=EXCLUDED.read_seq
		WHERE current.read_seq < EXCLUDED.read_seq`, state.ConversationID, userID, seq)
	if err != nil {
		return app.State{}, app.Fail(app.StorageUnavailable)
	}
	if tag.RowsAffected() == 0 {
		return loadWatermark(ctx, tx, state, userID)
	}

	// The UPSERT now owns the row. Sample the time after any competing writer's wait.
	var updatedAt time.Time
	err = tx.QueryRow(ctx, `UPDATE newim.im_read_states SET updated_at=clock_timestamp()
		WHERE conversation_id=$1 AND user_id=$2 RETURNING updated_at`, state.ConversationID, userID).Scan(&updatedAt)
	if err != nil {
		return app.State{}, app.Fail(app.StorageUnavailable)
	}
	at := updatedAt.UTC()
	state.LastReadSeq = seq
	state.UpdatedAt = &at
	return state, nil
}

func visibilityError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.NotFound)
	}
	return app.Fail(app.StorageUnavailable)
}

func authError(err error) error {
	switch session.ErrorCode(err) {
	case session.AuthInvalidInput, session.AuthTokenMalformed, session.AuthTokenUnknown, session.AuthTokenExpired,
		session.AuthTokenRevoked, session.AuthSessionRevoked, session.AuthForbidden:
		return app.Fail(app.AuthInvalidToken)
	default:
		return app.Fail(app.AuthUnavailable)
	}
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
