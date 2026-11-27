// Package friend stores friend relationships and their change stream in PostgreSQL.
// friend 在 PostgreSQL 中存取好友关系和变更流。
package friend

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/m-ice/NewIM/server/friend"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
	"github.com/m-ice/NewIM/server/storage/postgrespair"
	"github.com/m-ice/NewIM/server/user"
)

const requestTimeout = 5 * time.Second

const applicationFields = "request_id, applicant_id, recipient_id, message, state"

const edgeQuery = `SELECT e.peer_id, COALESCE(p.nickname, ''), COALESCE(p.face_url, ''), COALESCE(p.ex, ''), e.remark
	FROM newim.im_friend_edges AS e
	LEFT JOIN newim.im_user_profiles AS p ON p.user_id=e.peer_id
	WHERE e.owner_id=$1`

// Config holds trusted database settings.
// Config 保存可信数据库配置。
type Config struct {
	DSN              string
	AllowLocalSocket bool
	ApplicationName  string
	Guard            *postgresidentity.Guard
}

// Repository owns a bounded database pool.
// Repository 持有有界数据库连接池。
type Repository struct {
	pool *pgxpool.Pool
}

var _ app.Store = (*Repository)(nil)

// Open verifies transport and checks that the friend schema is ready.
// Open 验证数据库传输配置并检查好友关系表是否就绪。
func Open(ctx context.Context, config Config) (*Repository, error) {
	if ctx == nil || config.DSN == "" {
		return nil, app.StorageUnavailable
	}
	if config.ApplicationName == "" {
		config.ApplicationName = "newim-friend"
	}
	if !user.ValidID(config.ApplicationName) {
		return nil, app.InvalidInput
	}
	poolConfig, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, app.StorageUnavailable
	}
	connConfig := poolConfig.ConnConfig
	if strings.HasPrefix(connConfig.Host, "/") {
		if !config.AllowLocalSocket || len(connConfig.Fallbacks) != 0 {
			return nil, app.StorageUnavailable
		}
	} else {
		tlsConfig := connConfig.TLSConfig
		if tlsConfig == nil || tlsConfig.InsecureSkipVerify || tlsConfig.ServerName != connConfig.Host {
			return nil, app.StorageUnavailable
		}
		for _, fallback := range connConfig.Fallbacks {
			tlsConfig := fallback.TLSConfig
			if fallback.Host == "" || strings.HasPrefix(fallback.Host, "/") ||
				tlsConfig == nil || tlsConfig.InsecureSkipVerify || tlsConfig.ServerName != fallback.Host {
				return nil, app.StorageUnavailable
			}
		}
	}
	poolConfig.MaxConns = 8
	poolConfig.MinConns = 0
	poolConfig.MinIdleConns = 0
	connConfig.ConnectTimeout = 2 * time.Second
	connConfig.DialFunc = (&net.Dialer{Timeout: 2 * time.Second}).DialContext
	connConfig.MaxProtocolMessageBodyLen = 1 << 20
	connConfig.RuntimeParams["application_name"] = config.ApplicationName
	connConfig.RuntimeParams["statement_timeout"] = "3000"
	connConfig.RuntimeParams["lock_timeout"] = "1000"
	connConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	connConfig.RuntimeParams["transaction_timeout"] = "5000"
	if guard := config.Guard; guard != nil {
		poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			if err := guard.AfterConnect(ctx, conn); err != nil {
				return app.StorageUnavailable
			}
			return nil
		}
		poolConfig.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
			if err := guard.Verify(ctx, conn); err != nil {
				return false, app.StorageUnavailable
			}
			return true, nil
		}
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, app.StorageUnavailable
	}
	if _, err = pool.Exec(ctx, `SELECT e.peer_id, a.request_id, s.latest_revision, c.revision
		FROM newim.im_friend_edges AS e
		CROSS JOIN newim.im_friend_applications AS a
		CROSS JOIN newim.im_friend_sync_accounts AS s
		CROSS JOIN newim.im_friend_sync_changes AS c LIMIT 0`); err != nil {
		pool.Close()
		return nil, app.StorageUnavailable
	}
	return &Repository{pool: pool}, nil
}

// Close releases the database pool.
// Close 释放数据库连接池。
func (r *Repository) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

func (r *Repository) begin(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	if r == nil || r.pool == nil {
		return nil, app.StorageUnavailable
	}
	tx, err := r.pool.BeginTx(ctx, options)
	if err != nil {
		return nil, app.StorageUnavailable
	}
	return tx, nil
}

func lockPair(ctx context.Context, tx pgx.Tx, first, second string) error {
	if err := postgrespair.Lock(ctx, tx, first, second); err != nil {
		return app.StorageUnavailable
	}
	var count int
	err := tx.QueryRow(ctx, `SELECT count(*) FROM (
		SELECT user_id FROM newim.im_users WHERE user_id IN ($1, $2) ORDER BY user_id FOR KEY SHARE
	) AS accounts`, first, second).Scan(&count)
	if err != nil {
		return app.StorageUnavailable
	}
	if count != 2 {
		return app.NotFound
	}
	return nil
}

// Acquire all touched account heads before writing any relationship or event.
func lockHeads(ctx context.Context, tx pgx.Tx, owners ...string) error {
	slices.Sort(owners)
	for _, owner := range owners {
		if _, err := tx.Exec(ctx, `INSERT INTO newim.im_friend_sync_accounts(owner_id)
			VALUES ($1) ON CONFLICT (owner_id) DO NOTHING`, owner); err != nil {
			return app.StorageUnavailable
		}
		var locked string
		if err := tx.QueryRow(ctx, `SELECT owner_id FROM newim.im_friend_sync_accounts
			WHERE owner_id=$1 FOR UPDATE`, owner).Scan(&locked); err != nil {
			return app.StorageUnavailable
		}
	}
	return nil
}

func appendChange(ctx context.Context, tx pgx.Tx, owner, peer string, kind app.ChangeKind, payload any) error {
	var body []byte
	if payload != nil {
		var err error
		body, err = json.Marshal(payload)
		if err != nil {
			return app.StorageUnavailable
		}
	}
	var revision int64
	if err := tx.QueryRow(ctx, `UPDATE newim.im_friend_sync_accounts
		SET latest_revision=latest_revision+1 WHERE owner_id=$1 AND latest_revision<9223372036854775807
		RETURNING latest_revision`, owner).Scan(&revision); err != nil {
		return app.StorageUnavailable
	}
	_, err := tx.Exec(ctx, `INSERT INTO newim.im_friend_sync_changes(owner_id, revision, kind, peer_id, payload)
		VALUES ($1,$2,$3,$4,$5)`, owner, revision, kind, peer, body)
	return storageError(err)
}

func scanApplication(row pgx.Row) (app.Application, error) {
	var application app.Application
	err := row.Scan(&application.RequestID, &application.ApplicantID, &application.RecipientID, &application.Message, &application.State)
	return application, err
}

func scanEdge(row pgx.Row) (app.Edge, error) {
	var edge app.Edge
	err := row.Scan(&edge.Profile.UserID, &edge.Profile.Nickname, &edge.Profile.FaceURL, &edge.Profile.Ex, &edge.Remark)
	return edge, err
}

func validPair(first, second string) bool {
	return first != second && user.ValidID(first) && user.ValidID(second)
}

func validText(value string) bool {
	return len(value) <= app.MaxTextBytes && utf8.ValidString(value) && strings.IndexByte(value, 0) < 0
}

func storageError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return app.NotFound
	}
	return app.StorageUnavailable
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
