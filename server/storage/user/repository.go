// Package user stores account and profile data in PostgreSQL.
// user 在 PostgreSQL 中存取账号和公开资料。
package user

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/m-ice/NewIM/server/storage/conversationsync"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
	app "github.com/m-ice/NewIM/server/user"
)

const requestTimeout = 5 * time.Second

const profileQuery = `SELECT u.user_id, COALESCE(p.nickname, ''), COALESCE(p.face_url, ''), COALESCE(p.ex, '')
	FROM newim.im_users AS u
	LEFT JOIN newim.im_user_profiles AS p ON p.user_id=u.user_id`

// Config holds trusted database settings.
// Config 保存可信数据库配置。
type Config struct {
	DSN                  string
	AllowLocalSocket     bool
	ApplicationName      string
	Guard                *postgresidentity.Guard
	ProjectConversations bool
	ProjectReadState     bool
}

// Repository owns a bounded database pool.
// Repository 持有有界数据库连接池。
type Repository struct {
	pool                 *pgxpool.Pool
	projectConversations bool
	projectReadState     bool
}

var _ app.Store = (*Repository)(nil)

// Open verifies transport and checks that the profile schema is ready.
// Open 验证数据库传输配置并检查资料表是否就绪。
func Open(ctx context.Context, config Config) (*Repository, error) {
	if ctx == nil || config.DSN == "" || (config.ProjectReadState && !config.ProjectConversations) {
		return nil, app.Fail(app.StorageUnavailable)
	}
	if config.ApplicationName == "" {
		config.ApplicationName = "newim-user"
	}
	if !app.ValidID(config.ApplicationName) {
		return nil, app.Fail(app.InvalidInput)
	}
	poolConfig, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	connConfig := poolConfig.ConnConfig
	if strings.HasPrefix(connConfig.Host, "/") {
		if !config.AllowLocalSocket || len(connConfig.Fallbacks) != 0 {
			return nil, app.Fail(app.StorageUnavailable)
		}
	} else {
		tlsConfig := connConfig.TLSConfig
		if tlsConfig == nil || tlsConfig.InsecureSkipVerify || tlsConfig.ServerName != connConfig.Host {
			return nil, app.Fail(app.StorageUnavailable)
		}
		for _, fallback := range connConfig.Fallbacks {
			tlsConfig := fallback.TLSConfig
			if fallback.Host == "" || strings.HasPrefix(fallback.Host, "/") ||
				tlsConfig == nil || tlsConfig.InsecureSkipVerify || tlsConfig.ServerName != fallback.Host {
				return nil, app.Fail(app.StorageUnavailable)
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
				return app.Fail(app.StorageUnavailable)
			}
			return nil
		}
		poolConfig.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
			if err := guard.Verify(ctx, conn); err != nil {
				return false, app.Fail(app.StorageUnavailable)
			}
			return true, nil
		}
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	if _, err = pool.Exec(ctx, profileQuery+" LIMIT 0"); err != nil {
		pool.Close()
		return nil, app.Fail(app.StorageUnavailable)
	}
	if config.ProjectConversations && conversationsync.CheckSourceSchema(ctx, pool, config.ProjectReadState) != nil {
		pool.Close()
		return nil, app.Fail(app.StorageUnavailable)
	}
	return &Repository{
		pool: pool, projectConversations: config.ProjectConversations, projectReadState: config.ProjectReadState,
	}, nil
}

// Close releases the database pool.
// Close 释放数据库连接池。
func (r *Repository) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

// Provision creates a user and their initial profile in one transaction.
// Provision 在同一事务创建账号和初始资料，重复调用返回当前资料。
func (r *Repository) Provision(ctx context.Context, userID string, patch app.Patch) (app.Profile, error) {
	if ctx == nil || !app.ValidID(userID) || !patch.Valid() {
		return app.Profile{}, app.Fail(app.InvalidInput)
	}
	if r == nil || r.pool == nil {
		return app.Profile{}, app.Fail(app.StorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return app.Profile{}, app.Fail(app.StorageUnavailable)
	}
	defer rollback(tx)
	created, err := tx.Exec(ctx, "INSERT INTO newim.im_users(user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", userID)
	if err != nil {
		return app.Profile{}, app.Fail(app.StorageUnavailable)
	}
	newUser := created.RowsAffected() == 1
	var profile app.Profile
	if newUser {
		profile, err = scanProfile(tx.QueryRow(ctx, `INSERT INTO newim.im_user_profiles(user_id, nickname, face_url, ex)
			VALUES ($1, COALESCE($2, ''), COALESCE($3, ''), COALESCE($4, ''))
			RETURNING user_id, nickname, face_url, ex`, userID, patch.Nickname, patch.FaceURL, patch.Ex))
	} else {
		// A fresh snapshot sees the account committed during a conflict wait.
		profile, err = scanProfile(tx.QueryRow(ctx, profileQuery+" WHERE u.user_id=$1", userID))
	}
	if err != nil {
		return app.Profile{}, err
	}
	if newUser && r.projectConversations {
		initialize := conversationsync.InitializeSourceAccount
		if r.projectReadState {
			initialize = conversationsync.InitializeReadSourceAccount
		}
		if err = initialize(ctx, tx, userID); err != nil {
			return app.Profile{}, app.Fail(app.StorageUnavailable)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return app.Profile{}, app.Fail(app.StorageUnavailable)
	}
	return profile, nil
}

// Get reads the public profile of an existing account.
// Get 读取已有账号的公开资料。
func (r *Repository) Get(ctx context.Context, userID string) (app.Profile, error) {
	if ctx == nil || !app.ValidID(userID) {
		return app.Profile{}, app.Fail(app.InvalidInput)
	}
	if r == nil || r.pool == nil {
		return app.Profile{}, app.Fail(app.StorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return scanProfile(r.pool.QueryRow(ctx, profileQuery+" WHERE u.user_id=$1", userID))
}

// Update applies a partial profile change to an existing account.
// Update 为已有账号更新提供的资料字段。
func (r *Repository) Update(ctx context.Context, userID string, patch app.Patch) (app.Profile, error) {
	if ctx == nil || !app.ValidID(userID) || patch.Empty() || !patch.Valid() {
		return app.Profile{}, app.Fail(app.InvalidInput)
	}
	if r == nil || r.pool == nil {
		return app.Profile{}, app.Fail(app.StorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return scanProfile(r.pool.QueryRow(ctx, `INSERT INTO newim.im_user_profiles AS p (user_id, nickname, face_url, ex)
		SELECT user_id, COALESCE($2, ''), COALESCE($3, ''), COALESCE($4, '')
		FROM newim.im_users WHERE user_id=$1
		ON CONFLICT (user_id) DO UPDATE SET
			nickname=COALESCE($2, p.nickname), face_url=COALESCE($3, p.face_url), ex=COALESCE($4, p.ex)
		RETURNING user_id, nickname, face_url, ex`, userID, patch.Nickname, patch.FaceURL, patch.Ex))
}

// Lookup returns one profile per existing user in first-requested order.
// Lookup 按首次请求顺序返回已存在账号的资料。
func (r *Repository) Lookup(ctx context.Context, userIDs []string) ([]app.Profile, error) {
	if ctx == nil || len(userIDs) == 0 || len(userIDs) > app.MaxLookup {
		return nil, app.Fail(app.InvalidInput)
	}
	for _, userID := range userIDs {
		if !app.ValidID(userID) {
			return nil, app.Fail(app.InvalidInput)
		}
	}
	if r == nil || r.pool == nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	rows, err := r.pool.Query(ctx, profileQuery+`
		WHERE u.user_id=ANY($1::text[]) ORDER BY array_position($1::text[], u.user_id)`, userIDs)
	if err != nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	defer rows.Close()
	profiles := make([]app.Profile, 0, len(userIDs))
	for rows.Next() {
		profile, err := scanProfile(rows)
		if err != nil {
			return nil, err
		}
		profiles = append(profiles, profile)
	}
	if err = rows.Err(); err != nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	return profiles, nil
}

func scanProfile(row pgx.Row) (app.Profile, error) {
	var profile app.Profile
	err := row.Scan(&profile.UserID, &profile.Nickname, &profile.FaceURL, &profile.Ex)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Profile{}, app.Fail(app.NotFound)
	}
	if err != nil {
		return app.Profile{}, app.Fail(app.StorageUnavailable)
	}
	return profile, nil
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
