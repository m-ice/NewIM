// Package authsession stores opaque access tokens in PostgreSQL.
// authsession 在 PostgreSQL 中存储不透明访问令牌，不实现登录策略或网络鉴权。
package authsession

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/m-ice/NewIM/server/auth/session"
)

const requestTimeout = 5 * time.Second

// Config is trusted database configuration, never request input.
// Config 仅接受可信数据库配置，不接受请求内容。
type Config struct {
	DSN              string
	AllowLocalSocket bool
	MaxConnections   int
	ApplicationName  string
}

// Repository owns a bounded pgx pool; DSNs and driver errors stay private.
// Repository 持有有界 pgx 连接池，不暴露 DSN 或驱动错误。
type Repository struct{ pool *pgxpool.Pool }

// Open requires verified TLS for TCP or explicitly enabled local sockets.
// Open 的 TCP 必须校验主机证书，本地套接字须显式启用。
func Open(ctx context.Context, config Config) (*Repository, error) {
	if ctx == nil || config.DSN == "" || config.MaxConnections < 0 || config.MaxConnections > 8 {
		return nil, app.Fail(app.AuthStorageUnavailable)
	}
	if config.ApplicationName != "" && !identifier(config.ApplicationName) {
		return nil, app.Fail(app.AuthInvalidInput)
	}
	poolConfig, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, app.Fail(app.AuthStorageUnavailable)
	}
	local := len(poolConfig.ConnConfig.Host) > 0 && poolConfig.ConnConfig.Host[0] == '/'
	if local {
		if !config.AllowLocalSocket || len(poolConfig.ConnConfig.Fallbacks) != 0 {
			return nil, app.Fail(app.AuthStorageUnavailable)
		}
	} else {
		tlsConfig := poolConfig.ConnConfig.TLSConfig
		if tlsConfig == nil || tlsConfig.InsecureSkipVerify || tlsConfig.ServerName != poolConfig.ConnConfig.Host {
			return nil, app.Fail(app.AuthStorageUnavailable)
		}
		for _, fallback := range poolConfig.ConnConfig.Fallbacks {
			if len(fallback.Host) == 0 || fallback.Host[0] == '/' || fallback.TLSConfig == nil || fallback.TLSConfig.InsecureSkipVerify || fallback.TLSConfig.ServerName != fallback.Host {
				return nil, app.Fail(app.AuthStorageUnavailable)
			}
		}
	}
	poolConfig.MaxConns = 8
	if config.MaxConnections != 0 {
		poolConfig.MaxConns = int32(config.MaxConnections)
	}
	poolConfig.MinConns = 0
	poolConfig.MinIdleConns = 0
	poolConfig.ConnConfig.ConnectTimeout = 2 * time.Second
	poolConfig.ConnConfig.DialFunc = (&net.Dialer{Timeout: 2 * time.Second}).DialContext
	poolConfig.ConnConfig.MaxProtocolMessageBodyLen = 1 << 20
	poolConfig.ConnConfig.RuntimeParams["statement_timeout"] = "3000"
	poolConfig.ConnConfig.RuntimeParams["lock_timeout"] = "1000"
	poolConfig.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	poolConfig.ConnConfig.RuntimeParams["transaction_timeout"] = "5000"
	if config.ApplicationName != "" {
		poolConfig.ConnConfig.RuntimeParams["application_name"] = config.ApplicationName
	} else {
		poolConfig.ConnConfig.RuntimeParams["application_name"] = "newim-authsession"
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, app.Fail(app.AuthStorageUnavailable)
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, app.Fail(app.AuthStorageUnavailable)
	}
	return &Repository{pool: pool}, nil
}

// Close releases all pool resources after callers finish their transactions.
// Close 在调用方结束事务后释放全部连接池资源。
func (r *Repository) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

// Issue locks the session, checks ownership/revocation before entropy use, and
// commits the token row before returning. Issue 在熵使用前锁定并校验会话，提交后才返回令牌。
func (r *Repository) Issue(ctx context.Context, binding app.SessionBinding, generate func() (app.IssuedToken, error)) (app.IssuedToken, error) {
	if ctx == nil || !binding.Valid() || generate == nil {
		return app.IssuedToken{}, app.Fail(app.AuthInvalidInput)
	}
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return app.IssuedToken{}, err
	}
	defer rollback(tx)
	var userID, deviceID string
	var revokedAt *time.Time
	err = tx.QueryRow(ctx, "SELECT user_id,device_id,revoked_at FROM newim.im_sessions WHERE session_id=$1 FOR UPDATE", binding.SessionID).Scan(&userID, &deviceID, &revokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.IssuedToken{}, app.Fail(app.AuthSessionNotFound)
	}
	if err != nil {
		return app.IssuedToken{}, app.Fail(app.AuthStorageUnavailable)
	}
	if userID != binding.UserID || deviceID != binding.DeviceID {
		return app.IssuedToken{}, app.Fail(app.AuthForbidden)
	}
	if revokedAt != nil {
		return app.IssuedToken{}, app.Fail(app.AuthSessionRevoked)
	}
	issued, err := generate()
	if err != nil {
		return app.IssuedToken{}, fixed(err)
	}
	digest := issued.Digest()
	if issued.TokenID() == "" || issued.RawToken() == "" || issued.ExpiresAt().IsZero() || !issued.ExpiresAt().After(issued.IssuedAt()) {
		return app.IssuedToken{}, app.Fail(app.AuthStorageUnavailable)
	}
	_, err = tx.Exec(ctx, "INSERT INTO newim.im_auth_tokens(token_id,token_digest,session_id,created_at,expires_at) VALUES($1,$2,$3,$4,$5)",
		issued.TokenID(), digest[:], binding.SessionID, issued.IssuedAt(), issued.ExpiresAt())
	if err != nil {
		if tokenCollision(err) {
			return app.IssuedToken{}, app.Fail(app.AuthTokenCollision)
		}
		return app.IssuedToken{}, app.Fail(app.AuthStorageUnavailable)
	}
	if err = tx.Commit(ctx); err != nil {
		return app.IssuedToken{}, app.Fail(app.AuthStorageUnavailable)
	}
	return issued, nil
}

// Authenticate locks the token/session snapshot before invoking constant-time
// application verification. Authenticate 在调用常量时间应用校验前锁定令牌/会话快照。
func (r *Repository) Authenticate(ctx context.Context, tokenID string, verify func(app.TokenSnapshot) error) error {
	if ctx == nil || !lowerHexTokenID(tokenID) || verify == nil {
		return app.Fail(app.AuthInvalidInput)
	}
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)
	var sessionID string
	err = tx.QueryRow(ctx, "SELECT session_id FROM newim.im_auth_tokens WHERE token_id=$1", tokenID).Scan(&sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.AuthTokenUnknown)
	}
	if err != nil {
		return app.Fail(app.AuthStorageUnavailable)
	}
	var userID, deviceID string
	var sessionRevokedAt *time.Time
	err = tx.QueryRow(ctx, "SELECT user_id,device_id,revoked_at FROM newim.im_sessions WHERE session_id=$1 FOR SHARE", sessionID).Scan(&userID, &deviceID, &sessionRevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.AuthTokenUnknown)
	}
	if err != nil {
		return app.Fail(app.AuthStorageUnavailable)
	}
	var digest []byte
	var expiresAt time.Time
	var tokenRevokedAt *time.Time
	err = tx.QueryRow(ctx, "SELECT token_digest,expires_at,revoked_at FROM newim.im_auth_tokens WHERE token_id=$1 FOR SHARE", tokenID).Scan(&digest, &expiresAt, &tokenRevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.AuthTokenUnknown)
	}
	if err != nil {
		return app.Fail(app.AuthStorageUnavailable)
	}
	snapshot, err := app.NewTokenSnapshot(tokenID, digest, app.SessionBinding{UserID: userID, DeviceID: deviceID, SessionID: sessionID}, expiresAt, tokenRevokedAt, sessionRevokedAt)
	if err != nil {
		return app.Fail(app.AuthStorageUnavailable)
	}
	verifyErr := verify(snapshot)
	if err = tx.Commit(ctx); err != nil {
		return app.Fail(app.AuthStorageUnavailable)
	}
	if verifyErr != nil {
		return fixed(verifyErr)
	}
	return nil
}

// RevokeBearer locks the session before the presented token, verifies the
// application-supplied proof and revokes only that token. A repeated revoke is
// a no-op and never rewrites the original revocation time.
// RevokeBearer 按会话后令牌顺序加锁，校验应用提供的证明并仅撤销该令牌；重复撤销不改写原撤销时间。
func (r *Repository) RevokeBearer(ctx context.Context, tokenID string, verify func(app.TokenSnapshot) error, revokedAt time.Time) (app.RevocationOutcome, error) {
	if ctx == nil || !lowerHexTokenID(tokenID) || verify == nil || revokedAt.IsZero() {
		return "", app.Fail(app.AuthInvalidInput)
	}
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", err
	}
	defer rollback(tx)
	var sessionID string
	err = tx.QueryRow(ctx, "SELECT session_id FROM newim.im_auth_tokens WHERE token_id=$1", tokenID).Scan(&sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", app.Fail(app.AuthTokenUnknown)
	}
	if err != nil {
		return "", app.Fail(app.AuthStorageUnavailable)
	}
	var userID, deviceID string
	var sessionRevokedAt *time.Time
	err = tx.QueryRow(ctx, "SELECT user_id,device_id,revoked_at FROM newim.im_sessions WHERE session_id=$1 FOR UPDATE", sessionID).Scan(&userID, &deviceID, &sessionRevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", app.Fail(app.AuthTokenUnknown)
	}
	if err != nil {
		return "", app.Fail(app.AuthStorageUnavailable)
	}
	var digest []byte
	var expiresAt time.Time
	var tokenRevokedAt *time.Time
	err = tx.QueryRow(ctx, "SELECT token_digest,expires_at,revoked_at FROM newim.im_auth_tokens WHERE token_id=$1 FOR UPDATE", tokenID).Scan(&digest, &expiresAt, &tokenRevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", app.Fail(app.AuthTokenUnknown)
	}
	if err != nil {
		return "", app.Fail(app.AuthStorageUnavailable)
	}
	snapshot, err := app.NewTokenSnapshot(tokenID, digest, app.SessionBinding{UserID: userID, DeviceID: deviceID, SessionID: sessionID}, expiresAt, tokenRevokedAt, sessionRevokedAt)
	if err != nil {
		return "", app.Fail(app.AuthStorageUnavailable)
	}
	if err = verify(snapshot); err != nil {
		return "", fixed(err)
	}
	if tokenRevokedAt == nil {
		if _, err = tx.Exec(ctx, "UPDATE newim.im_auth_tokens SET revoked_at=$2 WHERE token_id=$1 AND revoked_at IS NULL", tokenID, revokedAt); err != nil {
			return "", app.Fail(app.AuthStorageUnavailable)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return "", app.Fail(app.AuthStorageUnavailable)
	}
	if tokenRevokedAt != nil {
		return app.RevokeNoop, nil
	}
	return app.RevokeOK, nil
}

// LookupSession returns a bounded non-mutating session view for policy validation.
// LookupSession 返回有界只读会话视图，供策略校验使用。
func (r *Repository) LookupSession(ctx context.Context, sessionID string) (app.SessionSnapshot, error) {
	if ctx == nil || !identifier(sessionID) {
		return app.SessionSnapshot{}, app.Fail(app.AuthInvalidInput)
	}
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return app.SessionSnapshot{}, err
	}
	defer rollback(tx)
	var userID, deviceID string
	var revokedAt *time.Time
	err = tx.QueryRow(ctx, "SELECT user_id,device_id,revoked_at FROM newim.im_sessions WHERE session_id=$1", sessionID).Scan(&userID, &deviceID, &revokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.SessionSnapshot{}, app.Fail(app.AuthSessionNotFound)
	}
	if err != nil {
		return app.SessionSnapshot{}, app.Fail(app.AuthStorageUnavailable)
	}
	snapshot, err := app.NewSessionSnapshot(app.SessionBinding{UserID: userID, DeviceID: deviceID, SessionID: sessionID}, revokedAt)
	if err != nil {
		return app.SessionSnapshot{}, app.Fail(app.AuthStorageUnavailable)
	}
	if err = tx.Commit(ctx); err != nil {
		return app.SessionSnapshot{}, app.Fail(app.AuthStorageUnavailable)
	}
	return snapshot, nil
}

// RevokeSession locks the session before updating it and all active tokens.
// RevokeSession 先锁定会话，再原子撤销会话及全部活跃令牌。
func (r *Repository) RevokeSession(ctx context.Context, binding app.SessionBinding, revokedAt time.Time) (app.RevocationOutcome, error) {
	if ctx == nil || !binding.Valid() || revokedAt.IsZero() {
		return "", app.Fail(app.AuthInvalidInput)
	}
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", err
	}
	defer rollback(tx)
	var userID, deviceID string
	var existingRevokedAt *time.Time
	err = tx.QueryRow(ctx, "SELECT user_id,device_id,revoked_at FROM newim.im_sessions WHERE session_id=$1 FOR UPDATE", binding.SessionID).Scan(&userID, &deviceID, &existingRevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", app.Fail(app.AuthSessionNotFound)
	}
	if err != nil {
		return "", app.Fail(app.AuthStorageUnavailable)
	}
	if userID != binding.UserID || deviceID != binding.DeviceID {
		return "", app.Fail(app.AuthForbidden)
	}
	if existingRevokedAt == nil {
		if _, err = tx.Exec(ctx, "UPDATE newim.im_sessions SET revoked_at=$2 WHERE session_id=$1 AND revoked_at IS NULL", binding.SessionID, revokedAt); err != nil {
			return "", app.Fail(app.AuthStorageUnavailable)
		}
	}
	if _, err = tx.Exec(ctx, "UPDATE newim.im_auth_tokens SET revoked_at=$2 WHERE session_id=$1 AND revoked_at IS NULL", binding.SessionID, revokedAt); err != nil {
		return "", app.Fail(app.AuthStorageUnavailable)
	}
	if err = tx.Commit(ctx); err != nil {
		return "", app.Fail(app.AuthStorageUnavailable)
	}
	if existingRevokedAt != nil {
		return app.RevokeNoop, nil
	}
	return app.RevokeOK, nil
}

// RevokeToken locks the owning session before the token to preserve lock order.
// RevokeToken 按会话后令牌的顺序加锁，避免与认证/会话撤销交叉。
func (r *Repository) RevokeToken(ctx context.Context, userID, tokenID string, revokedAt time.Time) (app.RevocationOutcome, error) {
	if ctx == nil || !identifier(userID) || !lowerHexTokenID(tokenID) || revokedAt.IsZero() {
		return "", app.Fail(app.AuthInvalidInput)
	}
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", err
	}
	defer rollback(tx)
	var sessionID string
	err = tx.QueryRow(ctx, "SELECT session_id FROM newim.im_auth_tokens WHERE token_id=$1", tokenID).Scan(&sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", app.Fail(app.AuthTokenUnknown)
	}
	if err != nil {
		return "", app.Fail(app.AuthStorageUnavailable)
	}
	var owner string
	err = tx.QueryRow(ctx, "SELECT user_id FROM newim.im_sessions WHERE session_id=$1 FOR UPDATE", sessionID).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", app.Fail(app.AuthTokenUnknown)
	}
	if err != nil {
		return "", app.Fail(app.AuthStorageUnavailable)
	}
	if owner != userID {
		return "", app.Fail(app.AuthForbidden)
	}
	var existingRevokedAt *time.Time
	err = tx.QueryRow(ctx, "SELECT revoked_at FROM newim.im_auth_tokens WHERE token_id=$1 FOR UPDATE", tokenID).Scan(&existingRevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", app.Fail(app.AuthTokenUnknown)
	}
	if err != nil {
		return "", app.Fail(app.AuthStorageUnavailable)
	}
	if existingRevokedAt == nil {
		if _, err = tx.Exec(ctx, "UPDATE newim.im_auth_tokens SET revoked_at=$2 WHERE token_id=$1 AND revoked_at IS NULL", tokenID, revokedAt); err != nil {
			return "", app.Fail(app.AuthStorageUnavailable)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return "", app.Fail(app.AuthStorageUnavailable)
	}
	if existingRevokedAt != nil {
		return app.RevokeNoop, nil
	}
	return app.RevokeOK, nil
}

func (r *Repository) begin(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	if r == nil || r.pool == nil {
		return nil, app.Fail(app.AuthStorageUnavailable)
	}
	tx, err := r.pool.BeginTx(ctx, options)
	if err != nil {
		return nil, app.Fail(app.AuthStorageUnavailable)
	}
	if err = configure(ctx, tx); err != nil {
		rollback(tx)
		return nil, err
	}
	return tx, nil
}

func configure(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '3s'; SET LOCAL lock_timeout = '1s'; SET LOCAL transaction_timeout = '5s'; SET LOCAL idle_in_transaction_session_timeout = '5s'")
	return fixed(err)
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func fixed(err error) error {
	if err == nil {
		return nil
	}
	var known *app.Error
	if errors.As(err, &known) && known != nil {
		return app.Fail(known.Code)
	}
	return app.Fail(app.AuthStorageUnavailable)
}

func tokenCollision(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		return false
	}
	return pgErr.ConstraintName == "im_auth_tokens_pkey" || pgErr.ConstraintName == "im_auth_tokens_digest_key"
}

func lowerHexTokenID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return false
		}
	}
	return true
}

func identifier(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if !((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}
