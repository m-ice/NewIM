// Package media stores upload grants and ready media assets in PostgreSQL.
// media 在 PostgreSQL 中存储上传凭据与就绪媒体资产；不保存对象 URL。
package media

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/m-ice/NewIM/server/auth/session"
	app "github.com/m-ice/NewIM/server/media"
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

// Repository owns a bounded pgx pool for media grants/assets.
// Repository 持有媒体凭据/资产使用的有界 pgx 连接池。
type Repository struct{ pool *pgxpool.Pool }

// Open requires verified TLS for TCP or explicitly enabled local sockets.
// Open 的 TCP 必须校验主机证书，本地套接字须显式启用。
func Open(ctx context.Context, config Config) (*Repository, error) {
	if ctx == nil || config.DSN == "" || config.MaxConnections < 0 || config.MaxConnections > 8 {
		return nil, app.Fail(app.MediaStorageUnavailable)
	}
	if config.ApplicationName != "" && !identifier(config.ApplicationName) {
		return nil, app.Fail(app.MediaInvalidInput)
	}
	poolConfig, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, app.Fail(app.MediaStorageUnavailable)
	}
	local := len(poolConfig.ConnConfig.Host) > 0 && poolConfig.ConnConfig.Host[0] == '/'
	if local {
		if !config.AllowLocalSocket || len(poolConfig.ConnConfig.Fallbacks) != 0 {
			return nil, app.Fail(app.MediaStorageUnavailable)
		}
	} else {
		tlsConfig := poolConfig.ConnConfig.TLSConfig
		if tlsConfig == nil || tlsConfig.InsecureSkipVerify || tlsConfig.ServerName != poolConfig.ConnConfig.Host {
			return nil, app.Fail(app.MediaStorageUnavailable)
		}
		for _, fallback := range poolConfig.ConnConfig.Fallbacks {
			if len(fallback.Host) == 0 || fallback.Host[0] == '/' || fallback.TLSConfig == nil || fallback.TLSConfig.InsecureSkipVerify || fallback.TLSConfig.ServerName != fallback.Host {
				return nil, app.Fail(app.MediaStorageUnavailable)
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
		poolConfig.ConnConfig.RuntimeParams["application_name"] = "newim-media"
	}
	openCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(openCtx, poolConfig)
	if err != nil {
		return nil, app.Fail(app.MediaStorageUnavailable)
	}
	if err = pool.Ping(openCtx); err != nil {
		pool.Close()
		return nil, app.Fail(app.MediaStorageUnavailable)
	}
	return &Repository{pool: pool}, nil
}

// Close releases pool resources after callers finish transactions.
// Close 在调用方结束事务后释放连接池资源。
func (r *Repository) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

// BeginUpload authorizes the complete identity before invoking create.
// BeginUpload 在调用 create 前授权完整身份。
func (r *Repository) BeginUpload(ctx context.Context, now time.Time, identity session.ConnectionIdentity, conversationID string, create func() (app.Grant, error)) error {
	if r == nil || r.pool == nil || ctx == nil || create == nil || now.IsZero() ||
		!app.ValidateIdentity(identity) || !identifier(conversationID) {
		return app.Fail(app.MediaInvalidInput)
	}
	tx, err := r.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err = authorizeTx(ctx, tx, now, identity, conversationID); err != nil {
		return err
	}
	grant, err := create()
	if err != nil {
		return fixed(err)
	}
	if !app.ValidGrant(grant) || grant.State != app.StatePending ||
		grant.OwnerUserID != identity.UserID() || grant.DeviceID != identity.DeviceID() ||
		grant.SessionID != identity.SessionID() || grant.ConnectionID != identity.ConnectionID() ||
		grant.TokenID != identity.TokenID() || grant.ConversationID != conversationID {
		return app.Fail(app.MediaStorageUnavailable)
	}
	tag, err := tx.Exec(ctx, `INSERT INTO newim.im_media_assets
	(media_key,owner_user_id,device_id,session_id,connection_id,token_id,conversation_id,
	 media_kind,content_type,declared_size_bytes,actual_size_bytes,sha256,upload_grant_id,
	 upload_grant_digest,upload_expires_at,completed_at,state)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULL,$11,$12,$13,$14,NULL,'pending')
	ON CONFLICT DO NOTHING`,
		grant.MediaKey, grant.OwnerUserID, grant.DeviceID, grant.SessionID, grant.ConnectionID,
		grant.TokenID, grant.ConversationID, grant.Kind, grant.ContentType, grant.DeclaredSize,
		grant.SHA256[:], grant.GrantID, grant.GrantDigest[:], grant.ExpiresAt)
	if err != nil {
		return mapStorageError(err)
	}
	if tag.RowsAffected() != 1 {
		return app.Fail(app.MediaGrantCollision)
	}
	if err = tx.Commit(ctx); err != nil {
		return mapStorageError(err)
	}
	return nil
}

// LoadGrant loads by the non-secret grant ID before constant-time digest compare.
// LoadGrant 先按非敏感 grant ID 加载，再进行常量时间摘要比较。
func (r *Repository) LoadGrant(ctx context.Context, grantID string) (app.Grant, error) {
	if r == nil || r.pool == nil || ctx == nil || !identifierText(grantID) {
		return app.Grant{}, app.Fail(app.MediaNotFound)
	}
	tx, err := r.beginReadOnly(ctx)
	if err != nil {
		return app.Grant{}, err
	}
	defer rollback(tx)
	grant, err := scanGrant(tx.QueryRow(ctx, `SELECT media_key,owner_user_id,device_id,session_id,connection_id,token_id,conversation_id,
media_kind,content_type,declared_size_bytes,actual_size_bytes,sha256,upload_grant_id,upload_grant_digest,
upload_expires_at,completed_at,state
FROM newim.im_media_assets WHERE upload_grant_id=$1`, grantID))
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Grant{}, app.Fail(app.MediaNotFound)
	}
	if err != nil {
		return app.Grant{}, mapStorageError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return app.Grant{}, mapStorageError(err)
	}
	return grant, nil
}

// CompleteReady marks a pending grant ready after the object was published.
// CompleteReady 在对象发布后原子地把 pending 凭据标记为 ready。
func (r *Repository) CompleteReady(ctx context.Context, now time.Time, identity session.ConnectionIdentity, grant app.Grant) (app.Grant, error) {
	if r == nil || r.pool == nil || ctx == nil || now.IsZero() || !app.ValidateIdentity(identity) || !app.ValidGrant(grant) {
		return app.Grant{}, app.Fail(app.MediaInvalidInput)
	}
	tx, err := r.begin(ctx)
	if err != nil {
		return app.Grant{}, err
	}
	defer rollback(tx)
	if err = authorizeTx(ctx, tx, now, identity, grant.ConversationID); err != nil {
		return app.Grant{}, err
	}
	ready, err := scanGrant(tx.QueryRow(ctx, `UPDATE newim.im_media_assets
SET actual_size_bytes=declared_size_bytes,completed_at=$2,state='ready'
WHERE media_key=$1 AND upload_grant_id=$3 AND upload_grant_digest=$4 AND state='pending'
  AND owner_user_id=$5 AND device_id=$6 AND session_id=$7 AND connection_id=$8 AND token_id=$9
RETURNING media_key,owner_user_id,device_id,session_id,connection_id,token_id,conversation_id,
media_kind,content_type,declared_size_bytes,actual_size_bytes,sha256,upload_grant_id,upload_grant_digest,
upload_expires_at,completed_at,state`,
		grant.MediaKey, now, grant.GrantID, grant.GrantDigest[:], grant.OwnerUserID, grant.DeviceID,
		grant.SessionID, grant.ConnectionID, grant.TokenID))
	if errors.Is(err, pgx.ErrNoRows) {
		existing, loadErr := scanGrant(tx.QueryRow(ctx, `SELECT media_key,owner_user_id,device_id,session_id,connection_id,token_id,conversation_id,
media_kind,content_type,declared_size_bytes,actual_size_bytes,sha256,upload_grant_id,upload_grant_digest,
upload_expires_at,completed_at,state FROM newim.im_media_assets WHERE media_key=$1`, grant.MediaKey))
		if loadErr != nil {
			return app.Grant{}, mapStorageError(loadErr)
		}
		if existing.State == app.StateReady && sameGrantIntent(existing, grant) {
			return existing, nil
		}
		return app.Grant{}, app.Fail(app.MediaConflict)
	}
	if err != nil {
		return app.Grant{}, mapStorageError(err)
	}
	if !app.ValidGrant(ready) || ready.State != app.StateReady {
		return app.Grant{}, app.Fail(app.MediaStorageUnavailable)
	}
	if err = tx.Commit(ctx); err != nil {
		return app.Grant{}, mapStorageError(err)
	}
	return ready, nil
}

// LoadReadyAsset returns a durable ready asset for private download resolution.
// LoadReadyAsset 返回用于私有下载解析的持久就绪资产。
func (r *Repository) LoadReadyAsset(ctx context.Context, mediaKey string) (app.Grant, error) {
	if r == nil || r.pool == nil || ctx == nil || !identifier(mediaKey) {
		return app.Grant{}, app.Fail(app.MediaNotFound)
	}
	readCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	grant, err := scanGrant(r.pool.QueryRow(readCtx, `SELECT media_key,owner_user_id,device_id,session_id,connection_id,token_id,conversation_id,
media_kind,content_type,declared_size_bytes,actual_size_bytes,sha256,upload_grant_id,upload_grant_digest,
upload_expires_at,completed_at,state FROM newim.im_media_assets WHERE media_key=$1 AND state='ready'`, mediaKey))
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Grant{}, app.Fail(app.MediaNotFound)
	}
	if err != nil {
		return app.Grant{}, mapStorageError(err)
	}
	return grant, nil
}

// Authorize verifies current membership plus complete live session/token binding.
// Authorize 校验当前成员关系及完整在线会话/令牌绑定。
func (r *Repository) Authorize(ctx context.Context, now time.Time, identity session.ConnectionIdentity, conversationID string) error {
	if r == nil || r.pool == nil || ctx == nil || now.IsZero() ||
		!app.ValidateIdentity(identity) || !identifier(conversationID) {
		return app.Fail(app.MediaUnauthorized)
	}
	readCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	var userID, deviceID string
	err := r.pool.QueryRow(readCtx, `SELECT s.user_id,s.device_id
FROM newim.im_conversation_members m
JOIN newim.im_sessions s ON s.session_id=$3
JOIN newim.im_auth_tokens t ON t.token_id=$5 AND t.session_id=s.session_id
WHERE m.conversation_id=$4 AND m.user_id=$1 AND s.user_id=$1 AND s.device_id=$2
  AND s.revoked_at IS NULL AND t.revoked_at IS NULL AND t.expires_at> $6`,
		identity.UserID(), identity.DeviceID(), identity.SessionID(), conversationID,
		identity.TokenID(), now).Scan(&userID, &deviceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.MediaUnauthorized)
	}
	if err != nil {
		return mapStorageError(err)
	}
	if userID != identity.UserID() || deviceID != identity.DeviceID() {
		return app.Fail(app.MediaUnauthorized)
	}
	return nil
}

// ValidateReadyForSend checks exact ready/owner/conversation-bound metadata.
// ValidateReadyForSend 校验精确的 ready/owner/conversation 绑定元数据。
func (r *Repository) ValidateReadyForSend(ctx context.Context, now time.Time, identity session.ConnectionIdentity, conversationID string, metadata app.Metadata) error {
	if r == nil || r.pool == nil || ctx == nil || now.IsZero() ||
		!app.ValidateIdentity(identity) || !identifier(conversationID) || !app.ValidMetadata(metadata) {
		return app.Fail(app.MediaInvalidInput)
	}
	readCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	var found int
	err := r.pool.QueryRow(readCtx, `SELECT 1
FROM newim.im_media_assets a
JOIN newim.im_conversation_members m ON m.conversation_id=a.conversation_id AND m.user_id=a.owner_user_id
JOIN newim.im_sessions s ON s.session_id=a.session_id AND s.user_id=a.owner_user_id AND s.device_id=a.device_id
JOIN newim.im_auth_tokens t ON t.token_id=a.token_id AND t.session_id=s.session_id
WHERE a.media_key=$1 AND a.state='ready'
  AND a.owner_user_id=$2 AND a.device_id=$3 AND a.session_id=$4 AND a.connection_id=$5 AND a.token_id=$6
  AND a.conversation_id=$7 AND a.media_kind=$8 AND a.content_type=$9
  AND a.declared_size_bytes=$10 AND a.actual_size_bytes=$10 AND a.sha256=$11
  AND m.user_id=$2 AND s.revoked_at IS NULL AND t.revoked_at IS NULL AND t.expires_at> $12`,
		metadata.MediaKey, identity.UserID(), identity.DeviceID(), identity.SessionID(),
		identity.ConnectionID(), identity.TokenID(), conversationID, metadata.Kind, metadata.ContentType,
		metadata.Size, metadata.SHA256[:], now).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.MediaUnauthorized)
	}
	if err != nil {
		return mapStorageError(err)
	}
	return nil
}

func (r *Repository) begin(ctx context.Context) (pgx.Tx, error) {
	beginCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.pool.BeginTx(beginCtx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, mapStorageError(err)
	}
	if err = configure(beginCtx, tx); err != nil {
		rollback(tx)
		return nil, err
	}
	return tx, nil
}

func (r *Repository) beginReadOnly(ctx context.Context) (pgx.Tx, error) {
	beginCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.pool.BeginTx(beginCtx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, mapStorageError(err)
	}
	if err = configure(beginCtx, tx); err != nil {
		rollback(tx)
		return nil, err
	}
	return tx, nil
}

func configure(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '3s'; SET LOCAL lock_timeout = '1s'; SET LOCAL transaction_timeout = '5s'; SET LOCAL idle_in_transaction_session_timeout = '5s'")
	return mapStorageError(err)
}

func authorizeTx(ctx context.Context, tx pgx.Tx, now time.Time, identity session.ConnectionIdentity, conversationID string) error {
	var userID, deviceID string
	err := tx.QueryRow(ctx, `SELECT s.user_id,s.device_id
FROM newim.im_conversation_members m
JOIN newim.im_sessions s ON s.session_id=$3
JOIN newim.im_auth_tokens t ON t.token_id=$5 AND t.session_id=s.session_id
WHERE m.conversation_id=$4 AND m.user_id=$1 AND s.user_id=$1 AND s.device_id=$2
  AND s.revoked_at IS NULL AND t.revoked_at IS NULL AND t.expires_at> $6
FOR UPDATE OF s`,
		identity.UserID(), identity.DeviceID(), identity.SessionID(), conversationID,
		identity.TokenID(), now).Scan(&userID, &deviceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.MediaUnauthorized)
	}
	if err != nil {
		return mapStorageError(err)
	}
	if userID != identity.UserID() || deviceID != identity.DeviceID() {
		return app.Fail(app.MediaUnauthorized)
	}
	return nil
}

func scanGrant(row pgx.Row) (app.Grant, error) {
	var grant app.Grant
	var actualSize *int64
	var completedAt *time.Time
	var sha, digest []byte
	var state string
	err := row.Scan(
		&grant.MediaKey, &grant.OwnerUserID, &grant.DeviceID, &grant.SessionID, &grant.ConnectionID,
		&grant.TokenID, &grant.ConversationID, &grant.Kind, &grant.ContentType, &grant.DeclaredSize,
		&actualSize, &sha, &grant.GrantID, &digest, &grant.ExpiresAt, &completedAt, &state,
	)
	if err != nil {
		return app.Grant{}, err
	}
	if len(sha) != 32 || len(digest) != 32 {
		return app.Grant{}, app.Fail(app.MediaStorageUnavailable)
	}
	copy(grant.SHA256[:], sha)
	copy(grant.GrantDigest[:], digest)
	grant.ActualSize = actualSize
	grant.CompletedAt = completedAt
	grant.State = app.State(state)
	return grant, nil
}

func sameGrantIntent(left, right app.Grant) bool {
	return left.MediaKey == right.MediaKey && left.OwnerUserID == right.OwnerUserID &&
		left.DeviceID == right.DeviceID && left.SessionID == right.SessionID &&
		left.ConnectionID == right.ConnectionID && left.TokenID == right.TokenID &&
		left.ConversationID == right.ConversationID && left.Kind == right.Kind &&
		left.ContentType == right.ContentType && left.DeclaredSize == right.DeclaredSize &&
		left.SHA256 == right.SHA256 && left.GrantID == right.GrantID && left.GrantDigest == right.GrantDigest
}

func rollback(tx pgx.Tx) {
	if tx == nil {
		return
	}
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
	return app.Fail(app.MediaStorageUnavailable)
}

func mapStorageError(err error) error {
	if err == nil {
		return nil
	}
	var known *app.Error
	if errors.As(err, &known) && known != nil {
		return app.Fail(known.Code)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return app.Fail(app.MediaGrantCollision)
		case "23503", "23514":
			return app.Fail(app.MediaUnauthorized)
		case "40001", "40P01", "55P03", "57014":
			return app.Fail(app.MediaStorageUnavailable)
		case "08000", "08001", "08003", "08004", "08006", "08007", "57P01", "57P02", "57P03":
			return app.Fail(app.MediaStorageUnavailable)
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return app.Fail(app.MediaStorageUnavailable)
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return app.Fail(app.MediaStorageUnavailable)
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return app.Fail(app.MediaStorageUnavailable)
	}
	return app.Fail(app.MediaStorageUnavailable)
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

func identifierText(value string) bool {
	return len(value) == 32 && identifier(value)
}
