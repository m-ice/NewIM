// Package messagesync reads persisted messages through the internal sync port.
// messagesync 通过内部同步端口读取持久化消息，不承载网络或公共协议。
package messagesync

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/m-ice/NewIM/server/sync/message"
)

const requestTimeout = 5 * time.Second

// readTxOptions is kept package-visible so the integration test can prove the
// adapter rejects a non-read-only or non-repeatable transaction.
// readTxOptions 允许包内集成测试证明适配器会拒绝非只读或非可重复读事务。
var readTxOptions = pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}

// Config is trusted database configuration, never request input.
// Config 是可信数据库配置，不接受请求输入。
type Config struct {
	DSN              string
	AllowLocalSocket bool
	MaxConnections   int
	ApplicationName  string
}

// Repository owns a bounded pgx pool and implements message.Store.
// Repository 持有有界 pgx 连接池并实现 message.Store。
type Repository struct{ pool *pgxpool.Pool }

// Open requires verified TLS for TCP or an explicitly enabled local socket.
// Open 的 TCP 必须校验主机证书，本地套接字须显式启用。
func Open(ctx context.Context, config Config) (*Repository, error) {
	if ctx == nil || config.DSN == "" || config.MaxConnections < 0 || config.MaxConnections > 8 {
		return nil, app.Fail(app.StorageUnavailable)
	}
	if config.ApplicationName != "" && !identifier(config.ApplicationName) {
		return nil, app.Fail(app.StorageUnavailable)
	}
	poolConfig, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	local := len(poolConfig.ConnConfig.Host) > 0 && strings.HasPrefix(poolConfig.ConnConfig.Host, "/")
	if local {
		if !config.AllowLocalSocket || len(poolConfig.ConnConfig.Fallbacks) != 0 {
			return nil, app.Fail(app.StorageUnavailable)
		}
	} else {
		tlsConfig := poolConfig.ConnConfig.TLSConfig
		if tlsConfig == nil || tlsConfig.InsecureSkipVerify || tlsConfig.ServerName != poolConfig.ConnConfig.Host {
			return nil, app.Fail(app.StorageUnavailable)
		}
		for _, fallback := range poolConfig.ConnConfig.Fallbacks {
			if len(fallback.Host) == 0 || strings.HasPrefix(fallback.Host, "/") || fallback.TLSConfig == nil || fallback.TLSConfig.InsecureSkipVerify || fallback.TLSConfig.ServerName != fallback.Host {
				return nil, app.Fail(app.StorageUnavailable)
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
	if config.ApplicationName == "" {
		config.ApplicationName = "newim-messagesync"
	}
	poolConfig.ConnConfig.RuntimeParams["application_name"] = config.ApplicationName
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, app.Fail(app.StorageUnavailable)
	}
	return &Repository{pool: pool}, nil
}

// Close releases all pool resources.
// Close 释放全部连接池资源。
func (r *Repository) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

// Read returns one bounded page from a single read-only snapshot.
// Read 从单一只读快照返回一个有界消息页。
func (r *Repository) Read(ctx context.Context, user string, request app.ReadRequest) (app.ReadResult, error) {
	return r.read(ctx, user, request, nil)
}

func (r *Repository) read(ctx context.Context, user string, request app.ReadRequest, afterSnapshot func() error) (app.ReadResult, error) {
	if r == nil || r.pool == nil || ctx == nil || !identifier(user) || !identifier(request.ConversationID) {
		return app.ReadResult{}, app.Fail(app.Forbidden)
	}
	tx, err := r.begin(ctx, readTxOptions)
	if err != nil {
		return app.ReadResult{}, err
	}
	defer rollback(tx)
	if err = verifyReadTransaction(ctx, tx); err != nil {
		return app.ReadResult{}, err
	}
	if afterSnapshot != nil {
		if err = afterSnapshot(); err != nil {
			return app.ReadResult{}, app.Fail(app.StorageUnavailable)
		}
	}
	latestSeq, latestID, latestSeqFromPointer, err := readHead(ctx, tx, user, request.ConversationID)
	if err != nil {
		return app.ReadResult{}, err
	}
	if err = validateHead(ctx, tx, request.ConversationID, latestSeq, latestID, latestSeqFromPointer); err != nil {
		return app.ReadResult{}, err
	}
	if request.HasAfterSeq && request.AfterSeq > 0 {
		present, err := anchorPresent(ctx, tx, request.ConversationID, request.AfterSeq)
		if err != nil {
			return app.ReadResult{}, err
		}
		if !present {
			return app.ReadResult{}, app.Fail(app.InvalidCursor)
		}
	}
	if (!request.HasAfterSeq && request.AfterSeq != 0) || request.AfterSeq < 0 || request.AfterSeq > latestSeq {
		return app.ReadResult{}, app.Fail(app.InvalidCursor)
	}
	limit, err := normalizeLimit(request.Limit)
	if err != nil {
		return app.ReadResult{}, err
	}
	lower := int64(-1)
	if request.HasAfterSeq {
		lower = request.AfterSeq
	}
	rows, err := readRows(ctx, tx, request.ConversationID, lower, latestSeq, limit+1)
	if err != nil {
		return app.ReadResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return app.ReadResult{}, app.Fail(app.StorageUnavailable)
	}
	return app.ReadResult{LatestSeq: latestSeq, Limit: limit, Rows: rows}, nil
}

func (r *Repository) begin(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	if r == nil || r.pool == nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	tx, err := r.pool.BeginTx(ctx, options)
	if err != nil {
		return nil, fixed(err)
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

func verifyReadTransaction(ctx context.Context, tx pgx.Tx) error {
	var isolation, readOnly string
	if err := tx.QueryRow(ctx, "SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')").Scan(&isolation, &readOnly); err != nil {
		return fixed(err)
	}
	if isolation != "repeatable read" || readOnly != "on" {
		return app.Fail(app.StorageUnavailable)
	}
	return nil
}

func readHead(ctx context.Context, tx pgx.Tx, user, conversationID string) (int64, *string, *int64, error) {
	var latestSeq int64
	var latestID *string
	var pointerSeq *int64
	err := tx.QueryRow(ctx, `SELECT c.last_seq, c.latest_server_msg_id, m.conversation_seq
FROM newim.im_conversations c
JOIN newim.im_conversation_members member
  ON member.conversation_id = c.conversation_id AND member.user_id = $2
LEFT JOIN newim.im_messages m
  ON m.conversation_id = c.conversation_id AND m.server_msg_id = c.latest_server_msg_id
WHERE c.conversation_id = $1`, conversationID, user).Scan(&latestSeq, &latestID, &pointerSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, nil, app.Fail(app.Forbidden)
	}
	if err != nil {
		return 0, nil, nil, fixed(err)
	}
	if latestSeq < 0 {
		return 0, nil, nil, app.Fail(app.StorageUnavailable)
	}
	return latestSeq, latestID, pointerSeq, nil
}

func validateHead(ctx context.Context, tx pgx.Tx, conversationID string, latestSeq int64, latestID *string, pointerSeq *int64) error {
	if latestSeq == 0 {
		if latestID != nil || pointerSeq != nil {
			return app.Fail(app.StorageUnavailable)
		}
	} else if latestID == nil || pointerSeq == nil || *pointerSeq != latestSeq {
		return app.Fail(app.StorageUnavailable)
	}
	minSeq, minFound, err := edgeSequence(ctx, tx, conversationID, false)
	if err != nil {
		return err
	}
	maxSeq, maxFound, err := edgeSequence(ctx, tx, conversationID, true)
	if err != nil {
		return err
	}
	if latestSeq == 0 {
		if minFound || maxFound {
			return app.Fail(app.StorageUnavailable)
		}
		return nil
	}
	if !minFound || !maxFound || minSeq != 1 || maxSeq != latestSeq {
		return app.Fail(app.StorageUnavailable)
	}
	return nil
}

func edgeSequence(ctx context.Context, tx pgx.Tx, conversationID string, descending bool) (int64, bool, error) {
	order := "ASC"
	if descending {
		order = "DESC"
	}
	var seq int64
	err := tx.QueryRow(ctx, "SELECT conversation_seq FROM newim.im_messages WHERE conversation_id=$1 ORDER BY conversation_seq "+order+" LIMIT 1", conversationID).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fixed(err)
	}
	if seq < 0 {
		return 0, false, app.Fail(app.StorageUnavailable)
	}
	return seq, true, nil
}

func anchorPresent(ctx context.Context, tx pgx.Tx, conversationID string, seq int64) (bool, error) {
	var present bool
	err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM newim.im_messages WHERE conversation_id=$1 AND conversation_seq=$2)", conversationID, seq).Scan(&present)
	if err != nil {
		return false, fixed(err)
	}
	return present, nil
}

func normalizeLimit(limit int) (int, error) {
	if limit == 0 {
		return app.DefaultPageItems, nil
	}
	if limit < 1 || limit > app.MaxPageItems {
		return 0, app.Fail(app.LimitExceeded)
	}
	return limit, nil
}

func readRows(ctx context.Context, tx pgx.Tx, conversationID string, lower, upper int64, limit int) ([]app.Item, error) {
	rows, err := tx.Query(ctx, `SELECT server_msg_id,client_msg_id,sender_id,conversation_id,conversation_seq,protocol_version,schema_version,message_type,server_time_ms,payload_bytes
FROM newim.im_messages
WHERE conversation_id=$1 AND conversation_seq>$2 AND conversation_seq<=$3
ORDER BY conversation_seq
LIMIT $4`, conversationID, lower, upper, limit)
	if err != nil {
		return nil, fixed(err)
	}
	defer rows.Close()
	items := make([]app.Item, 0, limit)
	for rows.Next() {
		var item app.Item
		var protocolVersion, schemaVersion int32
		if err = rows.Scan(&item.ServerMsgID, &item.ClientMsgID, &item.SenderID, &item.ConversationID, &item.ConversationSeq, &protocolVersion, &schemaVersion, &item.MessageType, &item.ServerTime, &item.Payload); err != nil {
			return nil, fixed(err)
		}
		if protocolVersion <= 0 || schemaVersion <= 0 || item.ConversationSeq < 0 {
			return nil, app.Fail(app.StorageUnavailable)
		}
		item.ProtocolVersion = uint32(protocolVersion)
		item.SchemaVersion = uint32(schemaVersion)
		item.Payload = append([]byte(nil), item.Payload...)
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, fixed(err)
	}
	return items, nil
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
	return app.Fail(app.StorageUnavailable)
}

func identifier(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
