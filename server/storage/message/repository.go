// Package message stores durable message sends through newim.persist_message.
// message 通过 newim.persist_message 存储持久发送，不实现网络或投递。
package message

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/conversation"
	app "github.com/m-ice/NewIM/server/message"
	"github.com/m-ice/NewIM/server/storage/conversationsync"
	"github.com/m-ice/NewIM/server/storage/membership"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
)

const requestTimeout = 5 * time.Second

// Config is trusted database configuration, never request input.
// Config 仅接受可信数据库配置，不接受请求内容。
type Config struct {
	DSN              string
	AllowLocalSocket bool
	MaxConnections   int
	ApplicationName  string
	// PrivatePolicy requires trusted conversation classification and enables
	// private-message policy in the send transaction.
	PrivatePolicy bool
	// StrangerPolicy limits an unanswered private conversation to its first send.
	StrangerPolicy bool
	// DirectOnly limits sends and receipt replay to verified direct conversations.
	// DirectOnly 仅允许在已核验的私聊中发送和重放回执。
	DirectOnly bool
	// ManagedGroups permits verified direct conversations and managed groups.
	ManagedGroups bool
	// ProjectConversations records source changes in the owning transaction.
	ProjectConversations bool
	ProjectReadState     bool
	// Guard enables shared backend identity validation when non-nil.
	// Guard 非 nil 时启用共享后端身份校验。
	Guard *postgresidentity.Guard
}

// Repository owns a bounded pgx pool and the membership authorizer.
// Repository 持有有界 pgx 连接池与会话成员授权器。
type Repository struct {
	pool                 *pgxpool.Pool
	authorizer           conversation.Authorizer
	privatePolicy        bool
	strangerPolicy       bool
	directOnly           bool
	managedGroups        bool
	projectConversations bool
	projectReadState     bool
	accountSettings      bool
	visibility           bool
	actorTypes           bool
}

// Open requires verified TLS for TCP or explicitly enabled local sockets.
// Open 的 TCP 必须校验主机证书，本地套接字须显式启用。
func Open(ctx context.Context, config Config) (*Repository, error) {
	if ctx == nil || config.DSN == "" || config.MaxConnections < 0 || config.MaxConnections > 8 {
		return nil, app.Fail(app.SendStorageUnavailable)
	}
	if config.DirectOnly && config.ManagedGroups {
		return nil, app.Fail(app.SendStorageUnavailable)
	}
	if (config.DirectOnly || config.ManagedGroups) && (!config.PrivatePolicy || config.Guard == nil) {
		return nil, app.Fail(app.SendStorageUnavailable)
	}
	if config.StrangerPolicy && !config.PrivatePolicy {
		return nil, app.Fail(app.SendStorageUnavailable)
	}
	if config.ProjectReadState && !config.ProjectConversations {
		return nil, app.Fail(app.SendStorageUnavailable)
	}
	if config.ApplicationName != "" && !identifier(config.ApplicationName) {
		return nil, app.Fail(app.SendInvalidInput)
	}
	poolConfig, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, app.Fail(app.SendStorageUnavailable)
	}
	local := len(poolConfig.ConnConfig.Host) > 0 && poolConfig.ConnConfig.Host[0] == '/'
	if local {
		if !config.AllowLocalSocket || len(poolConfig.ConnConfig.Fallbacks) != 0 {
			return nil, app.Fail(app.SendStorageUnavailable)
		}
	} else {
		tlsConfig := poolConfig.ConnConfig.TLSConfig
		if tlsConfig == nil || tlsConfig.InsecureSkipVerify || tlsConfig.ServerName != poolConfig.ConnConfig.Host {
			return nil, app.Fail(app.SendStorageUnavailable)
		}
		for _, fallback := range poolConfig.ConnConfig.Fallbacks {
			if len(fallback.Host) == 0 || fallback.Host[0] == '/' || fallback.TLSConfig == nil || fallback.TLSConfig.InsecureSkipVerify || fallback.TLSConfig.ServerName != fallback.Host {
				return nil, app.Fail(app.SendStorageUnavailable)
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
		poolConfig.ConnConfig.RuntimeParams["application_name"] = "newim-message"
	}
	installGuard(poolConfig, config.Guard)
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, app.Fail(app.SendStorageUnavailable)
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, app.Fail(app.SendStorageUnavailable)
	}
	if config.PrivatePolicy {
		if err = verifyConversationClassification(ctx, pool); err != nil {
			pool.Close()
			return nil, err
		}
	}
	accountSettings, err := conversationsync.AccountSettingsAvailable(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, app.Fail(app.SendStorageUnavailable)
	}
	actorTypes, err := ActorTypesAvailable(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, err
	}
	var visibility bool
	if accountSettings && config.ProjectReadState {
		visibility, err = conversationsync.VisibilityAvailable(ctx, pool)
		if err != nil {
			pool.Close()
			return nil, app.Fail(app.SendStorageUnavailable)
		}
	}
	repository := &Repository{
		pool: pool, authorizer: conversationAuthorizer{},
		privatePolicy: config.PrivatePolicy, strangerPolicy: config.StrangerPolicy,
		directOnly: config.DirectOnly, managedGroups: config.ManagedGroups,
		projectConversations: config.ProjectConversations,
		projectReadState:     config.ProjectReadState,
		accountSettings:      accountSettings,
		visibility:           visibility,
		actorTypes:           actorTypes,
	}
	if repository.managedGroups {
		if err = repository.checkManagedGroupSchema(ctx); err != nil {
			pool.Close()
			return nil, err
		}
	}
	if config.StrangerPolicy {
		if err = checkStrangerPolicySchema(ctx, pool); err != nil {
			pool.Close()
			return nil, err
		}
	}
	if config.ProjectConversations && conversationsync.CheckSourceSchema(ctx, pool, config.ProjectReadState) != nil {
		pool.Close()
		return nil, app.Fail(app.SendStorageUnavailable)
	}
	return repository, nil
}

func (r *Repository) checkManagedGroupSchema(ctx context.Context) error {
	tx, err := r.begin(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer rollback(tx)
	advanced, err := membership.Advanced(ctx, tx)
	if err != nil || !advanced {
		return app.Fail(app.SendStorageUnavailable)
	}
	if _, err = tx.Exec(ctx, "SELECT group_id, muted, dissolved_at FROM newim.im_groups LIMIT 0"); err != nil {
		return privateStorageError(err)
	}
	return privateStorageError(tx.Commit(ctx))
}

func installGuard(poolConfig *pgxpool.Config, guard *postgresidentity.Guard) {
	if guard == nil {
		return
	}
	poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if err := guard.AfterConnect(ctx, conn); err != nil {
			return app.Fail(app.SendStorageUnavailable)
		}
		return nil
	}
	poolConfig.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
		if err := guard.Verify(ctx, conn); err != nil {
			return false, app.Fail(app.SendStorageUnavailable)
		}
		return true, nil
	}
}

// Close releases all pool resources after callers finish their transactions.
// Close 在调用方结束事务后释放全部连接池资源。
func (r *Repository) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

// Persist authorizes, persists and commits one send atomically.
// Persist 原子完成授权、持久化与提交；提交成功前不返回结果。
func (r *Repository) Persist(ctx context.Context, principal conversation.Principal, request protocol.Send, validate func() error, generate func() (app.Generated, error)) (app.PersistedMessage, error) {
	if r == nil || r.pool == nil || r.authorizer == nil || ctx == nil || !principal.Valid() || validate == nil || generate == nil || !identifier(request.ConversationID) {
		return app.PersistedMessage{}, app.Fail(app.SendInvalidInput)
	}
	if _, err := protocol.EncodeSend(request); err != nil {
		return app.PersistedMessage{}, app.Fail(app.SendInvalidInput)
	}
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return app.PersistedMessage{}, err
	}
	defer rollback(tx)
	if r.projectConversations || r.accountSettings {
		if err = conversationsync.LockSourceMembership(ctx, tx); err != nil {
			return app.PersistedMessage{}, privateStorageError(err)
		}
	}
	advanced, err := membership.Advanced(ctx, tx)
	if err != nil {
		return app.PersistedMessage{}, privateStorageError(err)
	}
	if r.managedGroups && !advanced {
		return app.PersistedMessage{}, app.Fail(app.SendStorageUnavailable)
	}
	if err = r.authorizer.Authorize(ctx, conversationTx{tx: tx}, principal.UserID, request.ConversationID); err != nil {
		return app.PersistedMessage{}, fixed(err)
	}
	receipt, found, err := r.checkSendPolicy(ctx, tx, principal.UserID, request, advanced)
	if found || err != nil {
		return receipt, err
	}
	if err = validate(); err != nil {
		return app.PersistedMessage{}, fixed(err)
	}
	generated, err := generate()
	if err != nil {
		return app.PersistedMessage{}, fixed(err)
	}
	if !validGenerated(generated) {
		return app.PersistedMessage{}, app.Fail(app.SendInvalidInput)
	}
	stored, err := r.persistMessage(ctx, tx, principal.UserID, request, generated)
	if err != nil {
		return app.PersistedMessage{}, mapStorageError(err)
	}
	same, err := protocol.SameIntent(stored.intent(), request)
	if err != nil {
		return app.PersistedMessage{}, app.Fail(app.SendStorageUnavailable)
	}
	if !same {
		return app.PersistedMessage{}, app.Fail(app.SendIDConflict)
	}
	persisted := stored.persisted()
	// A concurrent equal-intent retry is returned by persist_message's
	// unique-violation path with the winner's generated identity.
	// 并发等 intent 重试会通过 unique-violation 路径返回胜者的生成身份。
	persisted.Replayed = stored.ServerMsgID != generated.ServerMsgID
	if r.accountSettings && !persisted.Replayed {
		if err = conversationsync.RestoreAccountSettings(ctx, tx, request.ConversationID, persisted.ConversationSeq); err != nil {
			return app.PersistedMessage{}, privateStorageError(err)
		}
	}
	if r.projectConversations && !persisted.Replayed {
		if err = conversationsync.RecordSourceSend(ctx, tx, request.ConversationID, r.projectReadState, r.accountSettings); err != nil {
			return app.PersistedMessage{}, privateStorageError(err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return app.PersistedMessage{}, app.Fail(app.SendStorageUnavailable)
	}
	return persisted, nil
}

func (r *Repository) loadReceipt(ctx context.Context, tx pgx.Tx, senderID string, request protocol.Send) (app.PersistedMessage, bool, error) {
	existing, err := r.loadMessage(ctx, tx, senderID, request.ClientMsgID)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.PersistedMessage{}, false, nil
	}
	if err != nil {
		return app.PersistedMessage{}, false, mapStorageError(err)
	}
	same, err := protocol.SameIntent(existing.intent(), request)
	if err != nil {
		return app.PersistedMessage{}, false, app.Fail(app.SendStorageUnavailable)
	}
	if !same {
		return app.PersistedMessage{}, false, app.Fail(app.SendIDConflict)
	}
	if err = tx.Commit(ctx); err != nil {
		return app.PersistedMessage{}, false, app.Fail(app.SendStorageUnavailable)
	}
	receipt := existing.persisted()
	receipt.Replayed = true
	return receipt, true, nil
}

func (r *Repository) loadMessage(ctx context.Context, tx pgx.Tx, senderID, clientMsgID string) (storedRow, error) {
	return r.scanStoredMessage(
		tx.QueryRow(ctx, r.selectStoredMessage("newim.im_messages")+
			" WHERE sender_id=$1 AND client_msg_id=$2", senderID, clientMsgID),
	)
}

func (r *Repository) persistMessage(ctx context.Context, tx pgx.Tx, senderID string, request protocol.Send, generated app.Generated) (storedRow, error) {
	row := tx.QueryRow(ctx, r.selectStoredMessage("newim.persist_message($1,$2,$3,$4,$5,$6,$7,$8,$9)"),
		senderID, request.ClientMsgID, request.ConversationID, generated.ServerMsgID,
		request.Type, int(request.Version), generated.ServerTime, []byte(request.Payload), generated.EventID,
	)
	return r.scanStoredMessage(row)
}

func (r *Repository) selectStoredMessage(source string) string {
	const columns = "server_msg_id,client_msg_id,sender_id,conversation_id,conversation_seq,protocol_version,schema_version,message_type,server_time_ms,payload_bytes"
	if !r.actorTypes {
		return "SELECT " + columns + " FROM " + source
	}
	return "SELECT " + columns + ",a.actor_type FROM " + source +
		" AS m LEFT JOIN newim.im_user_actor_types a ON a.user_id=m.sender_id"
}

func (r *Repository) scanStoredMessage(row pgx.Row) (storedRow, error) {
	var stored storedRow
	var actor *string
	dest := []any{
		&stored.ServerMsgID, &stored.ClientMsgID, &stored.SenderID, &stored.ConversationID,
		&stored.ConversationSeq, &stored.ProtocolVersion, &stored.SchemaVersion,
		&stored.MessageType, &stored.ServerTime, &stored.Payload,
	}
	if r.actorTypes {
		dest = append(dest, &actor)
	}
	if err := row.Scan(dest...); err != nil {
		return storedRow{}, err
	}
	if actor != nil {
		stored.SenderType = protocol.SenderType(*actor)
		if !stored.SenderType.Valid() || stored.SenderType.IsZero() {
			return storedRow{}, app.Fail(app.SendStorageUnavailable)
		}
	}
	return stored, nil
}

type storedRow struct {
	ServerMsgID     string
	ClientMsgID     string
	SenderID        string
	SenderType      protocol.SenderType
	ConversationID  string
	ConversationSeq int64
	ProtocolVersion int32
	SchemaVersion   int32
	MessageType     string
	ServerTime      int64
	Payload         []byte
}

func (s storedRow) intent() protocol.Send {
	return protocol.Send{
		ProtocolVersion: uint32(s.ProtocolVersion),
		ClientMsgID:     s.ClientMsgID,
		ConversationID:  s.ConversationID,
		Version:         uint32(s.SchemaVersion),
		Type:            s.MessageType,
		Payload:         append([]byte(nil), s.Payload...),
	}
}

func (s storedRow) persisted() app.PersistedMessage {
	return app.PersistedMessage{
		ClientMsgID:     s.ClientMsgID,
		ConversationID:  s.ConversationID,
		SenderID:        s.SenderID,
		SenderType:      s.SenderType,
		ServerMsgID:     s.ServerMsgID,
		ConversationSeq: s.ConversationSeq,
		ServerTime:      s.ServerTime,
	}
}

type conversationTx struct{ tx pgx.Tx }

func (c conversationTx) QueryRow(ctx context.Context, query string, args ...any) conversation.Row {
	return c.tx.QueryRow(ctx, query, args...)
}

type conversationAuthorizer struct{}

func (conversationAuthorizer) Authorize(ctx context.Context, tx conversation.Tx, senderID, conversationID string) error {
	var locked string
	err := tx.QueryRow(ctx, "SELECT conversation_id FROM newim.im_conversations WHERE conversation_id=$1 FOR UPDATE", conversationID).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.SendConversationMissing)
	}
	if err != nil {
		return mapStorageError(err)
	}
	var member string
	err = tx.QueryRow(ctx, "SELECT user_id FROM newim.im_conversation_members WHERE conversation_id=$1 AND user_id=$2 FOR SHARE", conversationID, senderID).Scan(&member)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.SendUnauthorized)
	}
	if err != nil {
		return mapStorageError(err)
	}
	return nil
}

func (r *Repository) begin(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	if r == nil || r.pool == nil {
		return nil, app.Fail(app.SendStorageUnavailable)
	}
	tx, err := r.pool.BeginTx(ctx, options)
	if err != nil {
		return nil, mapStorageError(err)
	}
	if err = configure(ctx, tx); err != nil {
		rollback(tx)
		return nil, err
	}
	return tx, nil
}

func configure(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '3s'; SET LOCAL lock_timeout = '1s'; SET LOCAL transaction_timeout = '5s'; SET LOCAL idle_in_transaction_session_timeout = '5s'")
	return mapStorageError(err)
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
	return app.Fail(app.SendUnknown)
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
		case "NI001":
			return app.Fail(app.SendConversationMissing)
		case "NI002":
			return app.Fail(app.SendSequenceExhausted)
		case "NI003":
			return app.Fail(app.SendInvalidInput)
		case "40001", "40P01", "55P03", "57014":
			return app.Fail(app.SendLockUnavailable)
		case "08000", "08001", "08003", "08004", "08006", "08007", "57P01", "57P02", "57P03":
			return app.Fail(app.SendStorageUnavailable)
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return app.Fail(app.SendStorageUnavailable)
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return app.Fail(app.SendStorageUnavailable)
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return app.Fail(app.SendStorageUnavailable)
	}
	return app.Fail(app.SendUnknown)
}

func validGenerated(generated app.Generated) bool {
	return identifier(generated.ServerMsgID) && identifier(generated.EventID) && generated.ServerTime >= 0
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
