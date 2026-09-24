// Package webhook stores durable webhook fan-out and delivery leases.
// webhook 存储持久 Webhook fan-out 和投递租约。
package webhook

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/m-ice/NewIM/server/webhook"
)

const requestTimeout = 5 * time.Second

// Config is trusted database configuration, never request input.
// Config 是可信数据库配置，不接受请求输入。
type Config struct {
	DSN              string
	AllowLocalSocket bool
	MaxConnections   int
	ApplicationName  string
}

// Repository owns a bounded pgx pool and webhook persistence operations.
// Repository 持有有界 pgx 池和 Webhook 持久化操作。
type Repository struct{ pool *pgxpool.Pool }

// AcquireWorkerLock enforces one webhook worker process per database.
// AcquireWorkerLock 强制每个数据库只有一个 Webhook worker 进程。
type WorkerLock struct {
	conn     *pgxpool.Conn
	released bool
}

func (r *Repository) AcquireWorkerLock(ctx context.Context) (*WorkerLock, error) {
	if r == nil || r.pool == nil || ctx == nil {
		return nil, app.Fail(app.CodeStorageUnavailable)
	}
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return nil, mapStorageError(err)
	}
	var acquired bool
	if err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext('newim.webhook.worker'))`).Scan(&acquired); err != nil || !acquired {
		conn.Release()
		if err != nil {
			return nil, mapStorageError(err)
		}
		return nil, app.Fail(app.CodeBacklogPaused)
	}
	return &WorkerLock{conn: conn}, nil
}

// Check verifies the lock is still held by the same live backend connection.
// Check 校验同一存活 backend 连接仍持有锁；连接丢失或锁消失返回错误。
func (l *WorkerLock) Check(ctx context.Context) error {
	if l == nil || l.conn == nil || l.released || ctx == nil {
		return app.Fail(app.CodeBacklogPaused)
	}
	var held bool
	if err := l.conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted)`).Scan(&held); err != nil || !held {
		return app.Fail(app.CodeBacklogPaused)
	}
	return nil
}

// Release unlocks the advisory lock and returns its dedicated connection.
// Release 释放 advisory lock 并归还其专用连接。
func (l *WorkerLock) Release() {
	if l == nil || l.conn == nil || l.released {
		return
	}
	l.released = true
	_, _ = l.conn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtext('newim.webhook.worker'))`)
	l.conn.Release()
}

// Open requires verified TLS for TCP or explicitly enabled local sockets.
// Open 的 TCP 必须校验主机证书，本地套接字须显式启用。
func Open(ctx context.Context, config Config) (*Repository, error) {
	if ctx == nil || config.DSN == "" || config.MaxConnections < 0 || config.MaxConnections > 8 {
		return nil, app.Fail(app.CodeInvalidConfig)
	}
	poolConfig, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, app.Fail(app.CodeStorageUnavailable)
	}
	local := len(poolConfig.ConnConfig.Host) > 0 && poolConfig.ConnConfig.Host[0] == '/'
	if local {
		if !config.AllowLocalSocket || len(poolConfig.ConnConfig.Fallbacks) != 0 {
			return nil, app.Fail(app.CodeStorageUnavailable)
		}
	} else {
		tlsConfig := poolConfig.ConnConfig.TLSConfig
		if tlsConfig == nil || tlsConfig.InsecureSkipVerify || tlsConfig.ServerName != poolConfig.ConnConfig.Host {
			return nil, app.Fail(app.CodeStorageUnavailable)
		}
		for _, fallback := range poolConfig.ConnConfig.Fallbacks {
			if len(fallback.Host) == 0 || fallback.Host[0] == '/' || fallback.TLSConfig == nil || fallback.TLSConfig.InsecureSkipVerify || fallback.TLSConfig.ServerName != fallback.Host {
				return nil, app.Fail(app.CodeStorageUnavailable)
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
	poolConfig.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	poolConfig.ConnConfig.RuntimeParams["lock_timeout"] = "2000"
	poolConfig.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "7000"
	poolConfig.ConnConfig.RuntimeParams["transaction_timeout"] = "7000"
	if config.ApplicationName != "" {
		poolConfig.ConnConfig.RuntimeParams["application_name"] = config.ApplicationName
	} else {
		poolConfig.ConnConfig.RuntimeParams["application_name"] = "newim-webhook"
	}
	poolCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(poolCtx, poolConfig)
	if err != nil {
		return nil, app.Fail(app.CodeStorageUnavailable)
	}
	if err = pool.Ping(poolCtx); err != nil {
		pool.Close()
		return nil, app.Fail(app.CodeStorageUnavailable)
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

// CancelRevoked requeues expired leases and cancels deliveries for revoked endpoints.
// CancelRevoked 重排过期租约，并取消已 revoke endpoint 的 deliveries。
func (r *Repository) CancelRevoked(ctx context.Context, now time.Time) (int, error) {
	if r == nil || r.pool == nil || ctx == nil || now.IsZero() {
		return 0, app.Fail(app.CodeStorageUnavailable)
	}
	tx, err := r.begin(ctx)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, `UPDATE newim.im_webhook_deliveries
SET status='retry', next_attempt_at=clock_timestamp(), lease_owner=NULL, lease_token=NULL, lease_expires_at=NULL, updated_at=clock_timestamp()
WHERE status='leased' AND lease_expires_at <= clock_timestamp()`); err != nil {
		return 0, mapStorageError(err)
	}
	tag, err := tx.Exec(ctx, `UPDATE newim.im_webhook_deliveries d
SET status='cancelled', completed_at=clock_timestamp(), lease_owner=NULL, lease_token=NULL, lease_expires_at=NULL,
    last_error_code=$1, updated_at=clock_timestamp()
FROM newim.im_webhook_endpoints e
WHERE d.destination_id=e.destination_id AND e.revoked_at IS NOT NULL
  AND d.status IN ('pending','retry','leased')`, app.CodeDeliveryCancelled)
	if err != nil {
		return 0, mapStorageError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, mapStorageError(err)
	}
	return int(tag.RowsAffected()), nil
}

// Counts returns bounded global and per-destination pending work.
// Counts 返回有界全局和每 destination 待处理工作。
func (r *Repository) Counts(ctx context.Context) (app.Counts, error) {
	var counts app.Counts
	if r == nil || r.pool == nil || ctx == nil {
		return counts, app.Fail(app.CodeStorageUnavailable)
	}
	if err := r.pool.QueryRow(ctx, `SELECT
count(*) FILTER (WHERE status IN ('pending','retry','leased'))::int,
count(*) FILTER (WHERE status='pending')::int,
count(*) FILTER (WHERE status='retry')::int,
count(*) FILTER (WHERE status='leased')::int,
count(*) FILTER (WHERE status='dead_letter')::int
FROM newim.im_webhook_deliveries`).Scan(&counts.Total, &counts.Pending, &counts.Retry, &counts.Leased, &counts.DeadLetter); err != nil {
		return counts, mapStorageError(err)
	}
	if err := r.pool.QueryRow(ctx, `SELECT COALESCE(max(c),0)::int FROM (
SELECT destination_id,count(*) c FROM newim.im_webhook_deliveries
WHERE status IN ('pending','retry','leased') GROUP BY destination_id) s`).Scan(&counts.MaxDestination); err != nil {
		return counts, mapStorageError(err)
	}
	return counts, nil
}

// Fanout durably creates delivery rows for one bounded page of unmarked outbox events.
// Fanout 为一个有界未标记 outbox 事件页持久创建 delivery 行。
func (r *Repository) Fanout(ctx context.Context, now time.Time, limit, maxDestinationQueue, maxGlobalQueue int) (int, error) {
	if r == nil || r.pool == nil || ctx == nil || now.IsZero() || limit < 1 || limit > 1000 || maxDestinationQueue < 1 || maxDestinationQueue > 1_000_000 || maxGlobalQueue < 1 || maxGlobalQueue > 1_000_000 {
		return 0, app.Fail(app.CodeInvalidConfig)
	}
	tx, err := r.begin(ctx)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('newim.webhook.fanout'))`); err != nil {
		return 0, mapStorageError(err)
	}
	var globalPending int
	if err = tx.QueryRow(ctx, `SELECT count(*)::int FROM newim.im_webhook_deliveries WHERE status IN ('pending','retry','leased')`).Scan(&globalPending); err != nil {
		return 0, mapStorageError(err)
	}
	rows, err := tx.Query(ctx, `SELECT event_id FROM newim.im_outbox_events
WHERE webhook_fanout_at IS NULL AND webhook_fanout_error IS NULL
ORDER BY created_at,event_id
LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, mapStorageError(err)
	}
	var eventIDs []string
	for rows.Next() {
		var eventID string
		if err = rows.Scan(&eventID); err != nil {
			rows.Close()
			return 0, mapStorageError(err)
		}
		eventIDs = append(eventIDs, eventID)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, mapStorageError(err)
	}
	var firstFanoutError error
	for _, eventID := range eventIDs {
		endpoints, err := tx.Query(ctx, `SELECT e.destination_id,COALESCE(e.active_revision,0)
FROM newim.im_webhook_endpoints e
WHERE e.status='active' AND e.revoked_at IS NULL
ORDER BY e.destination_id
LIMIT 65
FOR UPDATE`)
		if err != nil {
			return 0, mapStorageError(err)
		}
		type endpointRef struct {
			destination string
			revision    int64
		}
		var refs []endpointRef
		for endpoints.Next() {
			var ref endpointRef
			if err = endpoints.Scan(&ref.destination, &ref.revision); err != nil {
				endpoints.Close()
				return 0, mapStorageError(err)
			}
			refs = append(refs, ref)
		}
		if err = endpoints.Err(); err != nil {
			endpoints.Close()
			return 0, mapStorageError(err)
		}
		endpoints.Close()
		var fanoutError app.Code
		if len(refs) > app.MaxEndpointsPerEvent {
			fanoutError = app.CodeFanoutLimit
		}
		queueFull := false
		for _, ref := range refs {
			if fanoutError != "" {
				break
			}
			if ref.revision <= 0 {
				fanoutError = app.CodeFanoutLimit
				break
			}
			var pending int
			if err = tx.QueryRow(ctx, `SELECT count(*)::int FROM newim.im_webhook_deliveries WHERE destination_id=$1 AND status IN ('pending','retry','leased')`, ref.destination).Scan(&pending); err != nil {
				return 0, mapStorageError(err)
			}
			if pending >= maxDestinationQueue {
				queueFull = true
				break
			}
		}
		if fanoutError != "" {
			if _, err = tx.Exec(ctx, `UPDATE newim.im_outbox_events SET webhook_fanout_error=$2 WHERE event_id=$1 AND webhook_fanout_at IS NULL`, eventID, string(fanoutError)); err != nil {
				return 0, mapStorageError(err)
			}
			if firstFanoutError == nil {
				firstFanoutError = app.Fail(fanoutError)
			}
			continue
		}
		if queueFull {
			continue
		}
		// Admit at most one event's worth of fan-out above the high-water mark;
		// this avoids starving a valid event whose endpoint count is itself large.
		if globalPending >= maxGlobalQueue {
			continue
		}
		for _, ref := range refs {
			deliveryID, idErr := newID()
			if idErr != nil {
				return 0, app.Fail(app.CodeStorageUnavailable)
			}
			if _, err = tx.Exec(ctx, `INSERT INTO newim.im_webhook_deliveries
(delivery_id,event_id,destination_id,endpoint_revision,status,next_attempt_at,created_at,updated_at)
VALUES ($1,$2,$3,$4,'pending',clock_timestamp(),clock_timestamp(),clock_timestamp())
ON CONFLICT (event_id,destination_id) DO NOTHING`, deliveryID, eventID, ref.destination, ref.revision); err != nil {
				return 0, mapStorageError(err)
			}
		}
		globalPending += len(refs)
		if _, err = tx.Exec(ctx, `UPDATE newim.im_outbox_events SET webhook_fanout_at=clock_timestamp() WHERE event_id=$1 AND webhook_fanout_at IS NULL`, eventID); err != nil {
			return 0, mapStorageError(err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, mapStorageError(err)
	}
	return len(eventIDs), firstFanoutError
}

// Claim leases a bounded set of due deliveries for one worker owner.
// Claim 为单个 worker owner 租用一组有界到期 delivery。
func (r *Repository) Claim(ctx context.Context, now time.Time, owner string, leaseTTL time.Duration, limit int) ([]app.Delivery, error) {
	if r == nil || r.pool == nil || ctx == nil || now.IsZero() || owner == "" || leaseTTL <= 0 || leaseTTL > 10*time.Minute || limit < 1 || limit > 1000 {
		return nil, app.Fail(app.CodeInvalidConfig)
	}
	tx, err := r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, `UPDATE newim.im_webhook_deliveries
SET status='retry', next_attempt_at=clock_timestamp(), lease_owner=NULL, lease_token=NULL, lease_expires_at=NULL, updated_at=clock_timestamp()
WHERE status='leased' AND lease_expires_at <= clock_timestamp()`); err != nil {
		return nil, mapStorageError(err)
	}
	rows, err := tx.Query(ctx, `SELECT d.delivery_id,d.attempts,d.destination_id,d.endpoint_revision,
	r.url,r.key_id,r.secret_nonce,r.secret_ciphertext,
o.event_id,m.server_msg_id,m.client_msg_id,m.sender_id,m.conversation_id,m.conversation_seq,
m.server_time_ms,m.protocol_version,m.schema_version,m.message_type,m.payload_bytes
FROM newim.im_webhook_deliveries d
JOIN newim.im_webhook_endpoint_revisions r ON r.destination_id=d.destination_id AND r.revision=d.endpoint_revision
JOIN newim.im_webhook_endpoints e ON e.destination_id=d.destination_id
JOIN newim.im_outbox_events o ON o.event_id=d.event_id
JOIN newim.im_messages m ON m.server_msg_id=o.server_msg_id
WHERE d.status IN ('pending','retry') AND d.next_attempt_at <= clock_timestamp()
  AND d.endpoint_revision IS NOT NULL AND e.status='active' AND e.revoked_at IS NULL
ORDER BY d.next_attempt_at,d.delivery_id
LIMIT $1 FOR UPDATE OF d SKIP LOCKED`, limit)
	if err != nil {
		return nil, mapStorageError(err)
	}
	var deliveries []app.Delivery
	for rows.Next() {
		var delivery app.Delivery
		var payload []byte
		if err = rows.Scan(&delivery.ID, &delivery.Attempts, &delivery.DestinationID, &delivery.EndpointRevision,
			&delivery.URL, &delivery.KeyID, &delivery.Secret.Nonce, &delivery.Secret.Ciphertext,
			&delivery.Event.ID, &delivery.Event.ServerMsgID, &delivery.Event.ClientMsgID,
			&delivery.Event.SenderID, &delivery.Event.ConversationID, &delivery.Event.ConversationSeq,
			&delivery.Event.ServerTime, &delivery.Event.ProtocolVersion, &delivery.Event.SchemaVersion,
			&delivery.Event.MessageType, &payload); err != nil {
			rows.Close()
			return nil, mapStorageError(err)
		}
		delivery.Event.Payload = append([]byte(nil), payload...)
		delivery.Secret.DestinationID = delivery.DestinationID
		delivery.Secret.Revision = delivery.EndpointRevision
		delivery.Secret.URL = delivery.URL
		delivery.Secret.KeyID = delivery.KeyID
		delivery.LeaseToken, err = newID()
		if err != nil {
			rows.Close()
			return nil, app.Fail(app.CodeStorageUnavailable)
		}
		delivery.Status = "leased"
		delivery.NextAttemptAt = time.Now().Add(leaseTTL)
		deliveries = append(deliveries, delivery)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, mapStorageError(err)
	}
	rows.Close()
	for _, delivery := range deliveries {
		if _, err = tx.Exec(ctx, `UPDATE newim.im_webhook_deliveries
SET status='leased',lease_owner=$1,lease_token=$2,lease_expires_at=clock_timestamp()+make_interval(secs=>$3),next_attempt_at=clock_timestamp()+make_interval(secs=>$3),updated_at=clock_timestamp()
WHERE delivery_id=$4`, owner, delivery.LeaseToken, leaseTTL.Seconds(), delivery.ID); err != nil {
			return nil, mapStorageError(err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, mapStorageError(err)
	}
	return deliveries, nil
}

// BeginAttempt increments the HTTP-attempt counter only while the caller still owns a live lease.
// BeginAttempt 仅在调用方仍持有有效租约时递增 HTTP attempt 计数器。
func (r *Repository) BeginAttempt(ctx context.Context, deliveryID, leaseToken string, now time.Time, maxAttempts int, required time.Duration) (bool, error) {
	if r == nil || r.pool == nil || ctx == nil || deliveryID == "" || leaseToken == "" || now.IsZero() || maxAttempts < 1 || maxAttempts > 8 || required <= 0 || required > 2*time.Minute {
		return false, app.Fail(app.CodeInvalidConfig)
	}
	tx, err := r.begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	tag, err := tx.Exec(ctx, `UPDATE newim.im_webhook_deliveries d
SET attempts=attempts+1,lease_expires_at=GREATEST(lease_expires_at,clock_timestamp()+make_interval(secs=>$4)),updated_at=clock_timestamp()
FROM newim.im_webhook_endpoints e
WHERE d.delivery_id=$1 AND d.lease_token=$2 AND d.status='leased'
  AND d.lease_expires_at >= clock_timestamp()+make_interval(secs=>$4) AND d.attempts < $3
  AND d.destination_id=e.destination_id AND e.status='active' AND e.revoked_at IS NULL`, deliveryID, leaseToken, maxAttempts, required.Seconds())
	if err != nil {
		return false, mapStorageError(err)
	}
	if tag.RowsAffected() == 1 {
		if err = tx.Commit(ctx); err != nil {
			return false, mapStorageError(err)
		}
		return true, nil
	}
	var status string
	var currentToken string
	var attempts int
	if err = tx.QueryRow(ctx, `SELECT status,COALESCE(lease_token,''),attempts FROM newim.im_webhook_deliveries WHERE delivery_id=$1 FOR UPDATE`, deliveryID).Scan(&status, &currentToken, &attempts); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, app.Fail(app.CodeLeaseLost)
		}
		return false, mapStorageError(err)
	}
	if status == "leased" && currentToken == leaseToken && attempts >= maxAttempts {
		if _, err = tx.Exec(ctx, `UPDATE newim.im_webhook_deliveries
SET status='dead_letter',completed_at=clock_timestamp(),last_error_code=$2,lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=clock_timestamp()
WHERE delivery_id=$1`, deliveryID, app.CodeDeliveryDeadLetter); err != nil {
			return false, mapStorageError(err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, mapStorageError(err)
	}
	return false, nil
}

// Finish applies a fenced terminal or retry transition and rejects stale owners.
// Finish 应用带 fencing 的终态或重试转换，并拒绝 stale owner。
func (r *Repository) Finish(ctx context.Context, deliveryID, leaseToken string, attempts int, now time.Time, outcome app.Outcome) error {
	if r == nil || r.pool == nil || ctx == nil || deliveryID == "" || leaseToken == "" || attempts < 0 || now.IsZero() {
		return app.Fail(app.CodeInvalidConfig)
	}
	if outcome.Status == "retry" && outcome.RetryDelay <= 0 {
		outcome.RetryDelay = time.Millisecond
	}
	if outcome.CompletedAt.IsZero() {
		outcome.CompletedAt = now
	}
	if outcome.HTTPStatus < 100 || outcome.HTTPStatus > 599 {
		outcome.HTTPStatus = 0
	}
	tx, err := r.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	tag, err := tx.Exec(ctx, `UPDATE newim.im_webhook_deliveries
SET status=$2::text,
    next_attempt_at=CASE WHEN $2::text='retry' THEN clock_timestamp()+make_interval(secs=>$3) ELSE clock_timestamp() END,
    completed_at=CASE WHEN $2::text IN ('delivered','dead_letter','cancelled') THEN clock_timestamp() ELSE NULL END,
    lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,
    last_http_status=NULLIF($4,0),last_error_code=NULLIF($5::text,''),updated_at=clock_timestamp()
WHERE delivery_id=$1 AND lease_token=$6 AND status='leased' AND lease_expires_at > clock_timestamp()`,
		deliveryID, outcome.Status, outcome.RetryDelay.Seconds(), outcome.HTTPStatus, string(outcome.ErrorCode), leaseToken)
	if err != nil {
		return mapStorageError(err)
	}
	if tag.RowsAffected() != 1 {
		return app.Fail(app.CodeLeaseLost)
	}
	if err = tx.Commit(ctx); err != nil {
		return mapStorageError(err)
	}
	return nil
}

func (r *Repository) begin(ctx context.Context) (pgx.Tx, error) {
	if r == nil || r.pool == nil {
		return nil, app.Fail(app.CodeStorageUnavailable)
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, mapStorageError(err)
	}
	if _, err = tx.Exec(ctx, "SET LOCAL statement_timeout='5000'; SET LOCAL lock_timeout='2000'; SET LOCAL transaction_timeout='7000'; SET LOCAL idle_in_transaction_session_timeout='7000'"); err != nil {
		rollback(tx)
		return nil, mapStorageError(err)
	}
	return tx, nil
}

func newID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

func rollback(tx pgx.Tx) {
	if tx == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
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
		case "40001", "40P01", "55P03", "57014":
			return app.Fail(app.CodeStorageUnavailable)
		}
	}
	return app.Fail(app.CodeStorageUnavailable)
}
