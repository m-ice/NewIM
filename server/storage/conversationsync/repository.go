// Package conversationsync stores bounded, durable account projections.
// conversationsync 保存有界、持久的账户会话投影。
package conversationsync

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

const requestTimeout = 5 * time.Second

// Config is trusted service configuration, never request input.
// Config 仅接受可信服务配置，不接受客户端输入。
type Config struct {
	DSN              string
	AllowLocalSocket bool
	MaxConnections   int
}

// Repository owns a bounded pool; credentials and driver errors stay private.
// Repository 持有有界连接池，不暴露凭证或驱动错误。
type Repository struct{ pool *pgxpool.Pool }

// Open requires verified TLS for TCP, or explicitly enabled local sockets.
// Open 的 TCP 必须校验主机证书，本地套接字须显式启用。
func Open(ctx context.Context, config Config) (*Repository, error) {
	if config.DSN == "" || config.MaxConnections < 0 || config.MaxConnections > 8 {
		return nil, app.Fail(app.StorageUnavailable)
	}
	c, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, fixed(err)
	}
	local := strings.HasPrefix(c.ConnConfig.Host, "/")
	if local {
		if !config.AllowLocalSocket || len(c.ConnConfig.Fallbacks) != 0 {
			return nil, app.Fail(app.StorageUnavailable)
		}
	} else {
		t := c.ConnConfig.TLSConfig
		if t == nil || t.InsecureSkipVerify || t.ServerName != c.ConnConfig.Host {
			return nil, app.Fail(app.StorageUnavailable)
		}
		for _, f := range c.ConnConfig.Fallbacks {
			if strings.HasPrefix(f.Host, "/") || f.TLSConfig == nil || f.TLSConfig.InsecureSkipVerify || f.TLSConfig.ServerName != f.Host {
				return nil, app.Fail(app.StorageUnavailable)
			}
		}
	}
	c.MaxConns = 8
	if config.MaxConnections != 0 {
		c.MaxConns = int32(config.MaxConnections)
	}
	c.MinConns = 0
	c.MinIdleConns = 0
	c.ConnConfig.ConnectTimeout = 2 * time.Second
	c.ConnConfig.DialFunc = (&net.Dialer{Timeout: 2 * time.Second}).DialContext
	c.ConnConfig.MaxProtocolMessageBodyLen = 1 << 20
	c.ConnConfig.RuntimeParams["statement_timeout"] = "3000"
	c.ConnConfig.RuntimeParams["lock_timeout"] = "1000"
	c.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	c.ConnConfig.RuntimeParams["transaction_timeout"] = "5000"
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, fixed(err)
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fixed(err)
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

// Begin starts a fresh bounded READ COMMITTED transaction owned by the caller.
// Begin 创建有界 READ COMMITTED 事务；调用方负责提交或回滚。
func (r *Repository) Begin(ctx context.Context) (pgx.Tx, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
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
	if errors.As(err, &known) {
		return app.Fail(known.Code)
	}
	return app.Fail(app.StorageUnavailable)
}

func identifier(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func userExists(ctx context.Context, tx pgx.Tx, user string) error {
	var present bool
	err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM newim.im_users WHERE user_id=$1)", user).Scan(&present)
	if err != nil {
		return fixed(err)
	}
	if !present {
		return app.Fail(app.Forbidden)
	}
	return nil
}
