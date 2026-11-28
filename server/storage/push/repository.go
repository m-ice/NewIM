// Package push stores device registrations and notification work in PostgreSQL.
package push

import (
	"context"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/m-ice/NewIM/server/push"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
)

const dbTimeout = 5 * time.Second

type Config struct {
	DSN              string
	AllowLocalSocket bool
	ApplicationName  string
	Guard            *postgresidentity.Guard
}

type Repository struct {
	pool  *pgxpool.Pool
	vault *app.Vault
}

func Open(ctx context.Context, cfg Config, vault *app.Vault) (*Repository, error) {
	if ctx == nil || cfg.DSN == "" || vault == nil {
		return nil, app.ErrConfig
	}
	cfgPool, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil || !verifiedTransport(cfgPool, cfg.AllowLocalSocket) {
		return nil, app.ErrStorage
	}
	cfgPool.MaxConns, cfgPool.MinConns, cfgPool.MinIdleConns = 4, 0, 0
	cfgPool.ConnConfig.ConnectTimeout = 2 * time.Second
	cfgPool.ConnConfig.DialFunc = (&net.Dialer{Timeout: 2 * time.Second}).DialContext
	cfgPool.ConnConfig.MaxProtocolMessageBodyLen = 1 << 20
	cfgPool.ConnConfig.RuntimeParams["statement_timeout"] = "3000"
	cfgPool.ConnConfig.RuntimeParams["lock_timeout"] = "1000"
	cfgPool.ConnConfig.RuntimeParams["transaction_timeout"] = "5000"
	cfgPool.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	cfgPool.ConnConfig.RuntimeParams["application_name"] = cfg.ApplicationName
	if cfg.Guard != nil {
		cfgPool.AfterConnect = cfg.Guard.AfterConnect
		cfgPool.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
			err := cfg.Guard.Verify(ctx, conn)
			return err == nil, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfgPool)
	if err != nil {
		return nil, app.ErrStorage
	}
	_, err = pool.Exec(ctx, `SELECT t.registration_version,t.session_id FROM newim.im_push_tokens t
		CROSS JOIN newim.im_push_state s CROSS JOIN newim.im_push_deliveries d WHERE false`)
	if err != nil {
		pool.Close()
		return nil, app.ErrStorage
	}
	return &Repository{pool: pool, vault: vault}, nil
}

func verifiedTransport(pool *pgxpool.Config, allowLocal bool) bool {
	cfg := pool.ConnConfig
	if len(cfg.Host) != 0 && cfg.Host[0] == '/' {
		return allowLocal && len(cfg.Fallbacks) == 0
	}
	if cfg.TLSConfig == nil || cfg.TLSConfig.InsecureSkipVerify || cfg.TLSConfig.ServerName != cfg.Host {
		return false
	}
	for _, fallback := range cfg.Fallbacks {
		if len(fallback.Host) == 0 || fallback.Host[0] == '/' || fallback.TLSConfig == nil ||
			fallback.TLSConfig.InsecureSkipVerify || fallback.TLSConfig.ServerName != fallback.Host {
			return false
		}
	}
	return true
}

func (r *Repository) Close() {
	r.pool.Close()
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
