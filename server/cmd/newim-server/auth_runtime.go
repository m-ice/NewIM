package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	app "github.com/m-ice/NewIM/server/auth/session"
	store "github.com/m-ice/NewIM/server/storage/authsession"
)

// authRuntime owns the optional bearer-session route and its database pool.
// authRuntime 持有可选的 bearer-session 路由及其数据库池。
type authRuntime struct {
	repo         *store.Repository
	handler      *bearerhttp.SessionHandler
	tokenRevoker *bearerhttp.TokenRevocationHandler
}

// newAuthRuntimeFromEnv returns no runtime when NEWIM_AUTH_DSN is absent.
// newAuthRuntimeFromEnv 在 NEWIM_AUTH_DSN 缺失时返回 nil runtime；配置不完整必须 fail-closed。
func newAuthRuntimeFromEnv(ctx context.Context, cfg api.Config, logger *slog.Logger) (*authRuntime, []api.APIRoute, error) {
	dsn := strings.TrimSpace(os.Getenv("NEWIM_AUTH_DSN"))
	if dsn == "" {
		return nil, nil, nil
	}
	if !loopbackAPIAddress(cfg.APIAddr) {
		return nil, nil, api.ErrInvalidAuthConfig
	}
	repo, err := store.Open(ctx, store.Config{
		DSN:              dsn,
		AllowLocalSocket: os.Getenv("NEWIM_AUTH_ALLOW_LOCAL_SOCKET") == "1",
		MaxConnections:   8,
		ApplicationName:  "newim-auth-http",
	})
	if err != nil {
		return nil, nil, err
	}
	service, err := app.NewService(repo, app.Config{
		Clock:    app.ClockFunc(time.Now),
		Observer: authLogObserver{logger: logger},
	})
	if err != nil {
		repo.Close()
		return nil, nil, err
	}
	handler, err := bearerhttp.NewSessionHandler(service)
	if err != nil {
		repo.Close()
		return nil, nil, err
	}
	tokenRevoker, err := bearerhttp.NewTokenRevocationHandler(service)
	if err != nil {
		repo.Close()
		return nil, nil, err
	}
	runtime := &authRuntime{repo: repo, handler: handler, tokenRevoker: tokenRevoker}
	return runtime, []api.APIRoute{
		{Name: "session", Path: bearerhttp.Route, Handler: handler},
		{Name: "session_token_revoke", Path: bearerhttp.TokenRoute, AllowedMethods: []string{"DELETE"}, Handler: tokenRevoker},
	}, nil
}

// Close releases the auth database pool.
// Close 释放 auth 数据库池。
func (r *authRuntime) Close() {
	if r != nil && r.repo != nil {
		r.repo.Close()
	}
}

func loopbackAPIAddress(value string) bool {
	host, _, err := net.SplitHostPort(value)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type authLogObserver struct {
	logger *slog.Logger
}

func (o authLogObserver) Observe(observation app.Observation) {
	if o.logger == nil || observation.Operation == "" || observation.Code == "" {
		return
	}
	o.logger.Info("auth_session",
		slog.String("operation", observation.Operation),
		slog.String("code", string(observation.Code)),
		slog.Int64("elapsed_ms", observation.Elapsed.Milliseconds()),
	)
}
