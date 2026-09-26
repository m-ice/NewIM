package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	app "github.com/m-ice/NewIM/server/auth/session"
	messageservice "github.com/m-ice/NewIM/server/message"
	"github.com/m-ice/NewIM/server/messagehttp"
	store "github.com/m-ice/NewIM/server/storage/authsession"
	messagestore "github.com/m-ice/NewIM/server/storage/message"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
)

type messageConfigError struct{}

func (e *messageConfigError) Error() string { return string(api.CodeInvalidMessageConfig) }

var errInvalidMessageConfig error = &messageConfigError{}

const (
	messageHTTPEnv             = "NEWIM_MESSAGE_HTTP"
	authDSNEnv                 = "NEWIM_AUTH_DSN"
	authAllowLocalSocketEnv    = "NEWIM_AUTH_ALLOW_LOCAL_SOCKET"
	messageHTTPApplicationName = "newim-message-http"
)

// authRuntime owns the optional session/revoke routes and, when explicitly
// enabled, the exact loopback message-send route with its independent pool.
// authRuntime 持有可选的 session/revoke 路由；显式启用时还持有精确 loopback
// 消息发送路由及其独立连接池。
type authRuntime struct {
	repo         *store.Repository
	service      *app.Service
	handler      *bearerhttp.SessionHandler
	tokenRevoker *bearerhttp.TokenRevocationHandler
	messageRepo  *messagestore.Repository
	guard        *postgresidentity.Guard
	fatal        <-chan struct{}
	closer       *orderedCloser
}

// newAuthRuntimeFromEnv composes the enabled HTTP runtime from environment
// configuration. A missing DSN disables auth unless message HTTP explicitly
// requires the shared DSN.
// newAuthRuntimeFromEnv 根据环境配置组合已启用的 HTTP runtime；DSN 缺失时关闭
// auth，除非 message HTTP 已显式要求该共享 DSN。
func newAuthRuntimeFromEnv(ctx context.Context, cfg api.Config, logger *slog.Logger) (*authRuntime, []api.APIRoute, error) {
	dsn := strings.TrimSpace(os.Getenv(authDSNEnv))
	allowLocalSocket := os.Getenv(authAllowLocalSocketEnv) == "1"
	messageEnabled := os.Getenv(messageHTTPEnv) == "1"

	if !messageEnabled {
		if dsn == "" {
			return nil, nil, nil
		}
		return newAuthRuntimeWithGuard(ctx, cfg, dsn, allowLocalSocket, nil, logger)
	}
	if err := validateMessageRuntimeConfig(cfg, dsn, allowLocalSocket); err != nil {
		return nil, nil, err
	}

	fatal := make(chan struct{}, 1)
	guard := postgresidentity.NewGuard(func(error) {
		select {
		case fatal <- struct{}{}:
		default:
		}
	})
	runtime, routes, err := newAuthRuntimeWithGuard(ctx, cfg, dsn, allowLocalSocket, guard, logger)
	if err != nil {
		return nil, nil, messageStartupError(guard, err)
	}
	runtime.guard = guard
	runtime.fatal = fatal

	messageRepo, err := messagestore.Open(ctx, messagestore.Config{
		DSN:              dsn,
		AllowLocalSocket: allowLocalSocket,
		MaxConnections:   8,
		ApplicationName:  messageHTTPApplicationName,
		Guard:            guard,
	})
	if err != nil {
		runtime.Close()
		return nil, nil, messageStartupError(guard, err)
	}
	runtime.messageRepo = messageRepo
	runtime.closer = newOrderedCloser(messageRepo.Close, runtime.repo.Close)

	messageService, err := messageservice.NewService(messageRepo, messageservice.Config{
		IDs:      messageservice.NewCSPRNGIDGenerator(),
		Clock:    messageservice.ClockFunc(time.Now),
		Observer: messageLogObserver{logger: logger},
	})
	if err != nil {
		runtime.Close()
		return nil, nil, errInvalidMessageConfig
	}
	handler, err := messagehttp.NewHandler(runtime.service, messageService, protocol.MaxFrameBytes)
	if err != nil {
		runtime.Close()
		return nil, nil, errInvalidMessageConfig
	}
	if guard.Tripped() != nil {
		runtime.Close()
		return nil, nil, errInvalidMessageConfig
	}

	routes = append(routes, api.APIRoute{
		Name:           "message_send",
		Path:           messagehttp.Route,
		AllowedMethods: []string{http.MethodPost},
		MaxBodyBytes:   protocol.MaxFrameBytes,
		RejectQuery:    true,
		Handler:        handler,
	})
	return runtime, routes, nil
}

func newAuthRuntimeWithGuard(ctx context.Context, cfg api.Config, dsn string, allowLocalSocket bool, guard *postgresidentity.Guard, logger *slog.Logger) (*authRuntime, []api.APIRoute, error) {
	if !loopbackAPIAddress(cfg.APIAddr) {
		return nil, nil, api.ErrInvalidAuthConfig
	}
	repo, err := store.Open(ctx, store.Config{
		DSN:              dsn,
		AllowLocalSocket: allowLocalSocket,
		MaxConnections:   8,
		ApplicationName:  "newim-auth-http",
		Guard:            guard,
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
	runtime := &authRuntime{
		repo:         repo,
		service:      service,
		handler:      handler,
		tokenRevoker: tokenRevoker,
	}
	runtime.closer = newOrderedCloser(nil, repo.Close)
	return runtime, []api.APIRoute{
		{Name: "session", Path: bearerhttp.Route, Handler: handler},
		{Name: "session_token_revoke", Path: bearerhttp.TokenRoute, AllowedMethods: []string{http.MethodDelete}, Handler: tokenRevoker},
	}, nil
}

func validateMessageRuntimeConfig(cfg api.Config, dsn string, allowLocalSocket bool) error {
	if dsn == "" || !allowLocalSocket || !loopbackAPIAddress(cfg.APIAddr) {
		return errInvalidMessageConfig
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return errInvalidMessageConfig
	}
	host := poolConfig.ConnConfig.Host
	if host == "" || !filepath.IsAbs(host) || strings.Contains(host, ",") || len(poolConfig.ConnConfig.Fallbacks) != 0 {
		return errInvalidMessageConfig
	}
	return nil
}

func messageStartupError(guard *postgresidentity.Guard, err error) error {
	if guard != nil && guard.Tripped() != nil {
		return errInvalidMessageConfig
	}
	var configErr *api.Error
	if errors.As(err, &configErr) && configErr != nil && configErr.Code == api.CodeInvalidAuthConfig {
		return errInvalidMessageConfig
	}
	return err
}

// Fatal returns the server-owned terminal signal produced by the identity guard.
// Fatal 返回 identity guard 产生的 server 自有 terminal 信号。
func (r *authRuntime) Fatal() <-chan struct{} {
	if r == nil {
		return nil
	}
	return r.fatal
}

// Close releases the message pool before the auth pool and is idempotent.
// Close 先释放 message pool 再释放 auth pool，且幂等。
func (r *authRuntime) Close() {
	if r != nil && r.closer != nil {
		r.closer.Close()
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

type messageLogObserver struct {
	logger *slog.Logger
}

func (o messageLogObserver) Observe(observation messageservice.Observation) {
	if o.logger == nil || observation.Operation == "" || observation.Code == "" {
		return
	}
	o.logger.Info("message_send",
		slog.String("operation", observation.Operation),
		slog.String("code", string(observation.Code)),
		slog.Int64("elapsed_ms", observation.Elapsed.Milliseconds()),
	)
}
