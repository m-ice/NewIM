package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/auth/login"
	app "github.com/m-ice/NewIM/server/auth/session"
	messageservice "github.com/m-ice/NewIM/server/message"
	"github.com/m-ice/NewIM/server/messagehttp"
	store "github.com/m-ice/NewIM/server/storage/authsession"
	messagestore "github.com/m-ice/NewIM/server/storage/message"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
	userstore "github.com/m-ice/NewIM/server/storage/user"
)

type messageConfigError struct{}

func (e *messageConfigError) Error() string { return string(api.CodeInvalidMessageConfig) }

var errInvalidMessageConfig error = &messageConfigError{}

const (
	messageHTTPEnv             = "NEWIM_MESSAGE_HTTP"
	privateMessagePolicyEnv    = "NEWIM_PRIVATE_MESSAGE_POLICY"
	strangerMessagePolicyEnv   = "NEWIM_STRANGER_MESSAGE_POLICY"
	authDSNEnv                 = "NEWIM_AUTH_DSN"
	authAllowLocalSocketEnv    = "NEWIM_AUTH_ALLOW_LOCAL_SOCKET"
	authIssuerTokenFileEnv     = "NEWIM_AUTH_ISSUER_TOKEN_FILE"
	authIssuerTTLEnv           = "NEWIM_AUTH_ISSUER_TTL_SECONDS"
	authAdmissionEnv           = "NEWIM_AUTH_DEVICE_ADMISSION"
	authAccountRevocationEnv   = "NEWIM_AUTH_ACCOUNT_REVOCATION"
	conversationProjectionEnv  = "NEWIM_CONVERSATION_PROJECTION"
	conversationReadSyncEnv    = "NEWIM_CONVERSATION_READ_SYNC"
	authMaxDevicesEnv          = "NEWIM_AUTH_MAX_DEVICES"
	authMaxPerPlatformEnv      = "NEWIM_AUTH_MAX_DEVICES_PER_PLATFORM"
	authSingleDeviceEnv        = "NEWIM_AUTH_SINGLE_DEVICE"
	userProfileEnv             = "NEWIM_USER_PROFILE"
	messageHTTPApplicationName = "newim-message-http"
)

// authRuntime owns the loopback auth routes and optional message and profile pools.
// authRuntime 持有回环鉴权路由，以及可选的消息和资料连接池。
type authRuntime struct {
	repo         *store.Repository
	service      *app.Service
	handler      *bearerhttp.SessionHandler
	tokenRevoker *bearerhttp.TokenRevocationHandler
	messageRepo  *messagestore.Repository
	profileRepo  *userstore.Repository
	guard        *postgresidentity.Guard
	terminal     *terminalState
	closer       *orderedCloser
}

func newAuthRuntimeFromEnv(ctx context.Context, cfg api.Config, logger *slog.Logger) (*authRuntime, []api.APIRoute, error) {
	limits, admission, err := deviceAdmissionConfig()
	if err != nil {
		return nil, nil, err
	}
	revocationValue := os.Getenv(authAccountRevocationEnv)
	accountRevocation := revocationValue == "1"
	if (accountRevocation && !admission) || (revocationValue != "" && revocationValue != "0" && !accountRevocation) {
		return nil, nil, api.ErrInvalidAuthConfig
	}
	profileValue := os.Getenv(userProfileEnv)
	profileEnabled := profileValue == "1"
	if profileValue != "" && profileValue != "0" && !profileEnabled {
		return nil, nil, api.ErrInvalidAuthConfig
	}
	tokenFile, ttlConfig := os.Getenv(authIssuerTokenFileEnv), os.Getenv(authIssuerTTLEnv)
	if tokenFile == "" && ttlConfig == "" {
		if admission || profileEnabled {
			return nil, nil, api.ErrInvalidAuthConfig
		}
		return newAuthAndMessageRuntime(ctx, cfg, false, false, logger)
	}
	seconds, err := strconv.ParseInt(ttlConfig, 10, 32)
	if err != nil || seconds < int64(app.MinTTL/time.Second) || seconds > int64(app.MaxTTL/time.Second) ||
		!filepath.IsAbs(tokenFile) || !loopbackAPIAddress(cfg.APIAddr) || strings.TrimSpace(os.Getenv(authDSNEnv)) == "" {
		return nil, nil, api.ErrInvalidAuthConfig
	}
	runtime, routes, err := newAuthAndMessageRuntime(ctx, cfg, admission, profileEnabled, logger)
	if err != nil {
		if errors.Is(err, errInvalidMessageConfig) {
			return nil, nil, err
		}
		return nil, nil, api.ErrInvalidAuthConfig
	}
	handler, err := login.NewIssuerHandler(runtime.service, tokenFile, time.Duration(seconds)*time.Second)
	if err != nil {
		runtime.Close()
		return nil, nil, api.ErrInvalidAuthConfig
	}
	routes = append(routes, trustedPOSTRoute("auth_issuer", login.IssuerRoute, handler, login.IssuerBodyLimit))
	if admission {
		admit, err := login.NewAdmissionHandler(handler, runtime.service, limits)
		if err != nil {
			runtime.Close()
			return nil, nil, api.ErrInvalidAuthConfig
		}
		routes = append(routes, trustedPOSTRoute("auth_admission", login.AdmissionRoute, admit, login.IssuerBodyLimit))
	}
	if accountRevocation {
		if err := runtime.repo.CheckAccountRevocationSchema(ctx); err != nil {
			runtime.Close()
			return nil, nil, api.ErrInvalidAuthConfig
		}
		revocation, err := login.NewAccountRevocationHandler(handler, runtime.service)
		if err != nil {
			runtime.Close()
			return nil, nil, api.ErrInvalidAuthConfig
		}
		routes = append(routes, trustedPOSTRoute("auth_revoke_account", login.AccountRevocationRoute, revocation, login.IssuerBodyLimit))
	}
	if profileEnabled {
		provision, err := login.NewUserProvisionHandler(handler, runtime.profileRepo)
		if err != nil {
			runtime.Close()
			return nil, nil, api.ErrInvalidAuthConfig
		}
		routes = append(routes, trustedPOSTRoute("user_provision", login.UserProvisionRoute, provision, login.UserProvisionBodyLimit))
	}
	return runtime, routes, nil
}

func trustedPOSTRoute(name, path string, handler http.Handler, bodyLimit int64) api.APIRoute {
	return api.APIRoute{
		Name:           name,
		Path:           path,
		AllowedMethods: []string{http.MethodPost},
		MaxBodyBytes:   bodyLimit,
		RejectQuery:    true,
		Handler:        handler,
	}
}

func deviceAdmissionConfig() (app.DeviceLimits, bool, error) {
	switch os.Getenv(authAdmissionEnv) {
	case "", "0":
		return app.DeviceLimits{}, false, nil
	case "1":
	default:
		return app.DeviceLimits{}, false, api.ErrInvalidAuthConfig
	}
	account, accountOK := configuredDeviceLimit(os.Getenv(authMaxDevicesEnv), 8)
	platform, platformOK := configuredDeviceLimit(os.Getenv(authMaxPerPlatformEnv), 3)
	if !accountOK || !platformOK {
		return app.DeviceLimits{}, false, api.ErrInvalidAuthConfig
	}
	switch os.Getenv(authSingleDeviceEnv) {
	case "", "0":
	case "1":
		account = 1
	default:
		return app.DeviceLimits{}, false, api.ErrInvalidAuthConfig
	}
	return app.DeviceLimits{Account: account, Platform: platform}, true, nil
}

func configuredDeviceLimit(raw string, defaultValue int) (int, bool) {
	if raw == "" {
		return defaultValue, true
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	return int(value), err == nil && value > 0
}

func newAuthAndMessageRuntime(ctx context.Context, cfg api.Config, admission, profileEnabled bool, logger *slog.Logger) (*authRuntime, []api.APIRoute, error) {
	dsn := strings.TrimSpace(os.Getenv(authDSNEnv))
	allowLocalSocket := os.Getenv(authAllowLocalSocketEnv) == "1"
	messageEnabled := os.Getenv(messageHTTPEnv) == "1"
	projectionValue := os.Getenv(conversationProjectionEnv)
	if projectionValue != "" && projectionValue != "0" && projectionValue != "1" {
		return nil, nil, api.ErrInvalidAuthConfig
	}
	projectConversations := projectionValue == "1"
	readSyncValue := os.Getenv(conversationReadSyncEnv)
	projectReadState := readSyncValue == "1"
	if (projectReadState && !projectConversations) ||
		(readSyncValue != "" && readSyncValue != "0" && !projectReadState) {
		return nil, nil, api.ErrInvalidAuthConfig
	}
	privatePolicyValue := os.Getenv(privateMessagePolicyEnv)
	privatePolicy := privatePolicyValue == "1"
	if (privatePolicy && !messageEnabled) ||
		(privatePolicyValue != "" && privatePolicyValue != "0" && !privatePolicy) {
		return nil, nil, errInvalidMessageConfig
	}
	strangerPolicyValue := os.Getenv(strangerMessagePolicyEnv)
	strangerPolicy := strangerPolicyValue == "1"
	if (strangerPolicy && !privatePolicy) ||
		(strangerPolicyValue != "" && strangerPolicyValue != "0" && !strangerPolicy) {
		return nil, nil, errInvalidMessageConfig
	}

	if !messageEnabled && !profileEnabled {
		if dsn == "" {
			return nil, nil, nil
		}
		return newAuthRuntimeWithGuard(ctx, cfg, dsn, allowLocalSocket, admission, nil, logger)
	}
	guardError := api.ErrInvalidAuthConfig
	if messageEnabled {
		if err := validateMessageRuntimeConfig(cfg, dsn, allowLocalSocket); err != nil {
			return nil, nil, err
		}
		guardError = errInvalidMessageConfig
	}

	terminal := newTerminalState()
	guard := postgresidentity.NewGuard(func(error) { terminal.Trip(guardError) })
	runtime, routes, err := newAuthRuntimeWithGuard(ctx, cfg, dsn, allowLocalSocket, admission, guard, logger)
	if err != nil {
		if messageEnabled {
			return nil, nil, messageStartupError(guard, err)
		}
		return nil, nil, api.ErrInvalidAuthConfig
	}
	runtime.guard = guard
	runtime.terminal = terminal
	if profileEnabled {
		runtime.profileRepo, err = userstore.Open(ctx, userstore.Config{
			DSN:                  dsn,
			AllowLocalSocket:     allowLocalSocket,
			ApplicationName:      "newim-profile-http",
			Guard:                guard,
			ProjectConversations: projectConversations,
			ProjectReadState:     projectReadState,
		})
		if guard.Tripped() != nil {
			runtime.Close()
			return nil, nil, guardError
		}
		if err != nil {
			runtime.Close()
			return nil, nil, api.ErrInvalidAuthConfig
		}
	}
	if !messageEnabled {
		return runtime, routes, nil
	}

	messageRepo, err := messagestore.Open(ctx, messagestore.Config{
		DSN:                  dsn,
		AllowLocalSocket:     allowLocalSocket,
		MaxConnections:       8,
		ApplicationName:      messageHTTPApplicationName,
		Guard:                guard,
		PrivatePolicy:        privatePolicy,
		StrangerPolicy:       strangerPolicy,
		ProjectConversations: projectConversations,
		ProjectReadState:     projectReadState,
	})
	if err != nil {
		runtime.Close()
		if privatePolicy {
			return nil, nil, errInvalidMessageConfig
		}
		return nil, nil, messageStartupError(guard, err)
	}
	runtime.messageRepo = messageRepo

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
	if privatePolicy {
		policyHandler, err := messagehttp.NewPrivatePolicyHandler(runtime.service, messageRepo)
		if err != nil {
			runtime.Close()
			return nil, nil, errInvalidMessageConfig
		}
		for _, route := range []struct{ name, path string }{
			{"direct_conversation", messagehttp.DirectConversationRoute},
			{"relationship_block", messagehttp.BlockPeerRoute},
			{"relationship_unblock", messagehttp.UnblockPeerRoute},
		} {
			routes = append(routes, api.APIRoute{
				Name:           route.name,
				Path:           route.path,
				AllowedMethods: []string{http.MethodPost},
				MaxBodyBytes:   messagehttp.PrivatePolicyBodyLimit,
				RejectQuery:    true,
				Handler:        policyHandler,
			})
		}
	}
	return runtime, routes, nil
}

func newAuthRuntimeWithGuard(ctx context.Context, cfg api.Config, dsn string, allowLocalSocket, admission bool, guard *postgresidentity.Guard, logger *slog.Logger) (*authRuntime, []api.APIRoute, error) {
	if !loopbackAPIAddress(cfg.APIAddr) {
		return nil, nil, api.ErrInvalidAuthConfig
	}
	repo, err := store.Open(ctx, store.Config{
		DSN:               dsn,
		AllowLocalSocket:  allowLocalSocket,
		MaxConnections:    8,
		ApplicationName:   "newim-auth-http",
		Guard:             guard,
		AdmissionRequired: admission,
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
	runtime.closer = newOrderedCloser(func() {
		runtime.profileRepo.Close()
		runtime.messageRepo.Close()
	}, repo.Close)
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
	return r.terminal.Done()
}

func (r *authRuntime) TerminalErr() error {
	if r == nil {
		return nil
	}
	return r.terminal.Err()
}

// Close releases the profile and message pools before the auth pool.
// Close 先释放资料、消息连接池，再释放鉴权连接池。
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
