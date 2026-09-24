package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/m-ice/NewIM/server/api"
	session "github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/buildinfo"
	app "github.com/m-ice/NewIM/server/webhook"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, buildinfo.Read))
}

func run(args []string, stdout, stderr io.Writer, read func() (buildinfo.Info, error)) int {
	cfg, help, err := api.ParseConfig(args, stdout, stderr)
	if err != nil {
		fmt.Fprintln(stderr, errorCode(err))
		return 2
	}
	if help {
		return 0
	}

	info, err := read()
	if err != nil || strings.TrimSpace(info.ServerVersion) == "" {
		fmt.Fprintln(stderr, api.CodeBuildInfoUnavailable)
		return 2
	}
	cfg.ServerVersion = info.ServerVersion

	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	authRuntime, routes, authErr := newAuthRuntimeFromEnv(ctx, cfg, logger)
	if authErr != nil {
		fmt.Fprintln(stderr, errorCode(authErr))
		return 2
	}
	var authDone chan error
	var closeAuthOnce sync.Once
	closeAuth := func() {}
	if authRuntime != nil {
		authDone = make(chan error, 1)
		closeAuth = func() {
			closeAuthOnce.Do(func() {
				go func() {
					authRuntime.Close()
					authDone <- nil
				}()
			})
		}
	}

	server, err := api.NewWithRoutes(cfg, logger, routes)
	if err != nil {
		closeAuth()
		fmt.Fprintln(stderr, errorCode(err))
		return 2
	}

	runtime, runtimeErr := newWebhookRuntimeFromEnv(ctx, logger)
	if runtimeErr != nil {
		closeAuth()
		fmt.Fprintln(stderr, errorCode(runtimeErr))
		return 2
	}
	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	serverCtx, cancelServer := context.WithCancel(ctx)
	defer cancelServer()
	var runtimeDone chan error
	if runtime != nil {
		defer runtime.Close()
		runtimeDone = make(chan error, 1)
		go func() { runtimeDone <- runtime.Run(workerCtx) }()
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Run(serverCtx) }()
	var shutdown chan struct{}
	if authRuntime != nil {
		shutdown = make(chan struct{})
		go func() {
			<-ctx.Done()
			close(shutdown)
		}()
	}
	result := joinRuntimeServerAndAuth(serverDone, runtimeDone, authDone, shutdown, cancelServer, cancelWorker, closeAuth, 10*time.Second)
	cancelWorker()
	if result.timedOut {
		fmt.Fprintln(stderr, api.CodeShutdownFailed)
		return 1
	}
	serverErr := result.serverErr
	if serverErr == nil && result.workerErr != nil {
		serverErr = result.workerErr
	}
	if serverErr == nil && result.authErr != nil {
		serverErr = result.authErr
	}
	if serverErr != nil {
		fmt.Fprintln(stderr, errorCode(serverErr))
		return 1
	}
	return 0
}

type shutdownResult struct {
	serverErr error
	workerErr error
	authErr   error
	timedOut  bool
}

// joinRuntimeAndServer waits for server and optional webhook worker components.
// joinRuntimeAndServer 等待 server 与可选 Webhook worker；保留旧签名给既有调用方。
func joinRuntimeAndServer(serverDone, workerDone <-chan error, cancelServer, cancelWorker func(), grace time.Duration) shutdownResult {
	return joinRuntimeServerAndAuth(serverDone, workerDone, nil, nil, cancelServer, cancelWorker, nil, grace)
}

// joinRuntimeServerAndAuth waits for server, worker, auth-close and shutdown trigger components.
// joinRuntimeServerAndAuth 等待 server、worker、auth-close 与 shutdown trigger；首个事件启动一次 auth close 和共享 deadline。
func joinRuntimeServerAndAuth(serverDone, workerDone, authDone <-chan error, shutdown <-chan struct{}, cancelServer, cancelWorker func(), closeAuth func(), grace time.Duration) shutdownResult {
	var result shutdownResult
	var timer *time.Timer
	var deadline <-chan time.Time
	var closeOnce sync.Once
	armDeadline := func() {
		if timer == nil {
			timer = time.NewTimer(grace)
			deadline = timer.C
		}
	}
	stop := func() {
		if closeAuth != nil {
			closeOnce.Do(closeAuth)
		}
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for serverDone != nil || workerDone != nil || authDone != nil || shutdown != nil {
		select {
		case <-shutdown:
			shutdown = nil
			cancelServer()
			cancelWorker()
			armDeadline()
		case err := <-serverDone:
			serverDone = nil
			shutdown = nil
			result.serverErr = err
			cancelWorker()
			stop()
			armDeadline()
		case err := <-workerDone:
			workerDone = nil
			shutdown = nil
			result.workerErr = err
			cancelServer()
			armDeadline()
		case err := <-authDone:
			authDone = nil
			result.authErr = err
		case <-deadline:
			cancelServer()
			cancelWorker()
			result.timedOut = true
			return result
		}
	}
	return result
}

func errorCode(err error) string {
	var known *api.Error
	if errors.As(err, &known) && known != nil {
		return string(known.Code)
	}
	var sessionErr *session.Error
	if errors.As(err, &sessionErr) && sessionErr != nil {
		return string(sessionErr.Code)
	}
	var webhookErr *app.Error
	if errors.As(err, &webhookErr) && webhookErr != nil {
		return string(webhookErr.Code)
	}
	return string(api.CodeRuntimeFailed)
}
