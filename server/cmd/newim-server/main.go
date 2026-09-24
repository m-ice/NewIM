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
	"syscall"
	"time"

	"github.com/m-ice/NewIM/server/api"
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
	server, err := api.New(cfg, logger)
	if err != nil {
		fmt.Fprintln(stderr, errorCode(err))
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runtime, runtimeErr := newWebhookRuntimeFromEnv(ctx, logger)
	if runtimeErr != nil {
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
	result := joinRuntimeAndServer(serverDone, runtimeDone, cancelServer, cancelWorker, 10*time.Second)
	cancelWorker()
	if result.timedOut {
		fmt.Fprintln(stderr, api.CodeShutdownFailed)
		return 1
	}
	serverErr := result.serverErr
	if serverErr == nil && result.workerErr != nil {
		serverErr = result.workerErr
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
	timedOut  bool
}

// joinRuntimeAndServer waits for both components after either one exits.
// joinRuntimeAndServer 在任一组件退出后等待其余组件，只有首次退出才启动有界 join 截止时间。
func joinRuntimeAndServer(serverDone, workerDone <-chan error, cancelServer, cancelWorker func(), grace time.Duration) shutdownResult {
	var result shutdownResult
	var timer *time.Timer
	var deadline <-chan time.Time
	armDeadline := func() {
		if timer == nil {
			timer = time.NewTimer(grace)
			deadline = timer.C
		}
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for serverDone != nil || workerDone != nil {
		select {
		case err := <-serverDone:
			serverDone = nil
			result.serverErr = err
			cancelWorker()
			armDeadline()
		case err := <-workerDone:
			workerDone = nil
			result.workerErr = err
			cancelServer()
			armDeadline()
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
	var webhookErr *app.Error
	if errors.As(err, &webhookErr) && webhookErr != nil {
		return string(webhookErr.Code)
	}
	return string(api.CodeRuntimeFailed)
}
