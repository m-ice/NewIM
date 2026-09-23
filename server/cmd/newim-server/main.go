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
	var runtimeDone chan error
	if runtime != nil {
		defer runtime.Close()
		runtimeDone = make(chan error, 1)
		go func() { runtimeDone <- runtime.Run(workerCtx) }()
	}
	serverErr := server.Run(ctx)
	cancelWorker()
	if runtimeDone != nil {
		select {
		case workerErr := <-runtimeDone:
			if serverErr == nil && workerErr != nil {
				serverErr = workerErr
			}
		case <-time.After(10 * time.Second):
			if serverErr == nil {
				fmt.Fprintln(stderr, api.CodeShutdownFailed)
				return 1
			}
		}
	}
	if serverErr != nil {
		fmt.Fprintln(stderr, errorCode(serverErr))
		return 1
	}
	return 0
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
