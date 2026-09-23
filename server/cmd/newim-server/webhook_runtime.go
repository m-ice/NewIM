package main

import (
	"context"
	"encoding/base64"
	"log/slog"
	"os"
	"strings"
	"time"

	store "github.com/m-ice/NewIM/server/storage/webhook"
	app "github.com/m-ice/NewIM/server/webhook"
)

// webhookRuntime owns the optional in-process webhook worker and its database pool.
// webhookRuntime 持有可选的进程内 Webhook worker 及其数据库池。
type webhookRuntime struct {
	repo   *store.Repository
	lock   *store.WorkerLock
	worker *app.Worker
}

// newWebhookRuntimeFromEnv creates no runtime when the DSN is absent.
// newWebhookRuntimeFromEnv 在 DSN 缺失时返回 nil runtime；配置不完整必须 fail-closed。
func newWebhookRuntimeFromEnv(ctx context.Context, logger *slog.Logger) (*webhookRuntime, error) {
	dsn := strings.TrimSpace(os.Getenv("NEWIM_WEBHOOK_DSN"))
	if dsn == "" {
		return nil, nil
	}
	masterKey, err := base64.StdEncoding.DecodeString(os.Getenv("NEWIM_WEBHOOK_MASTER_KEY_B64"))
	if err != nil || len(masterKey) != 32 {
		return nil, app.Fail(app.CodeInvalidConfig)
	}
	repo, err := store.Open(ctx, store.Config{
		DSN:              dsn,
		AllowLocalSocket: os.Getenv("NEWIM_WEBHOOK_ALLOW_LOCAL_SOCKET") == "1",
		MaxConnections:   8,
		ApplicationName:  "newim-webhook",
	})
	if err != nil {
		return nil, err
	}
	lock, err := repo.AcquireWorkerLock(ctx)
	if err != nil {
		repo.Close()
		return nil, err
	}
	resolver, err := app.NewLocalSecretResolver(masterKey)
	if err != nil {
		lock.Release()
		repo.Close()
		return nil, err
	}
	client, err := app.NewSecureClient(nil, app.Policy{}, 10*time.Second, 64*1024)
	if err != nil {
		lock.Release()
		repo.Close()
		return nil, err
	}
	worker, err := app.NewWorker(app.Config{
		Owner:               "newim-server",
		BatchSize:           100,
		MaxConcurrent:       8,
		MaxPerDestination:   2,
		MaxAttempts:         8,
		MaxResponseBytes:    64 * 1024,
		LeaseTTL:            30 * time.Second,
		RequestTimeout:      10 * time.Second,
		BaseBackoff:         time.Second,
		MaxBackoff:          time.Hour,
		HighWater:           10000,
		LowWater:            5000,
		MaxDestinationQueue: 5000,
		RatePerSecond:       20,
		RateBurst:           40,
		IdleDelay:           100 * time.Millisecond,
		Observer:            app.NewLogObserver(logger),
	}, repo, client, resolver)
	if err != nil {
		lock.Release()
		repo.Close()
		return nil, err
	}
	clear(masterKey)
	return &webhookRuntime{repo: repo, lock: lock, worker: worker}, nil
}

// Run runs the worker until the process context is cancelled.
// Run 运行 worker 直到进程 context 被取消。
func (r *webhookRuntime) Run(ctx context.Context) error {
	if r == nil || r.worker == nil {
		return nil
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.worker.Run(workerCtx) }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			cancel()
			return <-done
		case <-ticker.C:
			if err := r.lock.Check(ctx); err != nil {
				cancel()
				<-done
				return err
			}
		}
	}
}

// Close releases the webhook database pool.
// Close 释放 Webhook 数据库池。
func (r *webhookRuntime) Close() {
	if r != nil && r.repo != nil {
		if r.lock != nil {
			r.lock.Release()
		}
		r.repo.Close()
	}
}
