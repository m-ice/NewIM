package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/push"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
	pushstore "github.com/m-ice/NewIM/server/storage/push"
)

type pushRuntime struct {
	repo   *pushstore.Repository
	worker *push.Worker
	logger *slog.Logger
}

func newPushRuntimeFromEnv(ctx context.Context, auth *authRuntime, logger *slog.Logger) (*pushRuntime, error) {
	switch os.Getenv("NEWIM_PUSH_WORKER") {
	case "", "0":
		return nil, nil
	case "1":
	default:
		return nil, push.ErrConfig
	}
	vault, err := push.LoadVault(os.Getenv("NEWIM_PUSH_KEYS_FILE"))
	if err != nil {
		return nil, err
	}
	providers, err := pushProviders()
	if err != nil {
		return nil, err
	}
	var guard *postgresidentity.Guard
	if auth != nil {
		guard = auth.guard
	}
	dsn := strings.TrimSpace(os.Getenv("NEWIM_PUSH_DSN"))
	if dsn == "" {
		dsn = os.Getenv(authDSNEnv)
	}
	repo, err := pushstore.Open(ctx, pushstore.Config{
		DSN: dsn, AllowLocalSocket: os.Getenv("NEWIM_PUSH_ALLOW_LOCAL_SOCKET") == "1",
		ApplicationName: "newim-server-push", Guard: guard,
	}, vault)
	if err != nil {
		return nil, err
	}
	worker, err := push.NewWorker(repo, providers)
	if err != nil {
		repo.Close()
		return nil, err
	}
	return &pushRuntime{repo: repo, worker: worker, logger: logger}, nil
}

func pushProviders() (map[push.Channel]push.Provider, error) {
	providers := make(map[push.Channel]push.Provider, 2)
	if path := os.Getenv("NEWIM_PUSH_FCM_CREDENTIALS_FILE"); path != "" {
		provider, err := push.NewFCM(path)
		if err != nil {
			return nil, err
		}
		providers[push.FCM] = provider
	}
	if path := os.Getenv("NEWIM_PUSH_APNS_KEY_FILE"); path != "" {
		environment := os.Getenv("NEWIM_PUSH_APNS_SANDBOX")
		if environment != "" && environment != "0" && environment != "1" {
			return nil, push.ErrConfig
		}
		provider, err := push.NewAPNs(push.APNsConfig{
			KeyFile: path, Sandbox: environment == "1",
			KeyID: os.Getenv("NEWIM_PUSH_APNS_KEY_ID"), TeamID: os.Getenv("NEWIM_PUSH_APNS_TEAM_ID"),
			Topic: os.Getenv("NEWIM_PUSH_APNS_TOPIC"),
		})
		if err != nil {
			return nil, err
		}
		providers[push.APNs] = provider
	}
	if len(providers) == 0 {
		return nil, push.ErrConfig
	}
	return providers, nil
}

func (r *pushRuntime) SnapshotMetrics(context.Context) ([]api.MetricSample, error) {
	counts := r.worker.Counts()
	results := [...]struct {
		result string
		value  uint64
	}{
		{"accepted", counts.Accepted}, {"retry", counts.Retry}, {"failed", counts.Failed},
		{"invalid", counts.Invalid}, {"cancelled", counts.Cancelled},
	}
	samples := make([]api.MetricSample, 0, len(results))
	for _, outcome := range results {
		samples = append(samples, api.MetricSample{
			Name: api.MetricPushDispatch, Value: float64(outcome.value),
			Labels: map[string]string{"result": outcome.result},
		})
	}
	return samples, nil
}

func (r *pushRuntime) Run(ctx context.Context) {
	var nextReport time.Time
	failure := func() {
		now := time.Now()
		if !now.Before(nextReport) {
			r.logger.Warn("push queue temporarily unavailable", "code", push.ErrStorage.Error())
			nextReport = now.Add(time.Minute)
		}
	}
	report := func(counts push.Counts) {
		r.logger.Info("push dispatch totals",
			"accepted", counts.Accepted, "retry", counts.Retry, "failed", counts.Failed,
			"invalid", counts.Invalid, "cancelled", counts.Cancelled)
	}
	for ctx.Err() == nil {
		if err := r.worker.Run(ctx, failure, report); err == nil || ctx.Err() != nil {
			return
		}
		failure()
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}
