package main

import (
	"context"
	"os"

	"github.com/m-ice/NewIM/server/api"
	store "github.com/m-ice/NewIM/server/storage/webhook"
	app "github.com/m-ice/NewIM/server/webhook"
	"github.com/m-ice/NewIM/server/webhookhttp"
)

const webhookManagementTokenFileEnv = "NEWIM_WEBHOOK_MANAGEMENT_TOKEN_FILE"

func newWebhookManagementRoutes(ctx context.Context, cfg api.Config, runtime *webhookRuntime) ([]api.APIRoute, error) {
	tokenFile := os.Getenv(webhookManagementTokenFileEnv)
	if tokenFile == "" {
		return nil, nil
	}
	if runtime == nil || !loopbackAPIAddress(cfg.APIAddr) {
		return nil, app.Fail(app.CodeInvalidConfig)
	}
	management, err := store.NewManagement(runtime.repo, runtime.secretCodec, webhookHighWater, webhookDestinationQueue)
	if err != nil {
		return nil, err
	}
	routes, err := webhookhttp.NewRoutes(management, tokenFile)
	if err != nil {
		return nil, err
	}
	if err := management.CheckSchema(ctx); err != nil {
		return nil, err
	}
	return routes, nil
}
