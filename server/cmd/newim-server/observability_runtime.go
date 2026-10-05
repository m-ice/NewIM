package main

import (
	"context"
	"log/slog"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/api"
	messageservice "github.com/m-ice/NewIM/server/message"
	"github.com/m-ice/NewIM/server/messagehttp"
	app "github.com/m-ice/NewIM/server/webhook"
)

const observabilitySourceTimeout = 2 * time.Second

// attachMessageObservability rebuilds only the send route composition with the
// process metrics sink. The original protected runtime construction remains
// unchanged; the replacement uses the same repository, IDs, clock and observer.
// attachMessageObservability 仅重建发送路由组合以接入进程 metrics sink；原受保护
// runtime 构造不变，替换服务复用同一 repository、ID、clock 与 observer。
func attachMessageObservability(runtime *authRuntime, routes []api.APIRoute, sink api.MetricsSink, logger *slog.Logger) error {
	if runtime == nil || runtime.messageRepo == nil || sink == nil {
		return nil
	}
	service, err := messageservice.NewService(runtime.messageRepo, messageservice.Config{
		IDs:      messageservice.NewCSPRNGIDGenerator(),
		Clock:    messageservice.ClockFunc(time.Now),
		Observer: messageLogObserver{logger: logger},
		Metrics:  sink,
	})
	if err != nil {
		return err
	}
	handler, err := messagehttp.NewHandler(runtime.service, service, protocol.MaxFrameBytes)
	if err != nil {
		return err
	}
	for index := range routes {
		if routes[index].Name == "message_send" {
			routes[index].Handler = handler
			return nil
		}
	}
	return nil
}

// webhookGaugeSource uses the existing durable Store.Counts port from command
// composition; it does not infer gauges from process-local worker state.
// webhookGaugeSource 在 command composition 中复用既有持久 Store.Counts 端口，
// 不从进程内 worker 状态推断 gauge。
type webhookGaugeSource struct {
	store interface {
		Counts(context.Context) (app.Counts, error)
	}
}

func (s webhookGaugeSource) SnapshotMetrics(ctx context.Context) ([]api.MetricSample, error) {
	if s.store == nil || ctx == nil {
		return nil, app.Fail(app.CodeStorageUnavailable)
	}
	queryCtx, cancel := context.WithTimeout(ctx, observabilitySourceTimeout)
	defer cancel()
	counts, err := s.store.Counts(queryCtx)
	if err != nil {
		return nil, err
	}
	return []api.MetricSample{
		{Name: api.MetricWebhookDeliveryPending, Labels: map[string]string{"state": "pending"}, Value: float64(counts.Pending)},
		{Name: api.MetricWebhookDeliveryPending, Labels: map[string]string{"state": "retry"}, Value: float64(counts.Retry)},
		{Name: api.MetricWebhookDeliveryPending, Labels: map[string]string{"state": "leased"}, Value: float64(counts.Leased)},
		{Name: api.MetricWebhookDeliveryPending, Labels: map[string]string{"state": "dead_letter"}, Value: float64(counts.DeadLetter)},
	}, nil
}

// AttachMetricsSink attaches the bounded process sink to the webhook worker.
// AttachMetricsSink 将有界进程 sink 接入 Webhook worker。
func (r *webhookRuntime) AttachMetricsSink(sink api.MetricsSink) {
	if r == nil || r.worker == nil || sink == nil {
		return
	}
	if worker, ok := r.worker.(interface{ AttachMetricsSink(api.MetricsSink) }); ok {
		worker.AttachMetricsSink(sink)
	}
}
