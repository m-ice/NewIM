package message

import (
	"context"
	"time"

	"github.com/m-ice/NewIM/server/api"
	app "github.com/m-ice/NewIM/server/message"
)

const durableMetricsQueryTimeout = 2 * time.Second

// SnapshotMetrics reads the durable outbox fanout gauge from authoritative rows.
// SnapshotMetrics 从权威行读取持久 outbox fanout gauge；失败时返回错误而不是猜测零值。
func (r *Repository) SnapshotMetrics(ctx context.Context) ([]api.MetricSample, error) {
	if r == nil || r.pool == nil || ctx == nil {
		return nil, app.Fail(app.SendStorageUnavailable)
	}
	queryCtx, cancel := context.WithTimeout(ctx, durableMetricsQueryTimeout)
	defer cancel()
	var pending, quarantined int64
	err := r.pool.QueryRow(queryCtx, `SELECT
count(*) FILTER (WHERE webhook_fanout_at IS NULL AND webhook_fanout_error IS NULL)::bigint,
count(*) FILTER (WHERE webhook_fanout_at IS NULL AND webhook_fanout_error IS NOT NULL)::bigint
FROM newim.im_outbox_events`).Scan(&pending, &quarantined)
	if err != nil {
		return nil, mapStorageError(err)
	}
	return []api.MetricSample{
		{Name: api.MetricMessageOutboxFanoutPending, Labels: map[string]string{"state": "pending"}, Value: float64(pending)},
		{Name: api.MetricMessageOutboxFanoutPending, Labels: map[string]string{"state": "quarantined"}, Value: float64(quarantined)},
	}, nil
}
