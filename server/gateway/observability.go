package gateway

import (
	"context"
	"time"

	"github.com/coder/websocket"
	"github.com/m-ice/NewIM/server/api"
)

// SnapshotMetrics reads the current admitted websocket subscriptions.
func (g *Gateway) SnapshotMetrics(context.Context) ([]api.MetricSample, error) {
	return []api.MetricSample{{Name: api.MetricGatewayActive, Value: float64(g.active.Load())}}, nil
}

func (g *Gateway) writeObserved(ctx context.Context, peer *peer, operation string, wire []byte) error {
	started := time.Now()
	err := peer.ws.Write(ctx, websocket.MessageText, wire)
	result := "success"
	if err != nil {
		result = "temporary"
	}
	api.ObserveGatewayOperation(g.config.Metrics, operation, result, time.Since(started))
	return err
}
