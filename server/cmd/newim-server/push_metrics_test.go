package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/push"
)

type dispatchQueue struct {
	committed push.Result
	claim     push.ClaimStatus
}

func (*dispatchQueue) Enable(context.Context) error          { return nil }
func (*dispatchQueue) Fanout(context.Context) (int64, error) { return 0, nil }
func (*dispatchQueue) PrunePresence(context.Context) error   { return nil }

func (q *dispatchQueue) Claim(context.Context) (push.Delivery, push.ClaimStatus, error) {
	return push.Delivery{TokenBinding: push.TokenBinding{Channel: push.FCM}}, q.claim, nil
}

func (*dispatchQueue) Prepare(context.Context, push.Delivery) ([]byte, bool, error) {
	return []byte("fixture-device-token"), true, nil
}

func (q *dispatchQueue) Finish(context.Context, push.Delivery, push.Outcome) (push.Result, error) {
	return q.committed, nil
}

type dispatchProvider struct{ result push.Result }

func (p *dispatchProvider) Send(context.Context, []byte) push.Outcome {
	return push.Outcome{Result: p.result}
}

func TestPushDispatchMetrics(t *testing.T) {
	queue := &dispatchQueue{claim: push.LeaseAcquired}
	provider := &dispatchProvider{}
	worker, err := push.NewWorker(queue, map[push.Channel]push.Provider{push.FCM: provider})
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range []struct {
		provider, committed push.Result
	}{
		{push.TemporaryFailure, push.TemporaryFailure},
		{push.TemporaryFailure, push.PermanentFailure},
		{push.Accepted, 0},
		{push.Accepted, push.Accepted},
	} {
		provider.result, queue.committed = attempt.provider, attempt.committed
		if progressed, err := worker.Cycle(t.Context()); progressed != (attempt.committed != 0) || err != nil {
			t.Fatalf("dispatch cycle: progress = %t, error = %v", progressed, err)
		}
	}
	queue.claim = push.LeaseExhausted
	if progressed, err := worker.Cycle(t.Context()); !progressed || err != nil {
		t.Fatalf("expired final lease: progress = %t, error = %v", progressed, err)
	}
	queue.claim = push.NoDelivery
	if progressed, err := worker.Cycle(t.Context()); progressed || err != nil {
		t.Fatalf("empty queue: progress = %t, error = %v", progressed, err)
	}
	recorder := httptest.NewRecorder()
	api.NewOperationalMetricsHandler("test", api.NewMetricsRegistry(), &pushRuntime{worker: worker}).
		ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, sample := range []string{
		"newim_push_dispatch_total{result=\"accepted\"} 1",
		"newim_push_dispatch_total{result=\"retry\"} 1",
		"newim_push_dispatch_total{result=\"failed\"} 2",
	} {
		if recorder.Code != http.StatusOK || !strings.Contains(body, sample+"\n") {
			t.Errorf("metrics status %d or missing %s", recorder.Code, sample)
		}
	}
	if strings.Contains(body, "fixture-device-token") {
		t.Fatal("push token in metrics")
	}
}
