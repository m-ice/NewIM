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
	completed []push.Result
}

func (*dispatchQueue) Enable(context.Context) error          { return nil }
func (*dispatchQueue) Fanout(context.Context) (int64, error) { return 0, nil }
func (*dispatchQueue) PrunePresence(context.Context) error   { return nil }

func (*dispatchQueue) Claim(context.Context) (push.Delivery, bool, error) {
	return push.Delivery{TokenBinding: push.TokenBinding{Channel: push.FCM}}, true, nil
}

func (*dispatchQueue) Prepare(context.Context, push.Delivery) ([]byte, bool, error) {
	return []byte("fixture-device-token"), true, nil
}

func (q *dispatchQueue) Finish(_ context.Context, _ push.Delivery, outcome push.Outcome) error {
	q.completed = append(q.completed, outcome.Result)
	return nil
}

type dispatchProvider struct{ result push.Result }

func (p *dispatchProvider) Send(context.Context, []byte) push.Outcome {
	return push.Outcome{Result: p.result}
}

func TestPushDispatchMetrics(t *testing.T) {
	queue := &dispatchQueue{}
	provider := &dispatchProvider{}
	worker, err := push.NewWorker(queue, map[push.Channel]push.Provider{push.FCM: provider})
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range []push.Result{push.TemporaryFailure, push.Accepted, push.Accepted} {
		provider.result = result
		if progressed, err := worker.Cycle(t.Context()); !progressed || err != nil {
			t.Fatalf("dispatch cycle: progress = %t, error = %v", progressed, err)
		}
	}
	if len(queue.completed) != 3 {
		t.Fatalf("committed decisions = %v", queue.completed)
	}
	recorder := httptest.NewRecorder()
	api.NewOperationalMetricsHandler("test", api.NewMetricsRegistry(), &pushRuntime{worker: worker}).
		ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, sample := range []string{
		"newim_push_dispatch_total{result=\"accepted\"} 2",
		"newim_push_dispatch_total{result=\"retry\"} 1",
		"newim_push_dispatch_total{result=\"failed\"} 0",
	} {
		if recorder.Code != http.StatusOK || !strings.Contains(body, sample+"\n") {
			t.Errorf("metrics status %d or missing %s", recorder.Code, sample)
		}
	}
	if strings.Contains(body, "fixture-device-token") {
		t.Fatal("push token in metrics")
	}
}
