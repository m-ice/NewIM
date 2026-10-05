package webhook

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/api"
)

func TestOperationAndCodeRegistryIsComplete(t *testing.T) {
	for _, operation := range AllOperations() {
		for _, code := range AllCodes() {
			classification := ClassifyObservation(Observation{Operation: string(operation), Code: code})
			if classification.Operation == "unknown" || classification.Stage == "unknown" || classification.Result == "unknown" {
				if code == CodeUnknown {
					continue
				}
				t.Fatalf("operation %q code %q did not classify: %+v", operation, code, classification)
			}
			if classification.Stage != "enqueued" && classification.Stage != "fanout" &&
				classification.Stage != "claimed" && classification.Stage != "attempt" &&
				classification.Stage != "finished" && classification.Stage != "delivery_success" &&
				classification.Stage != "delivery_retry" && classification.Stage != "delivery_dead_letter" {
				t.Fatalf("operation %q code %q produced unbounded stage %q", operation, code, classification.Stage)
			}
		}
	}
	unknown := ClassifyObservation(Observation{Operation: "raw_operation", Code: "raw-code"})
	if unknown.Operation != "unknown" || unknown.Stage != "unknown" || unknown.Result != "unknown" || unknown.Code != CodeUnknown {
		t.Fatalf("unknown values did not converge: %+v", unknown)
	}
}

func TestPanickingWebhookObserverDoesNotChangeDelivery(t *testing.T) {
	now := time.UnixMilli(1790189001000)
	store := &testStore{delivery: testDelivery(nil)}
	registry := api.NewMetricsRegistry()
	cfg := testConfig(now)
	cfg.Observer = panicWebhookObserver{}
	cfg.Metrics = registry
	worker, err := NewWorker(cfg, store, testDoer{status: http.StatusNoContent}, testResolver{secret: []byte("0123456789abcdef0123456789abcdef")})
	if err != nil {
		t.Fatal(err)
	}
	if err = worker.cycle(context.Background(), make(chan struct{}, 2)); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	finished := append([]Outcome(nil), store.finished...)
	store.mu.Unlock()
	if len(finished) != 1 || finished[0].Status != "delivered" || finished[0].ErrorCode != CodeDeliveryDelivered {
		t.Fatalf("panicking observer changed delivery outcome: %+v", finished)
	}
	if !waitForWebhookMetric(registry, api.MetricWebhookDeliveries, map[string]string{"stage": "delivery_success", "result": "success"}) {
		t.Fatalf("successful delivery metric missing: %+v", registry.Snapshot())
	}
	if !waitForWebhookMetric(registry, api.MetricObservabilityDropped, map[string]string{"reason": "writer_error"}) {
		t.Fatalf("writer_error drop missing: %+v", registry.Snapshot())
	}
}

func TestBlockedWebhookObserverDoesNotBlockDelivery(t *testing.T) {
	now := time.UnixMilli(1790189001000)
	store := &testStore{delivery: testDelivery(nil)}
	observer := &blockingWebhookObserver{started: make(chan struct{}), release: make(chan struct{})}
	cfg := testConfig(now)
	cfg.Observer = observer
	worker, err := NewWorker(cfg, store, testDoer{status: http.StatusNoContent}, testResolver{secret: []byte("0123456789abcdef0123456789abcdef")})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- worker.cycle(context.Background(), make(chan struct{}, 2)) }()
	select {
	case <-observer.started:
	case <-time.After(time.Second):
		t.Fatal("observer did not start")
	}
	select {
	case cycleErr := <-done:
		if cycleErr != nil {
			t.Fatalf("cycle failed: %v", cycleErr)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("blocked observer delayed delivery cycle")
	}
	close(observer.release)
	worker.Close()
}

type panicWebhookObserver struct{}

func (panicWebhookObserver) Observe(Observation) { panic("observer failure") }

type blockingWebhookObserver struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (o *blockingWebhookObserver) Observe(Observation) {
	o.once.Do(func() { close(o.started) })
	<-o.release
}

func waitForWebhookMetric(registry *api.MetricsRegistry, name string, labels map[string]string) bool {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if hasMetric(registry.Snapshot(), name, labels) {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func hasMetric(samples []api.MetricSample, name string, labels map[string]string) bool {
	for _, sample := range samples {
		if sample.Name != name {
			continue
		}
		match := true
		for key, value := range labels {
			if sample.Labels[key] != value {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
