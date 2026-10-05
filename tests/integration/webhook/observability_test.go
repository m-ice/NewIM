//go:build integration

package webhook_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/api"
	store "github.com/m-ice/NewIM/server/storage/webhook"
	app "github.com/m-ice/NewIM/server/webhook"
)

func testWebhookObservabilityAuthoritativeOutcomes(t *testing.T) {
	cases := []struct {
		name       string
		httpStatus int
		wantStatus string
		stage      string
		result     string
		code       app.Code
	}{
		{name: "success", httpStatus: http.StatusNoContent, wantStatus: "delivered", stage: "delivery_success", result: "success", code: app.CodeDeliveryDelivered},
		{name: "dead_letter", httpStatus: http.StatusBadRequest, wantStatus: "dead_letter", stage: "delivery_dead_letter", result: "permanent", code: app.CodeHTTPPermanent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := openFixture(t)
			conversation := f.id("webhook_observability_" + tc.name)
			eventID := f.seedEvent(conversation)
			secret := []byte("0123456789abcdef0123456789abcdef")
			_, master := f.insertEndpoint(1, "https://example.invalid/observability", secret)
			registry := api.NewMetricsRegistry()
			sink := api.NewBoundedMetricsSink(registry, 1024)
			observer := &captureObserver{}
			worker, err := app.NewWorker(app.Config{
				Owner: "observability_authoritative_owner", BatchSize: 8, MaxConcurrent: 2,
				MaxPerDestination: 2, MaxAttempts: 1, MaxResponseBytes: 64 * 1024,
				LeaseTTL: 2 * time.Second, RequestTimeout: time.Second,
				BaseBackoff: 10 * time.Millisecond, MaxBackoff: time.Second,
				HighWater: 100, LowWater: 10, MaxDestinationQueue: 100,
				RatePerSecond: 100, RateBurst: 100, IdleDelay: 10 * time.Millisecond,
				Clock:    app.ClockFunc(func() time.Time { return f.now }),
				Observer: observer, Metrics: sink,
			}, f.repo, doerFunc(func(context.Context, string, map[string]string, []byte) (app.Response, error) {
				return app.Response{StatusCode: tc.httpStatus}, nil
			}), f.resolver(master))
			must(t, err)
			runWorkerUntil(t, worker, 5*time.Second, func() bool {
				return f.optionalString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID) == tc.wantStatus
			}, func() string {
				return f.scalarString("SELECT COALESCE(string_agg(status||':'||attempts||':'||COALESCE(last_error_code,''),','),'') FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID)
			})
			worker.Close()
			sink.Close()

			waitForIntegrationMetric(t, registry, api.MetricMessageLifecycleEvents,
				map[string]string{"component": "webhook", "stage": tc.stage, "result": tc.result})
			waitForIntegrationMetric(t, registry, api.MetricWebhookDeliveries,
				map[string]string{"stage": tc.stage, "result": tc.result})
			if tc.wantStatus == "dead_letter" {
				waitForIntegrationMetric(t, registry, api.MetricMessageLifecycleErrors,
					map[string]string{"component": "webhook", "code": string(tc.code)})
			}
			if !strings.Contains(observer.text(), string(tc.code)) {
				t.Fatalf("observer missing authoritative code %s: %s", tc.code, observer.text())
			}
			if got := f.scalarString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID); got != tc.wantStatus {
				t.Fatalf("authoritative status=%s want=%s", got, tc.wantStatus)
			}
		})
	}
}

func waitForIntegrationMetric(t *testing.T, registry *api.MetricsRegistry, name string, labels map[string]string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, sample := range registry.Snapshot() {
			if sample.Name != name {
				continue
			}
			matches := true
			for key, value := range labels {
				if sample.Labels[key] != value {
					matches = false
					break
				}
			}
			if matches {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("metric %s labels=%v missing in %+v", name, labels, registry.Snapshot())
}

func TestWebhookObservabilityRecovery(t *testing.T) {
	f := openFixture(t)
	_ = f.seedEvent(f.id("webhook_observability_room"))
	_, _ = f.insertEndpoint(1, "https://example.invalid/observability", []byte("0123456789abcdef0123456789abcdef"))
	if n, err := f.repo.Fanout(ctx, f.now, 10, 1000, 10000); err != nil || n != 1 {
		t.Fatalf("fanout=%d err=%v", n, err)
	}
	assertDeliveryGauge(t, f, "pending", 1)
	assertDeliveryGauge(t, f, "retry", 0)
	assertDeliveryGauge(t, f, "leased", 0)
	assertDeliveryGauge(t, f, "dead_letter", 0)

	claimed, err := f.repo.Claim(ctx, f.now, "observability_owner", 2*time.Second, 10)
	must(t, err)
	if len(claimed) != 1 {
		t.Fatalf("claimed=%d", len(claimed))
	}
	assertDeliveryGauge(t, f, "pending", 0)
	assertDeliveryGauge(t, f, "leased", 1)
	allowed, err := f.repo.BeginAttempt(ctx, claimed[0].ID, claimed[0].LeaseToken, f.now, 3, time.Second)
	must(t, err)
	if !allowed {
		t.Fatal("attempt not admitted")
	}
	must(t, f.repo.Finish(ctx, claimed[0].ID, claimed[0].LeaseToken, claimed[0].Attempts+1, f.now, app.Outcome{
		Status: "retry", RetryDelay: time.Second, ErrorCode: app.CodeHTTPTemporary,
	}))
	assertDeliveryGauge(t, f, "retry", 1)
	assertDeliveryGauge(t, f, "leased", 0)

	f.sql("UPDATE newim.im_webhook_deliveries SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE delivery_id=$1", claimed[0].ID)
	claimed, err = f.repo.Claim(ctx, time.Now().Add(time.Second), "observability_owner", 2*time.Second, 10)
	must(t, err)
	if len(claimed) != 1 {
		t.Fatalf("second claimed=%d", len(claimed))
	}
	allowed, err = f.repo.BeginAttempt(ctx, claimed[0].ID, claimed[0].LeaseToken, f.now, 3, time.Second)
	must(t, err)
	if !allowed {
		t.Fatal("dead-letter attempt not admitted")
	}
	must(t, f.repo.Finish(ctx, claimed[0].ID, claimed[0].LeaseToken, claimed[0].Attempts+1, f.now, app.Outcome{
		Status: "dead_letter", ErrorCode: app.CodeHTTPPermanent,
	}))
	assertDeliveryGauge(t, f, "dead_letter", 1)
	assertDeliveryGauge(t, f, "retry", 0)

	// A fresh adapter represents restart and must rehydrate from the same authoritative rows.
	restartedRepo, err := openWebhookRepository(t)
	must(t, err)
	defer restartedRepo.Close()
	counts, err := restartedRepo.Counts(context.Background())
	must(t, err)
	if counts.DeadLetter != 1 {
		t.Fatalf("restarted dead_letter=%d want=1", counts.DeadLetter)
	}

	f.repo.Close()
	if _, err = f.repo.Counts(context.Background()); err == nil {
		t.Fatal("closed repository returned fabricated counts")
	}
	t.Run("authoritative-outcomes", testWebhookObservabilityAuthoritativeOutcomes)
}

func assertDeliveryGauge(t *testing.T, f *fixture, state string, want float64) {
	t.Helper()
	counts, err := f.repo.Counts(context.Background())
	must(t, err)
	var got int
	switch state {
	case "pending":
		got = counts.Pending
	case "retry":
		got = counts.Retry
	case "leased":
		got = counts.Leased
	case "dead_letter":
		got = counts.DeadLetter
	default:
		t.Fatalf("unknown gauge state %q", state)
	}
	if float64(got) != want {
		t.Fatalf("counts state=%s value=%d want=%v", state, got, want)
	}
}

func openWebhookRepository(t *testing.T) (*store.Repository, error) {
	t.Helper()
	dsn := fmt.Sprintf("host=/var/run/postgresql user=newim_test dbname=newim_test sslmode=disable")
	return store.Open(context.Background(), store.Config{DSN: dsn, AllowLocalSocket: true, MaxConnections: 8, ApplicationName: "nim_webhook_restart"})
}
