//go:build integration

package webhook_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	store "github.com/m-ice/NewIM/server/storage/webhook"
	app "github.com/m-ice/NewIM/server/webhook"
)

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
