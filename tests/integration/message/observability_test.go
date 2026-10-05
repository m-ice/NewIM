//go:build integration

package message_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/api"
	app "github.com/m-ice/NewIM/server/message"
	store "github.com/m-ice/NewIM/server/storage/message"
)

func TestMessageObservabilityRecovery(t *testing.T) {
	f := openFixture(t)
	user := "observability_user"
	conversationID := f.seedConversation("observability", user)
	service, err := app.NewService(f.repo, app.Config{
		IDs: &sequenceIDs{prefix: "observability_generated"}, Clock: app.ClockFunc(func() time.Time { return time.UnixMilli(f.now.Load()).UTC() }),
	})
	must(t, err)
	frame, err := f.send(service, f.identity(user), f.request("observability_client", conversationID, "gauge"))
	must(t, err)
	if frame.Ack == nil {
		t.Fatal("missing ACK")
	}
	assertMessageFanoutGauges(t, f, 1, 0)

	// A fresh adapter represents process restart: counters are new, gauges are rehydrated.
	restartedRepo, err := openMessageRepository(t)
	must(t, err)
	defer restartedRepo.Close()
	samples, err := restartedRepo.SnapshotMetrics(context.Background())
	must(t, err)
	assertMetricValue(t, samples, api.MetricMessageOutboxFanoutPending, "pending", 1)
	assertMetricValue(t, samples, api.MetricMessageOutboxFanoutPending, "quarantined", 0)

	// A closed repository must not guess zero when the authoritative snapshot fails.
	f.repo.Close()
	if _, err = f.repo.SnapshotMetrics(context.Background()); err == nil {
		t.Fatal("closed repository returned a fabricated gauge")
	}
}

func assertMessageFanoutGauges(t *testing.T, f *fixture, pending, quarantined int64) {
	t.Helper()
	samples, err := f.repo.SnapshotMetrics(context.Background())
	must(t, err)
	assertMetricValue(t, samples, api.MetricMessageOutboxFanoutPending, "pending", float64(pending))
	assertMetricValue(t, samples, api.MetricMessageOutboxFanoutPending, "quarantined", float64(quarantined))
}

func assertMetricValue(t *testing.T, samples []api.MetricSample, name, state string, want float64) {
	t.Helper()
	for _, sample := range samples {
		if sample.Name == name && sample.Labels["state"] == state {
			if sample.Value != want {
				t.Fatalf("metric %s state=%s value=%v want=%v", name, state, sample.Value, want)
			}
			return
		}
	}
	t.Fatalf("metric %s state=%s missing in %+v", name, state, samples)
}

func openMessageRepository(t *testing.T) (*store.Repository, error) {
	t.Helper()
	return store.Open(context.Background(), store.Config{
		DSN:              fmt.Sprintf("host=/var/run/postgresql user=newim_test dbname=newim_test sslmode=disable"),
		AllowLocalSocket: true, MaxConnections: 8, ApplicationName: "nim_message_restart",
	})
}
