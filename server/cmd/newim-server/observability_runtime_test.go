package main

import (
	"context"
	"testing"

	"github.com/m-ice/NewIM/server/api"
	app "github.com/m-ice/NewIM/server/webhook"
)

type countStoreStub struct {
	counts app.Counts
	err    error
	called bool
}

func (s *countStoreStub) Counts(context.Context) (app.Counts, error) {
	s.called = true
	return s.counts, s.err
}

func TestWebhookGaugeSourceUsesDurableCounts(t *testing.T) {
	store := &countStoreStub{counts: app.Counts{Pending: 1, Retry: 2, Leased: 3, DeadLetter: 4}}
	samples, err := (webhookGaugeSource{store: store}).SnapshotMetrics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !store.called {
		t.Fatal("durable Counts was not called")
	}
	for state, want := range map[string]float64{"pending": 1, "retry": 2, "leased": 3, "dead_letter": 4} {
		if !hasGauge(samples, state, want) {
			t.Fatalf("state=%s want=%v samples=%+v", state, want, samples)
		}
	}
}

func TestWebhookGaugeSourceFailureIsOmitted(t *testing.T) {
	store := &countStoreStub{err: app.Fail(app.CodeStorageUnavailable)}
	samples, err := (webhookGaugeSource{store: store}).SnapshotMetrics(context.Background())
	if err == nil || samples != nil {
		t.Fatalf("failure samples=%+v err=%v", samples, err)
	}
}

func hasGauge(samples []api.MetricSample, state string, want float64) bool {
	for _, sample := range samples {
		if sample.Name == api.MetricWebhookDeliveryPending && sample.Labels["state"] == state && sample.Value == want {
			return true
		}
	}
	return false
}
