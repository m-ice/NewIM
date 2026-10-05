//go:build integration

package message_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/api"
	app "github.com/m-ice/NewIM/server/message"
	store "github.com/m-ice/NewIM/server/storage/message"
)

func testMessageObservabilityAuthoritativeLifecycle(t *testing.T) {
	f := openFixture(t)
	user := "observability_lifecycle_user"
	conversationID := f.seedConversation("observability_lifecycle", user)
	registry := api.NewMetricsRegistry()
	sink := api.NewBoundedMetricsSink(registry, 1024)
	observer := &messageObservationCapture{}
	service, err := app.NewService(f.repo, app.Config{
		IDs:      &sequenceIDs{prefix: "observability_lifecycle_generated"},
		TraceIDs: &traceSequence{},
		Clock:    app.ClockFunc(func() time.Time { return time.UnixMilli(f.now.Load()).UTC() }),
		Observer: observer,
		Metrics:  sink,
	})
	must(t, err)
	defer service.Close()
	defer sink.Close()

	request := f.request("observability_lifecycle_client", conversationID, "authoritative")
	first, err := service.Send(ctx, f.identity(user), request)
	must(t, err)
	firstAck := ack(t, first)
	f.assertAtomic(conversationID, firstAck.ServerMsgID)

	second, err := service.Send(ctx, f.identity(user), request)
	must(t, err)
	secondAck := ack(t, second)
	if firstAck.ServerMsgID != secondAck.ServerMsgID || firstAck.ConversationSeq != secondAck.ConversationSeq {
		t.Fatalf("replay changed authoritative result: first=%+v second=%+v", firstAck, secondAck)
	}

	conflict := request
	conflict.Payload = []byte(`{"text":"different"}`)
	if _, err = service.Send(ctx, f.identity(user), conflict); err == nil {
		t.Fatal("conflicting intent unexpectedly succeeded")
	}
	mustCode(t, err, app.SendIDConflict)
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", user, request.ClientMsgID); got != 1 {
		t.Fatalf("authoritative message count=%d want 1", got)
	}

	want := []string{app.LifecycleAccepted, app.LifecycleValidated, app.LifecyclePersisting,
		app.LifecyclePersisted, app.LifecycleACKReady}
	for _, stage := range want {
		waitForMessageMetric(t, registry, api.MetricMessageLifecycleEvents,
			map[string]string{"component": "send", "stage": stage, "result": "success"})
	}
	waitForMessageMetric(t, registry, api.MetricMessageLifecycleEvents,
		map[string]string{"component": "outbox", "stage": "enqueued", "result": "success"})
	waitForMessageMetric(t, registry, api.MetricMessageSendIdempotency, map[string]string{"outcome": "new"})
	waitForMessageMetric(t, registry, api.MetricMessageSendIdempotency, map[string]string{"outcome": "replayed"})
	waitForMessageMetric(t, registry, api.MetricMessageSendIdempotency, map[string]string{"outcome": "conflict"})
	waitForMessageMetric(t, registry, api.MetricMessageLifecycleErrors,
		map[string]string{"component": "send", "code": string(app.SendIDConflict)})

	observations := observer.waitForCount(t, 12, 2*time.Second)
	firstStages := observations[:5]
	for index, stage := range want {
		if firstStages[index].Stage != stage {
			t.Fatalf("observation order=%v want prefix=%v", observationStages(observations), want)
		}
		if !validObservationTrace(firstStages[index].TraceID) {
			t.Fatalf("invalid trace id %q", firstStages[index].TraceID)
		}
	}
	attemptTraces := make([]string, 0, 3)
	for _, observation := range observations {
		if observation.Stage == app.LifecycleAccepted {
			attemptTraces = append(attemptTraces, observation.TraceID)
		}
	}
	if len(attemptTraces) < 2 || attemptTraces[0] == attemptTraces[1] {
		t.Fatalf("separate attempts reused or omitted a trace id: %v", attemptTraces)
	}
	for _, observation := range observations {
		for _, prohibited := range []string{request.ClientMsgID, conversationID, user} {
			if observation.TraceID == prohibited {
				t.Fatalf("trace id leaked business identity %q", prohibited)
			}
		}
	}
}

type messageObservationCapture struct {
	mu           sync.Mutex
	observations []app.Observation
}

func (o *messageObservationCapture) Observe(observation app.Observation) {
	o.mu.Lock()
	o.observations = append(o.observations, observation)
	o.mu.Unlock()
}

func (o *messageObservationCapture) waitForCount(t *testing.T, count int, timeout time.Duration) []app.Observation {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		o.mu.Lock()
		if len(o.observations) >= count {
			result := append([]app.Observation(nil), o.observations...)
			o.mu.Unlock()
			return result
		}
		o.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	t.Fatalf("observations=%d want at least %d: %+v", len(o.observations), count, o.observations)
	return nil
}

func observationStages(observations []app.Observation) []string {
	stages := make([]string, 0, len(observations))
	for _, observation := range observations {
		stages = append(stages, observation.Stage)
	}
	return stages
}

func validObservationTrace(value string) bool {
	if len(value) != 32 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func waitForMessageMetric(t *testing.T, registry *api.MetricsRegistry, name string, labels map[string]string) {
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
	t.Run("authoritative-lifecycle", testMessageObservabilityAuthoritativeLifecycle)
}

type traceSequence struct{ next atomic.Int64 }

func (g *traceSequence) NewID() (string, error) {
	return fmt.Sprintf("%032x", g.next.Add(1)), nil
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
