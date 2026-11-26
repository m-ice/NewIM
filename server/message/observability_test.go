package message

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/conversation"
)

type lifecycleStore struct {
	replay       bool
	conflict     bool
	validation   error
	persistedAt  time.Time
	validateCall bool
}

func (s *lifecycleStore) Persist(_ context.Context, principal conversation.Principal, request protocol.Send, validate func() error, generate func() (Generated, error)) (PersistedMessage, error) {
	if s.conflict {
		return PersistedMessage{}, Fail(SendIDConflict)
	}
	if s.validation != nil {
		return PersistedMessage{}, s.validation
	}
	if err := validate(); err != nil {
		return PersistedMessage{}, err
	}
	s.validateCall = true
	generated, err := generate()
	if err != nil {
		return PersistedMessage{}, err
	}
	s.persistedAt = time.Now()
	return PersistedMessage{
		ClientMsgID: request.ClientMsgID, ConversationID: request.ConversationID, SenderID: principal.UserID,
		ServerMsgID: generated.ServerMsgID, ConversationSeq: 7, ServerTime: generated.ServerTime, Replayed: s.replay,
	}, nil
}

type lifecycleIDs struct {
	mu     sync.Mutex
	next   int
	fail   bool
	prefix string
}

func (g *lifecycleIDs) NewID() (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fail {
		return "", errors.New("entropy unavailable")
	}
	g.next++
	raw := fmt.Sprintf("%032x", g.next)
	if g.prefix != "" {
		raw = g.prefix + raw[len(g.prefix):]
	}
	return raw, nil
}

type lifecycleObserver struct {
	mu           sync.Mutex
	observations []Observation
}

func (o *lifecycleObserver) Observe(observation Observation) {
	o.mu.Lock()
	o.observations = append(o.observations, observation)
	o.mu.Unlock()
}

func (o *lifecycleObserver) stages() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]string, 0, len(o.observations))
	for _, observation := range o.observations {
		if observation.Stage != "" {
			out = append(out, observation.Stage)
		}
	}
	return out
}

func TestMessageLifecycleCommitACKOrderingAndTrace(t *testing.T) {
	observer := &lifecycleObserver{}
	metrics := api.NewMetricsRegistry()
	store := &lifecycleStore{}
	service, err := NewService(store, Config{
		IDs: &lifecycleIDs{}, TraceIDs: &lifecycleIDs{}, Clock: ClockFunc(func() time.Time { return time.UnixMilli(1800000000000) }),
		Observer: observer, Metrics: metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	identity := mustIdentity(t, "alice")
	request := mustSend("client_a", "conversation_a")
	frame, err := service.Send(context.Background(), identity, request)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Ack == nil {
		t.Fatal("missing ACK")
	}
	want := []string{LifecycleAccepted, LifecycleValidated, LifecyclePersisting, LifecyclePersisted, LifecycleACKReady}
	stages := waitForObserverStages(t, observer, want)
	if strings.Join(stages, ",") != strings.Join(want, ",") {
		t.Fatalf("stages=%v want=%v", stages, want)
	}
	observer.mu.Lock()
	for _, observation := range observer.observations {
		if observation.Stage == "" {
			continue
		}
		if len(observation.TraceID) != 32 || !validTraceID(observation.TraceID) {
			t.Fatalf("invalid trace id: %+v", observation)
		}
		if strings.Contains(observation.TraceID, "client_a") || observation.TraceID == "" {
			t.Fatalf("trace id leaked business identity: %+v", observation)
		}
	}
	observer.mu.Unlock()
	snapshot := metrics.Snapshot()
	if !containsSample(snapshot, api.MetricMessageLifecycleEvents, map[string]string{"component": "send", "stage": "persisted", "result": "success"}) {
		t.Fatalf("persisted metric missing: %+v", snapshot)
	}
	if !containsSample(snapshot, api.MetricMessageSendIdempotency, map[string]string{"outcome": "new"}) {
		t.Fatalf("idempotency new metric missing: %+v", snapshot)
	}
}

func TestMessageReplayAndConflictObservation(t *testing.T) {
	metrics := api.NewMetricsRegistry()
	service, err := NewService(&lifecycleStore{replay: true}, Config{
		IDs: &lifecycleIDs{}, TraceIDs: &lifecycleIDs{}, Clock: ClockFunc(func() time.Time { return time.UnixMilli(1800000000000) }), Metrics: metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(context.Background(), mustIdentity(t, "alice"), mustSend("client_replay", "conversation_replay")); err != nil {
		t.Fatal(err)
	}
	if !containsSample(metrics.Snapshot(), api.MetricMessageSendIdempotency, map[string]string{"outcome": "replayed"}) {
		t.Fatalf("replay metric missing: %+v", metrics.Snapshot())
	}

	conflictMetrics := api.NewMetricsRegistry()
	conflicting, err := NewService(&lifecycleStore{conflict: true}, Config{
		IDs: &lifecycleIDs{}, TraceIDs: &lifecycleIDs{}, Clock: ClockFunc(func() time.Time { return time.UnixMilli(1800000000000) }), Metrics: conflictMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conflicting.Send(context.Background(), mustIdentity(t, "alice"), mustSend("client_conflict", "conversation_conflict")); ErrorCode(err) != SendIDConflict {
		t.Fatalf("conflict error=%v", err)
	}
	if !containsSample(conflictMetrics.Snapshot(), api.MetricMessageSendIdempotency, map[string]string{"outcome": "conflict"}) {
		t.Fatalf("conflict metric missing: %+v", conflictMetrics.Snapshot())
	}
}

func TestTraceIDFailureIsBestEffort(t *testing.T) {
	metrics := api.NewMetricsRegistry()
	service, err := NewService(&lifecycleStore{}, Config{
		IDs:      &lifecycleIDs{},
		TraceIDs: &lifecycleIDs{fail: true},
		Clock:    ClockFunc(func() time.Time { return time.UnixMilli(1800000000000) }), Metrics: metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(context.Background(), mustIdentity(t, "alice"), mustSend("client_trace_failure", "conversation_trace_failure")); err != nil {
		t.Fatalf("trace failure changed send result: %v", err)
	}
	if !containsSample(metrics.Snapshot(), api.MetricObservabilityDropped, map[string]string{"reason": "trace_id_failure"}) {
		t.Fatalf("trace failure drop missing: %+v", metrics.Snapshot())
	}
}

func TestPanickingObserverDoesNotChangeAuthoritativeResult(t *testing.T) {
	metrics := api.NewMetricsRegistry()
	service, err := NewService(&lifecycleStore{}, Config{
		IDs: &lifecycleIDs{}, TraceIDs: &lifecycleIDs{}, Clock: ClockFunc(func() time.Time { return time.UnixMilli(1800000000000) }),
		Observer: panicLifecycleObserver{}, Metrics: metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(context.Background(), mustIdentity(t, "alice"), mustSend("client_panic", "conversation_panic")); err != nil {
		t.Fatalf("panicking observer changed result: %v", err)
	}
	if !waitForSample(metrics, api.MetricObservabilityDropped, map[string]string{"reason": "writer_error"}) {
		t.Fatalf("writer_error drop missing: %+v", metrics.Snapshot())
	}
}

func TestBlockedObserverDoesNotBlockSend(t *testing.T) {
	observer := &blockingLifecycleObserver{started: make(chan struct{}), release: make(chan struct{})}
	service, err := NewService(&lifecycleStore{}, Config{
		IDs: &lifecycleIDs{}, TraceIDs: &lifecycleIDs{}, Clock: ClockFunc(func() time.Time { return time.UnixMilli(1800000000000) }),
		Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	identity := mustIdentity(t, "alice")
	request := mustSend("client_block", "conversation_block")
	result := make(chan error, 1)
	go func() {
		_, sendErr := service.Send(context.Background(), identity, request)
		result <- sendErr
	}()
	select {
	case <-observer.started:
	case <-time.After(time.Second):
		t.Fatal("observer did not start")
	}
	select {
	case sendErr := <-result:
		if sendErr != nil {
			t.Fatalf("send failed: %v", sendErr)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("blocked observer delayed authoritative send")
	}
	close(observer.release)
	service.Close()
}

type panicLifecycleObserver struct{}

func (panicLifecycleObserver) Observe(Observation) { panic("observer failure") }

type blockingLifecycleObserver struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (o *blockingLifecycleObserver) Observe(Observation) {
	o.once.Do(func() { close(o.started) })
	<-o.release
}

func waitForObserverStages(t *testing.T, observer *lifecycleObserver, want []string) []string {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stages := observer.stages()
		if len(stages) >= len(want) {
			return stages
		}
		time.Sleep(time.Millisecond)
	}
	return observer.stages()
}

func waitForSample(registry *api.MetricsRegistry, name string, labels map[string]string) bool {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if containsSample(registry.Snapshot(), name, labels) {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func mustIdentity(t *testing.T, user string) session.ConnectionIdentity {
	t.Helper()
	identity, err := session.NewConnectionIdentity(user, "device", "session", "connection", "0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func mustSend(clientID, conversationID string) protocol.Send {
	payload, _ := json.Marshal(map[string]string{"text": "hello"})
	return protocol.Send{
		ClientMsgID: clientID, ConversationID: conversationID, ProtocolVersion: 1, Version: 1, Type: "text", Payload: payload,
	}
}

func containsSample(samples []api.MetricSample, name string, labels map[string]string) bool {
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
