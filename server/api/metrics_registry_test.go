package api

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type blockingSource struct {
	started chan struct{}
	release chan struct{}
	err     error
	samples []MetricSample
}

func (s *blockingSource) SnapshotMetrics(context.Context) ([]MetricSample, error) {
	select {
	case <-s.started:
	default:
		close(s.started)
	}
	<-s.release
	return s.samples, s.err
}

func TestMetricsRegistryRejectsBusinessLabels(t *testing.T) {
	registry := NewMetricsRegistry()
	for i := 0; i < 10000; i++ {
		registry.Record(MetricSample{
			Name:   MetricMessageLifecycleEvents,
			Labels: map[string]string{"component": "send", "stage": "accepted", "result": "success", "client_msg_id": "client"},
			Value:  1,
		})
	}
	if got := len(registry.Snapshot()); got != 1 {
		t.Fatalf("invalid-label series count=%d want 1 dropped counter", got)
	}
	snapshot := registry.Snapshot()
	if len(snapshot) != 1 || snapshot[0].Name != MetricObservabilityDropped || snapshot[0].Labels["reason"] != "invalid_label" || snapshot[0].Value != 10000 {
		t.Fatalf("unexpected drop snapshot: %+v", snapshot)
	}
}

func TestMetricsRegistryConvergesUnknownCode(t *testing.T) {
	registry := NewMetricsRegistry()
	registry.Record(MetricSample{
		Name:   MetricMessageLifecycleErrors,
		Labels: map[string]string{"component": "send", "code": "CLIENT_SUPPLIED_VALUE"},
		Value:  1,
	})
	snapshot := registry.Snapshot()
	if len(snapshot) != 1 || snapshot[0].Labels["code"] != "SEND_UNKNOWN" {
		t.Fatalf("unknown code did not converge: %+v", snapshot)
	}
}

func TestSlowWriterDoesNotHoldRegistryOrBlockUpdates(t *testing.T) {
	m := newMetrics("test")
	writer := &blockingWriter{started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- m.writeTo(writer) }()
	<-writer.started
	updateDone := make(chan struct{})
	go func() {
		m.begin("api", "message_send", "POST")
		m.end("api", "message_send", "POST", 200, time.Millisecond)
		close(updateDone)
	}()
	select {
	case <-updateDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("registry update blocked behind slow writer")
	}
	close(writer.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSlowSourceDoesNotBlockUpdatesAndFailureDropsGauge(t *testing.T) {
	m := newMetrics("test")
	source := &blockingSource{started: make(chan struct{}), release: make(chan struct{}), err: errors.New("storage down")}
	m.attachSource(source)
	done := make(chan error, 1)
	go func() { done <- m.writeTo(&bytes.Buffer{}) }()
	<-source.started
	updateDone := make(chan struct{})
	go func() { m.begin("api", "health", "GET"); close(updateDone) }()
	select {
	case <-updateDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("source read blocked registry update")
	}
	close(source.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := m.writeTo(&output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `newim_observability_dropped_total{reason="storage_unavailable"}`) {
		t.Fatalf("storage failure drop missing: %s", output.String())
	}
}

func TestBoundedSinkQueueFullIsNonBlocking(t *testing.T) {
	registry := NewMetricsRegistry()
	sink := NewBoundedMetricsSink(registry, 1)
	defer sink.Close()
	for i := 0; i < 4; i++ {
		sink.Record(MetricSample{Name: MetricMessageLifecycleEvents, Labels: map[string]string{"component": "send", "stage": "accepted", "result": "success"}, Value: 1})
	}
	if !sink.WaitForDrain(250 * time.Millisecond) {
		t.Fatal("sink did not drain")
	}
	var output bytes.Buffer
	_ = writeMetricSamples(&output, registry.Snapshot())
	if !strings.Contains(output.String(), `newim_observability_dropped_total{reason="queue_full"}`) {
		t.Fatalf("queue overflow not counted: %s", output.String())
	}
}

func TestObserverDispatcherConstantsRemainBounded(t *testing.T) {
	dispatcher := NewObserverDispatcher(1<<20, func(int) {}, nil)
	if dispatcher == nil || cap(dispatcher.queue) != 4096 {
		t.Fatalf("observer queue capacity=%d", cap(dispatcher.queue))
	}
	dispatcher.Close()
}

func TestAttachedSourceSamplesAreValidated(t *testing.T) {
	m := newMetrics("test")
	source := staticSource{samples: []MetricSample{{
		Name:   MetricMessageOutboxFanoutPending,
		Labels: map[string]string{"state": "pending"},
		Value:  3,
	}}}
	m.attachSource(source)
	var output bytes.Buffer
	if err := m.writeTo(&output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `newim_message_outbox_fanout_pending{state="pending"} 3`) {
		t.Fatalf("durable gauge missing: %s", output.String())
	}
}

type staticSource struct{ samples []MetricSample }

func (s staticSource) SnapshotMetrics(context.Context) ([]MetricSample, error) { return s.samples, nil }

type blockingWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(p), nil
}
