package api

import (
	"context"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// MetricMessageLifecycleEvents is the additive send/outbox/webhook lifecycle counter.
	MetricMessageLifecycleEvents = "newim_message_lifecycle_events_total"
	// MetricMessageLifecycleErrors is the fixed-code lifecycle error counter.
	MetricMessageLifecycleErrors = "newim_message_lifecycle_errors_total"
	// MetricMessageLifecycleDurationSum is the lifecycle duration sum in seconds.
	MetricMessageLifecycleDurationSum = "newim_message_lifecycle_duration_seconds_sum"
	// MetricMessageLifecycleDurationCount is the lifecycle duration sample count.
	MetricMessageLifecycleDurationCount = "newim_message_lifecycle_duration_seconds_count"
	// MetricMessageSendIdempotency is the fixed outcome idempotency counter.
	MetricMessageSendIdempotency = "newim_message_send_idempotency_total"
	// MetricMessageOutboxFanoutPending is the durable outbox fanout gauge.
	MetricMessageOutboxFanoutPending = "newim_message_outbox_fanout_pending"
	// MetricWebhookDeliveryPending is the durable webhook delivery gauge.
	MetricWebhookDeliveryPending = "newim_webhook_delivery_pending"
	// MetricWebhookDeliveries is the bounded webhook lifecycle counter.
	MetricWebhookDeliveries = "newim_webhook_deliveries_total"
	// MetricObservabilityDropped is the fixed-reason observability drop counter.
	MetricObservabilityDropped = "newim_observability_dropped_total"
)

const maxMetricSeries = 4096

type metricKind uint8

const (
	metricCounter metricKind = iota
	metricGauge
)

type metricSpec struct {
	kind       metricKind
	labelKeys  []string
	labelRules map[string]map[string]struct{}
}

// MetricSample is one already-bounded Prometheus-style sample.
type MetricSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// MetricsSink accepts bounded metric samples without changing authoritative work.
type MetricsSink interface {
	Record(MetricSample)
	Drop(reason string)
}

// MetricsSource emits read-only durable gauges at scrape time.
type MetricsSource interface {
	SnapshotMetrics(context.Context) ([]MetricSample, error)
}

var metricSpecs = map[string]metricSpec{
	MetricMessageLifecycleEvents: {
		kind:      metricCounter,
		labelKeys: []string{"component", "stage", "result"},
		labelRules: map[string]map[string]struct{}{
			"component": {"send": {}, "outbox": {}, "webhook": {}},
			"stage": {
				"accepted": {}, "validated": {}, "persisting": {}, "persisted": {}, "ack_ready": {},
				"rejected": {}, "enqueued": {}, "fanout": {}, "claimed": {}, "attempt": {},
				"finished": {}, "delivery_success": {}, "delivery_retry": {}, "delivery_dead_letter": {},
				"unknown": {},
			},
			"result": {"success": {}, "retryable": {}, "permanent": {}, "unknown": {}},
		},
	},
	MetricMessageLifecycleErrors: {
		kind:       metricCounter,
		labelKeys:  []string{"component", "code"},
		labelRules: map[string]map[string]struct{}{"component": {"send": {}, "outbox": {}, "webhook": {}}},
	},
	MetricMessageLifecycleDurationSum: {
		kind:      metricCounter,
		labelKeys: []string{"component", "stage"},
		labelRules: map[string]map[string]struct{}{
			"component": {"send": {}, "outbox": {}, "webhook": {}},
			"stage": {
				"accepted": {}, "validated": {}, "persisting": {}, "persisted": {}, "ack_ready": {},
				"rejected": {}, "enqueued": {}, "fanout": {}, "claimed": {}, "attempt": {},
				"finished": {}, "delivery_success": {}, "delivery_retry": {}, "delivery_dead_letter": {},
				"unknown": {},
			},
		},
	},
	MetricMessageLifecycleDurationCount: {
		kind:      metricCounter,
		labelKeys: []string{"component", "stage"},
		labelRules: map[string]map[string]struct{}{
			"component": {"send": {}, "outbox": {}, "webhook": {}},
			"stage": {
				"accepted": {}, "validated": {}, "persisting": {}, "persisted": {}, "ack_ready": {},
				"rejected": {}, "enqueued": {}, "fanout": {}, "claimed": {}, "attempt": {},
				"finished": {}, "delivery_success": {}, "delivery_retry": {}, "delivery_dead_letter": {},
				"unknown": {},
			},
		},
	},
	MetricMessageSendIdempotency: {
		kind:       metricCounter,
		labelKeys:  []string{"outcome"},
		labelRules: map[string]map[string]struct{}{"outcome": {"new": {}, "replayed": {}, "conflict": {}}},
	},
	MetricMessageOutboxFanoutPending: {
		kind:       metricGauge,
		labelKeys:  []string{"state"},
		labelRules: map[string]map[string]struct{}{"state": {"pending": {}, "quarantined": {}}},
	},
	MetricWebhookDeliveryPending: {
		kind:       metricGauge,
		labelKeys:  []string{"state"},
		labelRules: map[string]map[string]struct{}{"state": {"pending": {}, "retry": {}, "leased": {}, "dead_letter": {}}},
	},
	MetricWebhookDeliveries: {
		kind:      metricCounter,
		labelKeys: []string{"stage", "result"},
		labelRules: map[string]map[string]struct{}{
			"stage": {
				"enqueued": {}, "fanout": {}, "claimed": {}, "attempt": {}, "finished": {},
				"delivery_success": {}, "delivery_retry": {}, "delivery_dead_letter": {}, "unknown": {},
			},
			"result": {"success": {}, "retryable": {}, "permanent": {}, "unknown": {}},
		},
	},
	MetricObservabilityDropped: {
		kind:      metricCounter,
		labelKeys: []string{"reason"},
		labelRules: map[string]map[string]struct{}{
			"reason": {
				"invalid_label": {}, "writer_error": {}, "trace_id_failure": {},
				"queue_full": {}, "storage_unavailable": {},
			},
		},
	},
}

var knownMetricCodes = map[string]struct{}{
	"SEND_INVALID_INPUT": {}, "SEND_UNAUTHORIZED": {}, "SEND_CONVERSATION_NOT_FOUND": {},
	"SEND_SEQUENCE_EXHAUSTED": {}, "SEND_ID_CONFLICT": {}, "SEND_LOCK_UNAVAILABLE": {},
	"SEND_STORAGE_UNAVAILABLE": {}, "SEND_UNKNOWN": {}, "SERVER_PERSISTED": {},
	"SEND_ACCEPTED": {}, "SEND_VALIDATED": {}, "SEND_PERSISTING": {}, "SEND_PERSISTED": {},
	"SEND_ACK_READY": {}, "SEND_REJECTED": {},
	"WEBHOOK_INVALID_CONFIG": {}, "WEBHOOK_STORAGE_UNAVAILABLE": {}, "WEBHOOK_LEASE_LOST": {},
	"WEBHOOK_SECRET_UNAVAILABLE": {}, "WEBHOOK_SECRET_INVALID": {}, "WEBHOOK_HTTP_TEMPORARY": {},
	"WEBHOOK_HTTP_PERMANENT": {}, "WEBHOOK_RESPONSE_TOO_LARGE": {}, "WEBHOOK_BACKLOG_PAUSED": {},
	"WEBHOOK_ENDPOINT_REVOKED": {}, "WEBHOOK_DELIVERY_DEAD_LETTER": {}, "WEBHOOK_PROTOCOL_INVALID": {},
	"WEBHOOK_DELIVERY_CANCELLED": {}, "WEBHOOK_DELIVERY_DELIVERED": {}, "WEBHOOK_DELIVERY_RETRY": {},
	"WEBHOOK_FANOUT_LIMIT": {}, "WEBHOOK_FANOUT_SUCCESS": {}, "WEBHOOK_CLAIM_SUCCESS": {},
	"WEBHOOK_ATTEMPT_SUCCESS": {}, "WEBHOOK_FINISH_SUCCESS": {}, "WEBHOOK_UNKNOWN": {},
	"OBSERVABILITY_UNKNOWN": {},
}

type registrySeries struct {
	sample MetricSample
}

// MetricsRegistry is a bounded in-process registry for additive observability.
type MetricsRegistry struct {
	mu      sync.Mutex
	series  map[string]registrySeries
	dropped map[string]float64
}

// NewMetricsRegistry constructs an empty bounded registry.
func NewMetricsRegistry() *MetricsRegistry {
	return &MetricsRegistry{series: make(map[string]registrySeries), dropped: make(map[string]float64)}
}

// Record adds or replaces one bounded sample.
func (r *MetricsRegistry) Record(sample MetricSample) {
	if r == nil {
		return
	}
	normalized, reason, ok := normalizeMetricSample(sample)
	if !ok {
		r.Drop(reason)
		return
	}
	key := metricSeriesKey(normalized)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.series[key]; !exists && len(r.series) >= maxMetricSeries {
		r.dropLocked("invalid_label")
		return
	}
	if existing, exists := r.series[key]; exists {
		if metricSpecs[normalized.Name].kind == metricCounter {
			normalized.Value += existing.sample.Value
		}
	}
	r.series[key] = registrySeries{sample: cloneSample(normalized)}
}

// Drop increments one fixed observability drop reason.
func (r *MetricsRegistry) Drop(reason string) {
	if r == nil {
		return
	}
	if _, ok := metricSpecs[MetricObservabilityDropped].labelRules["reason"][reason]; !ok {
		reason = "invalid_label"
	}
	r.mu.Lock()
	r.dropLocked(reason)
	r.mu.Unlock()
}

func (r *MetricsRegistry) dropLocked(reason string) {
	r.dropped[reason]++
}

// Snapshot returns a stable copy of the current samples.
func (r *MetricsRegistry) Snapshot() []MetricSample {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	samples := make([]MetricSample, 0, len(r.series)+len(r.dropped))
	for _, series := range r.series {
		samples = append(samples, cloneSample(series.sample))
	}
	for reason, value := range r.dropped {
		samples = append(samples, MetricSample{
			Name:   MetricObservabilityDropped,
			Labels: map[string]string{"reason": reason},
			Value:  value,
		})
	}
	r.mu.Unlock()
	sortMetricSamples(samples)
	return samples
}

func (r *MetricsRegistry) normalizeForSource(sample MetricSample) (MetricSample, bool) {
	normalized, reason, ok := normalizeMetricSample(sample)
	if !ok {
		r.Drop(reason)
		return MetricSample{}, false
	}
	return normalized, true
}

func normalizeMetricSample(sample MetricSample) (MetricSample, string, bool) {
	spec, ok := metricSpecs[sample.Name]
	if !ok || len(sample.Labels) != len(spec.labelKeys) {
		return MetricSample{}, "invalid_label", false
	}
	if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) || sample.Value < 0 {
		return MetricSample{}, "invalid_label", false
	}
	for _, key := range spec.labelKeys {
		if _, ok := sample.Labels[key]; !ok {
			return MetricSample{}, "invalid_label", false
		}
	}
	for key, value := range sample.Labels {
		if !containsString(spec.labelKeys, key) {
			return MetricSample{}, "invalid_label", false
		}
		if value == "" && key != "code" {
			return MetricSample{}, "invalid_label", false
		}
		if rule, ok := spec.labelRules[key]; ok {
			if _, ok := rule[value]; !ok {
				if key == "code" {
					continue
				}
				if key == "stage" || key == "result" {
					sample.Labels[key] = fallbackValue(key)
					continue
				}
				return MetricSample{}, "invalid_label", false
			}
		}
	}
	if _, hasCode := sample.Labels["code"]; hasCode {
		if _, ok := knownMetricCodes[sample.Labels["code"]]; !ok {
			component := sample.Labels["component"]
			sample.Labels["code"] = fallbackCode(component)
		}
	}
	return cloneSample(sample), "", true
}

func fallbackValue(key string) string {
	switch key {
	case "stage":
		return "unknown"
	case "result":
		return "unknown"
	default:
		return "unknown"
	}
}

func fallbackCode(component string) string {
	switch component {
	case "send":
		return "SEND_UNKNOWN"
	case "webhook":
		return "WEBHOOK_UNKNOWN"
	default:
		return "OBSERVABILITY_UNKNOWN"
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func cloneSample(sample MetricSample) MetricSample {
	labels := make(map[string]string, len(sample.Labels))
	for key, value := range sample.Labels {
		labels[key] = value
	}
	return MetricSample{Name: sample.Name, Labels: labels, Value: sample.Value}
}

func metricSeriesKey(sample MetricSample) string {
	keys := make([]string, 0, len(sample.Labels))
	for key := range sample.Labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	builder.WriteString(sample.Name)
	for _, key := range keys {
		builder.WriteByte('\x00')
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(sample.Labels[key])
	}
	return builder.String()
}

func sortMetricSamples(samples []MetricSample) {
	sort.Slice(samples, func(i, j int) bool {
		return metricSeriesKey(samples[i]) < metricSeriesKey(samples[j])
	})
}

// BoundedMetricsSink enqueues observations without blocking the authoritative path.
type BoundedMetricsSink struct {
	registry *MetricsRegistry
	queue    chan MetricSample
	closed   atomic.Bool
	done     chan struct{}
}

// NewBoundedMetricsSink constructs a non-blocking bounded sink.
func NewBoundedMetricsSink(registry *MetricsRegistry, capacity int) *BoundedMetricsSink {
	if registry == nil {
		registry = NewMetricsRegistry()
	}
	if capacity < 1 {
		capacity = 1
	}
	if capacity > 65536 {
		capacity = 65536
	}
	sink := &BoundedMetricsSink{
		registry: registry,
		queue:    make(chan MetricSample, capacity),
		done:     make(chan struct{}),
	}
	go sink.run()
	return sink
}

// Record performs a non-blocking enqueue; overflow is counted and dropped.
func (s *BoundedMetricsSink) Record(sample MetricSample) {
	if s == nil {
		return
	}
	defer func() {
		if recover() != nil {
			s.Drop("writer_error")
		}
	}()
	if s.closed.Load() {
		s.Drop("writer_error")
		return
	}
	select {
	case s.queue <- cloneSample(sample):
	default:
		s.Drop("queue_full")
	}
}

// Drop records one fixed reason without blocking.
func (s *BoundedMetricsSink) Drop(reason string) {
	if s == nil {
		return
	}
	defer func() { _ = recover() }()
	s.registry.Drop(reason)
}

// Registry returns the sink's registry for composition and bounded tests.
func (s *BoundedMetricsSink) Registry() *MetricsRegistry {
	if s == nil {
		return nil
	}
	return s.registry
}

// Close drains queued samples and stops the sink.
func (s *BoundedMetricsSink) Close() {
	if s == nil || !s.closed.CompareAndSwap(false, true) {
		return
	}
	close(s.queue)
	<-s.done
}

func (s *BoundedMetricsSink) run() {
	defer close(s.done)
	for sample := range s.queue {
		func() {
			defer func() {
				if recover() != nil {
					s.registry.Drop("writer_error")
				}
			}()
			s.registry.Record(sample)
		}()
	}
}

func formatMetricValue(value float64) string {
	if value == math.Trunc(value) {
		return strconv.FormatInt(int64(value), 10)
	}
	return strconv.FormatFloat(value, 'f', 9, 64)
}

// WaitForDrain is a bounded test helper for deterministic sink assertions.
func (s *BoundedMetricsSink) WaitForDrain(timeout time.Duration) bool {
	if s == nil {
		return false
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(s.queue) == 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return len(s.queue) == 0
}
