package api

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type metricKey struct {
	listener string
	route    string
	method   string
}

type totalKey struct {
	listener string
	route    string
	method   string
	status   string
}

type metrics struct {
	mu            sync.Mutex
	serverVersion string
	startedAt     time.Time
	inFlight      map[metricKey]int
	counts        map[totalKey]uint64
	durationSum   map[metricKey]time.Duration
	durationCount map[metricKey]uint64

	registry  *MetricsRegistry
	sourcesMu sync.RWMutex
	sources   []MetricsSource
}

func newMetrics(serverVersion string) *metrics {
	return &metrics{
		serverVersion: serverVersion,
		startedAt:     time.Now(),
		inFlight:      make(map[metricKey]int),
		counts:        make(map[totalKey]uint64),
		durationSum:   make(map[metricKey]time.Duration),
		durationCount: make(map[metricKey]uint64),
		registry:      NewMetricsRegistry(),
	}
}

func (m *metrics) begin(listener, route, method string) {
	key := metricKey{listener: listener, route: route, method: method}
	m.mu.Lock()
	m.inFlight[key]++
	m.mu.Unlock()
}

func (m *metrics) end(listener, route, method string, status int, elapsed time.Duration) {
	key := metricKey{listener: listener, route: route, method: method}
	statusText := fmt.Sprintf("%03d", status)
	m.mu.Lock()
	if m.inFlight[key] > 0 {
		m.inFlight[key]--
	}
	m.durationSum[key] += elapsed
	m.durationCount[key]++
	m.counts[totalKey{listener: listener, route: route, method: method, status: statusText}]++
	m.mu.Unlock()
}

func (m *metrics) writeTo(w io.Writer) error {
	if m == nil || w == nil {
		return fmt.Errorf("nil metrics writer")
	}
	if _, err := fmt.Fprintf(w, "newim_server_info{server_version=\"%s\"} 1\n", escapeLabel(m.serverVersion)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "newim_process_start_time_seconds %s\n", strconv.FormatFloat(float64(m.startedAt.UnixNano())/1e9, 'f', 3, 64)); err != nil {
		return err
	}
	if err := m.writeInflight(w); err != nil {
		m.registry.Drop("writer_error")
		return err
	}
	if err := m.writeDurations(w); err != nil {
		m.registry.Drop("writer_error")
		return err
	}
	if err := m.writeCounts(w); err != nil {
		m.registry.Drop("writer_error")
		return err
	}

	// Source and observer locks are never held across storage reads or writes.
	sourceSamples := m.collectSourceMetrics()
	registrySamples := m.registry.Snapshot()
	if err := writeMetricSamples(w, append(registrySamples, sourceSamples...)); err != nil {
		m.registry.Drop("writer_error")
		return err
	}
	return nil
}

func (m *metrics) writeInflight(w io.Writer) error {
	m.mu.Lock()
	aggregated := make(map[metricKey]int)
	for key, count := range m.inFlight {
		group := metricKey{listener: key.listener, route: key.route}
		aggregated[group] += count
	}
	m.mu.Unlock()

	keys := make([]metricKey, 0, len(aggregated))
	for key := range aggregated {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return metricKeyString(keys[i]) < metricKeyString(keys[j])
	})
	for _, key := range keys {
		if aggregated[key] == 0 {
			continue
		}
		if _, err := fmt.Fprintf(w, "newim_http_requests_in_flight{listener=%s,route=%s} %d\n",
			quoteLabel(key.listener), quoteLabel(key.route), aggregated[key]); err != nil {
			return err
		}
	}
	return nil
}

func (m *metrics) writeDurations(w io.Writer) error {
	m.mu.Lock()
	counts := make(map[metricKey]uint64, len(m.durationCount))
	for key, value := range m.durationCount {
		counts[key] = value
	}
	sums := make(map[metricKey]time.Duration, len(m.durationSum))
	for key, value := range m.durationSum {
		sums[key] = value
	}
	m.mu.Unlock()

	keys := make([]metricKey, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return metricKeyString(keys[i]) < metricKeyString(keys[j])
	})
	for _, key := range keys {
		seconds := sums[key].Seconds()
		if _, err := fmt.Fprintf(w, "newim_http_request_duration_seconds_sum{listener=%s,route=%s,method=%s} %s\n",
			quoteLabel(key.listener), quoteLabel(key.route), quoteLabel(key.method),
			strconv.FormatFloat(seconds, 'f', 9, 64)); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "newim_http_request_duration_seconds_count{listener=%s,route=%s,method=%s} %d\n",
			quoteLabel(key.listener), quoteLabel(key.route), quoteLabel(key.method), counts[key]); err != nil {
			return err
		}
	}
	return nil
}

func (m *metrics) writeCounts(w io.Writer) error {
	m.mu.Lock()
	counts := make(map[totalKey]uint64, len(m.counts))
	for key, value := range m.counts {
		counts[key] = value
	}
	m.mu.Unlock()

	keys := make([]totalKey, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return totalKeyString(keys[i]) < totalKeyString(keys[j])
	})
	for _, key := range keys {
		if _, err := fmt.Fprintf(w, "newim_http_requests_total{listener=%s,route=%s,method=%s,status=%s} %d\n",
			quoteLabel(key.listener), quoteLabel(key.route), quoteLabel(key.method), quoteLabel(key.status), counts[key]); err != nil {
			return err
		}
	}
	return nil
}

func metricKeyString(key metricKey) string {
	return key.listener + "\x00" + key.route + "\x00" + key.method
}

func totalKeyString(key totalKey) string {
	return key.listener + "\x00" + key.route + "\x00" + key.method + "\x00" + key.status
}

func quoteLabel(value string) string {
	return `"` + escapeLabel(value) + `"`
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}
