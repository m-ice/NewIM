package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
)

// AttachMetricsSource adds a generic read-only source to the ops exposition.
func (s *Server) AttachMetricsSource(source MetricsSource) {
	if s == nil || source == nil {
		return
	}
	s.metrics.attachSource(source)
}

// NewMetricsSink creates a bounded sink attached to this server's exposition.
func (s *Server) NewMetricsSink(capacity int) *BoundedMetricsSink {
	if s == nil {
		return nil
	}
	return NewBoundedMetricsSink(s.metrics.registry, capacity)
}

// AttachMetricsRegistry replaces the server registry with an externally
// composed bounded registry before the server starts.
// AttachMetricsRegistry 在服务启动前替换为外部组合的有界 registry。
func (s *Server) AttachMetricsRegistry(registry *MetricsRegistry) {
	if s == nil || s.metrics == nil || registry == nil {
		return
	}
	s.metrics.mu.Lock()
	s.metrics.registry = registry
	s.metrics.mu.Unlock()
}

// MetricsRegistry exposes the server registry for bounded composition tests.
func (s *Server) MetricsRegistry() *MetricsRegistry {
	if s == nil {
		return nil
	}
	return s.metrics.registry
}

func (m *metrics) attachSource(source MetricsSource) {
	if m == nil || source == nil {
		return
	}
	m.sourcesMu.Lock()
	m.sources = append(m.sources, source)
	m.sourcesMu.Unlock()
}

func (m *metrics) sourceSnapshot() []MetricsSource {
	if m == nil {
		return nil
	}
	m.sourcesMu.RLock()
	sources := append([]MetricsSource(nil), m.sources...)
	m.sourcesMu.RUnlock()
	return sources
}

func (m *metrics) collectSourceMetrics() []MetricSample {
	if m == nil || m.registry == nil {
		return nil
	}
	var collected []MetricSample
	seen := make(map[string]struct{})
	for _, source := range m.sourceSnapshot() {
		samples, err := snapshotSourceSafely(source)
		if err != nil {
			m.registry.Drop("storage_unavailable")
			continue
		}
		for _, sample := range samples {
			normalized, ok := m.registry.normalizeForSource(sample)
			if !ok {
				continue
			}
			key := metricSeriesKey(normalized)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			collected = append(collected, normalized)
		}
	}
	sortMetricSamples(collected)
	return collected
}

func snapshotSourceSafely(source MetricsSource) (samples []MetricSample, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			samples = nil
			err = errors.New("metrics source panic")
		}
	}()
	if source == nil {
		return nil, errors.New("nil metrics source")
	}
	samples, err = source.SnapshotMetrics(context.Background())
	return samples, err
}

func writeMetricSamples(w io.Writer, samples []MetricSample) error {
	if w == nil {
		return errors.New("nil metrics writer")
	}
	for _, sample := range samples {
		if err := writeMetricSample(w, sample); err != nil {
			return err
		}
	}
	return nil
}

func writeMetricSample(w io.Writer, sample MetricSample) error {
	if w == nil {
		return errors.New("nil metrics writer")
	}
	if _, ok := metricSpecs[sample.Name]; !ok {
		return fmt.Errorf("unknown metric")
	}
	keys := make([]string, 0, len(sample.Labels))
	for key := range sample.Labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if _, err := io.WriteString(w, sample.Name); err != nil {
		return err
	}
	if len(keys) != 0 {
		if _, err := io.WriteString(w, "{"); err != nil {
			return err
		}
		for index, key := range keys {
			if index != 0 {
				if _, err := io.WriteString(w, ","); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(w, "%s=%s", key, quoteLabel(sample.Labels[key])); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(w, "}"); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, " %s\n", formatMetricValue(sample.Value))
	return err
}
