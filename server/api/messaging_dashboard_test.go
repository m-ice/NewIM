package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestMessagingDashboard(t *testing.T) {
	raw, err := os.ReadFile("../../infra/observability/messaging-dashboard.json")
	if err != nil {
		t.Fatal(err)
	}
	var dashboard struct {
		Panels []struct {
			Title   string
			Targets []struct{ Expr string }
		}
	}
	if err := json.Unmarshal(raw, &dashboard); err != nil || len(dashboard.Panels) == 0 {
		t.Fatalf("invalid messaging dashboard: %v", err)
	}

	registry := NewMetricsRegistry()
	for _, stage := range []string{"accepted", "ack_ready"} {
		registry.Record(MetricSample{MetricMessageLifecycleEvents,
			map[string]string{"component": "send", "stage": stage, "result": "success"}, 1})
	}
	persisted := map[string]string{"component": "send", "stage": "persisted"}
	registry.Record(MetricSample{MetricMessageLifecycleDurationSum, persisted, 0.1})
	registry.Record(MetricSample{MetricMessageLifecycleDurationCount, persisted, 1})
	ObserveGatewayOperation(registry, "delivery", "success", 0)
	registry.Record(MetricSample{MetricSDKEvents, map[string]string{"event": "reconnect"}, 1})
	for _, name := range []string{
		MetricSDKAckDurationSum, MetricSDKAckDurationCount, MetricSDKOutboxDepthSum, MetricSDKOutboxDepthCount,
	} {
		registry.Record(MetricSample{Name: name, Value: 1})
	}
	gauges := staticSource{samples: []MetricSample{
		{Name: MetricGatewayActive, Value: 1},
		{MetricMessageOutboxFanoutPending, map[string]string{"state": "pending"}, 1},
		{MetricWebhookDeliveryPending, map[string]string{"state": "retry"}, 1},
		{MetricPushDispatch, map[string]string{"result": "accepted"}, 1},
	}}
	recorder := httptest.NewRecorder()
	NewOperationalMetricsHandler("test", registry, gauges).
		ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", recorder.Code)
	}
	lines := strings.Split(recorder.Body.String(), "\n")
	selector := regexp.MustCompile(`\b(newim_[a-z_]+)(?:\{([^{}]+)\})?`)
	grouping := regexp.MustCompile(`\bby\s*\(([^()]+)\)`)
	for _, panel := range dashboard.Panels {
		t.Run(panel.Title, func(t *testing.T) {
			if len(panel.Targets) == 0 {
				t.Fatal("panel has no query")
			}
			for _, target := range panel.Targets {
				selectors := selector.FindAllStringSubmatch(target.Expr, -1)
				if len(selectors) == 0 {
					t.Errorf("query contains no NewIM selector: %s", target.Expr)
				}
				for _, match := range selectors {
					spec, ok := metricSpecs[match[1]]
					if !ok || !hasDashboardSample(lines, match[1], match[2]) {
						t.Errorf("selector is absent from the scrape: %s", match[0])
						continue
					}
					for _, group := range grouping.FindAllStringSubmatch(target.Expr, -1) {
						for _, label := range strings.Split(group[1], ",") {
							if !containsString(spec.labelKeys, strings.TrimSpace(label)) {
								t.Errorf("%s has no grouping label %q", match[1], label)
							}
						}
					}
				}
			}
		})
	}
}

func hasDashboardSample(lines []string, name, filter string) bool {
	for _, line := range lines {
		series, _, _ := strings.Cut(line, " ")
		metric, raw, _ := strings.Cut(series, "{")
		if metric != name {
			continue
		}
		if filter == "" {
			return true
		}
		labels := strings.Split(strings.TrimSuffix(raw, "}"), ",")
		matched := true
		for _, equal := range strings.Split(filter, ",") {
			if !containsString(labels, strings.TrimSpace(equal)) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
