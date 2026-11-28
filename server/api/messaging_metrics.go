package api

import (
	"net/http"
	"time"
)

const (
	MetricGatewayActive        = "newim_gateway_active_subscriptions"
	MetricGatewayOperations    = "newim_gateway_operations_total"
	MetricGatewayDurationSum   = "newim_gateway_operation_duration_seconds_sum"
	MetricGatewayDurationCount = "newim_gateway_operation_duration_seconds_count"
	MetricSDKEvents            = "newim_sdk_events_total"
	MetricSDKAckDurationSum    = "newim_sdk_ack_duration_seconds_sum"
	MetricSDKAckDurationCount  = "newim_sdk_ack_duration_seconds_count"
	MetricSDKOutboxDepthSum    = "newim_sdk_outbox_depth_sum"
	MetricSDKOutboxDepthCount  = "newim_sdk_outbox_depth_count"
	MetricPushDispatch         = "newim_push_dispatch_total"
)

func values(items ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(items))
	for _, value := range items {
		result[value] = struct{}{}
	}
	return result
}

var gatewayOperations = values("handshake", "subscribe", "delivery", "visibility_notice",
	"directory_bootstrap", "directory_delta", "message_page", "visibility_page")
var gatewayResults = values("success", "rejected", "temporary")

func gatewayMetricSpec(withResult bool) metricSpec {
	spec := metricSpec{
		kind: metricCounter, labelKeys: []string{"operation"},
		labelRules: map[string]map[string]struct{}{"operation": gatewayOperations},
	}
	if withResult {
		spec.labelKeys = append(spec.labelKeys, "result")
		spec.labelRules["result"] = gatewayResults
	}
	return spec
}

// ObserveGatewayOperation adds count and elapsed time to the existing bounded sink.
func ObserveGatewayOperation(sink MetricsSink, operation, result string, elapsed time.Duration) {
	if sink == nil {
		return
	}
	sink.Record(MetricSample{
		Name: MetricGatewayOperations, Value: 1, Labels: map[string]string{"operation": operation, "result": result},
	})
	label := map[string]string{"operation": operation}
	sink.Record(MetricSample{Name: MetricGatewayDurationCount, Value: 1, Labels: label})
	sink.Record(MetricSample{Name: MetricGatewayDurationSum, Value: max(elapsed, 0).Seconds(), Labels: label})
}

// NewOperationalMetricsHandler uses the same bounded exposition on a separate ops listener.
func NewOperationalMetricsHandler(version string, registry *MetricsRegistry, sources ...MetricsSource) http.Handler {
	m := newMetrics(version)
	if registry != nil {
		m.registry = registry
	}
	for _, source := range sources {
		m.attachSource(source)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.EscapedPath() != "/metrics" || r.URL.ForceQuery || r.URL.RawQuery != "" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = m.writeTo(w)
	})
}
