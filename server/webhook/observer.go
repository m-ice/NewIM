package webhook

import (
	"log/slog"
)

// LogObserver emits bounded operation and stable-code fields only.
// LogObserver 只输出有界 operation 和稳定码字段。
type LogObserver struct{ logger *slog.Logger }

// NewLogObserver creates a redacted production observer.
// NewLogObserver 创建脱敏的生产观测器。
func NewLogObserver(logger *slog.Logger) *LogObserver {
	return &LogObserver{logger: logger}
}

// Observe records a webhook operation without URL, body, secret or signature fields.
// Observe 记录 Webhook 操作，不包含 URL、body、secret 或 signature 字段。
func (o *LogObserver) Observe(observation Observation) {
	if o == nil || o.logger == nil {
		return
	}
	o.logger.Info("webhook_observation",
		"operation", observation.Operation,
		"code", string(observation.Code),
		"elapsed_ms", observation.Elapsed.Milliseconds(),
		"pending_total", observation.Total,
		"pending_max_destination", observation.MaxDestination,
	)
}
