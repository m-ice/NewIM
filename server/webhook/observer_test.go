package webhook

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestLogObserverRedactsSensitiveFields(t *testing.T) {
	var output bytes.Buffer
	observer := NewLogObserver(slog.New(slog.NewJSONHandler(&output, nil)))
	observer.Observe(Observation{Operation: "delivery", Code: CodeHTTPTemporary, Total: 2, MaxDestination: 1})
	text := output.String()
	for _, required := range []string{"webhook_observation", "WEBHOOK_HTTP_TEMPORARY", "pending_total", "pending_max_destination"} {
		if !strings.Contains(text, required) {
			t.Fatalf("observer output missing %q: %s", required, text)
		}
	}
	for _, forbidden := range []string{"http://", "secret", "signature", "payload"} {
		if strings.Contains(strings.ToLower(text), forbidden) {
			t.Fatalf("observer output leaked %q: %s", forbidden, text)
		}
	}
}
