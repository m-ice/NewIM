// Package observability receives aggregate SDK metrics from authenticated clients.
package observability

import (
	"context"
	jsonv2 "encoding/json/v2"
	"io"
	"mime"
	"net/http"
	"net/netip"
	"time"

	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/auth/session"
)

const SDKRoute = "/api/v1/observability/sdk"
const maxReport = 2048
const maxCount = 1024
const maxAckMS = 600000

type SDKAuthenticator interface {
	AuthenticateBearer(context.Context, string) (session.BearerSession, error)
}

type SDKBudget interface {
	AllowAddress(netip.Addr) bool
	AllowAccount(string) bool
}

type SDKHandler struct {
	auth   SDKAuthenticator
	budget SDKBudget
	sink   api.MetricsSink
}

func NewSDKHandler(auth SDKAuthenticator, budget SDKBudget, sink api.MetricsSink) http.Handler {
	return &SDKHandler{auth: auth, budget: budget, sink: sink}
}

type report struct {
	Version          uint8  `json:"version"`
	SocketAttempt    uint32 `json:"socket_attempt"`
	Ready            uint32 `json:"ready"`
	Reconnect        uint32 `json:"reconnect"`
	AckCommitted     uint32 `json:"ack_committed"`
	AckFailed        uint32 `json:"ack_failed"`
	AckMillis        uint32 `json:"ack_millis"`
	GapDetected      uint32 `json:"gap_detected"`
	PageCommitted    uint32 `json:"page_committed"`
	PageFailed       uint32 `json:"page_failed"`
	MessageCommitted uint32 `json:"message_committed"`
	OutboxSamples    uint32 `json:"outbox_samples"`
	OutboxPending    uint32 `json:"outbox_pending"`
}

func (h *SDKHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.TLS == nil || !r.TLS.HandshakeComplete || r.URL.EscapedPath() != SDKRoute || r.URL.ForceQuery || r.URL.RawQuery != "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	address, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil || !h.budget.AllowAddress(address.Addr().Unmap()) {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	raw, ok := bearerhttp.ParseBearerHeader(r.Header.Values("Authorization"))
	if !ok {
		sdkUnauthorized(w)
		return
	}
	identity, err := h.auth.AuthenticateBearer(r.Context(), raw)
	if err != nil {
		status, _ := bearerhttp.MapAuthenticationError(err)
		if status == http.StatusUnauthorized {
			sdkUnauthorized(w)
		} else {
			w.WriteHeader(status)
		}
		return
	}
	if !time.Now().Before(identity.ExpiresAt()) {
		sdkUnauthorized(w)
		return
	}
	if !h.budget.AllowAccount(identity.UserID()) {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" || len(r.Header.Values("Content-Type")) != 1 ||
		len(r.Header.Values("Content-Encoding")) != 0 {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxReport))
	var sample report
	if err != nil || jsonv2.Unmarshal(body, &sample, jsonv2.RejectUnknownMembers(true)) != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if !sample.record(h.sink) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r report) record(sink api.MetricsSink) bool {
	if r.Version != 1 || r.AckCommitted > maxCount || r.AckMillis > maxAckMS*r.AckCommitted ||
		r.OutboxSamples > maxCount || r.OutboxPending > 10000*r.OutboxSamples {
		return false
	}
	events := [...]struct {
		label string
		count uint32
	}{
		{"socket_attempt", r.SocketAttempt}, {"ready", r.Ready}, {"reconnect", r.Reconnect},
		{"ack_committed", r.AckCommitted}, {"ack_failed", r.AckFailed}, {"gap_detected", r.GapDetected},
		{"page_committed", r.PageCommitted}, {"page_failed", r.PageFailed}, {"message_committed", r.MessageCommitted},
	}
	for _, event := range events {
		if event.count > maxCount {
			return false
		}
	}
	for _, event := range events {
		if event.count > 0 {
			sink.Record(api.MetricSample{
				Name: api.MetricSDKEvents, Value: float64(event.count), Labels: map[string]string{"event": event.label},
			})
		}
	}
	if r.AckCommitted > 0 {
		sink.Record(api.MetricSample{Name: api.MetricSDKAckDurationCount, Value: float64(r.AckCommitted)})
		sink.Record(api.MetricSample{Name: api.MetricSDKAckDurationSum, Value: float64(r.AckMillis) / 1000})
	}
	if r.OutboxSamples > 0 {
		sink.Record(api.MetricSample{Name: api.MetricSDKOutboxDepthCount, Value: float64(r.OutboxSamples)})
		sink.Record(api.MetricSample{Name: api.MetricSDKOutboxDepthSum, Value: float64(r.OutboxPending)})
	}
	return true
}

func sdkUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="newim-session"`)
	w.WriteHeader(http.StatusUnauthorized)
}
