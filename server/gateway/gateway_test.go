package gateway

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestUpgradeRequiresOneCanonicalKeyAndProtocol(t *testing.T) {
	nonce := []byte("0123456789abcdef")
	source := httptest.NewRequest(http.MethodGet, "https://example.test"+Route, nil)
	source.Header.Set("Upgrade", "websocket")
	source.Header.Set("Connection", "upgrade")
	source.Header.Set("Sec-WebSocket-Version", "13")
	source.Header.Set("Sec-WebSocket-Protocol", Subprotocol)
	source.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(nonce))
	if got, ok := validateUpgrade(source); !ok || got != sha256.Sum256(nonce) {
		t.Fatal("valid RFC 6455 handshake rejected")
	}
	for _, tc := range []struct {
		name   string
		change func(*http.Request)
	}{
		{"origin-even-empty", func(r *http.Request) { r.Header.Set("Origin", "") }},
		{"query-even-empty", func(r *http.Request) { r.URL.ForceQuery = true }},
		{"multiple-keys", func(r *http.Request) { r.Header.Add("Sec-WebSocket-Key", r.Header.Get("Sec-WebSocket-Key")) }},
		{"noncanonical-key", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", "MDEyMzQ1Njc4OWFiY2RlZh==") }},
		{"case-variant-protocol", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", "NewIM.v1") }},
		{"missing-tls", func(r *http.Request) { r.TLS = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := source.Clone(context.Background())
			tc.change(r)
			if _, ok := validateUpgrade(r); ok {
				t.Fatal("invalid handshake accepted")
			}
		})
	}
}

func TestSubscriptionCursorIsUnambiguous(t *testing.T) {
	correct := []byte(`{"afterSeq":"9223372036854775807","conversationId":"conv_1"}`)
	if got, ok := decodeSubscription(correct); !ok || got.afterSeq != 9223372036854775807 || got.conversationID != "conv_1" {
		t.Fatalf("valid subscription rejected: %+v", got)
	}
	for _, data := range []string{
		`{"conversationId":"conv_1","afterSeq":"0","afterSeq":"1"}`,
		`{"conversationId":"conv_1","afterSeq":0}`,
		`{"conversationId":"conv_1","afterSeq":"00"}`,
		`{"conversationId":"conv_1","afterSeq":"9223372036854775808"}`,
		`{"conversationId":"conv_1","afterSeq":"0"}{}`,
		`{"conversationId":"conv_1","afterSeq":"0","extra":true}`,
	} {
		if _, ok := decodeSubscription([]byte(data)); ok {
			t.Errorf("accepted invalid subscription %s", data)
		}
	}
}

func TestHardDrainKeepsRawTransport(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	ctx := ConnContext(context.Background(), tls.Server(server, &tls.Config{}))
	if conn, _ := ctx.Value(socketContextKey{}).(net.Conn); conn != server {
		t.Fatal("hard drain would close the TLS wrapper instead of the socket")
	}
	g := &Gateway{
		sockets:  map[*socket]struct{}{{conn: server}: {}},
		draining: make(chan struct{}), drained: make(chan struct{}),
	}
	drain, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if got := g.Drain(drain); got != context.DeadlineExceeded {
		t.Fatalf("drain = %v, want deadline", got)
	}
	if _, err := client.Write([]byte("closed")); err == nil {
		t.Fatal("transport remained writable after hard drain")
	}
}
