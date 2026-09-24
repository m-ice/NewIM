package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

type testStore struct {
	mu          sync.Mutex
	delivery    Delivery
	fanout      int
	fanoutCalls int
	counts      Counts
	persistent  bool
	finished    []Outcome
	attempts    int
}

func (s *testStore) CancelRevoked(context.Context, time.Time) (int, error) { return 0, nil }
func (s *testStore) Counts(context.Context) (Counts, error)                { return s.counts, nil }
func (s *testStore) Fanout(context.Context, time.Time, int, int, int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fanoutCalls++
	return s.fanout, nil
}
func (s *testStore) Claim(context.Context, time.Time, string, time.Duration, int) ([]Delivery, error) {
	if s.delivery.ID == "" {
		return nil, nil
	}
	return []Delivery{s.delivery}, nil
}
func (s *testStore) BeginAttempt(context.Context, string, string, time.Time, int, time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	return true, nil
}
func (s *testStore) Finish(_ context.Context, _ string, _ string, _ int, _ time.Time, outcome Outcome) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finished = append(s.finished, outcome)
	if !s.persistent {
		s.delivery.ID = ""
	}
	return nil
}

type testResolver struct{ secret []byte }

func (r testResolver) Resolve(context.Context, SecretMaterial) ([]byte, error) {
	return append([]byte(nil), r.secret...), nil
}

type testDoer struct {
	status int
	err    error
}

type headerCaptureDoer struct {
	mu      sync.Mutex
	status  int
	headers []map[string]string
	bodies  [][]byte
}

func (d *headerCaptureDoer) Do(_ context.Context, _ string, headers map[string]string, body []byte) (Response, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	clonedHeaders := make(map[string]string, len(headers))
	for key, value := range headers {
		clonedHeaders[key] = value
	}
	d.headers = append(d.headers, clonedHeaders)
	d.bodies = append(d.bodies, append([]byte(nil), body...))
	return Response{StatusCode: d.status}, nil
}

type captureObserver struct {
	values []string
}

func (o *captureObserver) Observe(observation Observation) {
	o.values = append(o.values, string(observation.Code))
}

func (d testDoer) Do(context.Context, string, map[string]string, []byte) (Response, error) {
	if d.err != nil {
		return Response{}, d.err
	}
	return Response{StatusCode: d.status}, nil
}

func testConfig(now time.Time) Config {
	return Config{
		Owner:               "test_owner",
		BatchSize:           4,
		MaxConcurrent:       2,
		MaxPerDestination:   2,
		MaxAttempts:         3,
		MaxResponseBytes:    1024,
		LeaseTTL:            2 * time.Second,
		RequestTimeout:      time.Second,
		BaseBackoff:         10 * time.Millisecond,
		MaxBackoff:          time.Second,
		HighWater:           10,
		LowWater:            2,
		MaxDestinationQueue: 100,
		RatePerSecond:       100,
		RateBurst:           100,
		IdleDelay:           10 * time.Millisecond,
		Clock:               ClockFunc(func() time.Time { return now }),
	}
}

func testDelivery(handled func(http.Header, []byte)) Delivery {
	return Delivery{
		ID:               "delivery_1",
		DestinationID:    "destination_1",
		EndpointRevision: 1,
		URL:              "",
		KeyID:            "key_test",
		Secret:           SecretMaterial{DestinationID: "destination_1", Revision: 1, KeyID: "key_test"},
		Attempts:         0,
		Event: Event{
			ID:              "event_1",
			ServerMsgID:     "server_1",
			ClientMsgID:     "client_1",
			SenderID:        "alice",
			ConversationID:  "room",
			ConversationSeq: 1,
			ServerTime:      1790189000000,
			ProtocolVersion: 1,
			SchemaVersion:   1,
			MessageType:     "text",
			Payload:         json.RawMessage(`{"text":"hello"}`),
		},
	}
}

func TestWorkerDeliversSignedRequest(t *testing.T) {
	var gotHeader http.Header
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Clone()
		gotBody = make([]byte, r.ContentLength)
		_, _ = r.Body.Read(gotBody)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	delivery := testDelivery(nil)
	delivery.URL = server.URL
	store := &testStore{delivery: delivery}
	secret := []byte("0123456789abcdef0123456789abcdef")
	client, err := NewSecureClient(nil, Policy{AllowHTTP: true, AllowLoopback: true}, time.Second, 1024)
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(1790189001000)
	worker, err := NewWorker(testConfig(now), store, client, testResolver{secret: secret})
	if err != nil {
		t.Fatal(err)
	}
	if err = worker.cycle(context.Background(), make(chan struct{}, 1)); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.finished) != 1 || store.finished[0].Status != "delivered" || store.attempts != 1 {
		t.Fatalf("outcomes=%+v attempts=%d", store.finished, store.attempts)
	}
	if gotHeader.Get(protocol.WebhookHeaderEventID) != "event_1" || gotHeader.Get(protocol.WebhookHeaderDeliveryID) != "delivery_1" {
		t.Fatalf("missing webhook identity headers: %v", gotHeader)
	}
	parsed, err := protocol.ParseWebhookHeaders(map[string][]string(gotHeader))
	if err != nil {
		t.Fatal(err)
	}
	if err = protocol.VerifyWebhookSignature(context.Background(), secret, parsed, gotBody, now, mustGuard(t)); err != nil {
		t.Fatalf("receiver verification failed: %v", err)
	}
}

func mustGuard(t *testing.T) protocol.WebhookReplayGuard {
	t.Helper()
	guard, err := protocol.NewMemoryWebhookReplayGuard(8)
	if err != nil {
		t.Fatal(err)
	}
	return guard
}

func TestWorkerRetriesAndDeadLetters(t *testing.T) {
	now := time.UnixMilli(1790189001000)
	for _, tc := range []struct {
		name   string
		status int
		want   string
	}{
		{"temporary", http.StatusServiceUnavailable, "retry"},
		{"permanent", http.StatusForbidden, "dead_letter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &testStore{delivery: testDelivery(nil)}
			worker, err := NewWorker(testConfig(now), store, testDoer{status: tc.status}, testResolver{secret: []byte("0123456789abcdef0123456789abcdef")})
			if err != nil {
				t.Fatal(err)
			}
			if err = worker.cycle(context.Background(), make(chan struct{}, 1)); err != nil {
				t.Fatal(err)
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if len(store.finished) != 1 || store.finished[0].Status != tc.want {
				t.Fatalf("outcomes=%+v want status %s", store.finished, tc.want)
			}
		})
	}
}

func TestWorkerRejectsInvalidSecret(t *testing.T) {
	now := time.UnixMilli(1790189001000)
	store := &testStore{delivery: testDelivery(nil)}
	resolver := testResolver{secret: []byte("short")}
	worker, err := NewWorker(testConfig(now), store, testDoer{status: http.StatusNoContent}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err = worker.cycle(context.Background(), make(chan struct{}, 1)); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.finished) != 1 || store.finished[0].Status != "dead_letter" || store.finished[0].ErrorCode != CodeSecretInvalid {
		t.Fatalf("invalid secret outcome=%+v", store.finished)
	}
}

func TestWorkerRetryBackoffBounds(t *testing.T) {
	now := time.UnixMilli(1790189001000)
	cfg := testConfig(now)
	delay := backoff(cfg, "delivery", 2)
	if delay < cfg.BaseBackoff/2 || delay > cfg.MaxBackoff {
		t.Fatalf("backoff out of bounds: %s", delay)
	}
}

func TestRetryOutcomeDeadLettersAtMax(t *testing.T) {
	now := time.UnixMilli(1790189001000)
	cfg := testConfig(now)
	outcome := retryOutcome(CodeHTTPTemporary, cfg.MaxAttempts, cfg, "delivery", now, 0)
	if outcome.Status != "dead_letter" || outcome.ErrorCode != CodeDeliveryDeadLetter {
		t.Fatalf("max-attempt outcome = %+v", outcome)
	}
}

func TestWorkerStorageFailureIsReturned(t *testing.T) {
	cfg := testConfig(time.UnixMilli(1790189001000))
	if _, err := NewWorker(cfg, nil, testDoer{}, testResolver{}); ErrorCode(err) != CodeInvalidConfig {
		t.Fatalf("nil store error = %v", err)
	}
	if ErrorCode(errors.New("unknown")) != CodeStorageUnavailable {
		t.Fatal("unknown error classification changed")
	}
}

func TestWorkerRunStopsOnCancellation(t *testing.T) {
	now := time.UnixMilli(1790189001000)
	worker, err := NewWorker(testConfig(now), &testStore{}, testDoer{}, testResolver{secret: []byte("0123456789abcdef0123456789abcdef")})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
}

func TestWorkerDrainsAboveHighWater(t *testing.T) {
	now := time.UnixMilli(1790189001000)
	store := &testStore{
		delivery:   testDelivery(nil),
		counts:     Counts{Total: 10, MaxDestination: 10},
		fanout:     1,
		persistent: true,
	}
	cfg := testConfig(now)
	cfg.HighWater = 5
	cfg.LowWater = 1
	worker, err := NewWorker(cfg, store, testDoer{status: http.StatusNoContent}, testResolver{secret: []byte("0123456789abcdef0123456789abcdef")})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = worker.cycle(context.Background(), make(chan struct{}, 2)); err != nil {
			t.Fatal(err)
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.fanoutCalls != 0 {
		t.Fatalf("fanout ran while paused: %d", store.fanoutCalls)
	}
	if len(store.finished) != 2 || store.finished[0].Status != "delivered" {
		t.Fatalf("paused worker did not drain existing delivery: %+v", store.finished)
	}
}

func TestWorkerRedactsDoerError(t *testing.T) {
	now := time.UnixMilli(1790189001000)
	const sentinel = "secret-token-sentinel"
	store := &testStore{delivery: testDelivery(nil)}
	observer := &captureObserver{}
	cfg := testConfig(now)
	cfg.Observer = observer
	worker, err := NewWorker(cfg, store, testDoer{err: errors.New(sentinel)}, testResolver{secret: []byte("0123456789abcdef0123456789abcdef")})
	if err != nil {
		t.Fatal(err)
	}
	if err = worker.cycle(context.Background(), make(chan struct{}, 1)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(observer.values, "|"), sentinel) {
		t.Fatal("observer leaked raw doer error")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.finished) != 1 || store.finished[0].ErrorCode != CodeHTTPTemporary || store.finished[0].Status != "retry" {
		t.Fatalf("redacted outcome = %+v", store.finished)
	}
}

func TestWorkerConfigRejectsLeaseShorterThanRequest(t *testing.T) {
	cfg := testConfig(time.UnixMilli(1790189001000))
	cfg.RequestTimeout = 2 * time.Second
	cfg.LeaseTTL = time.Second
	if ErrorCode(cfg.Validate()) != CodeInvalidConfig {
		t.Fatal("unsafe lease configuration accepted")
	}
}

func TestWorkerNonceIsFreshPerAttempt(t *testing.T) {
	now := time.UnixMilli(1790189001000)
	store := &testStore{delivery: testDelivery(nil), persistent: true}
	doer := &headerCaptureDoer{status: http.StatusServiceUnavailable}
	cfg := testConfig(now)
	clockCalls := 0
	cfg.Clock = ClockFunc(func() time.Time {
		clockCalls++
		return now.Add(time.Duration(clockCalls) * time.Millisecond)
	})
	secret := []byte("0123456789abcdef0123456789abcdef")
	worker, err := NewWorker(cfg, store, doer, testResolver{secret: secret})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = worker.cycle(context.Background(), make(chan struct{}, 1)); err != nil {
			t.Fatal(err)
		}
	}
	doer.mu.Lock()
	defer doer.mu.Unlock()
	if len(doer.headers) != 2 {
		t.Fatalf("attempts = %d want 2", len(doer.headers))
	}
	headers1, headers2 := doer.headers[0], doer.headers[1]
	if headers1[protocol.WebhookHeaderNonce] == headers2[protocol.WebhookHeaderNonce] {
		t.Fatal("nonce was reused across attempts")
	}
	if headers1[protocol.WebhookHeaderTimestamp] == headers2[protocol.WebhookHeaderTimestamp] {
		t.Fatal("timestamp was not refreshed across attempts")
	}
	for _, headers := range doer.headers {
		if headers[protocol.WebhookHeaderEventID] != "event_1" || headers[protocol.WebhookHeaderDeliveryID] != "delivery_1" {
			t.Fatalf("attempt identity changed: %v", headers)
		}
	}
	if _, err = base64.RawURLEncoding.DecodeString(headers1[protocol.WebhookHeaderNonce]); err != nil {
		t.Fatal(err)
	}
}

func TestBuildEnvelopePayloadContract(t *testing.T) {
	delivery := testDelivery(nil)
	body, err := buildEnvelope(delivery.Event)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := protocol.DecodeWebhookEnvelope(body)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]json.RawMessage
	if err = json.Unmarshal(envelope.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	normalKeys := []string{"clientMsgId", "conversationId", "conversationSeq", "serverMsgId", "serverTime", "senderId", "type", "protocolVersion", "version", "payload"}
	assertWebhookPayloadKeys(t, payload, normalKeys)
	var version int
	if err = json.Unmarshal(payload["version"], &version); err != nil || version != delivery.Event.SchemaVersion {
		t.Fatalf("payload version = %d, %v", version, err)
	}

	originalPayload := json.RawMessage(`{"data":"` + strings.Repeat("a", 70000) + `"}`)
	delivery.Event.Payload = originalPayload
	body, err = buildEnvelope(delivery.Event)
	if err != nil {
		t.Fatalf("fallback envelope rejected: %v", err)
	}
	envelope, err = protocol.DecodeWebhookEnvelope(body)
	if err != nil {
		t.Fatal(err)
	}
	payload = nil
	if err = json.Unmarshal(envelope.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	fallbackKeys := append(append([]string(nil), normalKeys...), "payloadOmitted", "payloadSha256", "payloadSize")
	assertWebhookPayloadKeys(t, payload, fallbackKeys)
	var omitted bool
	var digest string
	if err = json.Unmarshal(payload["payloadOmitted"], &omitted); err != nil || !omitted {
		t.Fatalf("payloadOmitted = %v, %v", omitted, err)
	}
	sum := sha256.Sum256(originalPayload)
	wantDigest := hex.EncodeToString(sum[:])
	if err = json.Unmarshal(payload["payloadSha256"], &digest); err != nil || digest != wantDigest {
		t.Fatalf("payloadSha256 = %q, %v", digest, err)
	}
	var size string
	if err = json.Unmarshal(payload["payloadSize"], &size); err != nil || size != strconv.Itoa(len(originalPayload)) {
		t.Fatalf("payloadSize is not a decimal string: %q, %v", size, err)
	}
}

func assertWebhookPayloadKeys(t *testing.T, payload map[string]json.RawMessage, want []string) {
	t.Helper()
	if len(payload) != len(want) {
		t.Fatalf("payload key count = %d want %d: %v", len(payload), len(want), payload)
	}
	allowed := make(map[string]struct{}, len(want))
	for _, key := range want {
		allowed[key] = struct{}{}
	}
	for key := range payload {
		if _, ok := allowed[key]; !ok {
			t.Fatalf("unexpected payload key %s: %v", key, payload)
		}
	}
}
