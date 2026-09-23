//go:build integration

package webhook_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	app "github.com/m-ice/NewIM/server/webhook"
)

type captureObserver struct {
	mu     sync.Mutex
	values []string
}

func (o *captureObserver) Observe(observation app.Observation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.values = append(o.values, string(observation.Code))
}

func (o *captureObserver) text() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return strings.Join(o.values, "|")
}

func TestWebhookProtocol(t *testing.T) {
	f := openFixture(t)
	conversation := f.id("webhook_protocol_room")
	eventID := f.seedEvent(conversation)
	secret := []byte("0123456789abcdef0123456789abcdef")
	received := make(chan struct{}, 1)
	var headers http.Header
	var body []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers = r.Header.Clone()
		body = make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		received <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	_, master := f.insertEndpoint(1, server.URL, secret)
	client, err := app.NewSecureClient(nil, app.Policy{AllowHTTP: true, AllowLoopback: true}, time.Second, 64*1024)
	must(t, err)
	observer := &captureObserver{}
	worker := f.workerWithObserver(master, client, observer)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()
	select {
	case <-received:
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if f.scalarString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID) == "delivered" {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
	case <-time.After(5 * time.Second):
		cancel()
		counts, countsErr := f.repo.Counts(ctx)
		t.Fatalf("webhook request was not delivered: event=%s outbox_count=%d counts=%+v counts_err=%v observer=%q outbox=%q delivery=%q endpoint=%q",
			eventID, f.scalarInt64("SELECT count(*) FROM newim.im_outbox_events WHERE event_id=$1", eventID),
			counts, countsErr, observer.text(),
			f.scalarString("SELECT COALESCE(webhook_fanout_at::text,'') FROM newim.im_outbox_events WHERE event_id=$1", eventID),
			f.scalarString("SELECT COALESCE(string_agg(status||':'||attempts||':'||COALESCE(last_error_code,''),','),'') FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID),
			f.scalarString("SELECT COALESCE(string_agg(status||':'||active_revision::text,','),'') FROM newim.im_webhook_endpoints"))
	}
	must(t, <-done)
	parsed, err := protocol.ParseWebhookHeaders(map[string][]string(headers))
	must(t, err)
	if parsed.EventID != eventID {
		t.Fatalf("event identity = %s want %s", parsed.EventID, eventID)
	}
	guard, err := protocol.NewMemoryWebhookReplayGuard(8)
	must(t, err)
	if err = protocol.VerifyWebhookSignature(ctx, secret, parsed, body, f.now, guard); err != nil {
		t.Fatalf("receiver signature verification failed: %v", err)
	}
	if got := f.scalarString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID); got != "delivered" {
		t.Fatalf("delivery status = %s observer=%q", got, observer.text())
	}
}

func TestWebhookRecovery(t *testing.T) {
	f := openFixture(t)
	conversation := f.id("webhook_recovery_room")
	eventID := f.seedEvent(conversation)
	oldDestination, _ := f.insertEndpoint(1, "https://example.invalid/hook", []byte("0123456789abcdef0123456789abcdef"))
	if n, err := f.repo.Fanout(ctx, f.now, 10, 1000, 10000); err != nil || n != 1 {
		t.Fatalf("fanout = %d, %v", n, err)
	}
	if f.scalarString("SELECT webhook_fanout_at::text FROM newim.im_outbox_events WHERE event_id=$1", eventID) == "" {
		t.Fatal("webhook-specific fanout marker missing")
	}
	if f.scalarString("SELECT COALESCE(completed_at::text,'') FROM newim.im_outbox_events WHERE event_id=$1", eventID) != "" {
		t.Fatal("shared outbox completion was modified")
	}
	claimed, err := f.repo.Claim(ctx, f.now, "owner_a", 2*time.Second, 10)
	must(t, err)
	if len(claimed) != 1 || claimed[0].Attempts != 0 {
		t.Fatalf("initial claim = %+v", claimed)
	}
	ok, err := f.repo.BeginAttempt(ctx, claimed[0].ID, claimed[0].LeaseToken, f.now, 3, time.Second)
	must(t, err)
	if !ok {
		t.Fatal("first attempt was not admitted")
	}
	// Simulate a worker crash before the HTTP request: the expired lease is
	// reclaimed without consuming an HTTP attempt.
	f.sql("UPDATE newim.im_webhook_deliveries SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE delivery_id=$1", claimed[0].ID)
	claimed, err = f.repo.Claim(ctx, time.Now(), "owner_b", 2*time.Second, 10)
	must(t, err)
	if len(claimed) != 1 || claimed[0].Attempts != 1 {
		t.Fatalf("claim-after-crash attempts = %+v", claimed)
	}
	ok, err = f.repo.BeginAttempt(ctx, claimed[0].ID, "wrong_token", f.now, 3, time.Second)
	must(t, err)
	if ok {
		t.Fatal("stale lease token was admitted")
	}
	ok, err = f.repo.BeginAttempt(ctx, claimed[0].ID, claimed[0].LeaseToken, f.now, 3, time.Second)
	must(t, err)
	if !ok {
		t.Fatal("live lease was not admitted")
	}
	if err = f.repo.Finish(ctx, claimed[0].ID, "wrong_token", 2, f.now, app.Outcome{Status: "delivered", ErrorCode: app.CodeDeliveryDelivered}); app.ErrorCode(err) != app.CodeLeaseLost {
		t.Fatalf("stale finish error = %v", err)
	}
	if err = f.repo.Finish(ctx, claimed[0].ID, claimed[0].LeaseToken, 2, f.now, app.Outcome{Status: "delivered", ErrorCode: app.CodeDeliveryDelivered}); err != nil {
		t.Fatal(err)
	}
	if err = f.repo.Finish(ctx, claimed[0].ID, claimed[0].LeaseToken, 2, f.now, app.Outcome{Status: "delivered", ErrorCode: app.CodeDeliveryDelivered}); app.ErrorCode(err) != app.CodeLeaseLost {
		t.Fatalf("second finish error = %v", err)
	}

	f.sql("UPDATE newim.im_webhook_endpoints SET status='revoked',revoked_at=clock_timestamp(),updated_at=clock_timestamp() WHERE destination_id=$1", oldDestination)
	revokedEvent := f.seedEvent(f.id("webhook_revocation_room"))
	destination, _ := f.insertEndpoint(1, "https://example.invalid/revoked", []byte("0123456789abcdef0123456789abcdef"))
	if n, err := f.repo.Fanout(ctx, time.Now(), 10, 1000, 10000); err != nil || n != 1 {
		t.Fatalf("revocation fanout = %d, %v", n, err)
	}
	claimed, err = f.repo.Claim(ctx, time.Now(), "owner_c", 2*time.Second, 10)
	must(t, err)
	if len(claimed) != 1 || claimed[0].Event.ID != revokedEvent {
		t.Fatalf("revocation claim = %+v", claimed)
	}
	f.sql("UPDATE newim.im_webhook_endpoints SET status='revoked',revoked_at=clock_timestamp(),updated_at=clock_timestamp() WHERE destination_id=$1", destination)
	ok, err = f.repo.BeginAttempt(ctx, claimed[0].ID, claimed[0].LeaseToken, time.Now(), 3, time.Second)
	must(t, err)
	if ok {
		t.Fatal("revoked endpoint was admitted at request start")
	}
	if _, err = f.repo.CancelRevoked(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := f.scalarString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", revokedEvent); got != "cancelled" {
		t.Fatalf("revoked delivery status = %s", got)
	}
}

func TestWebhookSecurity(t *testing.T) {
	client, err := app.NewSecureClient(nil, app.Policy{}, time.Second, 1024)
	must(t, err)
	if _, err = client.Do(ctx, "http://127.0.0.1/hook", nil, []byte(`{}`)); err == nil {
		t.Fatal("loopback/plaintext endpoint accepted by production policy")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/other", http.StatusFound)
	}))
	defer server.Close()
	testClient, err := app.NewSecureClient(nil, app.Policy{AllowHTTP: true, AllowLoopback: true}, time.Second, 1024)
	must(t, err)
	response, err := testClient.Do(ctx, server.URL, nil, []byte(`{}`))
	must(t, err)
	if response.StatusCode != http.StatusFound {
		t.Fatalf("redirect not returned as terminal: %d", response.StatusCode)
	}
}

func TestWebhookRedaction(t *testing.T) {
	f := openFixture(t)
	conversation := f.id("webhook_redaction_room")
	eventID := f.seedEvent(conversation)
	secret := []byte("0123456789abcdef0123456789abcdef")
	const sentinel = "secret-token-sentinel"
	badDoer := doerFunc(func(context.Context, string, map[string]string, []byte) (app.Response, error) {
		return app.Response{}, errors.New(sentinel)
	})
	observer := &captureObserver{}
	worker, err := app.NewWorker(app.Config{
		Owner: "redaction_owner", BatchSize: 4, MaxConcurrent: 2, MaxPerDestination: 2, MaxAttempts: 2,
		MaxResponseBytes: 1024, LeaseTTL: 2 * time.Second, RequestTimeout: time.Second,
		BaseBackoff: 10 * time.Millisecond, MaxBackoff: time.Second, HighWater: 100, LowWater: 10,
		MaxDestinationQueue: 100, RatePerSecond: 100, RateBurst: 100,
		IdleDelay: 10 * time.Millisecond, Clock: app.ClockFunc(func() time.Time { return f.now }), Observer: observer,
	}, f.repo, badDoer, f.resolver([]byte("0123456789abcdef0123456789abcdef")))
	must(t, err)
	_, _ = f.insertEndpoint(1, "https://example.invalid/hook?token="+sentinel, secret)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.scalarInt64("SELECT count(*) FROM newim.im_webhook_deliveries WHERE event_id=$1 AND last_error_code IS NOT NULL", eventID) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	must(t, <-done)
	if strings.Contains(observer.text(), sentinel) {
		t.Fatalf("observer leaked sentinel: %s", observer.text())
	}
	code := f.scalarString("SELECT COALESCE(last_error_code,'') FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID)
	if code == "" || strings.Contains(code, sentinel) {
		t.Fatalf("delivery error code = %q", code)
	}
}

type doerFunc func(context.Context, string, map[string]string, []byte) (app.Response, error)

func (f doerFunc) Do(ctx context.Context, target string, headers map[string]string, body []byte) (app.Response, error) {
	return f(ctx, target, headers, body)
}
