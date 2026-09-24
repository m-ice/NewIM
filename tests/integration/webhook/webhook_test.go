//go:build integration

package webhook_test

import (
	"context"
	"errors"
	"fmt"
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
	// Simulate a worker crash after Claim but before BeginAttempt: lease expiry
	// must not consume an HTTP-attempt slot.
	f.sql("UPDATE newim.im_webhook_deliveries SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE delivery_id=$1", claimed[0].ID)
	claimed, err = f.repo.Claim(ctx, time.Now(), "owner_b", 2*time.Second, 10)
	must(t, err)
	if len(claimed) != 1 || claimed[0].Attempts != 0 {
		t.Fatalf("claim-after-crash-before-attempt = %+v", claimed)
	}
	ok, err := f.repo.BeginAttempt(ctx, claimed[0].ID, "wrong_token", f.now, 3, time.Second)
	must(t, err)
	if ok {
		t.Fatal("stale lease token was admitted")
	}
	ok, err = f.repo.BeginAttempt(ctx, claimed[0].ID, claimed[0].LeaseToken, f.now, 3, time.Second)
	must(t, err)
	if !ok {
		t.Fatal("live lease was not admitted")
	}
	if got := f.scalarInt64("SELECT attempts FROM newim.im_webhook_deliveries WHERE delivery_id=$1", claimed[0].ID); got != 1 {
		t.Fatalf("attempt accounting after BeginAttempt = %d", got)
	}

	// Simulate a crash after BeginAttempt but before the HTTP request: the
	// consumed attempt must remain counted when the lease is reclaimed.
	f.sql("UPDATE newim.im_webhook_deliveries SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE delivery_id=$1", claimed[0].ID)
	claimed, err = f.repo.Claim(ctx, time.Now(), "owner_c", 2*time.Second, 10)
	must(t, err)
	if len(claimed) != 1 || claimed[0].Attempts != 1 {
		t.Fatalf("claim-after-attempt-crash = %+v", claimed)
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

func TestWebhookConcurrentRecovery(t *testing.T) {
	f := openFixture(t)
	if _, master := f.insertEndpoint(1, "https://example.invalid/concurrent", []byte("0123456789abcdef0123456789abcdef")); len(master) == 0 {
		t.Fatal("endpoint master key missing")
	}
	const eventCount = 8
	for range eventCount {
		f.seedEvent(f.id("webhook_concurrent_room"))
	}

	type fanoutResult struct {
		count int
		err   error
	}
	start := make(chan struct{})
	fanouts := make(chan fanoutResult, 2)
	for range 2 {
		go func() {
			<-start
			count, err := f.repo.Fanout(context.Background(), time.Now(), eventCount, 1000, 10000)
			fanouts <- fanoutResult{count: count, err: err}
		}()
	}
	close(start)
	fanned := 0
	for range 2 {
		result := <-fanouts
		if result.err != nil {
			t.Fatal(result.err)
		}
		fanned += result.count
	}
	if fanned != eventCount {
		t.Fatalf("concurrent fanout count = %d want %d", fanned, eventCount)
	}
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_webhook_deliveries WHERE event_id LIKE 'webhook_event_%' AND status='pending'"); got != eventCount {
		t.Fatalf("pending deliveries = %d want %d", got, eventCount)
	}

	claims := make(chan struct {
		deliveries []app.Delivery
		err        error
	}, 2)
	start = make(chan struct{})
	owners := []string{"owner_a", "owner_b"}
	for _, owner := range owners {
		owner := owner
		go func() {
			<-start
			deliveries, err := f.repo.Claim(context.Background(), time.Now(), owner, 5*time.Second, eventCount)
			claims <- struct {
				deliveries []app.Delivery
				err        error
			}{deliveries: deliveries, err: err}
		}()
	}
	close(start)
	seen := make(map[string]bool)
	var claimed []app.Delivery
	for range 2 {
		result := <-claims
		if result.err != nil {
			t.Fatal(result.err)
		}
		for _, delivery := range result.deliveries {
			if seen[delivery.ID] {
				t.Fatalf("delivery claimed twice: %s", delivery.ID)
			}
			seen[delivery.ID] = true
			claimed = append(claimed, delivery)
		}
	}
	if len(claimed) != eventCount {
		t.Fatalf("concurrent claims = %d want %d", len(claimed), eventCount)
	}
	for _, delivery := range claimed {
		ok, err := f.repo.BeginAttempt(context.Background(), delivery.ID, delivery.LeaseToken, time.Now(), 3, time.Second)
		must(t, err)
		if !ok {
			t.Fatalf("claimed delivery was not admitted: %s", delivery.ID)
		}
		must(t, f.repo.Finish(context.Background(), delivery.ID, delivery.LeaseToken, delivery.Attempts+1, time.Now(), app.Outcome{Status: "delivered", ErrorCode: app.CodeDeliveryDelivered}))
	}
}

func TestWebhookReceiverAmbiguity(t *testing.T) {
	f := openFixture(t)
	eventID := f.seedEvent(f.id("webhook_ambiguous_room"))
	var mu sync.Mutex
	var records []struct {
		deliveryID string
		nonce      string
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		records = append(records, struct {
			deliveryID string
			nonce      string
		}{deliveryID: r.Header.Get(protocol.WebhookHeaderDeliveryID), nonce: r.Header.Get(protocol.WebhookHeaderNonce)})
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	_, master := f.insertEndpoint(1, server.URL, []byte("0123456789abcdef0123456789abcdef"))
	client, err := app.NewSecureClient(nil, app.Policy{AllowHTTP: true, AllowLoopback: true}, time.Second, 64*1024)
	must(t, err)

	first := newFixtureWorker(t, f, f.repo, &lostResponseDoer{next: client}, master, 3, 1000, 100, 10*time.Millisecond)
	runWorkerUntil(t, first, 3*time.Second, func() bool {
		return f.optionalString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID) == "retry" &&
			f.scalarInt64("SELECT attempts FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID) == 1
	})
	if got := f.scalarString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID); got != "retry" {
		t.Fatalf("delivery status after lost response = %s", got)
	}
	if got := f.scalarInt64("SELECT attempts FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID); got != 1 {
		t.Fatalf("attempts after lost response = %d", got)
	}
	f.sql("UPDATE newim.im_webhook_deliveries SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE event_id=$1", eventID)

	observer := &captureObserver{}
	second := newFixtureWorker(t, f, f.repo, client, master, 3, 1000, 100, 10*time.Millisecond, observer)
	runWorkerUntil(t, second, 3*time.Second, func() bool {
		return f.optionalString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID) == "delivered"
	}, func() string {
		mu.Lock()
		attempts := len(records)
		mu.Unlock()
		return fmt.Sprintf("status=%q attempts=%d records=%d error=%q observed=%q", f.optionalString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID), f.scalarInt64("SELECT COALESCE(max(attempts),0) FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID), attempts, f.optionalString("SELECT COALESCE(last_error_code,'') FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID), observer.text())
	})
	if got := f.scalarString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID); got != "delivered" {
		t.Fatalf("delivery status after retry = %s", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(records) != 2 {
		t.Fatalf("HTTP attempts = %d want 2", len(records))
	}
	if records[0].deliveryID != records[1].deliveryID || records[0].deliveryID == "" {
		t.Fatalf("delivery identity changed across ambiguity: %+v", records)
	}
	if records[0].nonce == records[1].nonce || records[0].nonce == "" {
		t.Fatalf("nonce was not fresh across ambiguity: %+v", records)
	}
}

func TestWebhookRetryAndBacklog(t *testing.T) {
	t.Run("finite retry becomes dead letter", func(t *testing.T) {
		f := openFixture(t)
		eventID := f.seedEvent(f.id("webhook_dead_letter_room"))
		_, master := f.insertEndpoint(1, "https://example.invalid/dead-letter", []byte("0123456789abcdef0123456789abcdef"))
		doer := doerFunc(func(context.Context, string, map[string]string, []byte) (app.Response, error) {
			return app.Response{StatusCode: http.StatusServiceUnavailable}, nil
		})
		worker := newFixtureWorker(t, f, f.repo, doer, master, 2, 1000, 100, 10*time.Millisecond)
		runWorkerUntil(t, worker, 3*time.Second, func() bool {
			return f.optionalString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID) == "retry"
		})
		if got := f.scalarString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID); got != "retry" {
			t.Fatalf("first retry status = %s", got)
		}
		f.sql("UPDATE newim.im_webhook_deliveries SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE event_id=$1", eventID)
		runWorkerUntil(t, worker, 3*time.Second, func() bool {
			return f.optionalString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID) == "dead_letter"
		})
		if got := f.scalarString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID); got != "dead_letter" {
			t.Fatalf("dead-letter status = %s", got)
		}
		if got := f.scalarInt64("SELECT attempts FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID); got != 2 {
			t.Fatalf("dead-letter attempts = %d", got)
		}
	})

	t.Run("backlog pauses fanout and drains existing work", func(t *testing.T) {
		f := openFixture(t)
		firstEvent := f.seedEvent(f.id("webhook_backlog_first_room"))
		secondEvent := f.seedEvent(f.id("webhook_backlog_second_room"))
		_, master := f.insertEndpoint(1, "https://example.invalid/backlog", []byte("0123456789abcdef0123456789abcdef"))
		if count, err := f.repo.Fanout(context.Background(), time.Now(), 1, 1000, 10000); err != nil || count != 1 {
			t.Fatalf("backlog setup fanout = %d, %v", count, err)
		}
		firstMarked := f.scalarString("SELECT COALESCE(webhook_fanout_at::text,'') FROM newim.im_outbox_events WHERE event_id=$1", firstEvent) != ""
		secondMarked := f.scalarString("SELECT COALESCE(webhook_fanout_at::text,'') FROM newim.im_outbox_events WHERE event_id=$1", secondEvent) != ""
		if firstMarked == secondMarked {
			t.Fatalf("backlog setup marked events unexpectedly: first=%v second=%v", firstMarked, secondMarked)
		}
		doer := doerFunc(func(context.Context, string, map[string]string, []byte) (app.Response, error) {
			return app.Response{StatusCode: http.StatusNoContent}, nil
		})
		worker := newFixtureWorker(t, f, f.repo, doer, master, 3, 1, 0, 5*time.Second)
		runWorkerUntil(t, worker, 3*time.Second, func() bool {
			return f.scalarInt64("SELECT count(*) FROM newim.im_webhook_deliveries WHERE event_id IN ($1,$2) AND status='delivered'", firstEvent, secondEvent) == 1
		}, func() string {
			return fmt.Sprintf("delivery statuses=%q unfanned=%v/%v", f.scalarString("SELECT COALESCE(string_agg(event_id||':'||status||':'||attempts::text||':'||COALESCE(last_error_code,''),','),'') FROM newim.im_webhook_deliveries WHERE event_id IN ($1,$2)", firstEvent, secondEvent), f.scalarString("SELECT webhook_fanout_at::text FROM newim.im_outbox_events WHERE event_id=$1", firstEvent) != "", f.scalarString("SELECT webhook_fanout_at::text FROM newim.im_outbox_events WHERE event_id=$1", secondEvent) != "")
		})
		if got := f.scalarInt64("SELECT count(*) FROM newim.im_webhook_deliveries WHERE event_id IN ($1,$2) AND status='delivered'", firstEvent, secondEvent); got != 1 {
			t.Fatalf("drained deliveries = %d want 1", got)
		}
		unfanned := firstEvent
		if firstMarked {
			unfanned = secondEvent
		}
		if f.scalarString("SELECT COALESCE(webhook_fanout_at::text,'') FROM newim.im_outbox_events WHERE event_id=$1", unfanned) != "" {
			t.Fatalf("backlog-paused event was fanned out: %s", unfanned)
		}
		f.sql("UPDATE newim.im_outbox_events SET webhook_fanout_at=clock_timestamp() WHERE event_id=$1", unfanned)
	})

	t.Run("nonstandard status dead letters without invalid status write", func(t *testing.T) {
		f := openFixture(t)
		eventID := f.seedEvent(f.id("webhook_nonstandard_status_room"))
		_, master := f.insertEndpoint(1, "https://example.invalid/nonstandard", []byte("0123456789abcdef0123456789abcdef"))
		doer := doerFunc(func(context.Context, string, map[string]string, []byte) (app.Response, error) {
			return app.Response{StatusCode: 700}, nil
		})
		worker := newFixtureWorker(t, f, f.repo, doer, master, 3, 1000, 100, 10*time.Millisecond)
		runWorkerUntil(t, worker, 3*time.Second, func() bool {
			return f.optionalString("SELECT status FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID) == "dead_letter"
		})
		if got := f.optionalString("SELECT COALESCE(last_http_status::text,'') FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID); got != "" {
			t.Fatalf("nonstandard HTTP status persisted = %q", got)
		}
		if got := f.scalarString("SELECT last_error_code FROM newim.im_webhook_deliveries WHERE event_id=$1", eventID); got != string(app.CodeHTTPPermanent) {
			t.Fatalf("nonstandard HTTP status error = %s", got)
		}
	})
}

type lostResponseDoer struct {
	next    app.Doer
	mu      sync.Mutex
	dropped bool
}

func (d *lostResponseDoer) Do(ctx context.Context, target string, headers map[string]string, body []byte) (app.Response, error) {
	response, err := d.next.Do(ctx, target, headers, body)
	if err != nil {
		return response, err
	}
	d.mu.Lock()
	drop := !d.dropped
	if drop {
		d.dropped = true
	}
	d.mu.Unlock()
	if drop {
		return app.Response{}, errors.New("injected lost HTTP response")
	}
	return response, nil
}

func newFixtureWorker(t *testing.T, f *fixture, store app.Store, doer app.Doer, master []byte, maxAttempts, highWater, lowWater int, idleDelay time.Duration, observers ...app.Observer) *app.Worker {
	t.Helper()
	cfg := app.Config{
		Owner: "integration_recovery_owner", BatchSize: 8, MaxConcurrent: 4, MaxPerDestination: 2,
		MaxAttempts: maxAttempts, MaxResponseBytes: 64 * 1024, LeaseTTL: 5 * time.Second, RequestTimeout: time.Second,
		BaseBackoff: 10 * time.Millisecond, MaxBackoff: time.Second, HighWater: highWater, LowWater: lowWater,
		MaxDestinationQueue: 1000, RatePerSecond: 100, RateBurst: 100,
		IdleDelay: idleDelay, Clock: app.ClockFunc(func() time.Time { return f.now }),
	}
	if len(observers) != 0 {
		cfg.Observer = observers[0]
	}
	worker, err := app.NewWorker(cfg, store, doer, f.resolver(master))
	must(t, err)
	return worker
}

func runWorkerUntil(t *testing.T, worker *app.Worker, timeout time.Duration, ready func() bool, detail ...func() string) {
	t.Helper()
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			cancel()
			must(t, <-done)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	must(t, <-done)
	if len(detail) != 0 {
		t.Fatalf("worker condition deadline exceeded: %s", detail[0]())
	}
	t.Fatal("worker condition deadline exceeded")
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
