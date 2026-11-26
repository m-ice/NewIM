//go:build integration

package message_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/auth/session"
	app "github.com/m-ice/NewIM/server/message"
	store "github.com/m-ice/NewIM/server/storage/message"
)

func testMessageObservabilityAuthoritativeLifecycle(t *testing.T) {
	f := openFixture(t)
	user := "observability_lifecycle_user"
	conversationID := f.seedConversation("observability_lifecycle", user)
	registry := api.NewMetricsRegistry()
	sink := api.NewBoundedMetricsSink(registry, 1024)
	observer := &messageObservationCapture{}
	service, err := app.NewService(f.repo, app.Config{
		IDs:      &sequenceIDs{prefix: "observability_lifecycle_generated"},
		TraceIDs: &traceSequence{},
		Clock:    app.ClockFunc(func() time.Time { return time.UnixMilli(f.now.Load()).UTC() }),
		Observer: observer,
		Metrics:  sink,
	})
	must(t, err)
	defer service.Close()
	defer sink.Close()

	request := f.request("observability_lifecycle_client", conversationID, "authoritative")
	first, err := service.Send(ctx, f.identity(user), request)
	must(t, err)
	firstAck := ack(t, first)
	f.assertAtomic(conversationID, firstAck.ServerMsgID)

	second, err := service.Send(ctx, f.identity(user), request)
	must(t, err)
	secondAck := ack(t, second)
	if firstAck.ServerMsgID != secondAck.ServerMsgID || firstAck.ConversationSeq != secondAck.ConversationSeq {
		t.Fatalf("replay changed authoritative result: first=%+v second=%+v", firstAck, secondAck)
	}

	conflict := request
	conflict.Payload = []byte(`{"text":"different"}`)
	if _, err = service.Send(ctx, f.identity(user), conflict); err == nil {
		t.Fatal("conflicting intent unexpectedly succeeded")
	}
	mustCode(t, err, app.SendIDConflict)
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE sender_id=$1 AND client_msg_id=$2", user, request.ClientMsgID); got != 1 {
		t.Fatalf("authoritative message count=%d want 1", got)
	}

	want := []string{app.LifecycleAccepted, app.LifecycleValidated, app.LifecyclePersisting,
		app.LifecyclePersisted, app.LifecycleACKReady}
	for _, stage := range want {
		waitForMessageMetric(t, registry, api.MetricMessageLifecycleEvents,
			map[string]string{"component": "send", "stage": stage, "result": "success"})
	}
	waitForMessageMetric(t, registry, api.MetricMessageLifecycleEvents,
		map[string]string{"component": "outbox", "stage": "enqueued", "result": "success"})
	waitForMessageMetric(t, registry, api.MetricMessageSendIdempotency, map[string]string{"outcome": "new"})
	waitForMessageMetric(t, registry, api.MetricMessageSendIdempotency, map[string]string{"outcome": "replayed"})
	waitForMessageMetric(t, registry, api.MetricMessageSendIdempotency, map[string]string{"outcome": "conflict"})
	waitForMessageMetric(t, registry, api.MetricMessageLifecycleErrors,
		map[string]string{"component": "send", "code": string(app.SendIDConflict)})

	observations := observer.waitForCount(t, 12, 2*time.Second)
	firstStages := observations[:5]
	for index, stage := range want {
		if firstStages[index].Stage != stage {
			t.Fatalf("observation order=%v want prefix=%v", observationStages(observations), want)
		}
		if !validObservationTrace(firstStages[index].TraceID) {
			t.Fatalf("invalid trace id %q", firstStages[index].TraceID)
		}
	}
	attemptTraces := make([]string, 0, 3)
	for _, observation := range observations {
		if observation.Stage == app.LifecycleAccepted {
			attemptTraces = append(attemptTraces, observation.TraceID)
		}
	}
	if len(attemptTraces) < 2 || attemptTraces[0] == attemptTraces[1] {
		t.Fatalf("separate attempts reused or omitted a trace id: %v", attemptTraces)
	}
	for _, observation := range observations {
		for _, prohibited := range []string{request.ClientMsgID, conversationID, user} {
			if observation.TraceID == prohibited {
				t.Fatalf("trace id leaked business identity %q", prohibited)
			}
		}
	}
}

type messageObservationCapture struct {
	mu           sync.Mutex
	observations []app.Observation
}

func (o *messageObservationCapture) Observe(observation app.Observation) {
	o.mu.Lock()
	o.observations = append(o.observations, observation)
	o.mu.Unlock()
}

func (o *messageObservationCapture) waitForCount(t *testing.T, count int, timeout time.Duration) []app.Observation {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		o.mu.Lock()
		if len(o.observations) >= count {
			result := append([]app.Observation(nil), o.observations...)
			o.mu.Unlock()
			return result
		}
		o.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	t.Fatalf("observations=%d want at least %d: %+v", len(o.observations), count, o.observations)
	return nil
}

func observationStages(observations []app.Observation) []string {
	stages := make([]string, 0, len(observations))
	for _, observation := range observations {
		stages = append(stages, observation.Stage)
	}
	return stages
}

func validObservationTrace(value string) bool {
	if len(value) != 32 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func waitForMessageMetric(t *testing.T, registry *api.MetricsRegistry, name string, labels map[string]string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, sample := range registry.Snapshot() {
			if sample.Name != name {
				continue
			}
			matches := true
			for key, value := range labels {
				if sample.Labels[key] != value {
					matches = false
					break
				}
			}
			if matches {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("metric %s labels=%v missing in %+v", name, labels, registry.Snapshot())
}

func waitForMessageMetricValue(t *testing.T, registry *api.MetricsRegistry, name string, labels map[string]string) float64 {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, sample := range registry.Snapshot() {
			if sample.Name != name {
				continue
			}
			matches := true
			for key, value := range labels {
				if sample.Labels[key] != value {
					matches = false
					break
				}
			}
			if matches {
				return sample.Value
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("metric %s labels=%v missing in %+v", name, labels, registry.Snapshot())
	return 0
}

func TestMessageObservabilityRecovery(t *testing.T) {
	f := openFixture(t)
	user := "observability_user"
	conversationID := f.seedConversation("observability", user)
	service, err := app.NewService(f.repo, app.Config{
		IDs: &sequenceIDs{prefix: "observability_generated"}, Clock: app.ClockFunc(func() time.Time { return time.UnixMilli(f.now.Load()).UTC() }),
	})
	must(t, err)
	frame, err := f.send(service, f.identity(user), f.request("observability_client", conversationID, "gauge"))
	must(t, err)
	if frame.Ack == nil {
		t.Fatal("missing ACK")
	}
	assertMessageFanoutGauges(t, f, 1, 0)

	// A fresh adapter represents process restart: counters are new, gauges are rehydrated.
	restartedRepo, err := openMessageRepository(t)
	must(t, err)
	defer restartedRepo.Close()
	samples, err := restartedRepo.SnapshotMetrics(context.Background())
	must(t, err)
	assertMetricValue(t, samples, api.MetricMessageOutboxFanoutPending, "pending", 1)
	assertMetricValue(t, samples, api.MetricMessageOutboxFanoutPending, "quarantined", 0)

	// A closed repository must not guess zero when the authoritative snapshot fails.
	f.repo.Close()
	if _, err = f.repo.SnapshotMetrics(context.Background()); err == nil {
		t.Fatal("closed repository returned a fabricated gauge")
	}
	t.Run("authoritative-lifecycle", testMessageObservabilityAuthoritativeLifecycle)
	t.Run("concurrent-send-scrape", testMessageObservabilityConcurrentSendScrape)
	t.Run("concurrent-same-intent-replay", testMessageObservabilityConcurrentReplay)
	t.Run("source-timeout", testMessageObservabilitySourceTimeout)
}

func testMessageObservabilitySourceTimeout(t *testing.T) {
	f := openFixture(t)
	tx, err := f.db.Begin(ctx)
	must(t, err)
	if _, err = tx.Exec(ctx, "LOCK TABLE newim.im_outbox_events IN ACCESS EXCLUSIVE MODE"); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	started := time.Now()
	_, snapshotErr := f.repo.SnapshotMetrics(ctx)
	elapsed := time.Since(started)
	if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
		t.Fatal(rollbackErr)
	}
	if snapshotErr == nil {
		t.Fatal("source timeout unexpectedly returned a snapshot")
	}
	if elapsed < 900*time.Millisecond || elapsed > 4*time.Second {
		t.Fatalf("source timeout elapsed=%s", elapsed)
	}
}

func testMessageObservabilityConcurrentReplay(t *testing.T) {
	f := openFixture(t)
	user := "observability_replay_user"
	conversationID := f.seedConversation("observability_replay", user)
	registry := api.NewMetricsRegistry()
	sink := api.NewBoundedMetricsSink(registry, 1024)
	service, err := app.NewService(f.repo, app.Config{
		IDs:      &sequenceIDs{prefix: "observability_replay_generated"},
		TraceIDs: &traceSequence{},
		Clock:    app.ClockFunc(func() time.Time { return time.UnixMilli(f.now.Load()).UTC() }),
		Metrics:  sink,
	})
	must(t, err)
	defer service.Close()
	defer sink.Close()
	request := f.request("observability_replay_client", conversationID, "same")
	identity := f.identity(user)
	const attempts = 8
	results := make(chan string, attempts)
	errorsFound := make(chan error, attempts)
	var wait sync.WaitGroup
	for index := 0; index < attempts; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			frame, sendErr := service.Send(ctx, identity, request)
			if sendErr != nil {
				errorsFound <- sendErr
				return
			}
			if frame.Ack == nil {
				errorsFound <- fmt.Errorf("missing ACK")
				return
			}
			results <- frame.Ack.ServerMsgID
		}()
	}
	wait.Wait()
	close(results)
	close(errorsFound)
	for sendErr := range errorsFound {
		t.Fatal(sendErr)
	}
	serverIDs := make(map[string]struct{})
	for serverID := range results {
		serverIDs[serverID] = struct{}{}
	}
	if len(serverIDs) != 1 {
		t.Fatalf("concurrent same-intent replies used %d server identities", len(serverIDs))
	}
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE conversation_id=$1", conversationID); got != 1 {
		t.Fatalf("concurrent replay message rows=%d want=1", got)
	}
	if got := f.scalarInt64(`SELECT count(*) FROM newim.im_outbox_events e
JOIN newim.im_messages m ON m.server_msg_id=e.server_msg_id WHERE m.conversation_id=$1`, conversationID); got != 1 {
		t.Fatalf("concurrent replay outbox rows=%d want=1", got)
	}
	if got := waitForMessageMetricValue(t, registry, api.MetricMessageSendIdempotency, map[string]string{"outcome": "new"}); got != 1 {
		t.Fatalf("new metric=%v want=1", got)
	}
	if got := waitForMessageMetricValue(t, registry, api.MetricMessageSendIdempotency, map[string]string{"outcome": "replayed"}); got != float64(attempts-1) {
		t.Fatalf("replayed metric=%v want=%d", got, attempts-1)
	}
}

func testMessageObservabilityConcurrentSendScrape(t *testing.T) {
	f := openFixture(t)
	user := "observability_concurrent_user"
	conversationID := f.seedConversation("observability_concurrent", user)
	identity := f.identity(user)
	registry := api.NewMetricsRegistry()
	sink := api.NewBoundedMetricsSink(registry, 4096)
	handler := &concurrentMessageHandler{identity: identity, conversationID: conversationID, next: new(atomic.Int64)}
	service, err := app.NewService(f.repo, app.Config{
		IDs:      &sequenceIDs{prefix: "observability_concurrent_generated"},
		TraceIDs: &traceSequence{},
		Clock:    app.ClockFunc(func() time.Time { return time.UnixMilli(f.now.Load()).UTC() }),
		Metrics:  sink,
	})
	must(t, err)
	handler.service = service
	defer service.Close()
	defer sink.Close()

	server, err := api.NewWithRoutes(api.Config{
		APIAddr: "127.0.0.1:0", OpsAddr: "127.0.0.1:0", ServerVersion: "observability-test",
	}, nil, []api.APIRoute{{
		Name: "message_send", Path: "/api/v1/concurrent-send", AllowedMethods: []string{http.MethodPost}, Handler: handler,
	}})
	must(t, err)
	server.AttachMetricsRegistry(registry)
	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run(runCtx) }()
	waitForServerReady(t, server)

	client := &http.Client{Timeout: 2 * time.Second}
	const sends = 32
	var senders sync.WaitGroup
	sendErrors := make(chan error, sends)
	for index := 0; index < sends; index++ {
		senders.Add(1)
		go func() {
			defer senders.Done()
			request, requestErr := http.NewRequestWithContext(runCtx, http.MethodPost,
				"http://"+server.APIAddr()+"/api/v1/concurrent-send", nil)
			if requestErr != nil {
				sendErrors <- requestErr
				return
			}
			response, requestErr := client.Do(request)
			if requestErr != nil {
				sendErrors <- requestErr
				return
			}
			defer response.Body.Close()
			_, _ = io.Copy(io.Discard, response.Body)
			if response.StatusCode != http.StatusOK {
				sendErrors <- fmt.Errorf("send status %d", response.StatusCode)
			}
		}()
	}
	sendsDone := make(chan struct{})
	go func() { senders.Wait(); close(sendsDone) }()
	scrapeErrors := make(chan error, 2)
	var scrapers sync.WaitGroup
	for index := 0; index < 2; index++ {
		scrapers.Add(1)
		go func() {
			defer scrapers.Done()
			sawLifecycle := false
			for {
				select {
				case <-sendsDone:
					if !sawLifecycle {
						scrapeErrors <- fmt.Errorf("metrics scrape never exposed lifecycle samples")
					}
					return
				default:
				}
				response, requestErr := client.Get("http://" + server.OpsAddr() + "/metrics")
				if requestErr != nil {
					scrapeErrors <- requestErr
					return
				}
				body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
				response.Body.Close()
				if readErr != nil {
					scrapeErrors <- readErr
					return
				}
				if response.StatusCode != http.StatusOK {
					scrapeErrors <- fmt.Errorf("metrics status %d", response.StatusCode)
					return
				}
				sawLifecycle = sawLifecycle || strings.Contains(string(body), api.MetricMessageLifecycleEvents)
			}
		}()
	}
	<-sendsDone
	scrapers.Wait()
	close(sendErrors)
	close(scrapeErrors)
	for sendErr := range sendErrors {
		t.Fatal(sendErr)
	}
	for scrapeErr := range scrapeErrors {
		t.Fatal(scrapeErr)
	}
	if got := f.scalarInt64("SELECT count(*) FROM newim.im_messages WHERE conversation_id=$1", conversationID); got != sends {
		t.Fatalf("message rows=%d want=%d", got, sends)
	}
	if got := f.scalarInt64(`SELECT count(*) FROM newim.im_outbox_events e
JOIN newim.im_messages m ON m.server_msg_id=e.server_msg_id WHERE m.conversation_id=$1`, conversationID); got != sends {
		t.Fatalf("outbox rows=%d want=%d", got, sends)
	}
	cancel()
	if runErr := <-runDone; runErr != nil {
		t.Fatalf("server shutdown failed: %v", runErr)
	}
}

type concurrentMessageHandler struct {
	service        *app.Service
	identity       session.ConnectionIdentity
	conversationID string
	next           *atomic.Int64
}

func (h *concurrentMessageHandler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if h == nil || h.service == nil || request == nil || h.next == nil {
		http.Error(w, "invalid", http.StatusInternalServerError)
		return
	}
	clientID := fmt.Sprintf("concurrent_client_%d", h.next.Add(1))
	_, err := h.service.Send(request.Context(), h.identity, protocol.Send{
		ProtocolVersion: 1, ClientMsgID: clientID, ConversationID: h.conversationID,
		Version: 1, Type: "text", Payload: []byte(`{"text":"concurrent"}`),
	})
	if err != nil {
		http.Error(w, string(app.ErrorCode(err)), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func waitForServerReady(t *testing.T, server *api.Server) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if server.Ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("server did not become ready")
}

type traceSequence struct{ next atomic.Int64 }

func (g *traceSequence) NewID() (string, error) {
	return fmt.Sprintf("%032x", g.next.Add(1)), nil
}

func assertMessageFanoutGauges(t *testing.T, f *fixture, pending, quarantined int64) {
	t.Helper()
	samples, err := f.repo.SnapshotMetrics(context.Background())
	must(t, err)
	assertMetricValue(t, samples, api.MetricMessageOutboxFanoutPending, "pending", float64(pending))
	assertMetricValue(t, samples, api.MetricMessageOutboxFanoutPending, "quarantined", float64(quarantined))
}

func assertMetricValue(t *testing.T, samples []api.MetricSample, name, state string, want float64) {
	t.Helper()
	for _, sample := range samples {
		if sample.Name == name && sample.Labels["state"] == state {
			if sample.Value != want {
				t.Fatalf("metric %s state=%s value=%v want=%v", name, state, sample.Value, want)
			}
			return
		}
	}
	t.Fatalf("metric %s state=%s missing in %+v", name, state, samples)
}

func openMessageRepository(t *testing.T) (*store.Repository, error) {
	t.Helper()
	return store.Open(context.Background(), store.Config{
		DSN:              fmt.Sprintf("host=/var/run/postgresql user=newim_test dbname=newim_test sslmode=disable"),
		AllowLocalSocket: true, MaxConnections: 8, ApplicationName: "nim_message_restart",
	})
}
