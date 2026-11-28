package webhook

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/api"
)

const defaultIdleDelay = 100 * time.Millisecond
const defaultObserverQueueCapacity = 256

// Worker consumes the transactional outbox and delivers webhook-v1 requests.
// Worker 消费事务 outbox 并投递 Webhook v1 请求。
type Worker struct {
	cfg           Config
	store         Store
	doer          Doer
	resolver      SecretResolver
	paused        bool
	limiter       *destinationLimiter
	metricsMu     sync.RWMutex
	observerQueue *api.ObserverDispatcher[Observation]
	observerOnce  sync.Once
}

// NewWorker creates a worker; callers must provide real storage, HTTP and secret ports.
// NewWorker 创建 worker；调用方必须提供真实 storage、HTTP 与 secret 端口。
func NewWorker(cfg Config, store Store, doer Doer, resolver SecretResolver) (*Worker, error) {
	if cfg.IdleDelay == 0 {
		cfg.IdleDelay = defaultIdleDelay
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if store == nil || doer == nil || resolver == nil {
		return nil, Fail(CodeInvalidConfig)
	}
	if cfg.Clock == nil {
		cfg.Clock = ClockFunc(time.Now)
	}
	if cfg.Jitter == nil {
		cfg.Jitter = defaultJitter
	}
	return &Worker{
		cfg: cfg, store: store, doer: doer, resolver: resolver,
		limiter: newDestinationLimiter(cfg.MaxPerDestination, cfg.RatePerSecond, cfg.RateBurst),
	}, nil
}

// AttachMetricsSink attaches the process sink before the worker starts.
// AttachMetricsSink 在 worker 启动前附加进程 metrics sink。
func (w *Worker) AttachMetricsSink(sink api.MetricsSink) {
	if w == nil {
		return
	}
	w.metricsMu.Lock()
	w.cfg.Metrics = sink
	w.metricsMu.Unlock()
}

// Close stops best-effort observer submissions without waiting for a stuck observer.
// Close 停止最佳努力 observer 提交，但不会等待已经阻塞的 observer。
func (w *Worker) Close() {
	if w == nil {
		return
	}
	w.metricsMu.RLock()
	queue := w.observerQueue
	w.metricsMu.RUnlock()
	if queue != nil {
		queue.Close()
	}
}

// Run performs bounded cycles until cancellation; in-flight work is waited for.
// Run 执行有界循环直到取消，并等待 in-flight 工作结束。
func (w *Worker) Run(ctx context.Context) error {
	if w == nil || w.cfg.Clock == nil || w.cfg.Jitter == nil {
		return Fail(CodeInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	sem := make(chan struct{}, w.cfg.MaxConcurrent)
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if err := w.cycle(ctx, sem); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.observe(OperationCycle, ErrorCode(err), 0)
			if !sleepContext(ctx, w.cfg.IdleDelay) {
				return nil
			}
			continue
		}
		if !sleepContext(ctx, w.cfg.IdleDelay) {
			return nil
		}
	}
}

func (w *Worker) cycle(ctx context.Context, sem chan struct{}) error {
	now := w.cfg.Clock.Now()
	if _, err := w.store.CancelRevoked(ctx, now); err != nil {
		return err
	}
	counts, err := w.store.Counts(ctx)
	if err != nil {
		return err
	}
	if counts.Total > 0 {
		w.observeCounts(OperationQueue, CodeDeliveryRetry, counts)
	}
	if w.paused {
		if counts.Total > w.cfg.LowWater || counts.MaxDestination > w.cfg.LowWater {
			w.observe(OperationFanout, CodeBacklogPaused, 0)
		} else {
			w.paused = false
		}
	}
	if !w.paused && (counts.Total >= w.cfg.HighWater || counts.MaxDestination >= w.cfg.MaxDestinationQueue) {
		w.paused = true
	}
	if !w.paused {
		n, err := w.store.Fanout(ctx, now, w.cfg.BatchSize, w.cfg.MaxDestinationQueue, w.cfg.HighWater)
		if err != nil {
			w.observe(OperationFanout, ErrorCode(err), 0)
		} else {
			_ = n
			w.observe(OperationFanout, CodeFanoutSuccess, 0)
		}
	}
	deliveries, err := w.store.Claim(ctx, now, w.cfg.Owner, w.cfg.LeaseTTL, w.cfg.BatchSize)
	if err != nil {
		return err
	}
	w.observe(OperationClaim, CodeClaimSuccess, 0)
	var wg sync.WaitGroup
	for _, delivery := range deliveries {
		delivery := delivery
		if !w.limiter.acquire(delivery.DestinationID, now) {
			w.deferDelivery(ctx, delivery, now)
			continue
		}
		select {
		case sem <- struct{}{}:
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				defer w.limiter.release(delivery.DestinationID)
				w.deliver(ctx, delivery)
			}()
		case <-ctx.Done():
			w.limiter.release(delivery.DestinationID)
			wg.Wait()
			return nil
		}
	}
	wg.Wait()
	return nil
}

func (w *Worker) deliver(ctx context.Context, delivery Delivery) {
	started := time.Now()
	now := w.cfg.Clock.Now()
	allowed, err := w.store.BeginAttempt(ctx, delivery.ID, delivery.LeaseToken, now, w.cfg.MaxAttempts, w.cfg.RequestTimeout+w.cfg.RequestTimeout/2)
	if err != nil {
		code := ErrorCode(err)
		if code == CodeDeliveryDeadLetter {
			w.observe(OperationFinish, CodeDeliveryDeadLetter, 0)
		}
		w.observe(OperationAttempt, code, time.Since(started))
		return
	}
	if !allowed {
		w.observe(OperationAttempt, CodeLeaseLost, time.Since(started))
		return
	}
	w.observe(OperationAttempt, CodeAttemptSuccess, time.Since(started))
	attemptCtx, cancel := context.WithTimeout(ctx, w.cfg.RequestTimeout)
	defer cancel()
	attempts := delivery.Attempts + 1
	secret, err := w.resolver.Resolve(attemptCtx, delivery.Secret)
	if err != nil {
		w.finish(ctx, delivery, now, outcomeForSecretError(err, attempts, w.cfg, now))
		w.observe(OperationSecret, ErrorCode(err), time.Since(started))
		return
	}
	defer clear(secret)
	if len(secret) < protocol.WebhookMinSecretBytes {
		w.finish(ctx, delivery, now, terminalOutcome(CodeSecretInvalid, now))
		w.observe(OperationSecret, CodeSecretInvalid, time.Since(started))
		return
	}
	if store, ok := w.store.(VisibilityStore); ok {
		action, err := store.MessageVisibility(attemptCtx, delivery.Event.ID)
		if err != nil {
			w.finish(ctx, delivery, now, retryOutcome(CodeStorageUnavailable, attempts, w.cfg, delivery.ID, now, 0))
			w.observe(OperationEnvelope, CodeStorageUnavailable, time.Since(started))
			return
		}
		if action != "" {
			if err := delivery.Event.HideMessage(action); err != nil {
				w.finish(ctx, delivery, now, terminalOutcome(CodeProtocolInvalid, now))
				w.observe(OperationEnvelope, CodeProtocolInvalid, time.Since(started))
				return
			}
		}
	}
	body, err := buildEnvelope(delivery.Event)
	if err != nil {
		w.finish(ctx, delivery, now, terminalOutcome(CodeProtocolInvalid, now))
		w.observe(OperationEnvelope, CodeProtocolInvalid, time.Since(started))
		return
	}
	headers, err := signedHeaders(secret, delivery, body, now)
	if err != nil {
		w.finish(ctx, delivery, now, terminalOutcome(CodeProtocolInvalid, now))
		w.observe(OperationSignature, CodeProtocolInvalid, time.Since(started))
		return
	}
	response, err := w.doer.Do(attemptCtx, delivery.URL, headers, body)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.finish(ctx, delivery, now, retryOutcome(CodeHTTPTemporary, attempts, w.cfg, delivery.ID, now, 0))
		w.observe(OperationDelivery, CodeHTTPTemporary, time.Since(started))
		return
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		w.finish(ctx, delivery, now, Outcome{Status: "delivered", HTTPStatus: persistedHTTPStatus(response.StatusCode), ErrorCode: CodeDeliveryDelivered, CompletedAt: now})
		w.observe(OperationDelivery, CodeDeliveryDelivered, time.Since(started))
		return
	}
	if retryableStatus(response.StatusCode) {
		w.finish(ctx, delivery, now, retryOutcome(CodeHTTPTemporary, attempts, w.cfg, delivery.ID, now, response.RetryAfter))
		w.observe(OperationDelivery, CodeHTTPTemporary, time.Since(started))
		return
	}
	w.finish(ctx, delivery, now, Outcome{Status: "dead_letter", HTTPStatus: persistedHTTPStatus(response.StatusCode), ErrorCode: CodeHTTPPermanent, CompletedAt: now})
	w.observe(OperationDelivery, CodeHTTPPermanent, time.Since(started))
}

func persistedHTTPStatus(status int) int {
	if status < 100 || status > 599 {
		return 0
	}
	return status
}

func (w *Worker) finish(ctx context.Context, delivery Delivery, now time.Time, outcome Outcome) {
	if err := w.store.Finish(ctx, delivery.ID, delivery.LeaseToken, delivery.Attempts+1, now, outcome); err != nil {
		w.observe(OperationFinish, ErrorCode(err), 0)
		return
	}
	w.observe(OperationFinish, outcome.ErrorCode, 0)
}

func (w *Worker) deferDelivery(ctx context.Context, delivery Delivery, now time.Time) {
	outcome := Outcome{Status: "retry", RetryDelay: w.cfg.BaseBackoff, ErrorCode: CodeBacklogPaused}
	if err := w.store.Finish(ctx, delivery.ID, delivery.LeaseToken, 0, now, outcome); err != nil {
		w.observe(OperationFinish, ErrorCode(err), 0)
		return
	}
	w.observe(OperationFinish, outcome.ErrorCode, 0)
}

func (w *Worker) observe(operation Operation, code Code, elapsed time.Duration) {
	if w == nil {
		return
	}
	observation := ClassifyObservation(Observation{Operation: string(operation), Code: code, Elapsed: elapsed})
	w.metricsMu.RLock()
	observer := w.cfg.Observer
	metrics := w.cfg.Metrics
	w.metricsMu.RUnlock()
	if observer != nil {
		w.dispatchObservation(observer, observation)
	}
	w.recordMetrics(metrics, observation)
}

func (w *Worker) observeCounts(operation Operation, code Code, counts Counts) {
	observation := ClassifyObservation(Observation{
		Operation: string(operation), Code: code, Total: counts.Total, MaxDestination: counts.MaxDestination,
		Pending: counts.Pending, Retry: counts.Retry, Leased: counts.Leased, DeadLetter: counts.DeadLetter,
	})
	w.metricsMu.RLock()
	observer := w.cfg.Observer
	metrics := w.cfg.Metrics
	w.metricsMu.RUnlock()
	if observer != nil {
		w.dispatchObservation(observer, observation)
	}
	w.recordMetrics(metrics, observation)
}

func (w *Worker) dispatchObservation(observer Observer, observation Observation) {
	if w == nil || observer == nil {
		return
	}
	w.observerOnce.Do(func() {
		queue := api.NewObserverDispatcher(defaultObserverQueueCapacity, observer.Observe, w.dropCurrent)
		w.metricsMu.Lock()
		w.observerQueue = queue
		w.metricsMu.Unlock()
	})
	w.metricsMu.RLock()
	queue := w.observerQueue
	w.metricsMu.RUnlock()
	if queue != nil {
		queue.Enqueue(observation)
	}
}

func (w *Worker) recordMetrics(metrics api.MetricsSink, observation Observation) {
	if metrics == nil {
		return
	}
	defer func() {
		if recover() != nil {
			w.drop(metrics, "writer_error")
		}
	}()
	metrics.Record(api.MetricSample{Name: api.MetricMessageLifecycleEvents, Labels: map[string]string{
		"component": "webhook", "stage": observation.Stage, "result": observation.Result,
	}, Value: 1})
	if observation.Operation == string(OperationFinish) {
		metrics.Record(api.MetricSample{Name: api.MetricWebhookDeliveries, Labels: map[string]string{
			"stage": observation.Stage, "result": observation.Result,
		}, Value: 1})
	}
	if observation.Elapsed > 0 {
		metrics.Record(api.MetricSample{Name: api.MetricMessageLifecycleDurationSum, Labels: map[string]string{
			"component": "webhook", "stage": observation.Stage,
		}, Value: observation.Elapsed.Seconds()})
		metrics.Record(api.MetricSample{Name: api.MetricMessageLifecycleDurationCount, Labels: map[string]string{
			"component": "webhook", "stage": observation.Stage,
		}, Value: 1})
	}
	if observation.Result != "success" || observation.Code == CodeUnknown {
		code := observation.Code
		if code == "" {
			code = CodeUnknown
		}
		metrics.Record(api.MetricSample{Name: api.MetricMessageLifecycleErrors, Labels: map[string]string{
			"component": "webhook", "code": string(code),
		}, Value: 1})
	}
}

func (w *Worker) drop(metrics api.MetricsSink, reason string) {
	if metrics == nil {
		return
	}
	defer func() { _ = recover() }()
	metrics.Drop(reason)
}

func (w *Worker) dropCurrent(reason string) {
	if w == nil {
		return
	}
	w.metricsMu.RLock()
	metrics := w.cfg.Metrics
	w.metricsMu.RUnlock()
	w.drop(metrics, reason)
}

func buildEnvelope(event Event) ([]byte, error) {
	if event.ID == "" || event.ServerMsgID == "" || event.ClientMsgID == "" || event.SenderID == "" || event.ConversationID == "" || event.MessageType == "" || len(event.Payload) == 0 {
		return nil, Fail(CodeProtocolInvalid)
	}
	if !event.SenderType.IsZero() && !event.SenderType.Valid() {
		return nil, Fail(CodeProtocolInvalid)
	}
	payload, err := marshalWebhookPayload(messagePayload(event, false))
	if err != nil {
		return nil, Fail(CodeProtocolInvalid)
	}
	envelope := protocol.WebhookEnvelope{
		EventID:       event.ID,
		EventType:     "message.persisted",
		OccurredAt:    strconv.FormatInt(event.ServerTime, 10),
		SchemaVersion: protocol.WebhookSchemaVersion,
		Payload:       payload,
	}
	body, err := protocol.EncodeWebhookEnvelope(envelope)
	if err == nil {
		return body, nil
	}
	if code := protocol.WebhookErrorCode(err); code != protocol.WebhookEnvelopeTooLarge && code != protocol.WebhookEnvelopeTooDeep {
		return nil, err
	}
	compact, compactErr := marshalWebhookPayload(messagePayload(event, true))
	if compactErr != nil {
		return nil, Fail(CodeProtocolInvalid)
	}
	return protocol.EncodeWebhookEnvelope(protocol.WebhookEnvelope{
		EventID:       event.ID,
		EventType:     "message.persisted",
		OccurredAt:    strconv.FormatInt(event.ServerTime, 10),
		SchemaVersion: protocol.WebhookSchemaVersion,
		Payload:       compact,
	})
}

func messagePayload(event Event, omitBody bool) any {
	payload := json.RawMessage(`{}`)
	digest := ""
	payloadSize := ""
	if omitBody {
		sum := sha256.Sum256(event.Payload)
		digest = hex.EncodeToString(sum[:])
		payloadSize = strconv.Itoa(len(event.Payload))
	} else {
		payload = event.Payload
	}
	return struct {
		ClientMsgID     string              `json:"clientMsgId"`
		ServerMsgID     string              `json:"serverMsgId"`
		ConversationID  string              `json:"conversationId"`
		ConversationSeq string              `json:"conversationSeq"`
		SenderID        string              `json:"senderId"`
		SenderType      protocol.SenderType `json:"senderType,omitzero"`
		ProtocolVersion int                 `json:"protocolVersion"`
		SchemaVersion   int                 `json:"version"`
		Type            string              `json:"type"`
		ServerTime      string              `json:"serverTime"`
		Payload         json.RawMessage     `json:"payload"`
		PayloadOmitted  bool                `json:"payloadOmitted,omitempty"`
		PayloadSHA256   string              `json:"payloadSha256,omitempty"`
		PayloadSize     string              `json:"payloadSize,omitempty"`
	}{
		ClientMsgID:     event.ClientMsgID,
		ServerMsgID:     event.ServerMsgID,
		ConversationID:  event.ConversationID,
		ConversationSeq: strconv.FormatInt(event.ConversationSeq, 10),
		SenderID:        event.SenderID,
		SenderType:      event.SenderType,
		ProtocolVersion: event.ProtocolVersion,
		SchemaVersion:   event.SchemaVersion,
		Type:            event.MessageType,
		ServerTime:      strconv.FormatInt(event.ServerTime, 10),
		Payload:         payload,
		PayloadOmitted:  omitBody,
		PayloadSHA256:   digest,
		PayloadSize:     payloadSize,
	}
}

func marshalWebhookPayload(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n")), nil
}

func signedHeaders(secret []byte, delivery Delivery, body []byte, now time.Time) (map[string]string, error) {
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return nil, err
	}
	nonce := base64.RawURLEncoding.EncodeToString(nonceBytes)
	headers := protocol.WebhookHeaders{
		EventID:          delivery.Event.ID,
		DeliveryID:       delivery.ID,
		Timestamp:        strconv.FormatInt(now.UnixMilli(), 10),
		Nonce:            nonce,
		KeyID:            delivery.KeyID,
		SignatureVersion: protocol.WebhookSignatureVersion,
	}
	signature, err := protocol.SignWebhook(secret, headers, body)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		protocol.WebhookHeaderEventID:          headers.EventID,
		protocol.WebhookHeaderDeliveryID:       headers.DeliveryID,
		protocol.WebhookHeaderTimestamp:        headers.Timestamp,
		protocol.WebhookHeaderNonce:            headers.Nonce,
		protocol.WebhookHeaderKeyID:            headers.KeyID,
		protocol.WebhookHeaderSignatureVersion: headers.SignatureVersion,
		protocol.WebhookHeaderSignature:        signature,
	}, nil
}

func retryableStatus(status int) bool {
	return status == 408 || status == 429 || status >= 500 && status <= 599
}

func outcomeForSecretError(err error, attempts int, cfg Config, now time.Time) Outcome {
	if ErrorCode(err) == CodeSecretUnavailable {
		return retryOutcome(CodeSecretUnavailable, attempts, cfg, "", now, 0)
	}
	return terminalOutcome(ErrorCode(err), now)
}

func terminalOutcome(code Code, now time.Time) Outcome {
	return Outcome{Status: "dead_letter", ErrorCode: code, CompletedAt: now}
}

func retryOutcome(code Code, attempts int, cfg Config, deliveryID string, now time.Time, retryAfter time.Duration) Outcome {
	if attempts >= cfg.MaxAttempts {
		return Outcome{Status: "dead_letter", ErrorCode: CodeDeliveryDeadLetter, CompletedAt: now}
	}
	delay := backoff(cfg, deliveryID, attempts)
	if retryAfter > 0 {
		delay = retryAfter
		if delay > cfg.MaxBackoff {
			delay = cfg.MaxBackoff
		}
	}
	return Outcome{Status: "retry", ErrorCode: code, NextAttempt: now.Add(delay), RetryDelay: delay}
}

func backoff(cfg Config, deliveryID string, attempts int) time.Duration {
	delay := cfg.BaseBackoff
	for i := 1; i < attempts && delay < cfg.MaxBackoff; i++ {
		delay *= 2
		if delay > cfg.MaxBackoff {
			delay = cfg.MaxBackoff
		}
	}
	jitter := cfg.Jitter
	if jitter == nil {
		jitter = defaultJitter
	}
	jittered := jitter(deliveryID, delay)
	if jittered < 0 {
		return 0
	}
	if jittered > cfg.MaxBackoff {
		return cfg.MaxBackoff
	}
	return jittered
}

func defaultJitter(deliveryID string, base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(deliveryID))
	span := base / 4
	if span <= 0 {
		return base
	}
	return base - span/2 + time.Duration(hash.Sum64()%uint64(span))
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
