package webhook

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

const defaultIdleDelay = 100 * time.Millisecond

// Worker consumes the transactional outbox and delivers webhook-v1 requests.
// Worker 消费事务 outbox 并投递 Webhook v1 请求。
type Worker struct {
	cfg      Config
	store    Store
	doer     Doer
	resolver SecretResolver
	paused   bool
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
	return &Worker{cfg: cfg, store: store, doer: doer, resolver: resolver}, nil
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
			w.observe("cycle", ErrorCode(err), 0)
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
	if w.paused {
		if counts.Total > w.cfg.LowWater || counts.MaxDestination > w.cfg.LowWater {
			w.observe("fanout", CodeBacklogPaused, 0)
			return nil
		}
		w.paused = false
	}
	if counts.Total >= w.cfg.HighWater || counts.MaxDestination >= w.cfg.HighWater {
		w.paused = true
		w.observe("fanout", CodeBacklogPaused, 0)
		return nil
	}
	for i := 0; i < w.cfg.BatchSize; i++ {
		n, err := w.store.Fanout(ctx, now, w.cfg.BatchSize)
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
	}
	deliveries, err := w.store.Claim(ctx, now, w.cfg.Owner, w.cfg.LeaseTTL, w.cfg.BatchSize)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for _, delivery := range deliveries {
		delivery := delivery
		select {
		case sem <- struct{}{}:
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				w.deliver(ctx, delivery)
			}()
		case <-ctx.Done():
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
	allowed, err := w.store.BeginAttempt(ctx, delivery.ID, delivery.LeaseToken, now, w.cfg.MaxAttempts)
	if err != nil {
		w.observe("attempt", ErrorCode(err), time.Since(started))
		return
	}
	if !allowed {
		w.observe("attempt", CodeLeaseLost, time.Since(started))
		return
	}
	attempts := delivery.Attempts + 1
	secret, err := w.resolver.Resolve(ctx, delivery.Secret)
	if err != nil {
		w.finish(ctx, delivery, now, outcomeForSecretError(err, attempts, w.cfg, now))
		w.observe("secret", ErrorCode(err), time.Since(started))
		return
	}
	defer clear(secret)
	if len(secret) < protocol.WebhookMinSecretBytes {
		w.finish(ctx, delivery, now, terminalOutcome(CodeSecretInvalid, now))
		w.observe("secret", CodeSecretInvalid, time.Since(started))
		return
	}
	body, err := buildEnvelope(delivery.Event)
	if err != nil {
		w.finish(ctx, delivery, now, terminalOutcome(CodeProtocolInvalid, now))
		w.observe("envelope", CodeProtocolInvalid, time.Since(started))
		return
	}
	headers, err := signedHeaders(secret, delivery, body, now)
	if err != nil {
		w.finish(ctx, delivery, now, terminalOutcome(CodeProtocolInvalid, now))
		w.observe("signature", CodeProtocolInvalid, time.Since(started))
		return
	}
	response, err := w.doer.Do(ctx, delivery.URL, headers, body)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.finish(ctx, delivery, now, retryOutcome(CodeHTTPTemporary, attempts, w.cfg, delivery.ID, now))
		w.observe("delivery", CodeHTTPTemporary, time.Since(started))
		return
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		w.finish(ctx, delivery, now, Outcome{Status: "delivered", HTTPStatus: response.StatusCode, ErrorCode: CodeDeliveryDelivered, CompletedAt: now})
		w.observe("delivery", CodeDeliveryDelivered, time.Since(started))
		return
	}
	if retryableStatus(response.StatusCode) {
		w.finish(ctx, delivery, now, retryOutcome(CodeHTTPTemporary, attempts, w.cfg, delivery.ID, now))
		w.observe("delivery", CodeHTTPTemporary, time.Since(started))
		return
	}
	w.finish(ctx, delivery, now, Outcome{Status: "dead_letter", HTTPStatus: response.StatusCode, ErrorCode: CodeHTTPPermanent, CompletedAt: now})
	w.observe("delivery", CodeHTTPPermanent, time.Since(started))
}

func (w *Worker) finish(ctx context.Context, delivery Delivery, now time.Time, outcome Outcome) {
	if err := w.store.Finish(ctx, delivery.ID, delivery.LeaseToken, delivery.Attempts+1, now, outcome); err != nil {
		w.observe("finish", ErrorCode(err), 0)
	}
}

func (w *Worker) observe(operation string, code Code, elapsed time.Duration) {
	if w.cfg.Observer != nil {
		w.cfg.Observer.Observe(Observation{Operation: operation, Code: code, Elapsed: elapsed})
	}
}

func buildEnvelope(event Event) ([]byte, error) {
	if event.ID == "" || event.ServerMsgID == "" || event.ClientMsgID == "" || event.SenderID == "" || event.ConversationID == "" || event.MessageType == "" || len(event.Payload) == 0 {
		return nil, Fail(CodeProtocolInvalid)
	}
	payload, err := json.Marshal(struct {
		ClientMsgID     string          `json:"clientMsgId"`
		ServerMsgID     string          `json:"serverMsgId"`
		ConversationID  string          `json:"conversationId"`
		ConversationSeq string          `json:"conversationSeq"`
		SenderID        string          `json:"senderId"`
		ProtocolVersion int             `json:"protocolVersion"`
		SchemaVersion   int             `json:"schemaVersion"`
		Type            string          `json:"type"`
		ServerTime      string          `json:"serverTime"`
		Payload         json.RawMessage `json:"payload"`
	}{
		ClientMsgID:     event.ClientMsgID,
		ServerMsgID:     event.ServerMsgID,
		ConversationID:  event.ConversationID,
		ConversationSeq: strconv.FormatInt(event.ConversationSeq, 10),
		SenderID:        event.SenderID,
		ProtocolVersion: event.ProtocolVersion,
		SchemaVersion:   event.SchemaVersion,
		Type:            event.MessageType,
		ServerTime:      strconv.FormatInt(event.ServerTime, 10),
		Payload:         event.Payload,
	})
	if err != nil {
		return nil, Fail(CodeProtocolInvalid)
	}
	return protocol.EncodeWebhookEnvelope(protocol.WebhookEnvelope{
		EventID:       event.ID,
		EventType:     "message.persisted",
		OccurredAt:    strconv.FormatInt(event.ServerTime, 10),
		SchemaVersion: protocol.WebhookSchemaVersion,
		Payload:       payload,
	})
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
		return retryOutcome(CodeSecretUnavailable, attempts, cfg, "", now)
	}
	return terminalOutcome(ErrorCode(err), now)
}

func terminalOutcome(code Code, now time.Time) Outcome {
	return Outcome{Status: "dead_letter", ErrorCode: code, CompletedAt: now}
}

func retryOutcome(code Code, attempts int, cfg Config, deliveryID string, now time.Time) Outcome {
	if attempts >= cfg.MaxAttempts {
		return Outcome{Status: "dead_letter", ErrorCode: CodeDeliveryDeadLetter, CompletedAt: now}
	}
	return Outcome{Status: "retry", ErrorCode: code, NextAttempt: now.Add(backoff(cfg, deliveryID, attempts))}
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
