package message

import (
	"context"
	"strconv"
	"sync"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/conversation"
)

const defaultRequestTimeout = 5 * time.Second
const defaultObserverQueueCapacity = 256

// Config is trusted service configuration, never request input.
// Config 仅接受可信服务配置，不接受客户端请求输入。
type Config struct {
	IDs            IDGenerator
	TraceIDs       IDGenerator
	Clock          Clock
	Observer       Observer
	Metrics        MetricsSink
	MediaValidator MediaValidator
	RequestTimeout time.Duration
}

// Service coordinates send validation, authorization and durable persistence.
// Service 编排发送校验、授权与持久化，不拥有网络或数据库连接。
type Service struct {
	store          Store
	ids            IDGenerator
	traceIDs       IDGenerator
	clock          Clock
	metrics        MetricsSink
	mediaValidator MediaValidator
	requestTimeout time.Duration
	observability  sync.RWMutex
	observer       Observer
	observerQueue  *api.ObserverDispatcher[Observation]
	observerOnce   sync.Once
}

// NewService constructs a send service from trusted configuration.
// NewService 使用可信配置构造发送服务。
func NewService(store Store, config Config) (*Service, error) {
	if store == nil || config.IDs == nil || config.Clock == nil {
		return nil, Fail(SendInvalidInput)
	}
	timeout := config.RequestTimeout
	if timeout == 0 {
		timeout = defaultRequestTimeout
	}
	if timeout < time.Millisecond || timeout > defaultRequestTimeout {
		return nil, Fail(SendInvalidInput)
	}
	traceIDs := config.TraceIDs
	if traceIDs == nil {
		traceIDs = NewCSPRNGIDGenerator()
	}
	return &Service{
		store: store, ids: config.IDs, traceIDs: traceIDs, clock: config.Clock,
		observer: config.Observer, metrics: config.Metrics, mediaValidator: config.MediaValidator,
		requestTimeout: timeout,
	}, nil
}

// SetMetricsSink attaches a bounded observability sink before the service is used.
// SetMetricsSink 在服务启用前附加有界观测 sink。
func (s *Service) SetMetricsSink(sink MetricsSink) {
	if s == nil {
		return
	}
	s.observability.Lock()
	s.metrics = sink
	s.observability.Unlock()
}

// Close stops best-effort observer submissions without blocking on a stuck observer.
// Close 停止最佳努力 observer 提交，但不会等待已经阻塞的 observer。
func (s *Service) Close() {
	if s == nil {
		return
	}
	s.observability.RLock()
	queue := s.observerQueue
	s.observability.RUnlock()
	if queue != nil {
		queue.Close()
	}
}

// Send returns a SERVER_PERSISTED ACK only after the storage transaction commits.
// Send 仅在存储事务提交后返回 SERVER_PERSISTED ACK；提交不确定时绝不返回成功。
func (s *Service) Send(ctx context.Context, identity session.ConnectionIdentity, request protocol.Send) (frame protocol.ServerFrame, err error) {
	started := time.Now()
	if s == nil || s.store == nil || s.ids == nil || s.clock == nil || ctx == nil {
		return protocol.ServerFrame{}, Fail(SendInvalidInput)
	}
	traceID := s.newTraceID()
	s.emitLifecycle(traceID, LifecycleAccepted, LifecycleResultSuccess, LifecycleCodeAccepted, false, started)
	defer func() {
		if err != nil {
			s.emitLifecycle(traceID, LifecycleRejected, resultForError(err), ErrorCode(err), false, started)
		}
		s.observeLegacy(started, err)
	}()
	if !validIdentity(identity) {
		return protocol.ServerFrame{}, Fail(SendInvalidInput)
	}
	if _, encodeErr := protocol.EncodeSend(request); encodeErr != nil {
		return protocol.ServerFrame{}, Fail(SendInvalidInput)
	}
	principal := conversation.Principal{UserID: identity.UserID()}
	validationObserved := false
	validate := func() error {
		if validateErr := s.validateMedia(ctx, identity, request); validateErr != nil {
			return validateErr
		}
		if !validationObserved {
			s.emitLifecycle(traceID, LifecycleValidated, LifecycleResultSuccess, LifecycleCodeValidated, false, started)
			s.emitLifecycle(traceID, LifecyclePersisting, LifecycleResultSuccess, LifecycleCodePersisting, false, started)
			validationObserved = true
		}
		return nil
	}
	generate := func() (Generated, error) {
		now := s.clock.Now()
		if now.IsZero() {
			return Generated{}, Fail(SendInvalidInput)
		}
		serverTime := now.UnixMilli()
		if serverTime < 0 {
			return Generated{}, Fail(SendInvalidInput)
		}
		serverMsgID, idErr := s.ids.NewID()
		if idErr != nil {
			return Generated{}, Fail(SendStorageUnavailable)
		}
		eventID, idErr := s.ids.NewID()
		if idErr != nil {
			return Generated{}, Fail(SendStorageUnavailable)
		}
		if !validIdentifier(serverMsgID) || !validIdentifier(eventID) {
			return Generated{}, Fail(SendInvalidInput)
		}
		return Generated{ServerMsgID: serverMsgID, EventID: eventID, ServerTime: serverTime}, nil
	}
	attemptCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	persisted, persistErr := s.store.Persist(attemptCtx, principal, request, validate, generate)
	if persistErr != nil {
		return protocol.ServerFrame{}, redactedError(persistErr)
	}
	if persisted.ClientMsgID != request.ClientMsgID || persisted.ConversationID != request.ConversationID ||
		persisted.SenderID != identity.UserID() || persisted.ConversationSeq < 0 || persisted.ServerTime < 0 ||
		(!persisted.SenderType.IsZero() && !persisted.SenderType.Valid()) {
		return protocol.ServerFrame{}, Fail(SendStorageUnavailable)
	}
	s.emitLifecycle(traceID, LifecyclePersisted, LifecycleResultSuccess, LifecycleCodePersisted, persisted.Replayed, started)
	if !persisted.Replayed {
		s.recordOutboxEnqueued(started)
	}
	ack := protocol.Ack{
		ClientMsgID:     persisted.ClientMsgID,
		ConversationID:  persisted.ConversationID,
		SenderID:        persisted.SenderID,
		SenderType:      persisted.SenderType,
		ServerMsgID:     persisted.ServerMsgID,
		ConversationSeq: strconv.FormatInt(persisted.ConversationSeq, 10),
		ServerTime:      strconv.FormatInt(persisted.ServerTime, 10),
	}
	frame = protocol.ServerFrame{Ack: &ack}
	if _, encodeErr := protocol.EncodeServerFrame(frame); encodeErr != nil {
		return protocol.ServerFrame{}, Fail(SendStorageUnavailable)
	}
	s.emitLifecycle(traceID, LifecycleACKReady, LifecycleResultSuccess, LifecycleCodeACKReady, persisted.Replayed, started)
	return frame, nil
}

func (s *Service) newTraceID() string {
	defer func() {
		if recover() != nil {
			s.drop("trace_id_failure")
		}
	}()
	if s == nil || s.traceIDs == nil {
		s.drop("trace_id_failure")
		return ""
	}
	traceID, err := s.traceIDs.NewID()
	if err != nil || !validTraceID(traceID) {
		s.drop("trace_id_failure")
		return ""
	}
	return traceID
}

func validTraceID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for index := 0; index < len(value); index++ {
		ch := value[index]
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return false
		}
	}
	return true
}

func resultForError(err error) string {
	if err == nil {
		return LifecycleResultSuccess
	}
	switch RetryDisposition(err) {
	case protocol.RetrySameIntent:
		return LifecycleResultRetryable
	case protocol.StopAutomaticRetry:
		return LifecycleResultPermanent
	default:
		return LifecycleResultUnknown
	}
}

func (s *Service) emitLifecycle(traceID, stage, result string, code Code, replayed bool, started time.Time) {
	if s == nil {
		return
	}
	observation := Observation{
		Operation: stage,
		Stage:     stage,
		Result:    result,
		Code:      code,
		TraceID:   traceID,
		Replayed:  replayed,
		Elapsed:   time.Since(started),
	}
	s.emitObservation(observation)
}

func (s *Service) observeLegacy(started time.Time, err error) {
	if s == nil {
		return
	}
	code := ErrorCode(err)
	if err == nil {
		code = "SERVER_PERSISTED"
	}
	s.observe(Observation{Operation: "send", Code: code, Elapsed: time.Since(started)})
}

func (s *Service) observe(observation Observation) {
	if s == nil {
		return
	}
	s.observability.RLock()
	observer := s.observer
	s.observability.RUnlock()
	if observer != nil {
		s.dispatchObservation(observer, observation)
	}
}

func (s *Service) emitObservation(observation Observation) {
	if s == nil {
		return
	}
	s.observability.RLock()
	observer := s.observer
	metrics := s.metrics
	s.observability.RUnlock()
	if observer != nil {
		s.dispatchObservation(observer, observation)
	}
	s.recordLifecycle(metrics, observation)
}

func (s *Service) dispatchObservation(observer Observer, observation Observation) {
	if s == nil || observer == nil {
		return
	}
	s.observerOnce.Do(func() {
		queue := api.NewObserverDispatcher(defaultObserverQueueCapacity, observer.Observe, s.drop)
		s.observability.Lock()
		s.observerQueue = queue
		s.observability.Unlock()
	})
	s.observability.RLock()
	queue := s.observerQueue
	s.observability.RUnlock()
	if queue != nil {
		queue.Enqueue(observation)
	}
}

func (s *Service) recordLifecycle(metrics MetricsSink, observation Observation) {
	s.recordLifecycleForComponent(metrics, "send", observation)
}

func (s *Service) recordLifecycleForComponent(metrics MetricsSink, component string, observation Observation) {
	if metrics == nil {
		return
	}
	defer func() {
		if recover() != nil {
			s.dropWith(metrics, "writer_error")
		}
	}()
	metrics.Record(api.MetricSample{
		Name:   api.MetricMessageLifecycleEvents,
		Labels: map[string]string{"component": component, "stage": observation.Stage, "result": observation.Result},
		Value:  1,
	})
	if observation.Elapsed > 0 {
		seconds := observation.Elapsed.Seconds()
		metrics.Record(api.MetricSample{
			Name:   api.MetricMessageLifecycleDurationSum,
			Labels: map[string]string{"component": component, "stage": observation.Stage},
			Value:  seconds,
		})
		metrics.Record(api.MetricSample{
			Name:   api.MetricMessageLifecycleDurationCount,
			Labels: map[string]string{"component": component, "stage": observation.Stage},
			Value:  1,
		})
	}
	if observation.Stage == LifecycleRejected {
		code := observation.Code
		if code == "" {
			code = SendUnknown
		}
		metrics.Record(api.MetricSample{
			Name:   api.MetricMessageLifecycleErrors,
			Labels: map[string]string{"component": component, "code": string(code)},
			Value:  1,
		})
	}
	if observation.Stage == LifecyclePersisted {
		if observation.Replayed {
			metrics.Record(api.MetricSample{Name: api.MetricMessageSendIdempotency, Labels: map[string]string{"outcome": "replayed"}, Value: 1})
		} else {
			metrics.Record(api.MetricSample{Name: api.MetricMessageSendIdempotency, Labels: map[string]string{"outcome": "new"}, Value: 1})
		}
	}
	if observation.Stage == LifecycleRejected && observation.Code == SendIDConflict {
		metrics.Record(api.MetricSample{Name: api.MetricMessageSendIdempotency, Labels: map[string]string{"outcome": "conflict"}, Value: 1})
	}
}

func (s *Service) recordOutboxEnqueued(started time.Time) {
	s.observability.RLock()
	metrics := s.metrics
	s.observability.RUnlock()
	if metrics == nil {
		return
	}
	observation := Observation{Stage: "enqueued", Result: LifecycleResultSuccess, Elapsed: time.Since(started)}
	s.recordLifecycleForComponent(metrics, "outbox", observation)
}

func (s *Service) drop(reason string) {
	if s == nil {
		return
	}
	s.observability.RLock()
	metrics := s.metrics
	s.observability.RUnlock()
	s.dropWith(metrics, reason)
}

func (s *Service) dropWith(metrics MetricsSink, reason string) {
	if metrics == nil {
		return
	}
	defer func() { _ = recover() }()
	metrics.Drop(reason)
}

func validIdentity(identity session.ConnectionIdentity) bool {
	if !validIdentifier(identity.UserID()) || !validIdentifier(identity.DeviceID()) ||
		!validIdentifier(identity.SessionID()) || !validIdentifier(identity.ConnectionID()) {
		return false
	}
	tokenID := identity.TokenID()
	if len(tokenID) != 32 {
		return false
	}
	for i := 0; i < len(tokenID); i++ {
		ch := tokenID[i]
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return false
		}
	}
	return true
}

func validIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if !((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}

func redactedError(err error) error {
	if err == nil {
		return nil
	}
	code := ErrorCode(err)
	if code == "" {
		code = SendUnknown
	}
	return &Error{Code: code, Disposition: RetryDisposition(err)}
}
