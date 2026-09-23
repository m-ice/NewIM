// Package webhook implements durable at-least-once delivery from the message
// transactional outbox to preconfigured internal endpoints.
// webhook 实现从消息事务 outbox 到预配置内部 endpoint 的持久 at-least-once 投递。
package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"
)

// Code is a stable webhook delivery outcome or failure code.
// Code 是稳定的 Webhook 投递结果或失败码。
type Code string

const (
	CodeInvalidConfig      Code = "WEBHOOK_INVALID_CONFIG"
	CodeStorageUnavailable Code = "WEBHOOK_STORAGE_UNAVAILABLE"
	CodeLeaseLost          Code = "WEBHOOK_LEASE_LOST"
	CodeSecretUnavailable  Code = "WEBHOOK_SECRET_UNAVAILABLE"
	CodeSecretInvalid      Code = "WEBHOOK_SECRET_INVALID"
	CodeHTTPTemporary      Code = "WEBHOOK_HTTP_TEMPORARY"
	CodeHTTPPermanent      Code = "WEBHOOK_HTTP_PERMANENT"
	CodeResponseTooLarge   Code = "WEBHOOK_RESPONSE_TOO_LARGE"
	CodeBacklogPaused      Code = "WEBHOOK_BACKLOG_PAUSED"
	CodeEndpointRevoked    Code = "WEBHOOK_ENDPOINT_REVOKED"
	CodeDeliveryDeadLetter Code = "WEBHOOK_DELIVERY_DEAD_LETTER"
	CodeProtocolInvalid    Code = "WEBHOOK_PROTOCOL_INVALID"
	CodeDeliveryCancelled  Code = "WEBHOOK_DELIVERY_CANCELLED"
	CodeDeliveryDelivered  Code = "WEBHOOK_DELIVERY_DELIVERED"
	CodeDeliveryRetry      Code = "WEBHOOK_DELIVERY_RETRY"
	CodeFanoutLimit        Code = "WEBHOOK_FANOUT_LIMIT"
)

// MaxEndpointsPerEvent bounds endpoint fan-out for one event.
// MaxEndpointsPerEvent 限制单个事件的 endpoint fan-out 数量。
const MaxEndpointsPerEvent = 64

// Error carries only a stable code and never an endpoint URL, secret or body.
// Error 只携带稳定码，不包含 endpoint URL、secret 或 body。
type Error struct{ Code Code }

func (e *Error) Error() string { return string(e.Code) }

// Fail constructs a stable webhook error.
// Fail 构造稳定的 Webhook 错误。
func Fail(code Code) error { return &Error{Code: code} }

// ErrorCode returns a stable code; unknown errors are storage failures.
// ErrorCode 返回稳定码；未知错误按存储故障处理。
func ErrorCode(err error) Code {
	if err == nil {
		return ""
	}
	var known *Error
	if errors.As(err, &known) && known != nil {
		return known.Code
	}
	return CodeStorageUnavailable
}

// Clock supplies trusted worker time.
// Clock 提供可信 worker 时间。
type Clock interface{ Now() time.Time }

// ClockFunc adapts a function to Clock.
// ClockFunc 将函数适配为 Clock。
type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time { return f() }

// JitterFunc spreads retry deadlines using a stable delivery identity.
// JitterFunc 使用稳定的 delivery 身份散布重试期限。
type JitterFunc func(string, time.Duration) time.Duration

// Observation contains only bounded operation and code labels.
// Observation 只包含有界操作和码标签。
type Observation struct {
	Operation string
	Code      Code
	Elapsed   time.Duration
}

// Observer receives best-effort observations.
// Observer 接收最佳努力观测。
type Observer interface{ Observe(Observation) }

// Event is the persisted message identity needed to construct webhook-v1.
// Event 是构造 Webhook v1 所需的持久消息身份。
type Event struct {
	ID              string
	ServerMsgID     string
	ClientMsgID     string
	SenderID        string
	ConversationID  string
	ConversationSeq int64
	ServerTime      int64
	ProtocolVersion int
	SchemaVersion   int
	MessageType     string
	Payload         json.RawMessage
}

// SecretMaterial is the encrypted material resolved for one endpoint revision.
// SecretMaterial 是一个 endpoint revision 的加密 secret 材料。
type SecretMaterial struct {
	DestinationID string
	Revision      int64
	URL           string
	KeyID         string
	Nonce         []byte
	Ciphertext    []byte
}

// SecretResolver unwraps a revision-bound secret and never logs its result.
// SecretResolver 解封 revision 绑定的 secret，且不得记录结果。
type SecretResolver interface {
	Resolve(context.Context, SecretMaterial) ([]byte, error)
}

// Delivery is one durable event/destination pair claimed by a worker.
// Delivery 是 worker 领取的一个持久 event/destination 对。
type Delivery struct {
	ID               string
	Event            Event
	DestinationID    string
	EndpointRevision int64
	URL              string
	KeyID            string
	Secret           SecretMaterial
	Attempts         int
	LeaseToken       string
	Status           string
	NextAttemptAt    time.Time
}

// Counts summarizes the durable work queue for backlog gating.
// Counts 汇总持久工作队列用于 backlog 门控。
type Counts struct {
	Total          int
	MaxDestination int
}

// Outcome is a valid terminal or retry transition for a claimed delivery.
// Outcome 是已领取 delivery 的合法终态或重试转换。
type Outcome struct {
	Status      string
	NextAttempt time.Time
	HTTPStatus  int
	ErrorCode   Code
	CompletedAt time.Time
}

// Store is the application-to-storage port for webhook fan-out and delivery.
// Store 是 Webhook fan-out 和投递的应用到存储端口。
type Store interface {
	CancelRevoked(context.Context, time.Time) (int, error)
	Counts(context.Context) (Counts, error)
	Fanout(context.Context, time.Time, int) (int, error)
	Claim(context.Context, time.Time, string, time.Duration, int) ([]Delivery, error)
	BeginAttempt(context.Context, string, string, time.Time, int) (bool, error)
	Finish(context.Context, string, string, int, time.Time, Outcome) error
}

// Response is a bounded HTTP response returned by Doer.
// Response 是 Doer 返回的有界 HTTP 响应。
type Response struct {
	StatusCode int
	Body       []byte
	RetryAfter time.Duration
}

// Doer sends one signed request without exposing response bodies to logs.
// Doer 发送一个已签名请求，不把响应 body 暴露给日志。
type Doer interface {
	Do(context.Context, string, map[string]string, []byte) (Response, error)
}

// Config is trusted worker configuration and has no request-derived values.
// Config 是可信 worker 配置，不含请求派生值。
type Config struct {
	Owner               string
	BatchSize           int
	MaxConcurrent       int
	MaxPerDestination   int
	MaxAttempts         int
	MaxResponseBytes    int64
	LeaseTTL            time.Duration
	RequestTimeout      time.Duration
	BaseBackoff         time.Duration
	MaxBackoff          time.Duration
	HighWater           int
	LowWater            int
	MaxDestinationQueue int
	RatePerSecond       float64
	RateBurst           float64
	IdleDelay           time.Duration
	Clock               Clock
	Jitter              JitterFunc
	Observer            Observer
}

// Validate rejects unsafe or unbounded worker configuration.
// Validate 拒绝不安全或无界 worker 配置。
func (c Config) Validate() error {
	if c.Owner == "" || len(c.Owner) > 128 || c.BatchSize < 1 || c.BatchSize > 1000 ||
		c.MaxConcurrent < 1 || c.MaxConcurrent > 64 || c.MaxPerDestination < 1 || c.MaxPerDestination > c.MaxConcurrent ||
		c.MaxAttempts < 1 || c.MaxAttempts > 8 ||
		c.MaxResponseBytes < 1 || c.MaxResponseBytes > 1<<20 ||
		c.LeaseTTL <= c.RequestTimeout || c.LeaseTTL > 10*time.Minute ||
		c.RequestTimeout < time.Millisecond || c.RequestTimeout > time.Minute ||
		c.BaseBackoff < time.Millisecond || c.BaseBackoff > time.Minute ||
		c.MaxBackoff < c.BaseBackoff || c.MaxBackoff > time.Hour ||
		c.HighWater < 1 || c.LowWater < 0 || c.LowWater >= c.HighWater ||
		c.MaxDestinationQueue < 1 || c.MaxDestinationQueue > 1_000_000 ||
		math.IsNaN(c.RatePerSecond) || math.IsInf(c.RatePerSecond, 0) ||
		math.IsNaN(c.RateBurst) || math.IsInf(c.RateBurst, 0) ||
		c.RatePerSecond < 0.1 || c.RatePerSecond > 1000 || c.RateBurst < 1 || c.RateBurst > 1000 ||
		c.IdleDelay < time.Millisecond || c.IdleDelay > time.Minute {
		return Fail(CodeInvalidConfig)
	}
	return nil
}
