// Package message implements the internal durable 1v1 send transaction.
// message 实现内部持久化单聊发送事务；不提供网络、鉴权或投递语义。
package message

import (
	"context"
	"errors"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/conversation"
)

// Code is a stable internal send result code.
// Code 是稳定的内部发送结果码。
type Code string

const (
	SendInvalidInput        Code = "SEND_INVALID_INPUT"
	SendUnauthorized        Code = "SEND_UNAUTHORIZED"
	SendConversationMissing Code = "SEND_CONVERSATION_NOT_FOUND"
	SendSequenceExhausted   Code = "SEND_SEQUENCE_EXHAUSTED"
	SendIDConflict          Code = "SEND_ID_CONFLICT"
	SendLockUnavailable     Code = "SEND_LOCK_UNAVAILABLE"
	SendStorageUnavailable  Code = "SEND_STORAGE_UNAVAILABLE"
	SendUnknown             Code = "SEND_UNKNOWN"
)

// Error carries a stable code and local retry disposition.
// Error 只携带稳定码与本地重试处置。
type Error struct {
	Code        Code
	Disposition protocol.RetryDisposition
}

func (e *Error) Error() string { return string(e.Code) }

// Fail constructs a stable send error with its frozen disposition.
// Fail 按冻结的重试处置构造稳定发送错误。
func Fail(code Code) error {
	return &Error{Code: code, Disposition: dispositionFor(code)}
}

// ErrorCode returns a stable code; unknown errors stop automatic retries.
// ErrorCode 返回稳定错误码；未知错误停止自动重试。
func ErrorCode(err error) Code {
	if err == nil {
		return ""
	}
	var known *Error
	if errors.As(err, &known) && known != nil {
		return known.Code
	}
	return SendUnknown
}

// RetryDisposition returns the adapter/service classification.
// RetryDisposition 返回适配器/服务的重试分类。
func RetryDisposition(err error) protocol.RetryDisposition {
	if err == nil {
		return ""
	}
	var known *Error
	if errors.As(err, &known) && known != nil {
		return known.Disposition
	}
	return protocol.StopAutomaticRetry
}

// ProtocolCode maps internal retryable failures to the public temporary code.
// ProtocolCode 将内部可重试失败映射为公开临时错误码。
func ProtocolCode(err error) string {
	if err == nil {
		return ""
	}
	if RetryDisposition(err) == protocol.RetrySameIntent {
		return "SERVER_TEMPORARY_UNAVAILABLE"
	}
	return string(ErrorCode(err))
}

func dispositionFor(code Code) protocol.RetryDisposition {
	switch code {
	case SendLockUnavailable, SendStorageUnavailable:
		return protocol.RetrySameIntent
	default:
		return protocol.StopAutomaticRetry
	}
}

// Generated is trusted server identity produced after authorization.
// Generated 是授权后生成的可信服务端身份。
type Generated struct {
	ServerMsgID string
	EventID     string
	ServerTime  int64
}

// PersistedMessage is returned only after the outer transaction commits.
// PersistedMessage 仅在外部事务提交成功后返回。
type PersistedMessage struct {
	ClientMsgID     string
	ConversationID  string
	SenderID        string
	ServerMsgID     string
	ConversationSeq int64
	ServerTime      int64
}

// IDGenerator is trusted server-side identity generation.
// IDGenerator 是可信服务端标识生成器。
type IDGenerator interface {
	NewID() (string, error)
}

// Clock supplies server time for persisted message metadata.
// Clock 提供持久化消息元数据使用的服务端时间。
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
// ClockFunc 将函数适配为 Clock。
type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time { return f() }

// Observation contains only bounded operation/timing labels.
// Observation 只包含有界操作与耗时标签。
type Observation struct {
	Operation string
	Code      Code
	Elapsed   time.Duration
}

// Observer receives optional best-effort send observations.
// Observer 接收可选的最佳努力发送观测。
type Observer interface {
	Observe(Observation)
}

// Store is the application-to-storage port for the send transaction.
// Store 是发送事务的 application 到 storage 端口。
type Store interface {
	Persist(context.Context, conversation.Principal, protocol.Send, func() error, func() (Generated, error)) (PersistedMessage, error)
}
