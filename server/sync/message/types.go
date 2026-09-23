// Package message provides policy-neutral internal message delta reads.
// message 提供策略中立的内部消息增量读取，不提供网络或鉴权。
package message

import (
	"context"
	"errors"
	"time"
)

// Code is a stable internal sync error without storage or identity details.
// Code 是不含存储或身份细节的稳定内部同步错误码。
type Code string

const (
	Forbidden          Code = "SYNC_FORBIDDEN"
	InvalidCursor      Code = "SYNC_INVALID_CURSOR"
	LimitExceeded      Code = "SYNC_LIMIT_EXCEEDED"
	StorageUnavailable Code = "SYNC_STORAGE_UNAVAILABLE"
)

// Error is sealed: callers can inspect only the stable code.
// Error 为封口错误，调用方只能读取稳定错误码。
type Error struct{ Code Code }

func (e *Error) Error() string { return string(e.Code) }

// Unwrap deliberately returns nil so database details cannot escape.
// Unwrap 固定返回 nil，避免底层数据库细节外泄。
func (e *Error) Unwrap() error { return nil }

// Fail constructs a stable, content-free error.
// Fail 构造稳定且不含底层细节的错误。
func Fail(code Code) error { return &Error{Code: code} }

// ErrorCode returns the stable code; unknown errors are redacted.
// ErrorCode 返回稳定错误码；未知错误统一脱敏。
func ErrorCode(err error) Code {
	if err == nil {
		return ""
	}
	var known *Error
	if errors.As(err, &known) && known != nil {
		return known.Code
	}
	return StorageUnavailable
}

const (
	// DefaultPageItems is used when Limit is zero.
	DefaultPageItems = 100
	// MaxPageItems bounds every page and every candidate query.
	MaxPageItems = 100
	// MaxResponseBytes is the deterministic page-cost budget.
	MaxResponseBytes = 262144
	pageOverhead     = 64
	itemOverhead     = 128
	// MaxSequence is the signed BIGINT sequence domain.
	MaxSequence int64 = 9223372036854775807
)

// ReadRequest is an internal continuation request. It is not a wire object.
// ReadRequest 是内部续读请求，不是公共 wire 对象。
type ReadRequest struct {
	ConversationID string
	HasAfterSeq    bool
	AfterSeq       int64
	Limit          int
}

// Item is one immutable persisted message row.
// Item 是一条不可变的持久化消息行。
type Item struct {
	ServerMsgID     string
	ClientMsgID     string
	SenderID        string
	ConversationID  string
	ConversationSeq int64
	ProtocolVersion uint32
	SchemaVersion   uint32
	MessageType     string
	ServerTime      int64
	Payload         []byte
}

// Page is a bounded, deterministic message page.
// Page 是有界且确定性的消息页。
type Page struct {
	Items        []Item
	LatestSeq    int64
	HasNext      bool
	NextAfterSeq *int64
}

// ReadResult is one consistent storage snapshot. Limit is the normalized
// effective limit used by the adapter.
// ReadResult 是一次一致性存储快照；Limit 是适配器实际使用的规范化上限。
type ReadResult struct {
	LatestSeq int64
	Limit     int
	Rows      []Item
}

// Store is the application-to-storage port.
// Store 是应用层到存储层的端口。
type Store interface {
	Read(context.Context, string, ReadRequest) (ReadResult, error)
}

// Observation contains only fixed labels and aggregate values.
// Observation 只包含固定标签与聚合值。
type Observation struct {
	Operation string
	Code      Code
	Elapsed   time.Duration
	Items     int
	Bytes     int
}

// Observer receives best-effort content-free observations.
// Observer 接收尽力而为且内容无关的观测。
type Observer interface{ Observe(Observation) }

// Config is trusted service configuration, never request input.
// Config 是可信服务配置，不接受请求输入。
type Config struct {
	Observer       Observer
	RequestTimeout time.Duration
}
