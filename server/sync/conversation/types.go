// Package conversation provides internal, authenticated-caller sync primitives.
// conversation 提供内部同步原语；调用方负责建立可信身份，不提供网络鉴权。
package conversation

import (
	"context"
	"errors"
	"time"
)

// Code is a stable internal error, without identifiers or storage details.
// Code 是无身份或存储细节的稳定内部错误码。
type Code string

const (
	CursorExpired      Code = "SYNC_CURSOR_EXPIRED"
	InvalidCursor      Code = "SYNC_INVALID_CURSOR"
	Forbidden          Code = "SYNC_FORBIDDEN"
	NotReady           Code = "SYNC_NOT_READY"
	LimitExceeded      Code = "SYNC_LIMIT_EXCEEDED"
	StorageUnavailable Code = "SYNC_STORAGE_UNAVAILABLE"
	SequenceExhausted  Code = "SYNC_SEQUENCE_EXHAUSTED"
)

type Error struct{ Code Code }

func (e *Error) Error() string { return string(e.Code) }

// Fail constructs an error that never wraps sensitive database/cursor values.
// Fail 不包装可能含敏感值的数据库或游标错误。
func Fail(code Code) error { return &Error{Code: code} }

// ErrorCode returns the stable code; unknown storage failures are redacted.
// ErrorCode 将未知底层错误统一脱敏。
func ErrorCode(err error) Code {
	if err == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) && e != nil {
		return e.Code
	}
	return StorageUnavailable
}

const MaxSequence uint64 = 9223372036854775807
const MaxResponseBytes = 65536
const MaxCursorBytes = 2048
const MaxDirectoryCandidates = 200
const MaxPageItems = 100

// Principal must originate from a trusted authentication boundary.
// Principal 必须由可信鉴权边界提供，请求不得替换该身份。
type Principal struct{ UserID string }

// Item is an immutable account revision; a remove retains no summary.
// Item 是账户不可变版本；remove 不携带摘要。
type Item struct {
	ConversationID        string  `json:"conversationId"`
	Revision              uint64  `json:"revision,string"`
	Kind                  string  `json:"kind"`
	LatestConversationSeq *uint64 `json:"latestConversationSeq,omitempty,string"`
	LatestServerMsgID     *string `json:"latestServerMsgId,omitempty"`
}

// Page effects and NextCursor must be persisted atomically by consumers.
// 使用方必须将 Page 内容与 NextCursor 原子持久化。
type Page struct {
	Items      []Item `json:"items"`
	NextCursor string `json:"nextCursor"`
	HasMore    bool   `json:"hasMore"`
}

// ReadKind selects bootstrap directory or delta revision traversal.
// ReadKind 区分目录快照与版本增量读取。
type ReadKind uint8

const (
	BootstrapRead ReadKind = 1
	DeltaRead     ReadKind = 2
)

// ReadRequest is a validated internal query, not a client wire structure.
// ReadRequest 为已验证的内部查询，不是客户端可直接调用的协议。
type ReadRequest struct {
	Kind        ReadKind
	Start       bool
	Epoch       uint64
	Fence       uint64
	AfterSeq    uint64
	AfterKey    string
	TerminalKey string
	Limit       int
}

// Candidate carries same-snapshot membership and, only if unauthorized,
// the latest projection. 候选权限与最新投影必须来自同一数据库快照。
type Candidate struct {
	Key    string
	State  *Item
	Member bool
	Latest *Item
}

// ReadResult owns one bounded read-only snapshot; no transaction escapes.
// ReadResult 来自单次有界只读快照，不将事务传递到下一页。
type ReadResult struct {
	Epoch              uint64
	Head               uint64
	Floor              uint64
	Fence              uint64
	TerminalKey        string
	Candidates         []Candidate
	DirectoryExhausted bool
}

// Store enforces user/account predicates and a short consistent DB snapshot.
// Store 负责账户过滤及短生命周期一致数据库快照。
type Store interface {
	Read(context.Context, string, ReadRequest) (ReadResult, error)
}

// Observation contains only bounded operation/error labels and aggregate values.
// Observation 仅含固定操作/错误标签和统计数值，不包含用户或会话标识。
type Observation struct {
	Operation  string
	Code       Code
	Elapsed    time.Duration
	Candidates int
	Items      int
	Bytes      int
}

// Observer must be concurrency-safe and must not block request completion.
// Observer 必须并发安全，且不得阻塞请求完成。
type Observer interface{ Observe(Observation) }
