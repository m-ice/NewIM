// Package media implements authorized upload credentials, immutable object
// completion, private download resolution and message-send validation.
// media 实现授权上传凭据、不可变对象完成、私有下载解析与消息发送校验。
package media

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/m-ice/NewIM/server/auth/session"
)

// Code is a stable internal media error or observation outcome.
// Code 是稳定的内部媒体错误或观测结果。
type Code string

const (
	MediaInvalidInput       Code = "MEDIA_INVALID_INPUT"
	MediaInvalidToken       Code = "MEDIA_INVALID_TOKEN"
	MediaNotFound           Code = "MEDIA_NOT_FOUND"
	MediaUnauthorized       Code = "MEDIA_UNAUTHORIZED"
	MediaExpired            Code = "MEDIA_EXPIRED"
	MediaConflict           Code = "MEDIA_CONFLICT"
	MediaStorageUnavailable Code = "MEDIA_STORAGE_UNAVAILABLE"
	MediaGrantCollision     Code = "MEDIA_GRANT_COLLISION"
)

// Error carries only a stable code and no credential or object content.
// Error 只携带稳定错误码，不包含凭据或对象内容。
type Error struct{ Code Code }

func (e *Error) Error() string { return string(e.Code) }

// Fail constructs a stable redacted media error.
// Fail 构造稳定的脱敏媒体错误。
func Fail(code Code) error { return &Error{Code: code} }

// ErrorCode returns a stable code; unknown errors are storage failures.
// ErrorCode 返回稳定错误码；未知错误统一按存储失败脱敏。
func ErrorCode(err error) Code {
	if err == nil {
		return ""
	}
	var known *Error
	if errors.As(err, &known) && known != nil {
		return known.Code
	}
	return MediaStorageUnavailable
}

// Clock supplies trusted service time.
// Clock 提供可信服务时间。
type Clock interface{ Now() time.Time }

// ClockFunc adapts a function to Clock.
// ClockFunc 将函数适配为时钟。
type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time { return f() }

// Observation contains bounded labels only, never grants, digests or URLs.
// Observation 只包含有界标签，绝不包含凭据、摘要或 URL。
type Observation struct {
	Operation string
	Code      Code
	Elapsed   time.Duration
}

// Observer receives best-effort observations.
// Observer 接收最佳努力观测。
type Observer interface{ Observe(Observation) }

// State is the closed media asset state machine.
// State 是封闭的媒体资产状态机。
type State string

const (
	StatePending State = "pending"
	StateReady   State = "ready"
)

// Metadata is ready-asset metadata used by message validation and downloads.
// Metadata 是消息校验与下载使用的就绪资产元数据。
type Metadata struct {
	MediaKey    string
	Kind        string
	ContentType string
	Size        int64
	SHA256      [32]byte
}

// Intent is the trusted upload intent accepted by BeginUpload.
// Intent 是 BeginUpload 接受的可信上传意向。
type Intent struct {
	Kind        string
	ContentType string
	Size        int64
	SHA256      [32]byte
}

// Grant is a durable upload grant or its completed ready asset.
// Grant 是持久上传凭据或其完成后的就绪资产。
type Grant struct {
	MediaKey       string
	OwnerUserID    string
	DeviceID       string
	SessionID      string
	ConnectionID   string
	TokenID        string
	ConversationID string
	Kind           string
	ContentType    string
	DeclaredSize   int64
	ActualSize     *int64
	SHA256         [32]byte
	GrantID        string
	GrantDigest    [32]byte
	ExpiresAt      time.Time
	CompletedAt    *time.Time
	State          State
}

// UploadGrant is returned once from BeginUpload; RawToken is never persisted.
// UploadGrant 仅由 BeginUpload 返回一次；RawToken 绝不持久化。
type UploadGrant struct {
	RawToken  string
	MediaKey  string
	ExpiresAt time.Time
}

// ObjectInfo is the exact immutable object identity.
// ObjectInfo 是精确的不可变对象身份。
type ObjectInfo struct {
	Size   int64
	SHA256 [32]byte
}

// Signer creates an in-memory short-lived private URL.
// Signer 在内存中创建短时私有 URL。
type Signer interface {
	Sign(context.Context, string, time.Time) (string, error)
}

// Store is the application-to-storage port for grants, assets and authorization.
// Store 是凭据、资产和授权的应用到存储端口。
type Store interface {
	// BeginUpload must authorize the complete identity before invoking create.
	// BeginUpload 必须先授权完整身份，再调用 create。
	BeginUpload(context.Context, time.Time, session.ConnectionIdentity, string, func() (Grant, error)) error
	LoadGrant(context.Context, string) (Grant, error)
	// CompleteReady atomically marks an identity/intent-bound pending grant ready.
	// CompleteReady 原子地将身份/意向绑定的 pending 凭据标记为 ready。
	CompleteReady(context.Context, time.Time, session.ConnectionIdentity, Grant) (Grant, error)
	LoadReadyAsset(context.Context, string) (Grant, error)
	// Authorize verifies a current membership and complete live session identity.
	// Authorize 校验当前成员关系及完整在线会话身份。
	Authorize(context.Context, time.Time, session.ConnectionIdentity, string) error
	// ValidateReadyForSend checks exact ready/owner/conversation-bound metadata.
	// ValidateReadyForSend 校验精确的 ready/owner/conversation 绑定元数据。
	ValidateReadyForSend(context.Context, time.Time, session.ConnectionIdentity, string, Metadata) error
}

// ObjectStore provides race-safe immutable object publication and comparison.
// ObjectStore 提供竞态安全的不可变对象发布与比较。
type ObjectStore interface {
	// PutImmutable publishes only if absent; an existing object must match expected exactly.
	// PutImmutable 仅在对象不存在时发布；已有对象必须与 expected 精确一致。
	PutImmutable(context.Context, string, io.Reader, ObjectInfo) (ObjectInfo, error)
}

// ValidateIdentity rejects incomplete trusted connection identity.
// ValidateIdentity 拒绝不完整的可信连接身份。
func ValidateIdentity(identity session.ConnectionIdentity) bool {
	return validCompleteIdentity(identity.UserID(), identity.DeviceID(), identity.SessionID(), identity.ConnectionID(), identity.TokenID())
}

// validCompleteIdentity is the single binding check for all five trusted fields.
// validCompleteIdentity 是五个可信字段的唯一绑定校验。
func validCompleteIdentity(userID, deviceID, sessionID, connectionID, tokenID string) bool {
	return validIdentifier(userID) && validIdentifier(deviceID) && validIdentifier(sessionID) &&
		validIdentifier(connectionID) && validTokenID(tokenID)
}

// ValidIntent validates one upload intent.
// ValidIntent 校验一个上传意向。
func ValidIntent(intent Intent) bool {
	return validKind(intent.Kind) && validContentType(intent.ContentType) &&
		intent.Size >= 1 && intent.Size <= MaxMediaSize
}

// ValidMetadata validates ready metadata.
// ValidMetadata 校验就绪元数据。
func ValidMetadata(metadata Metadata) bool {
	return validMediaKey(metadata.MediaKey) && validKind(metadata.Kind) &&
		validContentType(metadata.ContentType) && metadata.Size >= 1 && metadata.Size <= MaxMediaSize
}

// ValidGrant validates a durable grant shape without trusting request data.
// ValidGrant 校验持久凭据形状，不信任请求内容。
func ValidGrant(grant Grant) bool {
	if !validMediaKey(grant.MediaKey) ||
		!validCompleteIdentity(grant.OwnerUserID, grant.DeviceID, grant.SessionID, grant.ConnectionID, grant.TokenID) ||
		!validIdentifier(grant.ConversationID) || !validKind(grant.Kind) ||
		!validContentType(grant.ContentType) || grant.DeclaredSize < 1 || grant.DeclaredSize > MaxMediaSize ||
		!validGrantID(grant.GrantID) || grant.ExpiresAt.IsZero() {
		return false
	}
	if grant.State == StatePending {
		return grant.ActualSize == nil && grant.CompletedAt == nil
	}
	return grant.State == StateReady && grant.ActualSize != nil && *grant.ActualSize == grant.DeclaredSize && grant.CompletedAt != nil
}

// ValidObjectInfo validates size and SHA-256 bounds.
// ValidObjectInfo 校验大小与 SHA-256 边界。
func ValidObjectInfo(info ObjectInfo) bool { return info.Size >= 1 && info.Size <= MaxMediaSize }

// ParseSHA256 parses exactly 64 lowercase hexadecimal bytes.
// ParseSHA256 解析精确的 64 个小写十六进制字节。
func ParseSHA256(value string) ([32]byte, bool) {
	var digest [32]byte
	if !validSHA256(value) {
		return digest, false
	}
	for i := 0; i < len(digest); i++ {
		high := hexNibble(value[i*2])
		low := hexNibble(value[i*2+1])
		digest[i] = high<<4 | low
	}
	return digest, true
}

func hexNibble(value byte) byte {
	if value >= '0' && value <= '9' {
		return value - '0'
	}
	return value - 'a' + 10
}
