// Package session provides policy-neutral durable access-token primitives.
// session 提供策略无关的持久访问令牌原语；调用方必须提供可信用户/设备/会话边界。
package session

import (
	"context"
	"errors"
	"time"
)

// Code is a stable internal error or observation outcome with no sensitive data.
// Code 是不含敏感数据的稳定内部错误或观测结果码。
type Code string

// Stable error and observation outcome codes.
// 稳定的错误与观测结果码。
const (
	AuthInvalidInput       Code = "AUTH_INVALID_INPUT"
	AuthTokenMalformed     Code = "AUTH_TOKEN_MALFORMED"
	AuthTokenExpired       Code = "AUTH_TOKEN_EXPIRED"
	AuthTokenRevoked       Code = "AUTH_TOKEN_REVOKED"
	AuthSessionRevoked     Code = "AUTH_SESSION_REVOKED"
	AuthSessionNotFound    Code = "AUTH_SESSION_NOT_FOUND"
	AuthForbidden          Code = "AUTH_FORBIDDEN"
	AuthStorageUnavailable Code = "AUTH_STORAGE_UNAVAILABLE"
	AuthPolicyRequired     Code = "AUTH_POLICY_REQUIRED"
	AuthPolicyInvalid      Code = "AUTH_POLICY_INVALID"
	AuthObserverRequired   Code = "AUTH_OBSERVER_REQUIRED"
	AuthClockRequired      Code = "AUTH_CLOCK_REQUIRED"
	AuthTokenUnknown       Code = "AUTH_TOKEN_UNKNOWN"
	AuthTokenCollision     Code = "AUTH_TOKEN_COLLISION"
	AuthEntropyUnavailable Code = "AUTH_ENTROPY_UNAVAILABLE"
	AuthIssueOK            Code = "AUTH_ISSUE_OK"
	AuthAuthenticateOK     Code = "AUTH_AUTHENTICATE_OK"
	AuthRevokeOK           Code = "AUTH_REVOKE_OK"
	AuthRevokeNoop         Code = "AUTH_REVOKE_NOOP"
	AuthPlanLoginOK        Code = "AUTH_PLAN_LOGIN_OK"
)

// Error carries only a stable code; underlying driver and policy errors are redacted.
// Error 只携带稳定错误码，不包装驱动或策略错误。
type Error struct{ Code Code }

func (e *Error) Error() string { return string(e.Code) }

// Fail constructs a stable redacted error.
// Fail 构造稳定的脱敏错误。
func Fail(code Code) error { return &Error{Code: code} }

// ErrorCode returns a stable code and redacts unknown errors as storage failure.
// ErrorCode 返回稳定错误码；未知错误统一脱敏为存储不可用。
func ErrorCode(err error) Code {
	if err == nil {
		return ""
	}
	var known *Error
	if errors.As(err, &known) && known != nil {
		return known.Code
	}
	return AuthStorageUnavailable
}

// Observation contains only bounded labels and timing.
// Observation 仅包含有界标签与耗时，不包含凭据、摘要或请求内容。
type Observation struct {
	Operation string
	Code      Code
	TokenID   string
	SessionID string
	Elapsed   time.Duration
}

// Observer must be concurrency-safe and must not block request completion.
// Observer 必须并发安全，且不得阻塞请求完成。
type Observer interface{ Observe(Observation) }

// Clock supplies the service time used for expiry and revocation.
// Clock 提供用于过期与撤销边界的服务时间。
type Clock interface{ Now() time.Time }

// ClockFunc adapts a function to Clock.
// ClockFunc 将函数适配为 Clock。
type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time { return f() }

// SessionBinding is trusted user/device/session identity, never request-invented.
// SessionBinding 是可信的用户/设备/会话身份，不由请求内容自行决定。
type SessionBinding struct {
	UserID    string
	DeviceID  string
	SessionID string
}

// Valid reports whether every binding identifier uses the accepted ASCII grammar.
// Valid 判断绑定字段是否符合既定 ASCII 标识符语法。
func (b SessionBinding) Valid() bool {
	return validIdentifier(b.UserID) && validIdentifier(b.DeviceID) && validIdentifier(b.SessionID)
}

// IssueRequest asks the trusted caller to issue one token for a session.
// IssueRequest 请求可信调用方为指定会话签发令牌。
type IssueRequest struct {
	Binding SessionBinding
	TTL     time.Duration
}

// AuthenticateRequest supplies a raw token and the expected trusted connection.
// AuthenticateRequest 提供原始令牌及预期可信连接身份。
type AuthenticateRequest struct {
	Token        string
	Binding      SessionBinding
	ConnectionID string
}

// RevocationOutcome distinguishes a real transition from an idempotent no-op.
// RevocationOutcome 区分真实撤销与幂等无操作。
type RevocationOutcome string

// Revocation outcomes distinguish a transition from an idempotent no-op.
// 撤销结果区分真实状态迁移与幂等无操作。
const (
	RevokeOK   RevocationOutcome = "AUTH_REVOKE_OK"
	RevokeNoop RevocationOutcome = "AUTH_REVOKE_NOOP"
)

// ConnectionIdentity is complete only when all five trusted IDs are present.
// ConnectionIdentity 仅在用户、设备、会话、连接和令牌五个标识均存在时有效。
type ConnectionIdentity struct {
	userID       string
	deviceID     string
	sessionID    string
	connectionID string
	tokenID      string
}

// NewConnectionIdentity validates and constructs a complete identity.
// NewConnectionIdentity 校验并构造完整连接身份，拒绝缺失字段。
func NewConnectionIdentity(userID, deviceID, sessionID, connectionID, tokenID string) (ConnectionIdentity, error) {
	if !validIdentifier(userID) || !validIdentifier(deviceID) || !validIdentifier(sessionID) ||
		!validIdentifier(connectionID) || !validTokenID(tokenID) {
		return ConnectionIdentity{}, Fail(AuthInvalidInput)
	}
	return ConnectionIdentity{
		userID: userID, deviceID: deviceID, sessionID: sessionID,
		connectionID: connectionID, tokenID: tokenID,
	}, nil
}

// UserID returns the trusted user identifier.
// UserID 返回可信用户标识。
func (i ConnectionIdentity) UserID() string { return i.userID }

// DeviceID returns the trusted device identifier.
// DeviceID 返回可信设备标识。
func (i ConnectionIdentity) DeviceID() string { return i.deviceID }

// SessionID returns the trusted session identifier.
// SessionID 返回可信会话标识。
func (i ConnectionIdentity) SessionID() string { return i.sessionID }

// ConnectionID returns the trusted connection identifier.
// ConnectionID 返回可信连接标识。
func (i ConnectionIdentity) ConnectionID() string { return i.connectionID }

// TokenID returns the non-secret stable token lookup identifier.
// TokenID 返回非敏感的稳定令牌查询标识。
func (i ConnectionIdentity) TokenID() string { return i.tokenID }

// KickTarget is a policy-selected session to kick after a later login decision.
// KickTarget 是后续登录决策选中的待踢会话，不由本组件选择。
type KickTarget struct {
	UserID    string
	DeviceID  string
	SessionID string
}

// LoginRequest is the trusted session boundary for a injected login policy.
// LoginRequest 是注入登录策略使用的可信会话边界。
type LoginRequest struct {
	Binding SessionBinding
}

// LoginDecision is the policy result; the service validates every target.
// LoginDecision 是策略结果；服务会校验每一个目标。
type LoginDecision struct {
	KickTargets []KickTarget
}

// LoginPolicy is implemented by a later product-policy boundary.
// LoginPolicy 由后续产品策略边界实现，本组件不提供默认选择。
type LoginPolicy interface {
	Decide(context.Context, LoginRequest) (LoginDecision, error)
}

// LoginPlan contains only validated identifiers; it does not perform a kick.
// LoginPlan 只包含已校验标识，不执行连接踢除。
type LoginPlan struct {
	Binding     SessionBinding
	KickTargets []KickTarget
}
