package session

import (
	"context"
	"time"
)

// TokenSnapshot is a locked, redacted token/session view used for authentication.
// TokenSnapshot 是认证使用的已加锁脱敏令牌/会话视图。
type TokenSnapshot struct {
	tokenID          string
	digest           [32]byte
	binding          SessionBinding
	expiresAt        time.Time
	tokenRevokedAt   *time.Time
	sessionRevokedAt *time.Time
}

// NewTokenSnapshot validates and copies a storage token snapshot.
// NewTokenSnapshot 校验并复制存储层令牌快照。
func NewTokenSnapshot(tokenID string, digest []byte, binding SessionBinding, expiresAt time.Time, tokenRevokedAt, sessionRevokedAt *time.Time) (TokenSnapshot, error) {
	if !validTokenID(tokenID) || len(digest) != 32 || !binding.Valid() || expiresAt.IsZero() {
		return TokenSnapshot{}, Fail(AuthStorageUnavailable)
	}
	var copied [32]byte
	copy(copied[:], digest)
	return TokenSnapshot{
		tokenID: tokenID, digest: copied, binding: binding, expiresAt: expiresAt,
		tokenRevokedAt: cloneTime(tokenRevokedAt), sessionRevokedAt: cloneTime(sessionRevokedAt),
	}, nil
}

// VerifyDigest compares the stored digest in constant time.
// VerifyDigest 使用常量时间比较存储摘要。
func (s TokenSnapshot) VerifyDigest(provided [32]byte) bool {
	return constantTimeEqual(s.digest, provided)
}

// TokenID returns the non-secret stable lookup identifier.
// TokenID 返回非敏感的稳定查询标识。
func (s TokenSnapshot) TokenID() string { return s.tokenID }

// Binding returns the token's persisted trusted session binding.
// Binding 返回令牌持久化的可信会话绑定。
func (s TokenSnapshot) Binding() SessionBinding { return s.binding }

// ExpiresAt returns the exact persisted expiry instant.
// ExpiresAt 返回持久化的精确过期时刻。
func (s TokenSnapshot) ExpiresAt() time.Time { return s.expiresAt }

// TokenRevokedAt returns the token revocation instant when present.
// TokenRevokedAt 返回令牌撤销时刻（如存在）。
func (s TokenSnapshot) TokenRevokedAt() *time.Time { return cloneTime(s.tokenRevokedAt) }

// SessionRevokedAt returns the session revocation instant when present.
// SessionRevokedAt 返回会话撤销时刻（如存在）。
func (s TokenSnapshot) SessionRevokedAt() *time.Time { return cloneTime(s.sessionRevokedAt) }

// SessionSnapshot is a bounded session lookup used by login-policy validation.
// SessionSnapshot 是登录策略校验使用的有界会话视图。
type SessionSnapshot struct {
	binding   SessionBinding
	revokedAt *time.Time
}

// NewSessionSnapshot validates and copies a storage session view.
// NewSessionSnapshot 校验并复制存储层会话视图。
func NewSessionSnapshot(binding SessionBinding, revokedAt *time.Time) (SessionSnapshot, error) {
	if !binding.Valid() {
		return SessionSnapshot{}, Fail(AuthStorageUnavailable)
	}
	return SessionSnapshot{binding: binding, revokedAt: cloneTime(revokedAt)}, nil
}

// Binding returns the persisted session binding.
// Binding 返回持久化会话绑定。
func (s SessionSnapshot) Binding() SessionBinding { return s.binding }

// RevokedAt returns the session revocation instant when present.
// RevokedAt 返回会话撤销时刻（如存在）。
func (s SessionSnapshot) RevokedAt() *time.Time { return cloneTime(s.revokedAt) }

// Store is the application-to-storage port; adapters own transactions and locks.
// Store 是 application 到 storage 的端口；适配器负责事务与锁。
type Store interface {
	Issue(context.Context, SessionBinding, func() (IssuedToken, error)) (IssuedToken, error)
	Authenticate(context.Context, string, func(TokenSnapshot) error) error
	RevokeBearer(context.Context, string, func(TokenSnapshot) error, time.Time) (RevocationOutcome, error)
	LookupSession(context.Context, string) (SessionSnapshot, error)
	RevokeSession(context.Context, SessionBinding, time.Time) (RevocationOutcome, error)
	RevokeToken(context.Context, string, string, time.Time) (RevocationOutcome, error)
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}
