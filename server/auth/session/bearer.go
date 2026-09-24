package session

import (
	"context"
	"time"
)

// BearerSession is the persisted identity and expiry resolved from one raw token.
// BearerSession 是从一个原始令牌解析出的持久化身份与过期时刻，不包含或伪造连接身份。
type BearerSession struct {
	binding   SessionBinding
	tokenID   string
	expiresAt time.Time
}

func newBearerSession(binding SessionBinding, tokenID string, expiresAt time.Time) (BearerSession, error) {
	if !binding.Valid() || !validTokenID(tokenID) || expiresAt.IsZero() {
		return BearerSession{}, Fail(AuthStorageUnavailable)
	}
	return BearerSession{
		binding: binding, tokenID: tokenID, expiresAt: expiresAt.UTC(),
	}, nil
}

// UserID returns the trusted user identifier from the persisted binding.
// UserID 返回持久化绑定中的可信用户标识。
func (s BearerSession) UserID() string { return s.binding.UserID }

// DeviceID returns the trusted device identifier from the persisted binding.
// DeviceID 返回持久化绑定中的可信设备标识。
func (s BearerSession) DeviceID() string { return s.binding.DeviceID }

// SessionID returns the trusted session identifier from the persisted binding.
// SessionID 返回持久化绑定中的可信会话标识。
func (s BearerSession) SessionID() string { return s.binding.SessionID }

// TokenID returns the non-secret stable token lookup identifier.
// TokenID 返回非敏感的稳定令牌查询标识。
func (s BearerSession) TokenID() string { return s.tokenID }

// ExpiresAt returns a copy of the exact persisted expiry instant in UTC.
// ExpiresAt 返回 UTC 的精确持久化过期时刻副本。
func (s BearerSession) ExpiresAt() time.Time { return s.expiresAt }

// AuthenticateBearer resolves the trusted binding from the persisted token row.
// AuthenticateBearer 从持久化令牌行解析可信绑定，不接受调用方预供身份。
func (s *Service) AuthenticateBearer(ctx context.Context, rawToken string) (session BearerSession, err error) {
	started := time.Now()
	var tokenID, observedSessionID string
	defer func() {
		if recovered := recover(); recovered != nil {
			err = Fail(AuthStorageUnavailable)
			s.observe("authenticate_bearer", started, err, tokenID, observedSessionID, AuthAuthenticateOK)
			panic(recovered)
		}
		s.observe("authenticate_bearer", started, err, tokenID, observedSessionID, AuthAuthenticateOK)
	}()
	if s == nil || s.store == nil {
		return BearerSession{}, Fail(AuthStorageUnavailable)
	}
	if ctx == nil {
		return BearerSession{}, Fail(AuthInvalidInput)
	}
	tokenID, digest, err := parseRawToken(rawToken)
	if err != nil {
		return BearerSession{}, err
	}

	var binding SessionBinding
	var expiresAt time.Time
	verify := func(snapshot TokenSnapshot) error {
		if !snapshot.VerifyDigest(digest) {
			return Fail(AuthTokenUnknown)
		}
		observedSessionID = snapshot.Binding().SessionID
		if snapshot.TokenRevokedAt() != nil {
			return Fail(AuthTokenRevoked)
		}
		if snapshot.SessionRevokedAt() != nil {
			return Fail(AuthSessionRevoked)
		}
		now, err := s.currentTime()
		if err != nil {
			return err
		}
		if !now.Before(snapshot.ExpiresAt()) {
			return Fail(AuthTokenExpired)
		}
		binding = snapshot.Binding()
		expiresAt = snapshot.ExpiresAt()
		return nil
	}

	attemptCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	if err = s.store.Authenticate(attemptCtx, tokenID, verify); err != nil {
		return BearerSession{}, redactedError(err)
	}
	session, err = newBearerSession(binding, tokenID, expiresAt)
	if err != nil {
		return BearerSession{}, redactedError(err)
	}
	return session, nil
}
