package session

import (
	"context"
	"time"
)

const (
	RevocationReasonPasswordChanged      = "password_changed"
	RevocationReasonSignedOutAll         = "signed_out_all"
	RevocationReasonAccountBlocked       = "account_blocked"
	AuthRevocationConflict          Code = "AUTH_REVOCATION_CONFLICT"
)

// AccountRevocationRequest identifies an action verified by the trusted application.
// AccountRevocationRequest 表示由可信应用核验并可重试的账号操作。
type AccountRevocationRequest struct {
	UserID    string
	RequestID string
	Reason    string
}

func (r AccountRevocationRequest) Valid() bool {
	if !validIdentifier(r.UserID) || !validIdentifier(r.RequestID) {
		return false
	}
	switch r.Reason {
	case RevocationReasonPasswordChanged, RevocationReasonSignedOutAll, RevocationReasonAccountBlocked:
		return true
	default:
		return false
	}
}

// AccountRevocationResult is the original committed count, including on replay.
// AccountRevocationResult 保存首次提交的撤销会话数，重试返回相同值。
type AccountRevocationResult struct {
	RevokedSessions int64
}

type AccountRevocationStore interface {
	RevokeAccount(context.Context, AccountRevocationRequest, time.Time) (AccountRevocationResult, error)
}

// RevokeAccount revokes the account's sessions for one verified action.
// RevokeAccount 执行一次已核验的账号级会话撤销。
func (s *Service) RevokeAccount(ctx context.Context, request AccountRevocationRequest) (result AccountRevocationResult, err error) {
	started := time.Now()
	defer func() {
		code := AuthRevokeOK
		if result.RevokedSessions == 0 {
			code = AuthRevokeNoop
		}
		s.observe("revoke_account", started, err, "", "", code)
	}()
	if ctx == nil || !request.Valid() {
		return AccountRevocationResult{}, Fail(AuthInvalidInput)
	}
	if s == nil {
		return AccountRevocationResult{}, Fail(AuthStorageUnavailable)
	}
	store, ok := s.store.(AccountRevocationStore)
	if !ok {
		return AccountRevocationResult{}, Fail(AuthStorageUnavailable)
	}
	now, err := s.currentTime()
	if err != nil {
		return AccountRevocationResult{}, err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	result, err = store.RevokeAccount(attemptCtx, request, now)
	if err != nil {
		return AccountRevocationResult{}, redactedError(err)
	}
	if result.RevokedSessions < 0 {
		return AccountRevocationResult{}, Fail(AuthStorageUnavailable)
	}
	return result, nil
}
