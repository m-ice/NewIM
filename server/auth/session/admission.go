package session

import (
	"context"
	"time"
)

// DeviceLimits are the trusted, positive account and per-platform limits.
type DeviceLimits struct {
	Account  int
	Platform int
}

// AdmissionRequest combines an application-verified device and trusted limits.
type AdmissionRequest struct {
	UserID   string
	DeviceID string
	Platform string
	TTL      time.Duration
	Limits   DeviceLimits
}

func (r AdmissionRequest) Valid() bool {
	return validIdentifier(r.UserID) && validIdentifier(r.DeviceID) && validPlatform(r.Platform) &&
		r.TTL >= MinTTL && r.TTL <= MaxTTL && r.Limits.Account > 0 && r.Limits.Platform > 0
}

func validPlatform(platform string) bool {
	if len(platform) == 0 || len(platform) > 32 || platform[0] < 'a' || platform[0] > 'z' {
		return false
	}
	for i := 1; i < len(platform); i++ {
		ch := platform[i]
		if !(ch >= 'a' && ch <= 'z') && !(ch >= '0' && ch <= '9') && ch != '_' && ch != '-' {
			return false
		}
	}
	return true
}

// AdmissionResult contains the exact committed binding and one issued token.
type AdmissionResult struct {
	Binding SessionBinding
	Token   IssuedToken
}

// AdmissionStore commits the device, session, token and evictions together.
type AdmissionStore interface {
	Admit(context.Context, AdmissionRequest, func() (IssuedToken, error)) (AdmissionResult, error)
}

const RevocationReasonDeviceLimit = "device_limit"

// SessionRevocationEvent is one committed revocation in a single account stream.
type SessionRevocationEvent struct {
	EventID   int64
	Binding   SessionBinding
	RevokedAt time.Time
	Reason    string
}

// Admit starts or reuses a device session and issues an opaque token after commit.
func (s *Service) Admit(ctx context.Context, request AdmissionRequest) (result AdmissionResult, err error) {
	started := time.Now()
	defer func() {
		s.observe("admit", started, err, result.Token.TokenID(), result.Binding.SessionID, AuthIssueOK)
	}()
	if ctx == nil || !request.Valid() {
		return AdmissionResult{}, Fail(AuthInvalidInput)
	}
	if s == nil {
		return AdmissionResult{}, Fail(AuthStorageUnavailable)
	}
	store, ok := s.store.(AdmissionStore)
	if !ok {
		return AdmissionResult{}, Fail(AuthStorageUnavailable)
	}
	generate := s.tokenGenerator(request.TTL)
	for range maxIssueAttempts {
		attemptCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
		result, err = store.Admit(attemptCtx, request, generate)
		cancel()
		if err == nil {
			return result, nil
		}
		if ErrorCode(err) != AuthTokenCollision {
			return AdmissionResult{}, redactedError(err)
		}
	}
	return AdmissionResult{}, Fail(AuthTokenCollision)
}
