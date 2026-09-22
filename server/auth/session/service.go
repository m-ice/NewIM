package session

import (
	"context"
	"crypto/rand"
	"io"
	"time"
)

const defaultRequestTimeout = 5 * time.Second

// Config is trusted service configuration, never request input.
// Config 仅接受可信服务配置，不接受客户端或请求内容。
type Config struct {
	Clock          Clock
	Observer       Observer
	Entropy        io.Reader
	Policy         LoginPolicy
	RequestTimeout time.Duration
}

// Service coordinates token lifecycle operations without owning transport or SQL.
// Service 编排令牌生命周期，不拥有传输层或 SQL 连接。
type Service struct {
	store          Store
	clock          Clock
	observer       Observer
	entropy        io.Reader
	policy         LoginPolicy
	requestTimeout time.Duration
}

// NewService constructs a completed service; clock precedes observer validation.
// NewService 构造完整服务；时钟校验先于观测器校验。
func NewService(store Store, config Config) (*Service, error) {
	if config.Clock == nil {
		return nil, Fail(AuthClockRequired)
	}
	if config.Observer == nil {
		return nil, Fail(AuthObserverRequired)
	}
	if store == nil {
		return nil, Fail(AuthInvalidInput)
	}
	timeout := config.RequestTimeout
	if timeout == 0 {
		timeout = defaultRequestTimeout
	}
	if timeout < time.Millisecond || timeout > defaultRequestTimeout {
		return nil, Fail(AuthInvalidInput)
	}
	entropy := config.Entropy
	if entropy == nil {
		entropy = rand.Reader
	}
	return &Service{
		store: store, clock: config.Clock, observer: config.Observer,
		entropy: entropy, policy: config.Policy, requestTimeout: timeout,
	}, nil
}

// Issue generates, inserts and commits one opaque token before returning it.
// Issue 在返回前生成、插入并提交一个不透明令牌；提交失败绝不返回令牌。
func (s *Service) Issue(ctx context.Context, request IssueRequest) (issued IssuedToken, err error) {
	started := time.Now()
	var tokenID, observedSessionID string
	defer func() {
		s.observe("issue", started, err, tokenID, observedSessionID, AuthIssueOK)
	}()
	if s == nil || s.store == nil {
		return IssuedToken{}, Fail(AuthStorageUnavailable)
	}
	if ctx == nil || !request.Binding.Valid() || request.TTL < MinTTL || request.TTL > MaxTTL {
		return IssuedToken{}, Fail(AuthInvalidInput)
	}
	observedSessionID = request.Binding.SessionID
	generate := func() (IssuedToken, error) {
		now, err := s.currentTime()
		if err != nil {
			return IssuedToken{}, err
		}
		return generateIssuedToken(s.entropy, now, request.TTL)
	}
	for attempt := 0; attempt < maxIssueAttempts; attempt++ {
		issued, err = s.issueAttempt(ctx, request.Binding, generate)
		if err == nil {
			tokenID = issued.TokenID()
			return issued, nil
		}
		if ErrorCode(err) != AuthTokenCollision {
			return IssuedToken{}, redactedError(err)
		}
	}
	return IssuedToken{}, Fail(AuthTokenCollision)
}

func (s *Service) issueAttempt(ctx context.Context, binding SessionBinding, generate func() (IssuedToken, error)) (IssuedToken, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	issued, err := s.store.Issue(attemptCtx, binding, generate)
	if err != nil {
		return IssuedToken{}, redactedError(err)
	}
	return issued, nil
}

// Authenticate verifies the token before trusting any persisted identity.
// Authenticate 先验证令牌，再信任任何持久化身份；失败关闭且不返回部分身份。
func (s *Service) Authenticate(ctx context.Context, request AuthenticateRequest) (identity ConnectionIdentity, err error) {
	started := time.Now()
	var tokenID, observedSessionID string
	var digest [32]byte
	defer func() {
		s.observe("authenticate", started, err, tokenID, observedSessionID, AuthAuthenticateOK)
	}()
	if s == nil || s.store == nil {
		return ConnectionIdentity{}, Fail(AuthStorageUnavailable)
	}
	if ctx == nil || !request.Binding.Valid() || !validIdentifier(request.ConnectionID) {
		return ConnectionIdentity{}, Fail(AuthInvalidInput)
	}
	observedSessionID = request.Binding.SessionID
	tokenID, digest, err = parseRawToken(request.Token)
	if err != nil {
		return ConnectionIdentity{}, err
	}
	verify := func(snapshot TokenSnapshot) error {
		if !snapshot.VerifyDigest(digest) {
			return Fail(AuthTokenUnknown)
		}
		if snapshot.Binding() != request.Binding {
			return Fail(AuthForbidden)
		}
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
		return nil
	}
	attemptCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	if err = s.store.Authenticate(attemptCtx, tokenID, verify); err != nil {
		return ConnectionIdentity{}, redactedError(err)
	}
	identity, err = NewConnectionIdentity(request.Binding.UserID, request.Binding.DeviceID, request.Binding.SessionID, request.ConnectionID, tokenID)
	if err != nil {
		return ConnectionIdentity{}, redactedError(err)
	}
	return identity, nil
}

// RevokeSession durably revokes one owned session and all its active tokens.
// RevokeSession 持久撤销一个同用户会话及其全部活跃令牌。
func (s *Service) RevokeSession(ctx context.Context, binding SessionBinding) (outcome RevocationOutcome, err error) {
	started := time.Now()
	observedSessionID := ""
	defer func() {
		s.observe("revoke_session", started, err, "", observedSessionID, revocationObservationCode(outcome))
	}()
	if s == nil || s.store == nil {
		return "", Fail(AuthStorageUnavailable)
	}
	if ctx == nil || !binding.Valid() {
		return "", Fail(AuthInvalidInput)
	}
	observedSessionID = binding.SessionID
	now, err := s.currentTime()
	if err != nil {
		return "", err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	outcome, err = s.store.RevokeSession(attemptCtx, binding, now)
	if err != nil {
		return "", redactedError(err)
	}
	if outcome != RevokeOK && outcome != RevokeNoop {
		return "", Fail(AuthStorageUnavailable)
	}
	return outcome, nil
}

// RevokeToken durably revokes one token owned by the trusted user.
// RevokeToken 持久撤销可信用户拥有的一个令牌。
func (s *Service) RevokeToken(ctx context.Context, userID, tokenID string) (outcome RevocationOutcome, err error) {
	started := time.Now()
	observedTokenID := ""
	defer func() {
		s.observe("revoke_token", started, err, observedTokenID, "", revocationObservationCode(outcome))
	}()
	if s == nil || s.store == nil {
		return "", Fail(AuthStorageUnavailable)
	}
	if ctx == nil || !validIdentifier(userID) || !validTokenID(tokenID) {
		return "", Fail(AuthInvalidInput)
	}
	observedTokenID = tokenID
	now, err := s.currentTime()
	if err != nil {
		return "", err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	outcome, err = s.store.RevokeToken(attemptCtx, userID, tokenID, now)
	if err != nil {
		return "", redactedError(err)
	}
	if outcome != RevokeOK && outcome != RevokeNoop {
		return "", Fail(AuthStorageUnavailable)
	}
	return outcome, nil
}

// revocationObservationCode keeps the observer aligned with the returned outcome.
// revocationObservationCode 使观测结果与返回的撤销结果保持一致。
func revocationObservationCode(outcome RevocationOutcome) Code {
	if outcome == RevokeNoop {
		return AuthRevokeNoop
	}
	return AuthRevokeOK
}

// PlanLogin requires an injected policy and returns only validated trusted IDs.
// PlanLogin 必须使用注入策略，仅返回已校验的可信标识，不执行踢除。
func (s *Service) PlanLogin(ctx context.Context, request LoginRequest) (plan LoginPlan, err error) {
	started := time.Now()
	observedSessionID := ""
	defer func() {
		s.observe("plan_login", started, err, "", observedSessionID, AuthPlanLoginOK)
	}()
	if s == nil || s.policy == nil {
		return LoginPlan{}, Fail(AuthPolicyRequired)
	}
	if s.store == nil {
		return LoginPlan{}, Fail(AuthStorageUnavailable)
	}
	if ctx == nil || !request.Binding.Valid() {
		return LoginPlan{}, Fail(AuthInvalidInput)
	}
	observedSessionID = request.Binding.SessionID
	policyCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	decision, err := s.policy.Decide(policyCtx, request)
	if err != nil {
		return LoginPlan{}, Fail(AuthPolicyInvalid)
	}
	if len(decision.KickTargets) > 100 {
		return LoginPlan{}, Fail(AuthPolicyInvalid)
	}
	targets := make([]KickTarget, 0, len(decision.KickTargets))
	seen := make(map[KickTarget]struct{}, len(decision.KickTargets))
	for _, target := range decision.KickTargets {
		if !validIdentifier(target.UserID) || !validIdentifier(target.DeviceID) || !validIdentifier(target.SessionID) {
			return LoginPlan{}, Fail(AuthPolicyInvalid)
		}
		if target.UserID != request.Binding.UserID {
			return LoginPlan{}, Fail(AuthForbidden)
		}
		if _, exists := seen[target]; exists {
			return LoginPlan{}, Fail(AuthPolicyInvalid)
		}
		seen[target] = struct{}{}
		snapshot, lookupErr := s.lookupSession(ctx, target.SessionID)
		if lookupErr != nil {
			if ErrorCode(lookupErr) == AuthStorageUnavailable {
				return LoginPlan{}, Fail(AuthStorageUnavailable)
			}
			return LoginPlan{}, Fail(AuthPolicyInvalid)
		}
		sessionBinding := snapshot.Binding()
		if sessionBinding.UserID != target.UserID || sessionBinding.DeviceID != target.DeviceID {
			return LoginPlan{}, Fail(AuthForbidden)
		}
		targets = append(targets, target)
	}
	current, err := s.lookupSession(ctx, request.Binding.SessionID)
	if err != nil {
		return LoginPlan{}, redactedError(err)
	}
	currentBinding := current.Binding()
	if currentBinding.UserID != request.Binding.UserID || currentBinding.DeviceID != request.Binding.DeviceID {
		return LoginPlan{}, Fail(AuthForbidden)
	}
	if current.RevokedAt() != nil {
		return LoginPlan{}, Fail(AuthSessionRevoked)
	}
	return LoginPlan{Binding: request.Binding, KickTargets: targets}, nil
}

func (s *Service) lookupSession(ctx context.Context, sessionID string) (SessionSnapshot, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	snapshot, err := s.store.LookupSession(lookupCtx, sessionID)
	if err != nil {
		return SessionSnapshot{}, redactedError(err)
	}
	return snapshot, nil
}

func (s *Service) currentTime() (time.Time, error) {
	if s == nil || s.clock == nil {
		return time.Time{}, Fail(AuthStorageUnavailable)
	}
	now := s.clock.Now()
	if now.IsZero() {
		return time.Time{}, Fail(AuthStorageUnavailable)
	}
	return now.UTC(), nil
}

func (s *Service) observe(operation string, started time.Time, err error, tokenID, sessionID string, success Code) {
	if s == nil || s.observer == nil {
		return
	}
	code := ErrorCode(err)
	if code == "" {
		code = success
	}
	observation := Observation{
		Operation: operation, Code: code, TokenID: tokenID, SessionID: sessionID,
		Elapsed: time.Since(started),
	}
	defer func() { _ = recover() }()
	s.observer.Observe(observation)
}
