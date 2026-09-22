package media

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"io"
	"time"

	"github.com/m-ice/NewIM/server/auth/session"
)

const (
	defaultRequestTimeout = 5 * time.Second
	maxGrantAttempts      = 8
)

// Config is trusted service configuration, never request input.
// Config 仅接受可信服务配置，不接受请求内容。
type Config struct {
	Objects        ObjectStore
	Signer         Signer
	Clock          Clock
	Entropy        io.Reader
	Observer       Observer
	RequestTimeout time.Duration
}

// Service coordinates upload grants, completion, download authorization and
// message validation without owning transport or SQL.
// Service 编排上传凭据、完成、下载授权与消息校验，不拥有传输层或 SQL。
type Service struct {
	store          Store
	objects        ObjectStore
	signer         Signer
	clock          Clock
	entropy        io.Reader
	observer       Observer
	requestTimeout time.Duration
}

// NewService constructs the media application service.
// NewService 构造媒体应用服务。
func NewService(store Store, config Config) (*Service, error) {
	if store == nil || config.Clock == nil {
		return nil, Fail(MediaInvalidInput)
	}
	timeout := config.RequestTimeout
	if timeout == 0 {
		timeout = defaultRequestTimeout
	}
	if timeout < time.Millisecond || timeout > defaultRequestTimeout {
		return nil, Fail(MediaInvalidInput)
	}
	entropy := config.Entropy
	if entropy == nil {
		entropy = rand.Reader
	}
	return &Service{
		store: store, objects: config.Objects, signer: config.Signer, clock: config.Clock,
		entropy: entropy, observer: config.Observer, requestTimeout: timeout,
	}, nil
}

// BeginUploadRequest is trusted host input; it cannot select identity or mediaKey.
// BeginUploadRequest 是可信宿主输入；不能选择身份或 mediaKey。
type BeginUploadRequest struct {
	ConversationID string
	Kind           string
	ContentType    string
	Size           int64
	SHA256         string
	Expiry         time.Duration
}

// BeginUpload authorizes first and only then creates a grant.
// BeginUpload 先授权，之后才创建凭据。
func (s *Service) BeginUpload(ctx context.Context, identity session.ConnectionIdentity, request BeginUploadRequest) (result UploadGrant, err error) {
	started := time.Now()
	defer func() { s.observe("begin_upload", started, err) }()
	if s == nil || s.store == nil || s.clock == nil || ctx == nil ||
		!ValidateIdentity(identity) || !validIdentifier(request.ConversationID) {
		return UploadGrant{}, Fail(MediaInvalidInput)
	}
	digest, ok := ParseSHA256(request.SHA256)
	if !ok {
		return UploadGrant{}, Fail(MediaInvalidInput)
	}
	ttl := request.Expiry
	if ttl == 0 {
		ttl = time.Duration(DefaultUploadTTL) * time.Second
	}
	if ttl <= 0 || ttl > time.Duration(MaxUploadTTL)*time.Second {
		return UploadGrant{}, Fail(MediaInvalidInput)
	}
	intent := Intent{Kind: request.Kind, ContentType: request.ContentType, Size: request.Size, SHA256: digest}
	if !ValidIntent(intent) {
		return UploadGrant{}, Fail(MediaInvalidInput)
	}
	now := s.currentTime()
	if now.IsZero() {
		return UploadGrant{}, Fail(MediaStorageUnavailable)
	}
	expiresAt := now.Add(ttl).UTC()
	var rawToken, mediaKey string
	create := func() (Grant, error) {
		grant, raw, createErr := s.generateGrant(identity, request.ConversationID, intent, expiresAt)
		if createErr != nil {
			return Grant{}, createErr
		}
		rawToken = raw
		mediaKey = grant.MediaKey
		return grant, nil
	}
	for attempt := 0; attempt < maxGrantAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
		storeErr := s.store.BeginUpload(attemptCtx, now, identity, request.ConversationID, create)
		cancel()
		if storeErr == nil {
			if rawToken == "" {
				return UploadGrant{}, Fail(MediaStorageUnavailable)
			}
			if mediaKey == "" {
				return UploadGrant{}, Fail(MediaStorageUnavailable)
			}
			return UploadGrant{RawToken: rawToken, MediaKey: mediaKey, ExpiresAt: expiresAt}, nil
		}
		if ErrorCode(storeErr) == MediaGrantCollision {
			rawToken = ""
			continue
		}
		return UploadGrant{}, redactedError(storeErr)
	}
	return UploadGrant{}, Fail(MediaGrantCollision)
}

// CompleteUpload verifies the complete grant scope before expiry and supports
// exact ready replay after expiry.
// CompleteUpload 在过期检查前校验完整凭据范围，并支持过期后的精确就绪重放。
func (s *Service) CompleteUpload(ctx context.Context, identity session.ConnectionIdentity, rawToken string, content io.Reader) (Grant, error) {
	return s.completeUpload(ctx, identity, rawToken, nil, content)
}

// CompleteUploadIntent additionally requires the caller's original media intent
// to equal the grant binding before expiry or ready replay is evaluated.
// CompleteUploadIntent 还要求调用方原始媒体意向与凭据绑定一致，之后才评估过期或就绪重放。
func (s *Service) CompleteUploadIntent(ctx context.Context, identity session.ConnectionIdentity, rawToken string, intent Intent, content io.Reader) (Grant, error) {
	if !ValidIntent(intent) {
		return Grant{}, Fail(MediaInvalidInput)
	}
	return s.completeUpload(ctx, identity, rawToken, &intent, content)
}

func (s *Service) completeUpload(ctx context.Context, identity session.ConnectionIdentity, rawToken string, expectedIntent *Intent, content io.Reader) (asset Grant, err error) {
	started := time.Now()
	defer func() { s.observe("complete_upload", started, err) }()
	if s == nil || s.store == nil || s.objects == nil || s.clock == nil || ctx == nil ||
		!ValidateIdentity(identity) || content == nil {
		return Grant{}, Fail(MediaInvalidInput)
	}
	grantID, suppliedDigest, ok := parseRawGrant(rawToken)
	if !ok {
		return Grant{}, Fail(MediaInvalidToken)
	}
	loadCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	grant, loadErr := s.store.LoadGrant(loadCtx, grantID)
	cancel()
	if loadErr != nil {
		return Grant{}, redactedError(loadErr)
	}
	if subtle.ConstantTimeCompare(grant.GrantDigest[:], suppliedDigest[:]) != 1 {
		return Grant{}, Fail(MediaNotFound)
	}
	if !sameIdentity(grant, identity) {
		return Grant{}, Fail(MediaUnauthorized)
	}
	if expectedIntent != nil && !sameIntent(grant, *expectedIntent) {
		return Grant{}, Fail(MediaUnauthorized)
	}
	if !ValidGrant(grant) {
		return Grant{}, Fail(MediaStorageUnavailable)
	}
	now := s.currentTime()
	if now.IsZero() {
		return Grant{}, Fail(MediaStorageUnavailable)
	}
	authCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	authErr := s.store.Authorize(authCtx, now, identity, grant.ConversationID)
	cancel()
	if authErr != nil {
		return Grant{}, redactedError(authErr)
	}
	if grant.State != StatePending && grant.State != StateReady {
		return Grant{}, Fail(MediaStorageUnavailable)
	}
	if grant.State == StatePending && !now.Before(grant.ExpiresAt) {
		return Grant{}, Fail(MediaExpired)
	}
	expected := ObjectInfo{Size: grant.DeclaredSize, SHA256: grant.SHA256}
	putCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	actual, putErr := s.objects.PutImmutable(putCtx, grant.MediaKey, content, expected)
	cancel()
	if putErr != nil {
		return Grant{}, redactedError(putErr)
	}
	if actual != expected {
		return Grant{}, Fail(MediaInvalidInput)
	}
	if grant.State == StateReady {
		return grant, nil
	}
	completeCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	ready, completeErr := s.store.CompleteReady(completeCtx, now, identity, grant)
	cancel()
	if completeErr != nil {
		return Grant{}, redactedError(completeErr)
	}
	if !ValidGrant(ready) || ready.State != StateReady {
		return Grant{}, Fail(MediaStorageUnavailable)
	}
	return ready, nil
}

// ResolvePrivateDownload authorizes before signing and never persists the URL.
// ResolvePrivateDownload 在签名前授权，绝不持久化 URL。
func (s *Service) ResolvePrivateDownload(ctx context.Context, identity session.ConnectionIdentity, mediaKey string, ttl time.Duration) (url string, err error) {
	started := time.Now()
	defer func() { s.observe("resolve_private_download", started, err) }()
	if s == nil || s.store == nil || s.signer == nil || s.clock == nil || ctx == nil ||
		!ValidateIdentity(identity) || !validMediaKey(mediaKey) {
		return "", Fail(MediaInvalidInput)
	}
	if ttl == 0 {
		ttl = time.Duration(DefaultDownloadTTL) * time.Second
	}
	if ttl <= 0 || ttl > time.Duration(MaxDownloadTTL)*time.Second {
		return "", Fail(MediaInvalidInput)
	}
	loadCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	asset, loadErr := s.store.LoadReadyAsset(loadCtx, mediaKey)
	cancel()
	if loadErr != nil {
		return "", redactedError(loadErr)
	}
	// Both the caller and the stored asset must carry all five trusted identity fields.
	// 调用方与持久资产都必须携带全部五个可信身份字段。
	if !ValidateIdentity(identity) || !ValidGrant(asset) || asset.State != StateReady {
		return "", Fail(MediaNotFound)
	}
	now := s.currentTime()
	if now.IsZero() {
		return "", Fail(MediaStorageUnavailable)
	}
	authCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	authErr := s.store.Authorize(authCtx, now, identity, asset.ConversationID)
	cancel()
	if authErr != nil {
		return "", redactedError(authErr)
	}
	signCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	url, signErr := s.signer.Sign(signCtx, asset.MediaKey, now.Add(ttl).UTC())
	cancel()
	if signErr != nil || url == "" || len(url) > 8192 {
		return "", Fail(MediaStorageUnavailable)
	}
	return url, nil
}

// ValidateForSend is the narrow message-service authorization seam.
// ValidateForSend 是消息服务使用的窄授权边界。
func (s *Service) ValidateForSend(ctx context.Context, identity session.ConnectionIdentity, conversationID string, metadata Metadata) error {
	started := time.Now()
	var err error
	defer func() { s.observe("validate_for_send", started, err) }()
	if s == nil || s.store == nil || s.clock == nil || ctx == nil ||
		!ValidateIdentity(identity) || !validIdentifier(conversationID) || !ValidMetadata(metadata) {
		err = Fail(MediaInvalidInput)
		return err
	}
	now := s.currentTime()
	if now.IsZero() {
		err = Fail(MediaStorageUnavailable)
		return err
	}
	validateCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	err = s.store.ValidateReadyForSend(validateCtx, now, identity, conversationID, metadata)
	cancel()
	return redactedError(err)
}

func (s *Service) generateGrant(identity session.ConnectionIdentity, conversationID string, intent Intent, expiresAt time.Time) (Grant, string, error) {
	grantIDBytes := make([]byte, 16)
	if _, err := io.ReadFull(s.entropy, grantIDBytes); err != nil {
		return Grant{}, "", Fail(MediaStorageUnavailable)
	}
	secretBytes := make([]byte, 32)
	if _, err := io.ReadFull(s.entropy, secretBytes); err != nil {
		return Grant{}, "", Fail(MediaStorageUnavailable)
	}
	mediaKeyBytes := make([]byte, 16)
	if _, err := io.ReadFull(s.entropy, mediaKeyBytes); err != nil {
		return Grant{}, "", Fail(MediaStorageUnavailable)
	}
	grantID := hex.EncodeToString(grantIDBytes)
	secret := base64.RawURLEncoding.EncodeToString(secretBytes)
	rawToken := grantID + "." + secret
	if !validGrantID(grantID) || len(secret) != 43 {
		return Grant{}, "", Fail(MediaStorageUnavailable)
	}
	digest := sha256.Sum256([]byte(rawToken))
	grant := Grant{
		MediaKey:       hex.EncodeToString(mediaKeyBytes),
		OwnerUserID:    identity.UserID(),
		DeviceID:       identity.DeviceID(),
		SessionID:      identity.SessionID(),
		ConnectionID:   identity.ConnectionID(),
		TokenID:        identity.TokenID(),
		ConversationID: conversationID,
		Kind:           intent.Kind,
		ContentType:    intent.ContentType,
		DeclaredSize:   intent.Size,
		SHA256:         intent.SHA256,
		GrantID:        grantID,
		GrantDigest:    digest,
		ExpiresAt:      expiresAt,
		State:          StatePending,
	}
	if !ValidGrant(grant) {
		return Grant{}, "", Fail(MediaStorageUnavailable)
	}
	return grant, rawToken, nil
}

func parseRawGrant(raw string) (string, [32]byte, bool) {
	var digest [32]byte
	if raw == "" {
		return "", digest, false
	}
	dot := -1
	for i := 0; i < len(raw); i++ {
		if raw[i] == '.' {
			if dot >= 0 {
				return "", digest, false
			}
			dot = i
		}
	}
	if dot != 32 || len(raw) != 76 {
		return "", digest, false
	}
	grantID := raw[:dot]
	secret := raw[dot+1:]
	if !validGrantID(grantID) || len(secret) != 43 {
		return "", digest, false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(secret)
	if err != nil || len(decoded) != 32 {
		return "", digest, false
	}
	for i := 0; i < len(secret); i++ {
		ch := secret[i]
		if !((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return "", digest, false
		}
	}
	digest = sha256.Sum256([]byte(raw))
	return grantID, digest, true
}

func sameIntent(grant Grant, intent Intent) bool {
	return grant.Kind == intent.Kind && grant.ContentType == intent.ContentType &&
		grant.DeclaredSize == intent.Size && grant.SHA256 == intent.SHA256
}

func sameIdentity(grant Grant, identity session.ConnectionIdentity) bool {
	return grant.OwnerUserID == identity.UserID() &&
		grant.DeviceID == identity.DeviceID() &&
		grant.SessionID == identity.SessionID() &&
		grant.ConnectionID == identity.ConnectionID() &&
		grant.TokenID == identity.TokenID()
}

func (s *Service) currentTime() time.Time {
	if s == nil || s.clock == nil {
		return time.Time{}
	}
	return s.clock.Now().UTC()
}

func redactedError(err error) error {
	if err == nil {
		return nil
	}
	code := ErrorCode(err)
	if code == "" {
		code = MediaStorageUnavailable
	}
	return Fail(code)
}

func (s *Service) observe(operation string, started time.Time, err error) {
	if s == nil || s.observer == nil {
		return
	}
	code := ErrorCode(err)
	if code == "" {
		code = MediaInvalidInput
	}
	observation := Observation{Operation: operation, Code: code, Elapsed: time.Since(started)}
	defer func() { _ = recover() }()
	s.observer.Observe(observation)
}
