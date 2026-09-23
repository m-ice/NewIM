package protocol

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// WebhookSchemaVersion is the only webhook schema implemented by this package.
	// WebhookSchemaVersion 是本包唯一实现的 Webhook schema 版本。
	WebhookSchemaVersion = 1
	// WebhookSignatureVersion identifies the canonical HMAC input.
	// WebhookSignatureVersion 标识 canonical HMAC 输入版本。
	WebhookSignatureVersion = "1"
	// WebhookTimestampWindow bounds accepted clock skew in both directions.
	// WebhookTimestampWindow 限制双向时钟偏差。
	WebhookTimestampWindow = 5 * time.Minute
	// WebhookMinSecretBytes is the minimum HMAC key length accepted by the reference signer.
	// WebhookMinSecretBytes 是参考签名器接受的最小 HMAC key 长度。
	WebhookMinSecretBytes = 32
	// WebhookMaxHeaderBytes bounds every security header value.
	// WebhookMaxHeaderBytes 限制每个安全 header 值。
	WebhookMaxHeaderBytes = 4096
	// WebhookMaxEnvelopeBytes bounds the canonical webhook body.
	// WebhookMaxEnvelopeBytes 限制 canonical Webhook body。
	WebhookMaxEnvelopeBytes = MaxBytes
	// WebhookMaxEnvelopeDepth follows the persisted protocol nesting limit.
	// WebhookMaxEnvelopeDepth 沿用持久协议嵌套上限。
	WebhookMaxEnvelopeDepth = MaxDepth
)

// WebhookHeaderNames are the exact case-insensitive security headers in v1.
// WebhookHeaderNames 是 v1 固定的不区分大小写安全 header。
var WebhookHeaderNames = struct {
	EventID          string
	DeliveryID       string
	Timestamp        string
	Nonce            string
	KeyID            string
	SignatureVersion string
	Signature        string
}{
	EventID:          "X-NewIM-Event-Id",
	DeliveryID:       "X-NewIM-Delivery-Id",
	Timestamp:        "X-NewIM-Timestamp",
	Nonce:            "X-NewIM-Nonce",
	KeyID:            "X-NewIM-Key-Id",
	SignatureVersion: "X-NewIM-Signature-Version",
	Signature:        "X-NewIM-Signature",
}

// WebhookCode is a stable webhook protocol error code.
// WebhookCode 是稳定的 Webhook 协议错误码。
type WebhookCode string

const (
	WebhookInvalidJSON       WebhookCode = "WEBHOOK_INVALID_JSON"
	WebhookInvalidEnvelope   WebhookCode = "WEBHOOK_INVALID_ENVELOPE"
	WebhookEnvelopeTooLarge  WebhookCode = "WEBHOOK_ENVELOPE_TOO_LARGE"
	WebhookEnvelopeTooDeep   WebhookCode = "WEBHOOK_ENVELOPE_TOO_DEEP"
	WebhookUnsupportedSchema WebhookCode = "WEBHOOK_UNSUPPORTED_SCHEMA"
	WebhookInvalidHeaders    WebhookCode = "WEBHOOK_INVALID_HEADERS"
	WebhookInvalidSignature  WebhookCode = "WEBHOOK_INVALID_SIGNATURE"
	WebhookTimestampMismatch WebhookCode = "WEBHOOK_TIMESTAMP_OUT_OF_WINDOW"
	WebhookReplayDetected    WebhookCode = "WEBHOOK_REPLAY_DETECTED"
	WebhookReplayUnavailable WebhookCode = "WEBHOOK_REPLAY_UNAVAILABLE"
	WebhookCapacityExceeded  WebhookCode = "WEBHOOK_REPLAY_CAPACITY_EXCEEDED"
)

func (e WebhookCode) Error() string { return string(e) }

// WebhookError carries only a stable code and never a secret, body or URL.
// WebhookError 只携带稳定错误码，不包含 secret、body 或 URL。
type WebhookError struct{ Code WebhookCode }

func (e *WebhookError) Error() string { return string(e.Code) }

func webhookFail(code WebhookCode) error { return &WebhookError{Code: code} }

// WebhookErrorCode returns the stable webhook code, if any.
// WebhookErrorCode 返回稳定 Webhook 错误码；未知错误返回空值。
func WebhookErrorCode(err error) WebhookCode {
	if err == nil {
		return ""
	}
	if known, ok := err.(*WebhookError); ok && known != nil {
		return known.Code
	}
	return ""
}

// WebhookEnvelope is the additive webhook-v1 event envelope.
// WebhookEnvelope 是可扩展的 Webhook v1 事件 envelope。
type WebhookEnvelope struct {
	EventID       string          `json:"eventId"`
	EventType     string          `json:"eventType"`
	OccurredAt    string          `json:"occurredAt"`
	SchemaVersion uint32          `json:"schemaVersion"`
	Payload       json.RawMessage `json:"payload"`
}

// WebhookHeaders is the parsed security header set for one HTTP attempt.
// WebhookHeaders 是一次 HTTP 尝试解析出的安全 header 集合。
type WebhookHeaders struct {
	EventID          string
	DeliveryID       string
	Timestamp        string
	Nonce            string
	KeyID            string
	SignatureVersion string
	Signature        string
}

// WebhookReplayGuard atomically reserves a nonce until expiresAt.
// WebhookReplayGuard 原子预留 nonce，直到 expiresAt；实现必须 fail-closed。
type WebhookReplayGuard interface {
	ReserveWebhookNonce(context.Context, string, string, time.Time) (bool, error)
}

// EncodeWebhookEnvelope validates and canonically encodes one webhook envelope.
// EncodeWebhookEnvelope 校验并规范编码一个 Webhook envelope。
func EncodeWebhookEnvelope(envelope WebhookEnvelope) ([]byte, error) {
	eventID, err := quoteWebhookString(envelope.EventID)
	if err != nil {
		return nil, err
	}
	eventType, err := quoteWebhookString(envelope.EventType)
	if err != nil {
		return nil, err
	}
	occurredAt, err := quoteWebhookString(envelope.OccurredAt)
	if err != nil {
		return nil, err
	}
	if envelope.SchemaVersion != WebhookSchemaVersion {
		return nil, webhookFail(WebhookUnsupportedSchema)
	}
	if len(envelope.Payload) == 0 || !json.Valid(envelope.Payload) {
		return nil, webhookFail(WebhookInvalidEnvelope)
	}
	var payload map[string]json.RawMessage
	if json.Unmarshal(envelope.Payload, &payload) != nil || payload == nil {
		return nil, webhookFail(WebhookInvalidEnvelope)
	}
	out := make([]byte, 0, len(envelope.Payload)+256)
	out = append(out, `{"eventId":`...)
	out = append(out, eventID...)
	out = append(out, `,"eventType":`...)
	out = append(out, eventType...)
	out = append(out, `,"occurredAt":`...)
	out = append(out, occurredAt...)
	out = append(out, `,"schemaVersion":`...)
	out = strconv.AppendUint(out, uint64(envelope.SchemaVersion), 10)
	out = append(out, `,"payload":`...)
	out = append(out, envelope.Payload...)
	out = append(out, '}')
	if _, err = DecodeWebhookEnvelope(out); err != nil {
		return nil, err
	}
	return out, nil
}

// DecodeWebhookEnvelope validates additive fields and returns a copied payload.
// DecodeWebhookEnvelope 校验可扩展字段并返回复制后的 payload。
func DecodeWebhookEnvelope(wire []byte) (WebhookEnvelope, error) {
	if len(wire) > WebhookMaxEnvelopeBytes {
		return WebhookEnvelope{}, webhookFail(WebhookEnvelopeTooLarge)
	}
	if err := strictJSONLimits(wire, WebhookMaxEnvelopeBytes, WebhookMaxEnvelopeDepth); err != nil {
		switch err {
		case TooLarge:
			return WebhookEnvelope{}, webhookFail(WebhookEnvelopeTooLarge)
		case TooDeep:
			return WebhookEnvelope{}, webhookFail(WebhookEnvelopeTooDeep)
		default:
			return WebhookEnvelope{}, webhookFail(WebhookInvalidJSON)
		}
	}
	fields, err := objectFields(wire)
	if err != nil {
		return WebhookEnvelope{}, webhookFail(WebhookInvalidEnvelope)
	}
	eventID, err := stringField(fields, "eventId")
	if err != nil || !identifier(eventID, 128, false) {
		return WebhookEnvelope{}, webhookFail(WebhookInvalidEnvelope)
	}
	eventType, err := stringField(fields, "eventType")
	if err != nil || !webhookEventType(eventType) {
		return WebhookEnvelope{}, webhookFail(WebhookInvalidEnvelope)
	}
	occurredAt, err := stringField(fields, "occurredAt")
	if err != nil || !decimal(occurredAt, "9223372036854775807") {
		return WebhookEnvelope{}, webhookFail(WebhookInvalidEnvelope)
	}
	versionRaw, ok := fields["schemaVersion"]
	if !ok || !decimal(string(versionRaw), "2147483647") || string(versionRaw) == "0" {
		return WebhookEnvelope{}, webhookFail(WebhookInvalidEnvelope)
	}
	version, err := strconv.ParseUint(string(versionRaw), 10, 32)
	if err != nil || uint32(version) != WebhookSchemaVersion {
		return WebhookEnvelope{}, webhookFail(WebhookUnsupportedSchema)
	}
	payload, ok := fields["payload"]
	if !ok || !json.Valid(payload) {
		return WebhookEnvelope{}, webhookFail(WebhookInvalidEnvelope)
	}
	if _, err = objectFields(payload); err != nil {
		return WebhookEnvelope{}, webhookFail(WebhookInvalidEnvelope)
	}
	return WebhookEnvelope{
		EventID:       eventID,
		EventType:     eventType,
		OccurredAt:    occurredAt,
		SchemaVersion: uint32(version),
		Payload:       append(json.RawMessage(nil), payload...),
	}, nil
}

// ParseWebhookHeaders rejects missing, duplicate or non-canonical security headers.
// ParseWebhookHeaders 拒绝缺失、重复或非规范安全 header。
func ParseWebhookHeaders(values map[string][]string) (WebhookHeaders, error) {
	required := map[string]string{
		strings.ToLower(WebhookHeaderNames.EventID):          WebhookHeaderNames.EventID,
		strings.ToLower(WebhookHeaderNames.DeliveryID):       WebhookHeaderNames.DeliveryID,
		strings.ToLower(WebhookHeaderNames.Timestamp):        WebhookHeaderNames.Timestamp,
		strings.ToLower(WebhookHeaderNames.Nonce):            WebhookHeaderNames.Nonce,
		strings.ToLower(WebhookHeaderNames.KeyID):            WebhookHeaderNames.KeyID,
		strings.ToLower(WebhookHeaderNames.SignatureVersion): WebhookHeaderNames.SignatureVersion,
		strings.ToLower(WebhookHeaderNames.Signature):        WebhookHeaderNames.Signature,
	}
	found := make(map[string]string, len(required))
	for key, list := range values {
		canonical, ok := required[strings.ToLower(key)]
		if !ok {
			continue
		}
		key = strings.ToLower(canonical)
		if len(list) != 1 || !validWebhookHeaderValue(list[0]) {
			return WebhookHeaders{}, webhookFail(WebhookInvalidHeaders)
		}
		if _, duplicate := found[key]; duplicate {
			return WebhookHeaders{}, webhookFail(WebhookInvalidHeaders)
		}
		found[key] = list[0]
	}
	for key := range required {
		if _, ok := found[key]; !ok {
			return WebhookHeaders{}, webhookFail(WebhookInvalidHeaders)
		}
	}
	headers := WebhookHeaders{
		EventID:          found[strings.ToLower(WebhookHeaderNames.EventID)],
		DeliveryID:       found[strings.ToLower(WebhookHeaderNames.DeliveryID)],
		Timestamp:        found[strings.ToLower(WebhookHeaderNames.Timestamp)],
		Nonce:            found[strings.ToLower(WebhookHeaderNames.Nonce)],
		KeyID:            found[strings.ToLower(WebhookHeaderNames.KeyID)],
		SignatureVersion: found[strings.ToLower(WebhookHeaderNames.SignatureVersion)],
		Signature:        found[strings.ToLower(WebhookHeaderNames.Signature)],
	}
	if err := validateWebhookHeaders(headers); err != nil {
		return WebhookHeaders{}, err
	}
	return headers, nil
}

// CanonicalWebhookSigningBytes returns the exact newline-delimited HMAC input.
// CanonicalWebhookSigningBytes 返回精确的换行分隔 HMAC 输入。
func CanonicalWebhookSigningBytes(headers WebhookHeaders, body []byte) ([]byte, error) {
	if err := validateWebhookSigningHeaders(headers); err != nil {
		return nil, err
	}
	if len(body) > WebhookMaxEnvelopeBytes {
		return nil, webhookFail(WebhookEnvelopeTooLarge)
	}
	digest := sha256.Sum256(body)
	var out bytes.Buffer
	out.Grow(128 + len(headers.KeyID) + len(headers.DeliveryID) + len(headers.EventID) + len(headers.Timestamp) + len(headers.Nonce))
	for _, part := range []string{
		WebhookSignatureVersion,
		headers.KeyID,
		headers.DeliveryID,
		headers.EventID,
		headers.Timestamp,
		headers.Nonce,
		hex.EncodeToString(digest[:]),
	} {
		out.WriteString(part)
		out.WriteByte('\n')
	}
	return out.Bytes(), nil
}

// SignWebhook returns the v1-prefixed base64url HMAC-SHA256 signature.
// SignWebhook 返回 v1 前缀的 base64url HMAC-SHA256 签名。
func SignWebhook(secret []byte, headers WebhookHeaders, body []byte) (string, error) {
	if len(secret) < WebhookMinSecretBytes {
		return "", webhookFail(WebhookInvalidHeaders)
	}
	signing, err := CanonicalWebhookSigningBytes(headers, body)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(signing)
	return "v1=" + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// VerifyWebhookSignature validates the signature, timestamp window and atomic nonce reservation.
// VerifyWebhookSignature 校验签名、时间窗与原子 nonce reservation。
func VerifyWebhookSignature(ctx context.Context, secret []byte, headers WebhookHeaders, body []byte, now time.Time, replay WebhookReplayGuard) error {
	if ctx == nil || now.IsZero() || replay == nil || len(secret) < WebhookMinSecretBytes {
		return webhookFail(WebhookReplayUnavailable)
	}
	if err := validateWebhookHeaders(headers); err != nil {
		return err
	}
	if headers.SignatureVersion != WebhookSignatureVersion || !strings.HasPrefix(headers.Signature, "v1=") {
		return webhookFail(WebhookInvalidSignature)
	}
	provided, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(headers.Signature, "v1="))
	if err != nil || len(provided) != sha256.Size {
		return webhookFail(WebhookInvalidSignature)
	}
	signing, err := CanonicalWebhookSigningBytes(headers, body)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(signing)
	if !hmac.Equal(provided, mac.Sum(nil)) {
		return webhookFail(WebhookInvalidSignature)
	}
	timestamp, err := strconv.ParseInt(headers.Timestamp, 10, 64)
	if err != nil || timestamp < 0 {
		return webhookFail(WebhookInvalidHeaders)
	}
	nowMillis := now.UnixMilli()
	lower := nowMillis - int64(WebhookTimestampWindow/time.Millisecond)
	upper := nowMillis + int64(WebhookTimestampWindow/time.Millisecond)
	if timestamp < lower || timestamp > upper {
		return webhookFail(WebhookTimestampMismatch)
	}
	expiresAt := time.UnixMilli(timestamp).Add(WebhookTimestampWindow)
	accepted, err := replay.ReserveWebhookNonce(ctx, headers.KeyID, headers.Nonce, expiresAt)
	if err != nil {
		if protocolCode := WebhookErrorCode(err); protocolCode != "" {
			return err
		}
		return webhookFail(WebhookReplayUnavailable)
	}
	if !accepted {
		return webhookFail(WebhookReplayDetected)
	}
	return nil
}

// MemoryWebhookReplayGuard is a bounded single-process reference guard.
// MemoryWebhookReplayGuard 是有界的单进程参考 guard；生产必须使用共享、持久实现。
type MemoryWebhookReplayGuard struct {
	mu      sync.Mutex
	limit   int
	entries map[string]time.Time
	now     func() time.Time
}

// NewMemoryWebhookReplayGuard creates a fail-closed in-memory guard.
// NewMemoryWebhookReplayGuard 创建 fail-closed 的内存 guard。
func NewMemoryWebhookReplayGuard(limit int) (*MemoryWebhookReplayGuard, error) {
	if limit <= 0 || limit > 1_000_000 {
		return nil, webhookFail(WebhookInvalidHeaders)
	}
	return &MemoryWebhookReplayGuard{
		limit:   limit,
		entries: make(map[string]time.Time, limit),
		now:     time.Now,
	}, nil
}

// ReserveWebhookNonce atomically reserves one nonce without evicting unexpired entries.
// ReserveWebhookNonce 原子预留 nonce，且不会淘汰尚未过期条目。
func (g *MemoryWebhookReplayGuard) ReserveWebhookNonce(ctx context.Context, keyID, nonce string, expiresAt time.Time) (bool, error) {
	if g == nil || ctx == nil {
		return false, webhookFail(WebhookReplayUnavailable)
	}
	select {
	case <-ctx.Done():
		return false, webhookFail(WebhookReplayUnavailable)
	default:
	}
	now := g.now()
	g.mu.Lock()
	defer g.mu.Unlock()
	for key, expiry := range g.entries {
		if !expiry.After(now) {
			delete(g.entries, key)
		}
	}
	key := keyID + "\n" + nonce
	if expiry, ok := g.entries[key]; ok && expiry.After(now) {
		return false, nil
	}
	if !expiresAt.After(now) {
		return false, webhookFail(WebhookReplayUnavailable)
	}
	if len(g.entries) >= g.limit {
		return false, webhookFail(WebhookCapacityExceeded)
	}
	g.entries[key] = expiresAt
	return true, nil
}

func quoteWebhookString(value string) ([]byte, error) {
	if !validWebhookHeaderValue(value) {
		return nil, webhookFail(WebhookInvalidEnvelope)
	}
	return json.Marshal(value)
}

func webhookEventType(value string) bool {
	if len(value) == 0 || len(value) > 128 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || i > 0 && (c == '_' || c == '.') {
			continue
		}
		return false
	}
	return true
}

func validWebhookHeaderValue(value string) bool {
	if len(value) == 0 || len(value) > WebhookMaxHeaderBytes {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

func validateWebhookHeaders(headers WebhookHeaders) error {
	if err := validateWebhookSigningHeaders(headers); err != nil {
		return err
	}
	if headers.Signature == "" || !strings.HasPrefix(headers.Signature, "v1=") {
		return webhookFail(WebhookInvalidSignature)
	}
	return nil
}

func validateWebhookSigningHeaders(headers WebhookHeaders) error {
	if !identifier(headers.EventID, 128, false) || !identifier(headers.DeliveryID, 128, false) ||
		!identifier(headers.KeyID, 64, false) || !decimal(headers.Timestamp, "9223372036854775807") ||
		headers.SignatureVersion != WebhookSignatureVersion || !validWebhookHeaderValue(headers.Nonce) ||
		headers.Signature != "" && !validWebhookHeaderValue(headers.Signature) {
		return webhookFail(WebhookInvalidHeaders)
	}
	nounceBytes, err := base64.RawURLEncoding.DecodeString(headers.Nonce)
	if err != nil || len(nounceBytes) != 16 || base64.RawURLEncoding.EncodeToString(nounceBytes) != headers.Nonce {
		return webhookFail(WebhookInvalidHeaders)
	}
	return nil
}
