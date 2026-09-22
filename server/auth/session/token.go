package session

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"io"
	"time"
)

// Token format and TTL bounds are frozen by ADR 0008.
// 令牌格式与 TTL 边界由 ADR 0008 冻结。
const (
	TokenPrefix      = "n1_"
	tokenIDHexBytes  = 16
	tokenSecretBytes = 32
	TokenIDHexLen    = tokenIDHexBytes * 2
	TokenSecretLen   = 43
	RawTokenLen      = len(TokenPrefix) + TokenIDHexLen + 1 + TokenSecretLen
	MinTTL           = time.Second
	MaxTTL           = 24 * time.Hour
	maxIssueAttempts = 3
)

// IssuedToken contains a raw bearer value that must be returned only to the
// trusted caller and must never be logged or persisted.
// IssuedToken 包含仅可返回给可信调用方的原始令牌，不得记录或持久化。
type IssuedToken struct {
	tokenID   string
	rawToken  string
	digest    [sha256.Size]byte
	issuedAt  time.Time
	expiresAt time.Time
}

// TokenID returns the non-secret 128-bit lookup identifier.
// TokenID 返回非敏感的 128 位查询标识。
func (t IssuedToken) TokenID() string { return t.tokenID }

// RawToken returns the raw bearer value; callers must not log or persist it.
// RawToken 返回原始令牌；调用方不得记录或持久化。
func (t IssuedToken) RawToken() string { return t.rawToken }

// Digest returns a copy of the stored token digest.
// Digest 返回存储摘要的副本。
func (t IssuedToken) Digest() [sha256.Size]byte { return t.digest }

// IssuedAt returns the service time used as the token issue instant.
// IssuedAt 返回作为令牌签发时刻的服务时间。
func (t IssuedToken) IssuedAt() time.Time { return t.issuedAt }

// ExpiresAt returns the exact persisted expiry instant.
// ExpiresAt 返回精确的持久化过期时刻。
func (t IssuedToken) ExpiresAt() time.Time { return t.expiresAt }

// String redacts the raw value for accidental formatting.
// String 在意外格式化时脱敏原始值。
func (t IssuedToken) String() string { return "AUTH_TOKEN_REDACTED" }

// GoString redacts the raw value in %#v formatting.
// GoString 在 %#v 格式化时脱敏原始值。
func (t IssuedToken) GoString() string { return t.String() }

func newIssuedToken(tokenID, rawToken string, digest [sha256.Size]byte, issuedAt, expiresAt time.Time) (IssuedToken, error) {
	if !validTokenID(tokenID) || len(rawToken) != RawTokenLen || issuedAt.IsZero() || expiresAt.IsZero() || !expiresAt.After(issuedAt) {
		return IssuedToken{}, Fail(AuthInvalidInput)
	}
	return IssuedToken{tokenID: tokenID, rawToken: rawToken, digest: digest, issuedAt: issuedAt, expiresAt: expiresAt}, nil
}

func generateIssuedToken(entropy io.Reader, now time.Time, ttl time.Duration) (IssuedToken, error) {
	if entropy == nil {
		return IssuedToken{}, Fail(AuthEntropyUnavailable)
	}
	if now.IsZero() || ttl < MinTTL || ttl > MaxTTL {
		return IssuedToken{}, Fail(AuthInvalidInput)
	}
	var tokenIDBytes [tokenIDHexBytes]byte
	var secretBytes [tokenSecretBytes]byte
	if _, err := io.ReadFull(entropy, tokenIDBytes[:]); err != nil {
		return IssuedToken{}, Fail(AuthEntropyUnavailable)
	}
	if _, err := io.ReadFull(entropy, secretBytes[:]); err != nil {
		return IssuedToken{}, Fail(AuthEntropyUnavailable)
	}
	tokenID := hex.EncodeToString(tokenIDBytes[:])
	secret := base64.RawURLEncoding.EncodeToString(secretBytes[:])
	raw := TokenPrefix + tokenID + "_" + secret
	digest := sha256.Sum256([]byte(raw))
	clear(tokenIDBytes[:])
	clear(secretBytes[:])
	issuedAt := now.UTC().Truncate(time.Microsecond)
	expiresAt := issuedAt.Add(ttl)
	if !expiresAt.After(issuedAt) {
		return IssuedToken{}, Fail(AuthInvalidInput)
	}
	return newIssuedToken(tokenID, raw, digest, issuedAt, expiresAt)
}

// parseRawToken enforces the exact token grammar before any storage lookup.
// parseRawToken 在任何存储查询前严格校验令牌语法。
func parseRawToken(raw string) (string, [sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if len(raw) != RawTokenLen || raw[:len(TokenPrefix)] != TokenPrefix || raw[len(TokenPrefix)+TokenIDHexLen] != '_' {
		return "", digest, Fail(AuthTokenMalformed)
	}
	tokenID := raw[len(TokenPrefix) : len(TokenPrefix)+TokenIDHexLen]
	if !validTokenID(tokenID) {
		return tokenID, digest, Fail(AuthTokenMalformed)
	}
	secret := raw[len(TokenPrefix)+TokenIDHexLen+1:]
	decoded, err := base64.RawURLEncoding.DecodeString(secret)
	if err != nil || len(decoded) != tokenSecretBytes || base64.RawURLEncoding.EncodeToString(decoded) != secret {
		return tokenID, digest, Fail(AuthTokenMalformed)
	}
	digest = sha256.Sum256([]byte(raw))
	return tokenID, digest, nil
}

func validTokenID(value string) bool {
	if len(value) != TokenIDHexLen {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return false
		}
	}
	return true
}

func validIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if !((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}

func constantTimeEqual(left, right [sha256.Size]byte) bool {
	return subtle.ConstantTimeCompare(left[:], right[:]) == 1
}

func redactedError(err error) error {
	if err == nil {
		return nil
	}
	code := ErrorCode(err)
	if code == "" {
		code = AuthStorageUnavailable
	}
	return Fail(code)
}
