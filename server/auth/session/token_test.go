package session

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTokenFormatAndDigest(t *testing.T) {
	entropy := bytes.NewReader(make([]byte, tokenIDHexBytes+tokenSecretBytes))
	now := time.Unix(1_800_000_000, 0).UTC()
	token, err := generateIssuedToken(entropy, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(token.RawToken()) != RawTokenLen || !strings.HasPrefix(token.RawToken(), TokenPrefix) {
		t.Fatalf("unexpected token length/prefix: length=%d prefix=%t", len(token.RawToken()), strings.HasPrefix(token.RawToken(), TokenPrefix))
	}
	parsedID, digest, err := parseRawToken(token.RawToken())
	if err != nil || parsedID != token.TokenID() || digest != sha256.Sum256([]byte(token.RawToken())) {
		t.Fatalf("roundtrip mismatch: id=%q err=%v", parsedID, err)
	}
	if token.String() != "AUTH_TOKEN_REDACTED" || token.GoString() != "AUTH_TOKEN_REDACTED" {
		t.Fatal("issued token formatting is not redacted")
	}
}

func TestParseRejectsNonCanonicalTokens(t *testing.T) {
	valid := TokenPrefix + strings.Repeat("a", TokenIDHexLen) + "_" + strings.Repeat("A", TokenSecretLen)
	if _, _, err := parseRawToken(valid); err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}
	for _, value := range []string{
		"", "n1_", valid + "=", strings.ToUpper(valid),
		TokenPrefix + strings.Repeat("g", TokenIDHexLen) + "_" + strings.Repeat("A", TokenSecretLen),
		TokenPrefix + strings.Repeat("a", TokenIDHexLen) + "_" + strings.Repeat("A", TokenSecretLen-1),
	} {
		if _, _, err := parseRawToken(value); ErrorCode(err) != AuthTokenMalformed {
			t.Fatalf("value %q got %v want %s", value, err, AuthTokenMalformed)
		}
	}
}

func TestGenerationRequiresEntropy(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	if _, err := generateIssuedToken(errorReader{}, now, time.Hour); ErrorCode(err) != AuthEntropyUnavailable {
		t.Fatalf("entropy failure got %v", err)
	}
	if _, err := generateIssuedToken(bytes.NewReader(nil), now, time.Hour); ErrorCode(err) != AuthEntropyUnavailable {
		t.Fatalf("short entropy got %v", err)
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("entropy unavailable") }

type noopObserver struct{}

func (noopObserver) Observe(Observation) {}

func TestServiceConstructionOrdering(t *testing.T) {
	if _, err := NewService(nil, Config{}); ErrorCode(err) != AuthClockRequired {
		t.Fatalf("missing clock got %v", err)
	}
	clock := ClockFunc(time.Now)
	if _, err := NewService(nil, Config{Clock: clock}); ErrorCode(err) != AuthObserverRequired {
		t.Fatalf("missing observer got %v", err)
	}
	if _, err := NewService(nil, Config{Clock: clock, Observer: noopObserver{}}); ErrorCode(err) != AuthInvalidInput {
		t.Fatalf("missing store got %v", err)
	}
}
