package webhook

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"testing"
)

func TestLocalSecretResolverRoundTripAndAAD(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	material := SecretMaterial{
		DestinationID: "destination_1",
		Revision:      3,
		URL:           "https://example.invalid/hook",
		KeyID:         "key_current",
		Nonce:         []byte("0123456789ab"),
	}
	secret := []byte("0123456789abcdef0123456789abcdef")
	ciphertext, err := SealSecret(aead, material, secret)
	if err != nil {
		t.Fatal(err)
	}
	material.Ciphertext = ciphertext
	resolver, err := NewLocalSecretResolver(key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolver.Resolve(context.Background(), material)
	if err != nil || !bytes.Equal(got, secret) {
		t.Fatalf("round trip = %x, %v", got, err)
	}
	tampered := material
	tampered.Revision++
	if _, err = resolver.Resolve(context.Background(), tampered); ErrorCode(err) != CodeSecretInvalid {
		t.Fatalf("tampered AAD error = %v", err)
	}
}

func TestLocalSecretResolverRejectsShortSecret(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	material := SecretMaterial{DestinationID: "destination_1", Revision: 1, KeyID: "key", Nonce: []byte("0123456789ab")}
	if _, err := SealSecret(aead, material, []byte("short")); ErrorCode(err) != CodeInvalidConfig {
		t.Fatalf("short secret seal error = %v", err)
	}
	resolver, err := NewLocalSecretResolver(key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = resolver.Resolve(context.Background(), material); ErrorCode(err) != CodeSecretInvalid {
		t.Fatalf("short material error = %v", err)
	}
}
