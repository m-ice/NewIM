package webhook

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"strconv"
)

// LocalSecretResolver unwraps AES-256-GCM secret material with a process-held master key.
// LocalSecretResolver 使用进程持有的主密钥解封 AES-256-GCM secret 材料。
type LocalSecretResolver struct {
	aead cipher.AEAD
}

// NewLocalSecretResolver creates a resolver from exactly one 32-byte master key.
// NewLocalSecretResolver 使用恰好 32 bytes 的主密钥创建 resolver。
func NewLocalSecretResolver(key []byte) (*LocalSecretResolver, error) {
	if len(key) != 32 {
		return nil, Fail(CodeInvalidConfig)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, Fail(CodeInvalidConfig)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, Fail(CodeInvalidConfig)
	}
	return &LocalSecretResolver{aead: aead}, nil
}

// Resolve unwraps one revision-bound secret without retaining or logging plaintext.
// Resolve 解封一个 revision 绑定的 secret，不保留或记录明文。
func (r *LocalSecretResolver) Resolve(ctx context.Context, material SecretMaterial) ([]byte, error) {
	if r == nil || r.aead == nil || ctx == nil || material.DestinationID == "" || material.Revision <= 0 || material.URL == "" || material.KeyID == "" || len(material.Nonce) != r.aead.NonceSize() || len(material.Ciphertext) < r.aead.Overhead()+1 {
		return nil, Fail(CodeSecretInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, Fail(CodeSecretUnavailable)
	}
	plaintext, err := r.aead.Open(nil, material.Nonce, material.Ciphertext, secretAAD(material))
	if err != nil {
		return nil, Fail(CodeSecretInvalid)
	}
	if len(plaintext) < 32 {
		clear(plaintext)
		return nil, Fail(CodeSecretInvalid)
	}
	return plaintext, nil
}

// SealSecret encrypts one secret for an endpoint revision; callers must keep the master key outside logs.
// SealSecret 为 endpoint revision 加密 secret；调用方必须确保主密钥不进入日志。
func SealSecret(aead cipher.AEAD, material SecretMaterial, secret []byte) ([]byte, error) {
	if aead == nil || len(secret) < 32 || len(material.Nonce) != aead.NonceSize() || material.DestinationID == "" || material.Revision <= 0 || material.URL == "" || material.KeyID == "" {
		return nil, Fail(CodeInvalidConfig)
	}
	return aead.Seal(nil, material.Nonce, secret, secretAAD(material)), nil
}

func secretAAD(material SecretMaterial) []byte {
	return []byte(material.DestinationID + "\n" + strconv.FormatInt(material.Revision, 10) + "\n" + material.URL + "\n" + material.KeyID)
}
