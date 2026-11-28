// Package push delivers notifications for already persisted messages.
package push

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"strconv"
	"strings"

	"github.com/m-ice/NewIM/server/auth/session"
)

type Channel string

const (
	APNs    Channel = "apns"
	FCM     Channel = "fcm"
	PushKit Channel = "pushkit"
)

func (c Channel) Valid() bool {
	return c == APNs || c == FCM || c == PushKit
}

var (
	ErrInvalidInput = errors.New("PUSH_INVALID_INPUT")
	ErrConfig       = errors.New("PUSH_CONFIG_INVALID")
	ErrStorage      = errors.New("PUSH_STORAGE_UNAVAILABLE")
	ErrCiphertext   = errors.New("PUSH_TOKEN_UNAVAILABLE")
)

type RegistrationStore interface {
	Register(context.Context, session.BearerSession, Channel, []byte) (int64, error)
	Unregister(context.Context, session.BearerSession, Channel, int64) error
}

// TokenBinding joins an encrypted token to one authenticated installation and version.
type TokenBinding struct {
	UserID    string
	DeviceID  string
	SessionID string
	Channel   Channel
	Version   int64
}

type SealedToken struct {
	KeyID      string
	Nonce      []byte
	Ciphertext []byte
}

type Vault struct {
	active string
	keys   map[string]cipher.AEAD
}

func NewVault(active string, keys map[string][]byte) (*Vault, error) {
	if !validID(active) || len(keys) == 0 || len(keys) > 8 {
		return nil, ErrConfig
	}
	vault := &Vault{active: active, keys: make(map[string]cipher.AEAD, len(keys))}
	for id, key := range keys {
		if !validID(id) || len(key) != 32 {
			return nil, ErrConfig
		}
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, ErrConfig
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, ErrConfig
		}
		vault.keys[id] = aead
	}
	if vault.keys[active] == nil {
		return nil, ErrConfig
	}
	return vault, nil
}

func (v *Vault) ActiveKey() string {
	return v.active
}

func (v *Vault) Seal(binding TokenBinding, token []byte) (SealedToken, error) {
	aad := binding.aad()
	if aad == nil || !ValidToken(binding.Channel, token) {
		return SealedToken{}, ErrInvalidInput
	}
	aead := v.keys[v.active]
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return SealedToken{}, ErrCiphertext
	}
	return SealedToken{
		KeyID: v.active, Nonce: nonce,
		Ciphertext: aead.Seal(nil, nonce, token, aad),
	}, nil
}

func (v *Vault) Open(binding TokenBinding, sealed SealedToken) ([]byte, error) {
	aead, aad := v.keys[sealed.KeyID], binding.aad()
	if aead == nil || aad == nil || len(sealed.Nonce) != aead.NonceSize() ||
		len(sealed.Ciphertext) <= aead.Overhead() || len(sealed.Ciphertext) > maxTokenBytes+aead.Overhead() {
		return nil, ErrCiphertext
	}
	token, err := aead.Open(nil, sealed.Nonce, sealed.Ciphertext, aad)
	if err != nil || !ValidToken(binding.Channel, token) {
		clear(token)
		return nil, ErrCiphertext
	}
	return token, nil
}

func (b TokenBinding) aad() []byte {
	if !validID(b.UserID) || !validID(b.DeviceID) || !validID(b.SessionID) || !b.Channel.Valid() || b.Version < 1 {
		return nil
	}
	return []byte(strings.Join([]string{
		"newim.push.token.v1", b.UserID, b.DeviceID, b.SessionID, string(b.Channel), strconv.FormatInt(b.Version, 10),
	}, ":"))
}

func validID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for i := range len(value) {
		c := value[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

const maxTokenBytes = 4096

func ValidToken(channel Channel, token []byte) bool {
	if !channel.Valid() || len(token) == 0 || len(token) > maxTokenBytes {
		return false
	}
	if channel == APNs && (len(token)%2 != 0 || len(token) > 1024) {
		return false
	}
	for _, c := range token {
		if channel == APNs {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
				return false
			}
		} else if c < 0x21 || c > 0x7e {
			return false
		}
	}
	return true
}
