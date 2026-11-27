package login

import (
	"crypto/sha256"
	"errors"

	"github.com/m-ice/NewIM/server/credentialfile"
)

const issuerCredentialLength = 64

var errInvalidIssuerConfig = errors.New("SERVER_INVALID_AUTH_CONFIG")

func loadIssuerDigest(path string) ([sha256.Size]byte, error) {
	raw, err := credentialfile.Read(path, issuerCredentialLength, issuerCredentialLength+1)
	if err != nil {
		return [sha256.Size]byte{}, errInvalidIssuerConfig
	}
	defer clear(raw)
	if len(raw) == issuerCredentialLength+1 {
		if raw[issuerCredentialLength] != '\n' {
			return [sha256.Size]byte{}, errInvalidIssuerConfig
		}
		raw = raw[:issuerCredentialLength]
	}
	if !validIssuerCredential(raw) {
		return [sha256.Size]byte{}, errInvalidIssuerConfig
	}
	return sha256.Sum256(raw), nil
}

func validIssuerCredential(raw []byte) bool {
	if len(raw) != issuerCredentialLength {
		return false
	}
	for _, ch := range raw {
		if !(ch >= '0' && ch <= '9') && !(ch >= 'a' && ch <= 'f') {
			return false
		}
	}
	return true
}
