package login

import (
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const issuerCredentialLength = 64

var errInvalidIssuerConfig = errors.New("SERVER_INVALID_AUTH_CONFIG")

func loadIssuerDigest(path string) ([sha256.Size]byte, error) {
	if !filepath.IsAbs(path) {
		return [sha256.Size]byte{}, errInvalidIssuerConfig
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return [sha256.Size]byte{}, errInvalidIssuerConfig
	}
	file := os.NewFile(uintptr(fd), "issuer credential")
	defer file.Close()

	info, err := file.Stat()
	if err != nil || (info.Mode() != 0o400 && info.Mode() != 0o600) {
		return [sha256.Size]byte{}, errInvalidIssuerConfig
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || info.Size() < issuerCredentialLength || info.Size() > issuerCredentialLength+1 {
		return [sha256.Size]byte{}, errInvalidIssuerConfig
	}

	var buffer [issuerCredentialLength + 2]byte
	defer clear(buffer[:])
	n, err := io.ReadFull(file, buffer[:])
	if err != io.ErrUnexpectedEOF || (n != issuerCredentialLength && (n != issuerCredentialLength+1 || buffer[n-1] != '\n')) {
		return [sha256.Size]byte{}, errInvalidIssuerConfig
	}
	raw := buffer[:issuerCredentialLength]
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
