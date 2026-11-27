// Package credentialfile reads bounded, owner-restricted credential files.
package credentialfile

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// ErrInvalidFile is returned without exposing paths, contents, or system errors.
var ErrInvalidFile = errors.New("invalid credential file")

// Read returns the original file bytes after verifying the opened descriptor.
// The caller owns the returned buffer and can clear it after use.
func Read(path string, minSize, maxSize int) ([]byte, error) {
	if !filepath.IsAbs(path) || minSize < 1 || maxSize < minSize || maxSize+1 <= maxSize {
		return nil, ErrInvalidFile
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, ErrInvalidFile
	}
	file := os.NewFile(uintptr(fd), "credential file")
	defer file.Close()

	info, err := file.Stat()
	if err != nil || (info.Mode() != 0o400 && info.Mode() != 0o600) {
		return nil, ErrInvalidFile
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || info.Size() < int64(minSize) || info.Size() > int64(maxSize) {
		return nil, ErrInvalidFile
	}
	if !aclIsRestricted(fd) {
		return nil, ErrInvalidFile
	}

	raw := make([]byte, maxSize+1)
	n, err := io.ReadFull(file, raw)
	if err != io.ErrUnexpectedEOF || n < minSize {
		clear(raw)
		return nil, ErrInvalidFile
	}
	return raw[:n], nil
}
