// Package localfs implements the media object-store port on one confined,
// owned filesystem root. It never overwrites an existing object.
// localfs 在单一受限且归当前用户所有的文件系统根上实现媒体对象端口；绝不覆盖已有对象。
package localfs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	app "github.com/m-ice/NewIM/server/media"
)

const maxObjectBytes = int64(app.MaxMediaSize)

var errObjectTooLarge = errors.New("media object too large")

// Store owns one mode-0700 root and publishes immutable objects below it.
// Store 持有一个 mode-0700 根，并在其下发布不可变对象。
type Store struct {
	root     *os.Root
	rootPath string
	publish  func(string, string) error
	syncHook func() error
}

// Open creates or validates an owned mode-0700 object root.
// Open 创建或校验归当前用户所有的 mode-0700 对象根。
func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, app.Fail(app.MediaInvalidInput)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, app.Fail(app.MediaStorageUnavailable)
	}
	info, statErr := os.Lstat(absolute)
	if errors.Is(statErr, fs.ErrNotExist) {
		if err = os.MkdirAll(absolute, 0o700); err != nil {
			return nil, app.Fail(app.MediaStorageUnavailable)
		}
		if err = os.Chmod(absolute, 0o700); err != nil {
			return nil, app.Fail(app.MediaStorageUnavailable)
		}
		info, statErr = os.Lstat(absolute)
	}
	if statErr != nil || info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return nil, app.Fail(app.MediaStorageUnavailable)
	}
	if err = checkRootOwner(info); err != nil {
		return nil, app.Fail(app.MediaStorageUnavailable)
	}
	root, err := os.OpenRoot(absolute)
	if err != nil {
		return nil, app.Fail(app.MediaStorageUnavailable)
	}
	return &Store{root: root, rootPath: absolute}, nil
}

// Close releases the confined root descriptor.
// Close 释放受限根描述符。
func (s *Store) Close() error {
	if s == nil || s.root == nil {
		return nil
	}
	return s.root.Close()
}

// PutImmutable creates one object atomically or verifies an identical one.
// PutImmutable 原子创建一个对象，或校验已有对象完全一致。
func (s *Store) PutImmutable(ctx context.Context, key string, content io.Reader, expected app.ObjectInfo) (app.ObjectInfo, error) {
	if s == nil || s.root == nil || ctx == nil || !validObjectKey(key) || content == nil || !app.ValidObjectInfo(expected) {
		return app.ObjectInfo{}, app.Fail(app.MediaInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
	}
	if exists, err := s.objectExists(key); err != nil {
		return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
	} else if exists {
		if incoming, inspectErr := inspectBounded(ctx, content); inspectErr != nil {
			return app.ObjectInfo{}, mapReadError(inspectErr)
		} else if incoming != expected {
			return app.ObjectInfo{}, app.Fail(app.MediaConflict)
		}
		return s.verifyExistingDurable(key, expected)
	}
	tempName, temp, err := s.createTemp()
	if err != nil {
		return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = temp.Close()
			_ = s.root.Remove(tempName)
		}
	}()
	hasher := sha256.New()
	size, err := copyBounded(ctx, io.MultiWriter(temp, hasher), content)
	if err != nil {
		return app.ObjectInfo{}, mapReadError(err)
	}
	actual := app.ObjectInfo{Size: size, SHA256: finishDigest(hasher)}
	if actual != expected {
		return app.ObjectInfo{}, app.Fail(app.MediaInvalidInput)
	}
	if err = temp.Sync(); err != nil {
		return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
	}
	if err = temp.Close(); err != nil {
		return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
	}
	target := key
	if s.publish != nil {
		err = s.publish(tempName, target)
	} else {
		err = s.root.Link(tempName, target)
	}
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			if removeErr := s.root.Remove(tempName); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
				return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
			}
			cleanup = false
			return s.verifyExistingDurable(key, expected)
		}
		return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
	}
	if err = s.root.Remove(tempName); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
	}
	cleanup = false
	if err = s.syncRoot(); err != nil {
		return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
	}
	return expected, nil
}

func (s *Store) objectExists(key string) (bool, error) {
	_, err := s.root.Lstat(key)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func (s *Store) verifyExistingDurable(key string, expected app.ObjectInfo) (app.ObjectInfo, error) {
	actual, err := s.verifyExisting(key, expected)
	if err != nil {
		return app.ObjectInfo{}, err
	}
	// A loser may observe the winner's directory entry before the winner's
	// syncRoot completes. Sync before any caller can commit ready metadata.
	// 竞争失败方可能先观察到胜出者的目录项；在允许提交 ready 元数据前必须同步父目录。
	if err = s.syncRoot(); err != nil {
		return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
	}
	return actual, nil
}

func (s *Store) verifyExisting(key string, expected app.ObjectInfo) (app.ObjectInfo, error) {
	info, err := s.root.Lstat(key)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() != expected.Size {
		return app.ObjectInfo{}, app.Fail(app.MediaConflict)
	}
	file, err := s.root.OpenFile(key, os.O_RDONLY, 0)
	if err != nil {
		return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
	}
	defer file.Close()
	hasher := sha256.New()
	size, err := copyBounded(context.Background(), hasher, file)
	if err != nil || size != expected.Size || finishDigest(hasher) != expected.SHA256 {
		return app.ObjectInfo{}, app.Fail(app.MediaConflict)
	}
	return expected, nil
}

func inspectBounded(ctx context.Context, content io.Reader) (app.ObjectInfo, error) {
	hasher := sha256.New()
	size, err := copyBounded(ctx, hasher, content)
	if err != nil {
		return app.ObjectInfo{}, err
	}
	return app.ObjectInfo{Size: size, SHA256: finishDigest(hasher)}, nil
}

func (s *Store) createTemp() (string, *os.File, error) {
	for attempt := 0; attempt < 32; attempt++ {
		var raw [16]byte
		if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
			return "", nil, err
		}
		name := ".tmp-" + hex.EncodeToString(raw[:])
		file, err := s.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		return name, file, err
	}
	return "", nil, fs.ErrExist
}

func copyBounded(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	limited := &io.LimitedReader{R: source, N: maxObjectBytes + 1}
	buffer := make([]byte, 32*1024)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		count, err := limited.Read(buffer)
		if count > 0 {
			if total > maxObjectBytes-int64(count) {
				return 0, errObjectTooLarge
			}
			if _, writeErr := destination.Write(buffer[:count]); writeErr != nil {
				return 0, writeErr
			}
			total += int64(count)
		}
		if errors.Is(err, io.EOF) {
			return total, nil
		}
		if err != nil {
			return 0, err
		}
		if limited.N <= 0 {
			return 0, errObjectTooLarge
		}
	}
}

func mapReadError(err error) error {
	if errors.Is(err, errObjectTooLarge) {
		return app.Fail(app.MediaInvalidInput)
	}
	return app.Fail(app.MediaStorageUnavailable)
}

func finishDigest(hasher hash.Hash) [32]byte {
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}

func (s *Store) syncRoot() error {
	if s.syncHook != nil {
		return s.syncHook()
	}
	root, err := s.root.Open(".")
	if err != nil {
		return err
	}
	defer root.Close()
	if err = root.Sync(); err != nil && !errors.Is(err, os.ErrInvalid) {
		return err
	}
	return nil
}

func validObjectKey(key string) bool {
	if len(key) == 0 || len(key) > 128 {
		return false
	}
	for i := 0; i < len(key); i++ {
		ch := key[i]
		if !((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}
