// Package localfs implements the media object-store port on one confined,
// owned filesystem root. It never overwrites an existing object.
// localfs 在单一受限且归当前用户所有的文件系统根上实现媒体对象端口；绝不覆盖已有对象。
package localfs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	app "github.com/m-ice/NewIM/server/media"
	"github.com/m-ice/NewIM/server/media/internal/objectio"
)

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
	root, absolute, err := openRoot(path)
	if err != nil {
		return nil, err
	}
	return &Store{root: root, rootPath: absolute}, nil
}

func openRoot(path string) (*os.Root, string, error) {
	if strings.TrimSpace(path) == "" {
		return nil, "", app.Fail(app.MediaInvalidInput)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, "", app.Fail(app.MediaStorageUnavailable)
	}
	info, statErr := os.Lstat(absolute)
	if errors.Is(statErr, fs.ErrNotExist) {
		if err = os.MkdirAll(absolute, 0o700); err != nil {
			return nil, "", app.Fail(app.MediaStorageUnavailable)
		}
		info, statErr = os.Lstat(absolute)
	}
	if statErr != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return nil, "", app.Fail(app.MediaStorageUnavailable)
	}
	root, err := os.OpenRoot(absolute)
	if err != nil {
		return nil, "", app.Fail(app.MediaStorageUnavailable)
	}
	if err := checkOpenedRoot(root, info); err != nil {
		_ = root.Close()
		return nil, "", err
	}
	return root, absolute, nil
}

func checkOpenedRoot(root *os.Root, before os.FileInfo) error {
	dir, err := root.Open(".")
	if err != nil {
		return app.Fail(app.MediaStorageUnavailable)
	}
	defer dir.Close()
	info, err := dir.Stat()
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 ||
		!os.SameFile(before, info) || checkRootOwner(info) != nil || !rootACLRestricted(dir) {
		return app.Fail(app.MediaStorageUnavailable)
	}
	return nil
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
	if s == nil || s.root == nil || ctx == nil || !objectio.ValidKey(key) || content == nil || !app.ValidObjectInfo(expected) {
		return app.ObjectInfo{}, app.Fail(app.MediaInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
	}
	if exists, err := s.objectExists(key); err != nil {
		return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
	} else if exists {
		if incoming, inspectErr := objectio.Inspect(ctx, io.Discard, content); inspectErr != nil {
			return app.ObjectInfo{}, mapReadError(inspectErr)
		} else if incoming != expected {
			return app.ObjectInfo{}, app.Fail(app.MediaConflict)
		}
		return s.verifyExistingDurable(ctx, key, expected)
	}
	tempName, temp, err := createTemp(s.root, os.O_WRONLY)
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
	actual, err := objectio.Inspect(ctx, temp, content)
	if err != nil {
		return app.ObjectInfo{}, mapReadError(err)
	}
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
			return s.verifyExistingDurable(ctx, key, expected)
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

// OpenVerified 验证已提交的文件，并返回从文件开头读取的同一描述符。
func (s *Store) OpenVerified(ctx context.Context, key string, expected app.ObjectInfo) (io.ReadCloser, error) {
	if s == nil || s.root == nil || ctx == nil || !objectio.ValidKey(key) || !app.ValidObjectInfo(expected) {
		return nil, app.Fail(app.MediaInvalidInput)
	}
	file, err := s.openVerified(ctx, key, expected)
	if err != nil {
		return nil, app.Fail(app.MediaStorageUnavailable)
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, app.Fail(app.MediaStorageUnavailable)
	}
	return file, nil
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

func (s *Store) verifyExistingDurable(ctx context.Context, key string, expected app.ObjectInfo) (app.ObjectInfo, error) {
	file, err := s.openVerified(ctx, key, expected)
	if err != nil {
		return app.ObjectInfo{}, err
	}
	_ = file.Close()
	// A loser may observe the winner's directory entry before the winner's
	// syncRoot completes. Sync before any caller can commit ready metadata.
	// 竞争失败方可能先观察到胜出者的目录项；在允许提交 ready 元数据前必须同步父目录。
	if err = s.syncRoot(); err != nil {
		return app.ObjectInfo{}, app.Fail(app.MediaStorageUnavailable)
	}
	return expected, nil
}

func (s *Store) openVerified(ctx context.Context, key string, expected app.ObjectInfo) (*os.File, error) {
	if ctx.Err() != nil {
		return nil, app.Fail(app.MediaStorageUnavailable)
	}
	before, err := s.root.Lstat(key)
	if err != nil || !before.Mode().IsRegular() || before.Size() != expected.Size {
		return nil, app.Fail(app.MediaConflict)
	}
	file, err := s.root.OpenFile(key, objectReadFlags, 0)
	if err != nil {
		return nil, app.Fail(app.MediaStorageUnavailable)
	}
	if err = s.verifyOpened(ctx, key, before, file, expected); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func (s *Store) verifyOpened(ctx context.Context, key string, before os.FileInfo, file *os.File, expected app.ObjectInfo) error {
	opened, err := file.Stat()
	if err != nil {
		return app.Fail(app.MediaStorageUnavailable)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return app.Fail(app.MediaConflict)
	}
	actual, err := objectio.Inspect(ctx, io.Discard, file)
	if ctx.Err() != nil {
		return app.Fail(app.MediaStorageUnavailable)
	}
	if err != nil || actual != expected {
		return app.Fail(app.MediaConflict)
	}
	after, err := s.root.Lstat(key)
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(opened, after) ||
		after.Size() != expected.Size || !after.ModTime().Equal(before.ModTime()) {
		return app.Fail(app.MediaConflict)
	}
	return nil
}

func createTemp(root *os.Root, access int) (string, *os.File, error) {
	for attempt := 0; attempt < 32; attempt++ {
		var raw [16]byte
		if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
			return "", nil, err
		}
		name := ".tmp-" + hex.EncodeToString(raw[:])
		file, err := root.OpenFile(name, access|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		return name, file, err
	}
	return "", nil, fs.ErrExist
}

func mapReadError(err error) error {
	if errors.Is(err, objectio.ErrTooLarge) {
		return app.Fail(app.MediaInvalidInput)
	}
	return app.Fail(app.MediaStorageUnavailable)
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
