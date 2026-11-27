// Package objectio shares key and stream checks between media backends.
package objectio

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"

	"github.com/m-ice/NewIM/server/media"
)

var ErrTooLarge = errors.New("media object too large")

func ValidKey(key string) bool {
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

func Inspect(ctx context.Context, destination io.Writer, source io.Reader) (media.ObjectInfo, error) {
	hasher := sha256.New()
	writer := io.MultiWriter(destination, hasher)
	limited := &io.LimitedReader{R: source, N: media.MaxMediaSize + 1}
	buffer := make([]byte, 32*1024)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return media.ObjectInfo{}, err
		}
		count, err := limited.Read(buffer)
		if count > 0 {
			if total > media.MaxMediaSize-int64(count) {
				return media.ObjectInfo{}, ErrTooLarge
			}
			if _, writeErr := writer.Write(buffer[:count]); writeErr != nil {
				return media.ObjectInfo{}, writeErr
			}
			total += int64(count)
		}
		if errors.Is(err, io.EOF) {
			if err = ctx.Err(); err != nil {
				return media.ObjectInfo{}, err
			}
			var digest [32]byte
			copy(digest[:], hasher.Sum(nil))
			return media.ObjectInfo{Size: total, SHA256: digest}, nil
		}
		if err != nil {
			return media.ObjectInfo{}, err
		}
	}
}
