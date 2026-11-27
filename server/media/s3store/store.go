package s3store

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/m-ice/NewIM/server/media"
	"github.com/m-ice/NewIM/server/media/internal/objectio"
	"github.com/m-ice/NewIM/server/media/localfs"
)

const putAttempts = 3

type Store struct {
	client    *s3.Client
	staging   *localfs.Staging
	transport *http.Transport
	bucket    string
	transfers chan struct{}
	closed    atomic.Bool
}

var (
	_ media.ObjectStore  = (*Store)(nil)
	_ media.ObjectReader = (*Store)(nil)
)

// Close releases the staging root and idle object connections.
func (s *Store) Close() error {
	if s == nil || s.staging == nil || !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.transport.CloseIdleConnections()
	if err := s.staging.Close(); err != nil {
		return unavailable()
	}
	return nil
}

func (s *Store) PutImmutable(ctx context.Context, key string, content io.Reader, expected media.ObjectInfo) (media.ObjectInfo, error) {
	if s == nil || ctx == nil || !objectio.ValidKey(key) || content == nil || !media.ValidObjectInfo(expected) {
		return media.ObjectInfo{}, media.Fail(media.MediaInvalidInput)
	}
	if !s.acquire(ctx) {
		return media.ObjectInfo{}, unavailable()
	}
	defer s.release()

	file, err := s.staging.Create()
	if err != nil {
		return media.ObjectInfo{}, unavailable()
	}
	defer file.Close()
	actual, err := objectio.Inspect(ctx, file, content)
	if errors.Is(err, objectio.ErrTooLarge) {
		return media.ObjectInfo{}, media.Fail(media.MediaInvalidInput)
	}
	if err != nil {
		return media.ObjectInfo{}, unavailable()
	}
	if actual != expected {
		return media.ObjectInfo{}, s.uploadMismatch(ctx, key)
	}
	if err = s.publish(ctx, key, file, expected); err != nil {
		return media.ObjectInfo{}, err
	}
	return expected, nil
}

func (s *Store) uploadMismatch(ctx context.Context, key string) error {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	switch {
	case err == nil:
		return media.Fail(media.MediaConflict)
	case responseStatus(err) == http.StatusNotFound:
		return media.Fail(media.MediaInvalidInput)
	default:
		return unavailable()
	}
}

func (s *Store) publish(ctx context.Context, key string, file *os.File, expected media.ObjectInfo) error {
	for attempt := 0; attempt < putAttempts; attempt++ {
		if ctx.Err() != nil {
			return unavailable()
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return unavailable()
		}
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:            aws.String(s.bucket),
			Key:               aws.String(key),
			Body:              file,
			ContentLength:     aws.Int64(expected.Size),
			IfNoneMatch:       aws.String("*"),
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
			ChecksumSHA256:    aws.String(base64.StdEncoding.EncodeToString(expected.SHA256[:])),
		})
		if err == nil {
			return nil
		}
		status := responseStatus(err)
		if status == http.StatusPreconditionFailed {
			return s.download(ctx, key, io.Discard, expected)
		}
		if status != http.StatusConflict || !conditionalConflict(err) || attempt+1 == putAttempts {
			return unavailable()
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 25 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return unavailable()
		}
	}
	return unavailable()
}

func conditionalConflict(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "ConditionalRequestConflict"
}

func responseStatus(err error) int {
	var response interface{ HTTPStatusCode() int }
	if errors.As(err, &response) {
		return response.HTTPStatusCode()
	}
	return 0
}

func (s *Store) OpenVerified(ctx context.Context, key string, expected media.ObjectInfo) (io.ReadCloser, error) {
	if s == nil || ctx == nil || !objectio.ValidKey(key) || !media.ValidObjectInfo(expected) {
		return nil, media.Fail(media.MediaInvalidInput)
	}
	if !s.acquire(ctx) {
		return nil, unavailable()
	}
	file, err := s.staging.Create()
	if err != nil {
		s.release()
		return nil, unavailable()
	}
	body := &downloadBody{file: file, release: s.release}
	if err = s.download(ctx, key, file, expected); err != nil {
		_ = body.Close()
		return nil, unavailable()
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		_ = body.Close()
		return nil, unavailable()
	}
	return body, nil
}

func (s *Store) download(ctx context.Context, key string, out io.Writer, expected media.ObjectInfo) error {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil || object == nil || object.Body == nil {
		return unavailable()
	}
	defer object.Body.Close()
	if object.ContentLength != nil && *object.ContentLength != expected.Size {
		return media.Fail(media.MediaConflict)
	}
	actual, err := objectio.Inspect(ctx, out, object.Body)
	switch {
	case errors.Is(err, objectio.ErrTooLarge):
		return media.Fail(media.MediaConflict)
	case err != nil:
		return unavailable()
	case actual != expected:
		return media.Fail(media.MediaConflict)
	default:
		return nil
	}
}

func (s *Store) acquire(ctx context.Context) bool {
	if s.closed.Load() || ctx.Err() != nil {
		return false
	}
	select {
	case s.transfers <- struct{}{}:
		if s.closed.Load() {
			s.release()
			return false
		}
		return true
	default:
		return false
	}
}

func (s *Store) release() {
	<-s.transfers
}

type downloadBody struct {
	file    *os.File
	release func()
	close   sync.Once
	err     error
}

func (b *downloadBody) Read(buf []byte) (int, error) {
	n, err := b.file.Read(buf)
	if err != nil && err != io.EOF {
		err = unavailable()
	}
	return n, err
}

func (b *downloadBody) Close() error {
	b.close.Do(func() {
		if err := b.file.Close(); err != nil {
			b.err = unavailable()
		}
		b.release()
	})
	return b.err
}

func unavailable() error {
	return media.Fail(media.MediaStorageUnavailable)
}
