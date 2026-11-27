package media

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/auth/session"
)

type transferStore struct {
	*fakeMediaStore
	authorize     func(context.Context, time.Time) error
	completeCalls int
}

func (s *transferStore) Authorize(ctx context.Context, now time.Time, _ session.ConnectionIdentity, _ string) error {
	return s.authorize(ctx, now)
}

func (s *transferStore) CompleteReady(ctx context.Context, now time.Time, identity session.ConnectionIdentity, grant Grant) (Grant, error) {
	s.completeCalls++
	return s.fakeMediaStore.CompleteReady(ctx, now, identity, grant)
}

type uploadObjectFunc func(context.Context, string, io.Reader, ObjectInfo) (ObjectInfo, error)

func (f uploadObjectFunc) PutImmutable(ctx context.Context, key string, body io.Reader, expected ObjectInfo) (ObjectInfo, error) {
	return f(ctx, key, body, expected)
}

type downloadObjectFunc func(context.Context, string, ObjectInfo) (io.ReadCloser, error)

func (f downloadObjectFunc) OpenVerified(ctx context.Context, key string, expected ObjectInfo) (io.ReadCloser, error) {
	return f(ctx, key, expected)
}

type trackedReadCloser struct {
	io.Reader
	closed bool
}

func (r *trackedReadCloser) Close() error {
	r.closed = true
	return nil
}

func TestUploadRechecksAfterObjectTransfer(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	for _, fixture := range []struct {
		name    string
		state   State
		authErr error
		want    Code
	}{
		{"pending expired", StatePending, nil, MediaExpired},
		{"ready permission revoked", StateReady, Fail(MediaUnauthorized), MediaUnauthorized},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			identity := testIdentity(t, "alice")
			now := start
			grant, raw := testGrant(t, identity, fixture.state, start.Add(time.Second))
			var times []time.Time
			store := &transferStore{
				fakeMediaStore: &fakeMediaStore{grant: grant},
				authorize: func(ctx context.Context, checkedAt time.Time) error {
					deadline, ok := ctx.Deadline()
					if !ok || time.Until(deadline) > time.Second {
						t.Fatal("authorization did not keep its short timeout")
					}
					times = append(times, checkedAt)
					if len(times) > 1 {
						return fixture.authErr
					}
					return nil
				},
			}
			service, err := NewService(store, Config{
				RequestTimeout: time.Second,
				ObjectTimeout:  time.Minute,
				Clock:          ClockFunc(func() time.Time { return now }),
				Objects: uploadObjectFunc(func(ctx context.Context, _ string, _ io.Reader, expected ObjectInfo) (ObjectInfo, error) {
					deadline, ok := ctx.Deadline()
					if !ok || time.Until(deadline) < 10*time.Second {
						t.Fatal("object transfer inherited the short authorization timeout")
					}
					now = grant.ExpiresAt
					return expected, nil
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.CompleteUpload(context.Background(), identity, raw, bytes.NewReader([]byte("abc")))
			if ErrorCode(err) != fixture.want || store.completeCalls != 0 || len(times) != 2 ||
				!times[0].Equal(start) || !times[1].Equal(grant.ExpiresAt) {
				t.Fatalf("completion error=%v calls=%d authorization times=%v", err, store.completeCalls, times)
			}
		})
	}
}

func TestOpenPrivateDownloadRechecksAuthorization(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	for _, denied := range []bool{false, true} {
		name := "authorized"
		if denied {
			name = "permission revoked during verification"
		}
		t.Run(name, func(t *testing.T) {
			identity := testIdentity(t, "alice")
			grant, _ := testGrant(t, identity, StateReady, start)
			now := start
			var times []time.Time
			store := &transferStore{
				fakeMediaStore: &fakeMediaStore{grant: grant},
				authorize: func(_ context.Context, checkedAt time.Time) error {
					times = append(times, checkedAt)
					if denied && len(times) > 1 {
						return Fail(MediaUnauthorized)
					}
					return nil
				},
			}
			body := &trackedReadCloser{Reader: bytes.NewReader([]byte("abc"))}
			var objectCtx context.Context
			service, err := NewService(store, Config{
				Clock: ClockFunc(func() time.Time { return now }),
				ObjectReader: downloadObjectFunc(func(ctx context.Context, key string, expected ObjectInfo) (io.ReadCloser, error) {
					if key != grant.MediaKey || expected != (ObjectInfo{Size: grant.DeclaredSize, SHA256: grant.SHA256}) {
						t.Fatal("reader did not receive the committed object identity")
					}
					objectCtx = ctx
					now = now.Add(time.Second)
					return body, nil
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			download, err := service.OpenPrivateDownload(context.Background(), identity, grant.MediaKey)
			if len(times) != 2 || !times[0].Equal(start) || !times[1].Equal(now) {
				t.Fatalf("authorization times=%v", times)
			}
			if denied {
				if ErrorCode(err) != MediaUnauthorized || download.Body != nil || !body.closed || objectCtx.Err() == nil {
					t.Fatalf("denied stream err=%v open=%t", err, !body.closed)
				}
				return
			}
			if err != nil || download.Body == nil || download.Size != grant.DeclaredSize || objectCtx.Err() != nil {
				t.Fatalf("verified stream was not returned: %v", err)
			}
			data, err := io.ReadAll(download.Body)
			if err != nil || string(data) != "abc" {
				t.Fatalf("read verified stream: %v", err)
			}
			if err = download.Body.Close(); err != nil || !body.closed || objectCtx.Err() == nil {
				t.Fatalf("close verified stream: %v", err)
			}
		})
	}
}
