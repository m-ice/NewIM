//go:build integration

package message_test

import (
	"context"
	"errors"
	"sync/atomic"

	session "github.com/m-ice/NewIM/server/auth/session"
	media "github.com/m-ice/NewIM/server/media"
)

type flipMediaValidator struct {
	calls atomic.Int64
}

func (v *flipMediaValidator) ValidateForSend(context.Context, session.ConnectionIdentity, string, media.Metadata) error {
	if v.calls.Add(1) > 1 {
		return media.Fail(media.MediaUnauthorized)
	}
	return nil
}

type limitedIDs struct {
	calls atomic.Int64
}

func (g *limitedIDs) NewID() (string, error) {
	n := g.calls.Add(1)
	if n > 2 {
		return "", errors.New("generator exhausted")
	}
	return "dup_retry_" + string(rune('a'+n-1)), nil
}
