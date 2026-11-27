// Package read provides account-scoped read watermarks and direct-message receipts.
package read

import (
	"context"
	"errors"
	"time"

	"github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/user"
)

type Code string

const (
	InvalidInput       Code = "READ_INVALID_INPUT"
	NotFound           Code = "READ_NOT_FOUND"
	StorageUnavailable Code = "READ_STORAGE_UNAVAILABLE"
	AuthInvalidToken   Code = "AUTH_INVALID_TOKEN"
	AuthUnavailable    Code = "AUTH_UNAVAILABLE"
)

func (c Code) Error() string { return string(c) }

func Fail(code Code) error { return code }

func ErrorCode(err error) Code {
	if err == nil {
		return ""
	}
	var code Code
	if errors.As(err, &code) {
		switch code {
		case InvalidInput, NotFound, StorageUnavailable, AuthInvalidToken, AuthUnavailable:
			return code
		}
	}
	return StorageUnavailable
}

type State struct {
	ConversationID  string
	LastReadSeq     int64
	HistoryAfterSeq int64
	LatestSeq       int64
	UpdatedAt       *time.Time
}

type Receipt struct {
	ServerMsgID string
	Read        bool
}

type Store interface {
	Get(context.Context, session.BearerSession, string) (State, error)
	Advance(context.Context, session.BearerSession, string, int64) (State, error)
	GetReceipt(context.Context, session.BearerSession, string, string) (Receipt, error)
}

type Service struct {
	store Store
}

func NewService(store Store) (*Service, error) {
	if store == nil {
		return nil, Fail(StorageUnavailable)
	}
	return &Service{store: store}, nil
}

func ValidConversationID(id string) bool { return user.ValidID(id) }

func ValidMessageID(id string) bool { return user.ValidID(id) }

func (s *Service) Get(ctx context.Context, bearer session.BearerSession, conversationID string) (State, error) {
	if !ValidConversationID(conversationID) {
		return State{}, Fail(InvalidInput)
	}
	if err := s.validate(ctx, bearer); err != nil {
		return State{}, err
	}
	state, err := s.store.Get(ctx, bearer, conversationID)
	if err != nil {
		return State{}, Fail(ErrorCode(err))
	}
	return state, nil
}

func (s *Service) Advance(ctx context.Context, bearer session.BearerSession, conversationID string, seq int64) (State, error) {
	if !ValidConversationID(conversationID) || seq < 0 {
		return State{}, Fail(InvalidInput)
	}
	if err := s.validate(ctx, bearer); err != nil {
		return State{}, err
	}
	state, err := s.store.Advance(ctx, bearer, conversationID, seq)
	if err != nil {
		return State{}, Fail(ErrorCode(err))
	}
	return state, nil
}

func (s *Service) GetReceipt(ctx context.Context, bearer session.BearerSession, conversationID, serverMsgID string) (Receipt, error) {
	if !ValidConversationID(conversationID) || !ValidMessageID(serverMsgID) {
		return Receipt{}, Fail(InvalidInput)
	}
	if err := s.validate(ctx, bearer); err != nil {
		return Receipt{}, err
	}
	receipt, err := s.store.GetReceipt(ctx, bearer, conversationID, serverMsgID)
	if err != nil {
		return Receipt{}, Fail(ErrorCode(err))
	}
	return receipt, nil
}

func (s *Service) validate(ctx context.Context, bearer session.BearerSession) error {
	if ctx == nil || s == nil || s.store == nil {
		return Fail(StorageUnavailable)
	}
	if bearer.TokenID() == "" || bearer.ExpiresAt().IsZero() {
		return Fail(AuthInvalidToken)
	}
	return nil
}
