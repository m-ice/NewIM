// Package user implements account provisioning and profile access.
package user

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxLookup   = 50
	MaxNickname = 128
	MaxFaceURL  = 2048
	MaxExtra    = 4096
)

type Code string

const (
	InvalidInput       Code = "USER_INVALID_INPUT"
	NotFound           Code = "USER_NOT_FOUND"
	StorageUnavailable Code = "USER_STORAGE_UNAVAILABLE"
)

func (e Code) Error() string { return string(e) }

func Fail(code Code) error {
	return code
}

func ErrorCode(err error) Code {
	var code Code
	if errors.As(err, &code) {
		return code
	}
	return StorageUnavailable
}

type Profile struct {
	UserID   string `json:"userId"`
	Nickname string `json:"nickname"`
	FaceURL  string `json:"faceUrl"`
	Ex       string `json:"ex"`
}

// Patch distinguishes an absent value from an explicitly cleared value.
type Patch struct {
	Nickname *string
	FaceURL  *string
	Ex       *string
}

func (p Patch) Empty() bool {
	return p.Nickname == nil && p.FaceURL == nil && p.Ex == nil
}

func (p Patch) Valid() bool {
	return validField(p.Nickname, MaxNickname) && validField(p.FaceURL, MaxFaceURL) && validField(p.Ex, MaxExtra)
}

func validField(value *string, limit int) bool {
	return value == nil || len(*value) <= limit && utf8.ValidString(*value) && strings.IndexByte(*value, 0) < 0
}

func ValidID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for i := range len(id) {
		switch c := id[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

type Store interface {
	Provision(context.Context, string, Patch) (Profile, error)
	Get(context.Context, string) (Profile, error)
	Update(context.Context, string, Patch) (Profile, error)
	Lookup(context.Context, []string) ([]Profile, error)
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

func (s *Service) Provision(ctx context.Context, id string, profile Patch) (Profile, error) {
	if !ValidID(id) || !profile.Valid() {
		return Profile{}, Fail(InvalidInput)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.store.Provision(ctx, id, profile)
}

func (s *Service) Get(ctx context.Context, id string) (Profile, error) {
	if !ValidID(id) {
		return Profile{}, Fail(InvalidInput)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.store.Get(ctx, id)
}

func (s *Service) Update(ctx context.Context, id string, patch Patch) (Profile, error) {
	if !ValidID(id) || patch.Empty() || !patch.Valid() {
		return Profile{}, Fail(InvalidInput)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.store.Update(ctx, id, patch)
}

func (s *Service) Lookup(ctx context.Context, ids []string) ([]Profile, error) {
	if len(ids) == 0 || len(ids) > MaxLookup {
		return nil, Fail(InvalidInput)
	}
	unique := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if !ValidID(id) {
			return nil, Fail(InvalidInput)
		}
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			unique = append(unique, id)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	profiles, err := s.store.Lookup(ctx, unique)
	if profiles == nil && err == nil {
		profiles = []Profile{}
	}
	return profiles, err
}
