package friend

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/m-ice/NewIM/server/user"
)

const requestTimeout = 5 * time.Second

type Service struct {
	store Store
}

func NewService(store Store) (*Service, error) {
	if store == nil {
		return nil, StorageUnavailable
	}
	return &Service{store: store}, nil
}

func (s *Service) Apply(ctx context.Context, owner string, req ApplyRequest) (Application, error) {
	if ctx == nil || !validPair(owner, req.PeerID) || !user.ValidID(req.RequestID) || !validText(req.Message) {
		return Application{}, InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return s.store.Apply(ctx, owner, req)
}

func (s *Service) Respond(ctx context.Context, owner string, req ResponseRequest) (Application, error) {
	if ctx == nil || !validPair(owner, req.ApplicantID) || !user.ValidID(req.RequestID) ||
		(req.Decision != "accept" && req.Decision != "reject") {
		return Application{}, InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return s.store.Respond(ctx, owner, req)
}

func (s *Service) Remark(ctx context.Context, owner, peer, remark string) (Edge, error) {
	if ctx == nil || !validPair(owner, peer) || !validText(remark) {
		return Edge{}, InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return s.store.Remark(ctx, owner, peer, remark)
}

func (s *Service) Remove(ctx context.Context, owner, peer string) (bool, error) {
	if ctx == nil || !validPair(owner, peer) {
		return false, InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return s.store.Remove(ctx, owner, peer)
}

func (s *Service) List(ctx context.Context, owner string, req ListRequest) (EdgePage, error) {
	limit, ok := pageLimit(req.Limit)
	if ctx == nil || !ok || !user.ValidID(owner) || (req.AfterPeerID != "" && !user.ValidID(req.AfterPeerID)) {
		return EdgePage{}, InvalidInput
	}
	req.Limit = limit
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	page, err := s.store.List(ctx, owner, req)
	if page.Items == nil && err == nil {
		page.Items = []Edge{}
	}
	return page, err
}

func (s *Service) ListApplications(ctx context.Context, owner string, req ApplicationListRequest) (ApplicationPage, error) {
	limit, ok := pageLimit(req.Limit)
	var hasCursor = req.AfterApplicantID != "" || req.AfterRequestID != ""
	if ctx == nil || !ok || !user.ValidID(owner) || (req.Direction != "incoming" && req.Direction != "outgoing") ||
		(hasCursor && (!user.ValidID(req.AfterApplicantID) || !user.ValidID(req.AfterRequestID))) {
		return ApplicationPage{}, InvalidInput
	}
	req.Limit = limit
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	page, err := s.store.ListApplications(ctx, owner, req)
	if page.Items == nil && err == nil {
		page.Items = []Application{}
	}
	return page, err
}

func (s *Service) Changes(ctx context.Context, owner string, after int64, limit int) (ChangePage, error) {
	limit, ok := pageLimit(limit)
	if ctx == nil || !ok || !user.ValidID(owner) || after < 0 {
		return ChangePage{}, InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	page, err := s.store.Changes(ctx, owner, after, limit)
	if page.Items == nil && err == nil {
		page.Items = []Change{}
	}
	return page, err
}

func validPair(owner, peer string) bool {
	return user.ValidID(owner) && user.ValidID(peer) && owner != peer
}

func validText(value string) bool {
	return len(value) <= MaxTextBytes && utf8.ValidString(value) && strings.IndexByte(value, 0) < 0
}

func pageLimit(limit int) (int, bool) {
	if limit == 0 {
		return DefaultLimit, true
	}
	return limit, limit > 0 && limit <= MaxLimit
}
