package group

import (
	"context"
	"math"
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

func (s *Service) Create(ctx context.Context, actor string, req CreateRequest) (View, error) {
	if !validContext(ctx, actor, req.GroupID) || !validProfile(req.Profile) || !validJoinMode(req.JoinMode) ||
		len(req.MemberIDs) > MaxMemberLimit {
		return View{}, InvalidInput
	}
	if req.MemberLimit == 0 {
		req.MemberLimit = DefaultMemberLimit
	}
	if req.MemberLimit < 1 || req.MemberLimit > MaxMemberLimit {
		return View{}, InvalidInput
	}
	members := make([]string, 0, len(req.MemberIDs))
	seen := map[string]struct{}{actor: {}}
	for _, id := range req.MemberIDs {
		if !user.ValidID(id) {
			return View{}, InvalidInput
		}
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			members = append(members, id)
		}
	}
	if len(seen) > req.MemberLimit {
		return View{}, CapacityExceeded
	}
	req.MemberIDs = members
	enabled := true
	if req.MembersCanInvite == nil {
		req.MembersCanInvite = &enabled
	}
	if req.HistoryEnabled == nil {
		req.HistoryEnabled = &enabled
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return s.store.Create(ctx, actor, req)
}

func (s *Service) Get(ctx context.Context, actor, groupID string) (View, error) {
	if !validContext(ctx, actor, groupID) {
		return View{}, InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return s.store.Get(ctx, actor, groupID)
}

func (s *Service) Update(ctx context.Context, actor string, req UpdateRequest) (View, error) {
	if !validContext(ctx, actor, req.GroupID) || !validUpdate(req) {
		return View{}, InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return s.store.Update(ctx, actor, req)
}

func (s *Service) RequestEntry(ctx context.Context, actor string, req JoinRequest) (Entry, error) {
	if !validContext(ctx, actor, req.GroupID) || !user.ValidID(req.RequestID) || !validText(req.Message, 512) {
		return Entry{}, InvalidInput
	}
	switch req.Kind {
	case Join:
		if req.UserID != "" && req.UserID != actor {
			return Entry{}, InvalidInput
		}
		req.UserID = actor
	case Invite:
		if !user.ValidID(req.UserID) || req.UserID == actor {
			return Entry{}, InvalidInput
		}
	default:
		return Entry{}, InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return s.store.RequestEntry(ctx, actor, req)
}

func (s *Service) Respond(ctx context.Context, actor string, req ResponseRequest) (Entry, error) {
	if !validContext(ctx, actor, req.GroupID) || !user.ValidID(req.ActorID) || !user.ValidID(req.RequestID) ||
		(req.Decision != Accepted && req.Decision != Rejected) {
		return Entry{}, InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return s.store.Respond(ctx, actor, req)
}

func (s *Service) ChangeMember(ctx context.Context, actor string, req MemberRequest) (Membership, error) {
	if !validContext(ctx, actor, req.GroupID) {
		return Membership{}, InvalidInput
	}
	switch req.Action {
	case Leave:
		if (req.UserID != "" && req.UserID != actor) || req.MuteSeconds != 0 {
			return Membership{}, InvalidInput
		}
		req.UserID = actor
	case Remove, Promote, Demote, TransferOwner, UnmuteMember:
		if !user.ValidID(req.UserID) || req.UserID == actor || req.MuteSeconds != 0 {
			return Membership{}, InvalidInput
		}
	case MuteMember:
		if !user.ValidID(req.UserID) || req.UserID == actor || req.MuteSeconds < 1 || req.MuteSeconds > math.MaxInt32 {
			return Membership{}, InvalidInput
		}
	default:
		return Membership{}, InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return s.store.ChangeMember(ctx, actor, req)
}

func (s *Service) SetMuted(ctx context.Context, actor, groupID string, muted bool) (View, error) {
	if !validContext(ctx, actor, groupID) {
		return View{}, InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return s.store.SetMuted(ctx, actor, groupID, muted)
}

func (s *Service) Dissolve(ctx context.Context, actor, groupID string) (bool, error) {
	if !validContext(ctx, actor, groupID) {
		return false, InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return s.store.Dissolve(ctx, actor, groupID)
}

func (s *Service) ListMembers(ctx context.Context, actor string, req MemberListRequest) (Page[Membership], error) {
	limit, ok := pageSize(req.Limit)
	if !validContext(ctx, actor, req.GroupID) || !ok || (req.AfterUserID != "" && !user.ValidID(req.AfterUserID)) {
		return Page[Membership]{}, InvalidInput
	}
	req.Limit = limit
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	page, err := s.store.ListMembers(ctx, actor, req)
	if err == nil && page.Items == nil {
		page.Items = []Membership{}
	}
	return page, err
}

func (s *Service) ListEntries(ctx context.Context, actor string, req EntryListRequest) (Page[Entry], error) {
	limit, ok := pageSize(req.Limit)
	cursor := req.AfterActorID != "" || req.AfterRequestID != ""
	if !validContext(ctx, actor, req.GroupID) || !ok ||
		(cursor && (!user.ValidID(req.AfterActorID) || !user.ValidID(req.AfterRequestID))) {
		return Page[Entry]{}, InvalidInput
	}
	req.Limit = limit
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	page, err := s.store.ListEntries(ctx, actor, req)
	if err == nil && page.Items == nil {
		page.Items = []Entry{}
	}
	return page, err
}

func (s *Service) Changes(ctx context.Context, actor string, after int64, limit int) (ChangePage, error) {
	limit, ok := pageSize(limit)
	if ctx == nil || !user.ValidID(actor) || !ok || after < 0 {
		return ChangePage{}, InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	page, err := s.store.Changes(ctx, actor, after, limit)
	if err == nil && page.Items == nil {
		page.Items = []Change{}
	}
	return page, err
}

func validContext(ctx context.Context, actor, groupID string) bool {
	return ctx != nil && user.ValidID(actor) && user.ValidID(groupID)
}

func validJoinMode(mode JoinMode) bool {
	return mode >= ApproveApplications && mode <= OpenJoining
}

func validText(s string, limit int) bool {
	return len(s) <= limit && utf8.ValidString(s) && strings.IndexByte(s, 0) < 0
}

func validProfile(profile Profile) bool {
	return profile.Name != "" && validText(profile.Name, 128) &&
		validText(profile.FaceURL, 2048) && validText(profile.Description, 4096) &&
		validText(profile.Notification, 4096) && validText(profile.Ex, 4096)
}

func validUpdate(req UpdateRequest) bool {
	if req.Name == nil && req.FaceURL == nil && req.Description == nil && req.Notification == nil &&
		req.Ex == nil && req.JoinMode == nil && req.MembersCanInvite == nil && req.HistoryEnabled == nil {
		return false
	}
	validField := func(value *string, limit int) bool {
		return value == nil || validText(*value, limit)
	}
	return (req.Name == nil || *req.Name != "") && validField(req.Name, 128) &&
		validField(req.FaceURL, 2048) && validField(req.Description, 4096) &&
		validField(req.Notification, 4096) && validField(req.Ex, 4096) &&
		(req.JoinMode == nil || validJoinMode(*req.JoinMode))
}

func pageSize(limit int) (int, bool) {
	if limit == 0 {
		return DefaultPageSize, true
	}
	return limit, limit > 0 && limit <= MaxPageSize
}
