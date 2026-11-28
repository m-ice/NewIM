package message

import (
	"context"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/auth/session"
)

const VisibilityRoute = "/api/v1/sync/message-visibility"

type VisibilityRequest struct {
	ConversationID  string
	AfterRevision   int64
	ThroughRevision int64
	Limit           int
}

type VisibilityPage struct {
	Items             []protocol.VisibilityEvent `json:"items"`
	NextAfterRevision int64                      `json:"nextAfterRevision,string"`
	HasNext           bool                       `json:"hasNext"`
}

type VisibilityStore interface {
	SupportsVisibility() bool
	ReadVisibility(context.Context, string, VisibilityRequest) ([]protocol.VisibilityEvent, error)
}

func (s *Service) SupportsVisibility() bool {
	if s == nil {
		return false
	}
	store, ok := s.store.(VisibilityStore)
	return ok && store.SupportsVisibility()
}

func (s *Service) ReadVisibility(ctx context.Context, identity session.ConnectionIdentity, request VisibilityRequest) (VisibilityPage, error) {
	if ctx == nil || !s.SupportsVisibility() {
		return VisibilityPage{}, Fail(StorageUnavailable)
	}
	if !validIdentity(identity) || !identifier(request.ConversationID) {
		return VisibilityPage{}, Fail(Forbidden)
	}
	if request.AfterRevision < 0 || request.ThroughRevision < request.AfterRevision {
		return VisibilityPage{}, Fail(InvalidCursor)
	}
	if request.Limit == 0 {
		request.Limit = protocol.VisibilityPageItems
	}
	if request.Limit > protocol.VisibilityPageItems || request.Limit < 1 {
		return VisibilityPage{}, Fail(LimitExceeded)
	}
	ctx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	rows, err := s.store.(VisibilityStore).ReadVisibility(ctx, identity.UserID(), request)
	if err != nil {
		return VisibilityPage{}, redact(err)
	}
	if len(rows) > request.Limit+1 {
		return VisibilityPage{}, Fail(StorageUnavailable)
	}
	previous := request.AfterRevision
	for _, item := range rows {
		if item.ConversationID != request.ConversationID || !identifier(item.ServerMsgID) ||
			item.ConversationSeq <= 0 || item.Revision <= previous || item.Revision > request.ThroughRevision ||
			!item.Action.Valid() || item.CountAsUnread {
			return VisibilityPage{}, Fail(StorageUnavailable)
		}
		previous = item.Revision
	}
	page := VisibilityPage{Items: rows, NextAfterRevision: request.ThroughRevision}
	if len(rows) > request.Limit {
		page.HasNext = true
		page.Items = rows[:request.Limit]
		page.NextAfterRevision = page.Items[request.Limit-1].Revision
	} else if page.Items == nil {
		page.Items = []protocol.VisibilityEvent{}
	}
	return page, nil
}
