package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/netip"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/messagehttp"
)

// MessageRoutes applies the public HTTP budget before delegating to the
// existing send and private relationship handlers.
type MessageRoutes struct {
	send       *messagehttp.Handler
	private    *messagehttp.PrivatePolicyHandler
	visibility *messagehttp.VisibilityHandler
	budget     *ProfileBudget
}

func NewMessageRoutes(auth messagehttp.Authenticator, sender messagehttp.Sender, store messagehttp.PrivatePolicyStore, budget *ProfileBudget) (*MessageRoutes, error) {
	return newMessageRoutes(auth, sender, store, budget, messagehttp.NewHandler)
}

func NewMessageRoutesWithMedia(auth messagehttp.Authenticator, sender messagehttp.Sender, store messagehttp.PrivatePolicyStore, budget *ProfileBudget) (*MessageRoutes, error) {
	return newMessageRoutes(auth, sender, store, budget, messagehttp.NewHandlerWithMedia)
}

func newMessageRoutes(auth messagehttp.Authenticator, sender messagehttp.Sender, store messagehttp.PrivatePolicyStore,
	budget *ProfileBudget, newSend func(messagehttp.Authenticator, messagehttp.Sender, int64) (*messagehttp.Handler, error),
) (*MessageRoutes, error) {
	if auth == nil || budget == nil {
		return nil, errors.New("invalid message route configuration")
	}
	authenticator := messageAuthenticator{auth: auth, budget: budget}
	sendAuth := messageAuthenticator{auth: auth, budget: budget, send: true}
	send, err := newSend(sendAuth, sender, protocol.MaxFrameBytes)
	if err != nil {
		return nil, err
	}
	private, err := messagehttp.NewPrivatePolicyHandler(authenticator, store)
	if err != nil {
		return nil, err
	}
	var visibility *messagehttp.VisibilityHandler
	if operations, ok := store.(messagehttp.VisibilityStore); ok && operations.SupportsVisibility() {
		visibility, err = messagehttp.NewVisibilityHandler(authenticator, operations)
		if err != nil {
			return nil, err
		}
	}
	return &MessageRoutes{send: send, private: private, visibility: visibility, budget: budget}, nil
}

func ContainsMessageRoute(path string) bool {
	switch path {
	case messagehttp.Route, messagehttp.DirectConversationRoute, messagehttp.BlockPeerRoute, messagehttp.UnblockPeerRoute, messagehttp.VisibilityRoute:
		return true
	default:
		return false
	}
}

func (m *MessageRoutes) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	addr, err := netip.ParseAddrPort(r.RemoteAddr)
	if r.TLS == nil || err != nil {
		reject(w, http.StatusBadRequest)
		return
	}
	if !m.budget.AllowAddress(addr.Addr().Unmap()) {
		messagehttp.WriteRateLimited(w)
		return
	}
	switch r.URL.EscapedPath() {
	case messagehttp.Route:
		m.send.ServeHTTP(w, r)
	case messagehttp.VisibilityRoute:
		if m.visibility == nil {
			reject(w, http.StatusNotFound)
			return
		}
		m.visibility.ServeHTTP(w, r)
	default:
		m.private.ServeHTTP(w, r)
	}
}

type messageAuthenticator struct {
	auth   messagehttp.Authenticator
	budget *ProfileBudget
	send   bool
}

func (a messageAuthenticator) AuthenticateBearer(ctx context.Context, token string) (session.BearerSession, error) {
	identity, err := a.auth.AuthenticateBearer(ctx, token)
	if err != nil {
		return session.BearerSession{}, err
	}
	var allowed bool
	if a.send {
		allowed = a.budget.allowSend(identity.UserID(), identity.DeviceID())
	} else {
		allowed = a.budget.AllowAccount(identity.UserID())
	}
	if !allowed {
		return session.BearerSession{}, messagehttp.ErrRateLimited
	}
	return identity, nil
}
