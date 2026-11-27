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
	send    *messagehttp.Handler
	private *messagehttp.PrivatePolicyHandler
	budget  *ProfileBudget
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
	send, err := newSend(authenticator, sender, protocol.MaxFrameBytes)
	if err != nil {
		return nil, err
	}
	private, err := messagehttp.NewPrivatePolicyHandler(authenticator, store)
	if err != nil {
		return nil, err
	}
	return &MessageRoutes{send: send, private: private, budget: budget}, nil
}

func ContainsMessageRoute(path string) bool {
	switch path {
	case messagehttp.Route, messagehttp.DirectConversationRoute, messagehttp.BlockPeerRoute, messagehttp.UnblockPeerRoute:
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
	if r.URL.EscapedPath() == messagehttp.Route {
		m.send.ServeHTTP(w, r)
		return
	}
	m.private.ServeHTTP(w, r)
}

type messageAuthenticator struct {
	auth   messagehttp.Authenticator
	budget *ProfileBudget
}

func (a messageAuthenticator) AuthenticateBearer(ctx context.Context, token string) (session.BearerSession, error) {
	identity, err := a.auth.AuthenticateBearer(ctx, token)
	if err != nil {
		return session.BearerSession{}, err
	}
	if !a.budget.AllowAccount(identity.UserID()) {
		return session.BearerSession{}, messagehttp.ErrRateLimited
	}
	return identity, nil
}
