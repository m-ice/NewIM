package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/auth/session"
	syncmessage "github.com/m-ice/NewIM/server/sync/message"
)

const (
	maxSubscriptionBytes = 1024
	presenceTimeout      = 250 * time.Millisecond
	subscriptionTTL      = 45 * time.Second
)

type end struct {
	code   websocket.StatusCode
	reason string
}

var (
	draining      = end{websocket.StatusGoingAway, "server_draining"}
	invalid       = end{websocket.StatusPolicyViolation, "invalid_subscription"}
	subscribeLate = end{websocket.StatusPolicyViolation, "subscription_timeout"}
	forbidden     = end{websocket.StatusPolicyViolation, "conversation_forbidden"}
	revoked       = end{websocket.StatusPolicyViolation, "session_revoked"}
	unavailable   = end{websocket.StatusInternalError, "temporarily_unavailable"}
	socketGone    = end{code: -1}
)

type subscription struct {
	conversationID          string
	afterSeq                int64
	afterVisibilityRevision *int64
}

type peer struct {
	ws       *websocket.Conn
	identity session.ConnectionIdentity
	auth     session.AuthenticateRequest
	stopped  chan end
	active   bool
	started  time.Time
}

func (g *Gateway) serveConnection(ctx context.Context, ws *websocket.Conn, raw string, identity session.ConnectionIdentity) (result end) {
	p := peer{
		ws: ws, identity: identity, stopped: make(chan end, 1), started: time.Now(),
		auth: session.AuthenticateRequest{
			Token: raw, ConnectionID: identity.ConnectionID(),
			Binding: session.SessionBinding{
				UserID: identity.UserID(), DeviceID: identity.DeviceID(), SessionID: identity.SessionID(),
			},
		},
	}
	defer func() {
		if p.active {
			g.active.Add(-1)
		} else {
			outcome := "rejected"
			if result == draining || result == unavailable || result == socketGone {
				outcome = "temporary"
			}
			api.ObserveGatewayOperation(g.config.Metrics, "subscribe", outcome, time.Since(p.started))
		}
	}()
	ws.SetReadLimit(maxSubscriptionBytes)
	subscriptions := make(chan subscription, 1)
	go p.read(ctx, subscriptions)

	timer := time.NewTimer(g.config.SubscribeTimeout)
	defer timer.Stop()
	var request subscription
	select {
	case <-g.draining:
		return draining
	case result := <-p.stopped:
		return result
	case <-timer.C:
		return subscribeLate
	case request = <-subscriptions:
	}
	timer.Stop()
	defer func() {
		if g.presence != nil && p.active {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), presenceTimeout)
			defer cancel()
			_ = g.presence.ReleaseGatewaySubscription(cleanup, p.identity)
		}
	}()
	return g.deliver(ctx, &p, request)
}

// read keeps control frames flowing while the delivery loop owns every app write.
func (p *peer) read(ctx context.Context, requests chan<- subscription) {
	messageType, payload, err := p.ws.Read(ctx)
	if err != nil {
		p.stopped <- socketGone
		return
	}
	request, ok := decodeSubscription(payload)
	if !ok || messageType != websocket.MessageText {
		p.stopped <- invalid
		return
	}
	requests <- request
	_, _, err = p.ws.Reader(ctx)
	if err != nil {
		p.stopped <- socketGone
		return
	}
	p.stopped <- invalid
}

func decodeSubscription(payload []byte) (subscription, bool) {
	var sub subscription
	if len(payload) == 0 || len(payload) > maxSubscriptionBytes || !utf8.Valid(payload) {
		return sub, false
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	open, err := decoder.Token()
	if err != nil || open != json.Delim('{') {
		return sub, false
	}
	fields := make(map[string]string, 3)
	for decoder.More() {
		token, err := decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok || (name != "conversationId" && name != "afterSeq" && name != "afterVisibilityRevision") {
			return sub, false
		}
		if _, repeated := fields[name]; repeated {
			return sub, false
		}
		value, err := decoder.Token()
		text, ok := value.(string)
		if err != nil || !ok {
			return sub, false
		}
		fields[name] = text
	}
	if _, err := decoder.Token(); err != nil {
		return sub, false
	}
	if _, err := decoder.Token(); err != io.EOF {
		return sub, false
	}
	id, seq := fields["conversationId"], fields["afterSeq"]
	after, ok := decodeCursor(seq)
	if !validConversation(id) || !ok {
		return sub, false
	}
	sub.conversationID, sub.afterSeq = id, after
	if raw, present := fields["afterVisibilityRevision"]; present {
		revision, ok := decodeCursor(raw)
		if !ok {
			return subscription{}, false
		}
		sub.afterVisibilityRevision = &revision
	}
	return sub, true
}

func decodeCursor(raw string) (int64, bool) {
	value, err := strconv.ParseInt(raw, 10, 64)
	return value, err == nil && value >= 0 && strconv.FormatInt(value, 10) == raw
}

func validConversation(id string) bool {
	if len(id) < 1 || len(id) > 128 {
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

func (g *Gateway) deliver(ctx context.Context, p *peer, request subscription) end {
	poll := time.NewTicker(pollInterval)
	defer poll.Stop()
	heartbeat := time.NewTicker(heartbeatPeriod)
	defer heartbeat.Stop()

	for {
		if result := g.shouldStop(p); result.code != 0 {
			return result
		}

		select {
		case <-heartbeat.C:
			if result := g.heartbeat(ctx, p, request.conversationID); result.code != 0 {
				return result
			}
		default:
		}
		if result := g.sendPage(ctx, p, &request); result.code != 0 {
			return result
		}

		select {
		case <-g.draining:
			return draining
		case result := <-p.stopped:
			return result
		case <-poll.C:
		}
	}
}

func (g *Gateway) shouldStop(p *peer) end {
	if g.isDraining() {
		return draining
	}
	select {
	case result := <-p.stopped:
		return result
	default:
		return end{}
	}
}

func (g *Gateway) heartbeat(ctx context.Context, p *peer, conversationID string) end {
	if result := g.authenticate(ctx, p); result.code != 0 {
		return result
	}
	ping, cancel := context.WithTimeout(ctx, g.config.WriteTimeout)
	err := p.ws.Ping(ping)
	cancel()
	if err != nil {
		return socketGone
	}
	return g.markActivity(ctx, p, conversationID)
}

func (g *Gateway) markActivity(ctx context.Context, p *peer, conversationID string) end {
	if err := g.store.MarkGatewayActivity(ctx, p.identity); err != nil {
		return g.authorizationFailure(ctx, p, err)
	}
	if g.presence != nil {
		// Presence helps the notification worker. Its availability does not gate websocket delivery.
		write, cancel := context.WithTimeout(ctx, presenceTimeout)
		defer cancel()
		_ = g.presence.RenewGatewaySubscription(write, p.identity, conversationID, subscriptionTTL)
	}
	return end{}
}

func (g *Gateway) authenticate(ctx context.Context, p *peer) end {
	identity, err := g.auth.Authenticate(ctx, p.auth)
	if err == nil {
		if identity != p.identity {
			return revoked
		}
		return end{}
	}
	return g.authorizationFailure(ctx, p, err)
}

func (g *Gateway) authorizationFailure(ctx context.Context, p *peer, err error) end {
	status, _ := bearerhttp.MapAuthenticationError(err)
	if status != http.StatusUnauthorized {
		return unavailable
	}
	reason, err := g.store.GatewayRevocationReason(ctx, p.identity.UserID(), p.identity.SessionID())
	if err != nil {
		return unavailable
	}
	if reason == session.RevocationReasonDeviceLimit {
		return end{websocket.StatusPolicyViolation, reason}
	}
	return revoked
}

func (g *Gateway) sendPage(ctx context.Context, p *peer, request *subscription) end {
	if result := g.authenticate(ctx, p); result.code != 0 {
		return result
	}
	page, err := g.messages.ReadAfterSeq(ctx, p.identity, syncmessage.ReadRequest{
		ConversationID: request.conversationID, HasAfterSeq: true, AfterSeq: request.afterSeq, Limit: pageItems,
		TrackVisibility: request.afterVisibilityRevision != nil,
	})
	if err != nil {
		switch syncmessage.ErrorCode(err) {
		case syncmessage.Forbidden:
			return forbidden
		case syncmessage.InvalidCursor:
			return invalid
		default:
			return unavailable
		}
	}
	if !p.active {
		if result := g.markActivity(ctx, p, request.conversationID); result.code != 0 {
			return result
		}
		p.active = true
		g.active.Add(1)
		api.ObserveGatewayOperation(g.config.Metrics, "subscribe", "success", time.Since(p.started))
	}
	notice := request.afterVisibilityRevision != nil && page.VisibilityRevision > *request.afterVisibilityRevision
	if len(page.Items) == 0 && !notice {
		return end{}
	}
	if result := g.authenticate(ctx, p); result.code != 0 {
		return result
	}
	write, cancel := context.WithTimeout(ctx, g.config.WriteTimeout)
	defer cancel()
	if notice {
		if result := g.shouldStop(p); result.code != 0 {
			return result
		}
		wire, err := protocol.EncodeVisibilityNotice(request.conversationID, page.VisibilityRevision)
		if err != nil {
			return unavailable
		}
		if err = g.writeObserved(write, p, "visibility_notice", wire); err != nil {
			return socketGone
		}
		*request.afterVisibilityRevision = page.VisibilityRevision
	}
	for _, item := range page.Items {
		if result := g.shouldStop(p); result.code != 0 {
			return result
		}
		wire, err := syncmessage.EncodeServerFrame(item)
		if err != nil {
			return unavailable
		}
		if err = g.writeObserved(write, p, "delivery", wire); err != nil {
			return socketGone
		}
		request.afterSeq = item.ConversationSeq
	}
	return end{}
}
