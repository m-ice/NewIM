// Package gateway serves authenticated delivery of committed messages over WSS.
package gateway

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/m-ice/NewIM/server/auth/session"
	syncmessage "github.com/m-ice/NewIM/server/sync/message"
)

const (
	Route       = "/api/v1/gateway"
	Subprotocol = "newim.v1"

	maxRateKeys     = 8192
	handshakeLimit  = 5 * time.Second
	pollInterval    = 500 * time.Millisecond
	heartbeatPeriod = 20 * time.Second
	pageItems       = 32
)

type SessionService interface {
	AuthenticateBearer(context.Context, string) (session.BearerSession, error)
	Authenticate(context.Context, session.AuthenticateRequest) (session.ConnectionIdentity, error)
}

type MessageReader interface {
	ReadAfterSeq(context.Context, session.ConnectionIdentity, syncmessage.ReadRequest) (syncmessage.Page, error)
}

type HandshakeStore interface {
	ReserveGatewayHandshake(context.Context, string, [32]byte, time.Time, time.Time) (bool, error)
	GatewayRevocationReason(context.Context, string, string) (string, error)
	MarkGatewayActivity(context.Context, session.ConnectionIdentity) error
}

type Config struct {
	MaxConnections   int
	IPAttempts       int
	DeviceAttempts   int
	SubscribeTimeout time.Duration
	WriteTimeout     time.Duration
}

type deviceKey struct {
	user, device string
}

type socket struct {
	conn net.Conn
}

type Gateway struct {
	auth     SessionService
	messages MessageReader
	store    HandshakeStore
	config   Config

	mu        sync.Mutex
	window    time.Time
	addresses map[netip.Addr]int
	devices   map[deviceKey]int
	sockets   map[*socket]struct{}
	draining  chan struct{}
	drained   chan struct{}
}

func New(auth SessionService, messages MessageReader, store HandshakeStore, config Config) (*Gateway, error) {
	if auth == nil || messages == nil || store == nil ||
		config.MaxConnections < 1 || config.MaxConnections > 1024 ||
		config.IPAttempts < 1 || config.IPAttempts > 4096 ||
		config.DeviceAttempts < 1 || config.DeviceAttempts > 4096 ||
		config.SubscribeTimeout < 100*time.Millisecond || config.SubscribeTimeout > 15*time.Second ||
		config.WriteTimeout < 100*time.Millisecond || config.WriteTimeout > 5*time.Second {
		return nil, errors.New("invalid gateway configuration")
	}
	return &Gateway{
		auth: auth, messages: messages, store: store, config: config,
		addresses: make(map[netip.Addr]int), devices: make(map[deviceKey]int),
		sockets:  make(map[*socket]struct{}),
		draining: make(chan struct{}), drained: make(chan struct{}),
	}, nil
}

type socketContextKey struct{}

// ConnContext must be installed on the gateway HTTP server. It preserves the
// transport so Drain can terminate sockets that outlive HTTP shutdown.
func ConnContext(ctx context.Context, conn net.Conn) context.Context {
	// Hard drain must close the socket without waiting for TLS close_notify.
	if wrapped, ok := conn.(*tls.Conn); ok {
		conn = wrapped.NetConn()
	}
	return context.WithValue(ctx, socketContextKey{}, conn)
}

func (g *Gateway) enter(conn net.Conn, addr netip.Addr) (*socket, int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.isDraining() {
		return nil, http.StatusServiceUnavailable
	}
	g.advanceWindow()
	if !countAttempt(g.addresses, addr, g.config.IPAttempts) {
		return nil, http.StatusTooManyRequests
	}
	if conn == nil || len(g.sockets) >= g.config.MaxConnections {
		return nil, http.StatusServiceUnavailable
	}
	s := &socket{conn: conn}
	g.sockets[s] = struct{}{}
	return s, 0
}

func (g *Gateway) allowDevice(key deviceKey) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.advanceWindow()
	return countAttempt(g.devices, key, g.config.DeviceAttempts)
}

func (g *Gateway) advanceWindow() {
	if newRateWindow(&g.window) {
		clear(g.addresses)
		clear(g.devices)
	}
}

func newRateWindow(previous *time.Time) bool {
	now := time.Now().UTC().Truncate(time.Minute)
	if now.Equal(*previous) {
		return false
	}
	*previous = now
	return true
}

func countAttempt[K comparable](entries map[K]int, key K, limit int) bool {
	count, exists := entries[key]
	if count >= limit || (!exists && len(entries) >= maxRateKeys) {
		return false
	}
	entries[key] = count + 1
	return true
}

func (g *Gateway) leave(s *socket) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.sockets, s)
	if g.isDraining() && len(g.sockets) == 0 {
		close(g.drained)
	}
}

func (g *Gateway) isDraining() bool {
	select {
	case <-g.draining:
		return true
	default:
		return false
	}
}

// BeginDrain stops admission and notifies accepted connections.
func (g *Gateway) BeginDrain() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.isDraining() {
		close(g.draining)
		if len(g.sockets) == 0 {
			close(g.drained)
		}
	}
}

// Drain waits for active handlers and interrupts their transports at the deadline.
func (g *Gateway) Drain(ctx context.Context) error {
	g.BeginDrain()
	select {
	case <-g.drained:
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		connections := make([]net.Conn, 0, len(g.sockets))
		for s := range g.sockets {
			connections = append(connections, s.conn)
		}
		g.mu.Unlock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		return ctx.Err()
	}
}
