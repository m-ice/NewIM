package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/auth/session"
)

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.EscapedPath() != Route {
		reject(w, http.StatusNotFound)
		return
	}
	started := time.Now()
	outcome := "rejected"
	defer func() {
		if outcome != "success" {
			api.ObserveGatewayOperation(g.config.Metrics, "handshake", outcome, time.Since(started))
		}
	}()
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		reject(w, http.StatusMethodNotAllowed)
		return
	}
	nonce, valid := validateUpgrade(r)
	addr, addrErr := netip.ParseAddrPort(r.RemoteAddr)
	if !valid || addrErr != nil {
		reject(w, http.StatusBadRequest)
		return
	}
	conn, _ := r.Context().Value(socketContextKey{}).(net.Conn)
	entry, status := g.enter(conn, addr.Addr().Unmap())
	if status != 0 {
		outcome = handshakeOutcome(status)
		reject(w, status)
		return
	}
	defer g.leave(entry)

	ctx, cancel := context.WithTimeout(r.Context(), handshakeLimit)
	rawToken, identity, status := g.authorizeUpgrade(ctx, r, nonce)
	cancel()
	if status != 0 {
		outcome = handshakeOutcome(status)
		reject(w, status)
		return
	}
	if g.isDraining() {
		outcome = "temporary"
		reject(w, http.StatusServiceUnavailable)
		return
	}

	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{Subprotocol}})
	if err != nil {
		return
	}
	defer ws.CloseNow()
	connectionCtx, stop := context.WithCancel(context.WithoutCancel(r.Context()))
	defer stop()
	result := invalid
	if ws.Subprotocol() == Subprotocol {
		outcome = "success"
		api.ObserveGatewayOperation(g.config.Metrics, "handshake", outcome, time.Since(started))
		result = g.serveConnection(connectionCtx, ws, rawToken, identity)
	}
	g.finishConnection(ws, entry.conn, result)
}

func handshakeOutcome(status int) string {
	if status == http.StatusTooManyRequests || status >= 500 {
		return "temporary"
	}
	return "rejected"
}

func (g *Gateway) finishConnection(ws *websocket.Conn, transport net.Conn, result end) {
	if result.code <= 0 {
		_ = transport.Close()
		return
	}
	// Buffered reads in the close handshake need a transport deadline. The
	// timer also bounds TLS cleanup, which can set its own write deadline.
	deadline := time.Now().Add(g.config.WriteTimeout)
	if err := transport.SetDeadline(deadline); err != nil {
		_ = transport.Close()
		return
	}
	timer := time.AfterFunc(time.Until(deadline), func() { _ = transport.Close() })
	defer timer.Stop()
	_ = ws.Close(result.code, result.reason)
}

func (g *Gateway) authorizeUpgrade(ctx context.Context, r *http.Request, nonce [32]byte) (string, session.ConnectionIdentity, int) {
	rawToken, ok := bearerhttp.ParseBearerHeader(r.Header.Values("Authorization"))
	if !ok {
		return "", session.ConnectionIdentity{}, http.StatusUnauthorized
	}
	bearer, err := g.auth.AuthenticateBearer(ctx, rawToken)
	if err != nil {
		status, _ := bearerhttp.MapAuthenticationError(err)
		return "", session.ConnectionIdentity{}, status
	}
	if !g.allowDevice(deviceKey{user: bearer.UserID(), device: bearer.DeviceID()}) {
		return "", session.ConnectionIdentity{}, http.StatusTooManyRequests
	}
	identity, err := session.NewConnectionIdentity(
		bearer.UserID(), bearer.DeviceID(), bearer.SessionID(), rand.Text(), bearer.TokenID(),
	)
	if err != nil {
		return "", session.ConnectionIdentity{}, http.StatusServiceUnavailable
	}
	reserved, err := g.store.ReserveGatewayHandshake(ctx, bearer.TokenID(), nonce, time.Now(), bearer.ExpiresAt())
	if err != nil {
		status, _ := bearerhttp.MapAuthenticationError(err)
		return "", session.ConnectionIdentity{}, status
	}
	if !reserved {
		return "", session.ConnectionIdentity{}, http.StatusConflict
	}
	return rawToken, identity, 0
}

func validateUpgrade(r *http.Request) ([32]byte, bool) {
	var hash [32]byte
	if r.TLS == nil || r.ProtoMajor != 1 || r.ProtoMinor != 1 ||
		r.URL.RawQuery != "" || r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 ||
		len(r.Header.Values("Origin")) != 0 ||
		!headerEquals(r.Header, "Sec-WebSocket-Protocol", Subprotocol) ||
		!headerEquals(r.Header, "Sec-WebSocket-Version", "13") {
		return hash, false
	}
	upgrade := r.Header.Values("Upgrade")
	if len(upgrade) != 1 || !strings.EqualFold(upgrade[0], "websocket") || !hasConnectionUpgrade(r.Header) {
		return hash, false
	}
	keys := r.Header.Values("Sec-WebSocket-Key")
	if len(keys) != 1 || len(keys[0]) != 24 {
		return hash, false
	}
	nonce, err := base64.StdEncoding.Strict().DecodeString(keys[0])
	if err != nil || len(nonce) != 16 || base64.StdEncoding.EncodeToString(nonce) != keys[0] {
		return hash, false
	}
	return sha256.Sum256(nonce), true
}

func hasConnectionUpgrade(header http.Header) bool {
	for _, value := range header.Values("Connection") {
		for token := range strings.SplitSeq(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				return true
			}
		}
	}
	return false
}

func headerEquals(header http.Header, key, value string) bool {
	values := header.Values(key)
	return len(values) == 1 && values[0] == value
}

func reject(w http.ResponseWriter, status int) {
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	http.Error(w, http.StatusText(status), status)
}
