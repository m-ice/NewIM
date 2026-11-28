//go:build integration

package conversationsynchttp_test

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	auth "github.com/m-ice/NewIM/server/auth/session"
)

const connectionEmptyRoom = "sdk_bridge_group_empty"

type connectionCredential struct {
	UserID    string    `json:"userId"`
	DeviceID  string    `json:"deviceId"`
	SessionID string    `json:"sessionId"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expiresAt"`
}

func admitConnectionDevice(t *testing.T, f *fixture, server *process, user, device, platform string) connectionCredential {
	t.Helper()
	reply := f.call(server, http.MethodPost, "/api/v1/internal/session/admit", f.issuerKey,
		encode(t, map[string]string{"userId": user, "deviceId": device, "platform": platform}), nil)
	requireStatus(t, reply, http.StatusOK)
	var credential connectionCredential
	must(t, json.Unmarshal(reply.body, &credential))
	if credential.UserID != user || credential.DeviceID != device || credential.SessionID == "" ||
		len(credential.Token) != auth.RawTokenLen || credential.ExpiresAt.IsZero() {
		t.Fatal("issuer did not return the admitted device session")
	}
	return credential
}

func issueConnectionToken(t *testing.T, f *fixture, server *process, device connectionCredential) connectionCredential {
	t.Helper()
	body := encode(t, map[string]string{"userId": device.UserID, "deviceId": device.DeviceID, "sessionId": device.SessionID})
	reply := f.call(server, http.MethodPost, "/api/v1/internal/session/token", f.issuerKey, body, nil)
	requireStatus(t, reply, http.StatusOK)
	var credential connectionCredential
	must(t, json.Unmarshal(reply.body, &credential))
	if len(credential.Token) != auth.RawTokenLen || credential.Token == device.Token || credential.ExpiresAt.IsZero() {
		t.Fatal("issuer did not return a new session token")
	}
	return credential
}

type connectionSocket struct {
	conn   *websocket.Conn
	cancel context.CancelFunc
	done   chan struct{}
	reads  chan string
}

func (s *connectionSocket) close() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.conn != nil {
		_ = s.conn.CloseNow()
	}
	if s.done != nil {
		<-s.done
	}
	*s = connectionSocket{}
}

func (s *connectionSocket) dial(t *testing.T, f *fixture, gateway *process, token string) {
	t.Helper()
	if s.conn != nil {
		t.Fatal("close the preceding fixture socket before dialing")
	}
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	conn, reply, err := websocket.Dial(ctx, "wss://"+strings.TrimPrefix(gateway.url, "https://")+"/api/v1/gateway",
		&websocket.DialOptions{
			HTTPClient: f.client, HTTPHeader: http.Header{"Authorization": {"Bearer " + token}},
			Subprotocols: []string{"newim.v1"},
		})
	if reply == nil || reply.TLS == nil || len(reply.TLS.VerifiedChains) == 0 {
		t.Fatal("fixture gateway did not use verified TLS")
	}
	if reply.StatusCode == http.StatusUnauthorized && err != nil && conn == nil {
		mustBridgeWrite(t, "SOCKET\t401\t")
		return
	}
	must(t, err)
	s.conn = conn
	if reply.StatusCode != http.StatusSwitchingProtocols || conn.Subprotocol() != "newim.v1" {
		t.Fatal("fixture gateway did not negotiate the v1 subprotocol")
	}
	conn.SetReadLimit(protocol.MaxFrameBytes)
	mustBridgeWrite(t, "SOCKET\t101\t"+conn.Subprotocol())
}

func (s *connectionSocket) subscribe(t *testing.T, parent context.Context, raw string) {
	t.Helper()
	if s.conn == nil || s.reads != nil || len(raw) > 2048 {
		t.Fatal("invalid fixture subscription command")
	}
	payload, err := hex.DecodeString(raw)
	must(t, err)
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	must(t, s.conn.Write(ctx, websocket.MessageText, payload))
	readCtx, stop := context.WithCancel(parent)
	s.cancel, s.done, s.reads = stop, make(chan struct{}), make(chan string, 1)
	go readConnectionSocket(readCtx, s.conn, s.done, s.reads)
	mustBridgeWrite(t, "SUBSCRIBED")
}

func readConnectionSocket(ctx context.Context, conn *websocket.Conn, done chan<- struct{}, reads chan<- string) {
	defer close(done)
	for {
		kind, wire, err := conn.Read(ctx)
		var reply string
		switch {
		case err == nil && kind == websocket.MessageText:
			reply = "FRAME\t" + hex.EncodeToString(wire)
		case err == nil:
			reply = "INVALID_FRAME"
		default:
			var closed websocket.CloseError
			if errors.As(err, &closed) {
				reply = fmt.Sprintf("CLOSED\t%d\t%s", closed.Code, closed.Reason)
			} else {
				reply = "SOCKET_ERROR"
			}
		}
		select {
		case reads <- reply:
		case <-ctx.Done():
			return
		}
		if err != nil || kind != websocket.MessageText {
			return
		}
	}
}

func (s *connectionSocket) poll(t *testing.T) {
	t.Helper()
	if s.reads == nil {
		t.Fatal("subscribe the fixture socket before polling")
	}
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case reply := <-s.reads:
		mustBridgeWrite(t, reply)
	case <-timer.C:
		mustBridgeWrite(t, "EMPTY")
	}
}

func TestConnectionSDKBridge(t *testing.T) {
	runConnectionSDKBridge(t, 18, messageSDKHistory)
}

func TestMessageVisibilitySDKBridge(t *testing.T) {
	runConnectionSDKBridge(t, 21, 3)
}

func runConnectionSDKBridge(t *testing.T, migration, messages int) {
	t.Helper()
	phase := "database"
	defer func() {
		if t.Failed() {
			mustBridgeWrite(t, "FAILED\t"+phase)
		}
	}()
	f := openFixture(t)
	f.requireMigration(migration)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
	defer cancel()
	f.ctx = ctx

	phase = "gateway"
	gateway := f.syncGateway(writeKeys(t, "sdk", "sdk"), map[string]string{
		"NEWIM_CONVERSATION_READ_SYNC":                        "1",
		"NEWIM_CONVERSATION_ACCOUNT_SETTINGS":                 "1",
		"NEWIM_TLS_MESSAGE_IP_ATTEMPTS_PER_MINUTE":            "512",
		"NEWIM_TLS_MESSAGE_ACCOUNT_ATTEMPTS_PER_MINUTE":       "256",
		"NEWIM_TLS_MESSAGE_DEVICE_ATTEMPTS_PER_MINUTE":        "256",
		"NEWIM_CONVERSATION_SYNC_IP_ATTEMPTS_PER_MINUTE":      "512",
		"NEWIM_CONVERSATION_SYNC_ACCOUNT_ATTEMPTS_PER_MINUTE": "256",
	})
	phase = "accounts"
	server := f.server("1", map[string]string{
		"NEWIM_CONVERSATION_READ_SYNC": "1", "NEWIM_AUTH_MAX_DEVICES": "2", "NEWIM_AUTH_MAX_DEVICES_PER_PLATFORM": "1",
	})
	shortIssuer := f.server("1", map[string]string{"NEWIM_AUTH_ISSUER_TTL_SECONDS": "1"})
	provision(t, f, server, bridgeAccount)
	provision(t, f, server, bridgePeer)
	phone := admitConnectionDevice(t, f, server, bridgeAccount, "phone", "phone")
	tablet := admitConnectionDevice(t, f, server, bridgeAccount, "tablet", "tablet")
	peer := admitConnectionDevice(t, f, server, bridgePeer, "phone", "phone")
	short := issueConnectionToken(t, f, shortIssuer, phone)
	tokens := map[string]string{"phone": phone.Token, "next": tablet.Token, "peer": peer.Token}

	phase = "groups"
	createGroup(t, f, gateway, phone.Token, bridgeRoom, []string{bridgePeer}, true)
	createGroup(t, f, gateway, phone.Token, connectionEmptyRoom, []string{bridgePeer}, true)
	phase = "messages"
	for number := 1; number <= messages; number++ {
		send(t, f, gateway, peer.Token, bridgeRoom, fmt.Sprintf("connect_%03d", number), "connection history")
	}

	var socket connectionSocket
	defer socket.close()
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 2*protocol.MaxFrameBytes+256)
	mustBridgeWrite(t, "READY")
	for range messageSDKLimit {
		phase = "transport"
		if !scanner.Scan() {
			t.Fatal("SDK connection fixture input ended before QUIT")
		}
		command, value, _ := strings.Cut(scanner.Text(), "\t")
		switch command {
		case "HTTP":
			forwardMessageSDKRequest(t, f, gateway, tokens, nil, scanner.Text())
		case "SOCKET":
			if value == "expired" {
				deadline := time.NewTimer(time.Until(short.ExpiresAt) + 10*time.Millisecond)
				select {
				case <-ctx.Done():
					deadline.Stop()
					t.Fatal("short fixture credential did not expire before the transport deadline")
				case <-deadline.C:
				}
				_, err := f.auth.AuthenticateBearer(ctx, short.Token)
				if auth.ErrorCode(err) != auth.AuthTokenExpired {
					t.Fatal("short fixture credential did not expire")
				}
				tokens[value] = short.Token
				tokens["renewed"] = issueConnectionToken(t, f, server, phone).Token
			}
			token, ok := tokens[value]
			if !ok {
				t.Fatal("unknown fixture device role")
			}
			socket.dial(t, f, gateway, token)
		case "SUBSCRIBE":
			socket.subscribe(t, ctx, value)
		case "POLL":
			socket.poll(t)
		case "CLOSE":
			socket.close()
			mustBridgeWrite(t, "CLOSE_OK")
		case "BULK_HIDE":
			if migration != 21 {
				t.Fatal("visibility fixture requires migration 021")
			}
			for offset := range protocol.VisibilityPageItems + 1 {
				ack := send(t, f, gateway, peer.Token, bridgeRoom, fmt.Sprintf("visibility_bulk_%d", offset), "extra history")
				messageVisibility(t, f, gateway, tablet.Token, protocol.VisibilityRequest{
					ConversationID: bridgeRoom, ServerMsgID: ack.ServerMsgID,
					RequestID: fmt.Sprintf("hide_bulk_%d", offset), Action: protocol.DeleteForMe,
				}, http.StatusOK)
			}
			mustBridgeWrite(t, "BULK_HIDDEN")
		case "KICK":
			replacement := admitConnectionDevice(t, f, server, bridgeAccount, "replacement", "phone")
			tokens["replacement"] = replacement.Token
			mustBridgeWrite(t, "KICKED")
		case "QUIT":
			phase = "shutdown"
			for _, process := range []*process{gateway, server, shortIssuer} {
				secrets := []string{f.issuerKey, testDSN, short.Token}
				for _, token := range tokens {
					secrets = append(secrets, token)
				}
				process.requireRedacted(t, secrets...)
			}
			mustBridgeWrite(t, "DONE")
			return
		default:
			t.Fatal("unknown SDK connection fixture command")
		}
	}
	t.Fatal("SDK connection fixture exceeded its command bound")
}
