//go:build integration

package conversationsynchttp_test

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/group"
	syncmessage "github.com/m-ice/NewIM/server/sync/message"
)

const (
	messageSDKHistory = 70
	messageSDKLimit   = 128
	messageSDKDelta   = "/api/v1/sync/messages"
)

type messageSDKFrame struct {
	seq  uint64
	wire []byte
}

type messageSDKCapture struct {
	conn   *websocket.Conn
	latest uint64
	frames [messageSDKLimit]messageSDKFrame
}

func TestMessageSDKBridge(t *testing.T) {
	phase := "database"
	defer func() {
		if t.Failed() {
			_, _ = fmt.Fprintln(os.Stdout, "FAILED\t"+phase)
		}
	}()
	f := openFixture(t)
	f.requireMigration(18)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
	defer cancel()
	f.ctx = ctx

	phase = "gateway"
	gateway := f.syncGateway(writeKeys(t, "sdk", "sdk"), map[string]string{
		"NEWIM_CONVERSATION_READ_SYNC":                        "1",
		"NEWIM_CONVERSATION_ACCOUNT_SETTINGS":                 "1",
		"NEWIM_READ_WATERMARK_HTTP":                           "1",
		"NEWIM_TLS_MESSAGE_IP_ATTEMPTS_PER_MINUTE":            "512",
		"NEWIM_TLS_MESSAGE_ACCOUNT_ATTEMPTS_PER_MINUTE":       "256",
		"NEWIM_TLS_MESSAGE_DEVICE_ATTEMPTS_PER_MINUTE":        "256",
		"NEWIM_CONVERSATION_SYNC_IP_ATTEMPTS_PER_MINUTE":      "512",
		"NEWIM_CONVERSATION_SYNC_ACCOUNT_ATTEMPTS_PER_MINUTE": "256",
	})
	phase = "accounts"
	server := f.readSyncServer()
	provision(t, f, server, bridgeAccount)
	provision(t, f, server, bridgePeer)
	phone, peer := f.admit(bridgeAccount, "phone"), f.admit(bridgePeer, "phone")
	tokens := map[string]string{"phone": phone, "peer": peer}

	phase = "group"
	view := groupResult[group.View](t, f, gateway, phone, createGroupPath, "view", group.CreateRequest{
		GroupID: bridgeRoom, Profile: group.Profile{Name: "Message sync fixture"},
		JoinMode: group.ApproveApplications, MemberIDs: []string{bridgePeer},
	})
	if view.Group.GroupID != bridgeRoom || view.Membership.Role != group.Owner {
		t.Fatal("message SDK fixture group did not commit its owner and peer")
	}
	phase = "messages"
	var seed [messageSDKHistory]protocol.Ack
	for i := range seed {
		seed[i] = send(t, f, gateway, peer, bridgeRoom, fmt.Sprintf("gap_%03d", i+1), fmt.Sprintf("fixture message %03d", i+1))
	}
	phase = "socket"
	conn := accountHistorySocket(t, f, gateway, phone, bridgeRoom)
	conn.SetReadLimit(protocol.MaxFrameBytes)
	capture := messageSDKCapture{conn: conn}
	for _, ack := range seed {
		capture.record(t, ctx, ack)
	}

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 2*protocol.MaxFrameBytes+256)
	mustBridgeWrite(t, "READY")
	for range messageSDKLimit {
		phase = "transport"
		if !scanner.Scan() {
			t.Fatal("message SDK bridge transport ended before shutdown")
		}
		line := scanner.Text()
		if line == "QUIT" {
			phase = "shutdown"
			gateway.requireRedacted(t, phone, peer, f.issuerKey, testDSN)
			server.requireRedacted(t, phone, peer, f.issuerKey, testDSN)
			mustBridgeWrite(t, "DONE")
			return
		}
		if rawSeq, ok := strings.CutPrefix(line, "FRAME\t"); ok {
			phase = "frame"
			mustBridgeWrite(t, "FRAME\t"+hex.EncodeToString(capture.frame(t, rawSeq)))
			continue
		}
		forwardMessageSDKRequest(t, f, gateway, tokens, &capture, line)
	}
	t.Fatal("message SDK bridge command limit exceeded")
}

func forwardMessageSDKRequest(t *testing.T, f *fixture, gateway *process, tokens map[string]string, capture *messageSDKCapture, line string) {
	t.Helper()
	parts := strings.SplitN(line, "\t", 5)
	if len(parts) != 5 || parts[0] != "HTTP" {
		t.Fatal("invalid message SDK fixture command")
	}
	member, method, path, encoded := parts[1], parts[2], parts[3], parts[4]
	isSend := method == http.MethodPost && path == sendPath
	token, ok := tokens[member]
	limit := bridgeBodyLimit
	if isSend {
		limit = protocol.MaxFrameBytes
	}
	if !ok || len(encoded) > 2*limit || (!isSend && !allowedAccountSDKRoute(method, path)) {
		t.Fatal("invalid message SDK fixture request")
	}
	var body []byte
	if encoded != "" {
		var err error
		body, err = hex.DecodeString(encoded)
		if err != nil {
			t.Fatal("invalid message SDK fixture body encoding")
		}
	}
	reply := f.call(gateway, method, path, token, body, nil)
	responseLimit := bridgePageLimit
	if path == messageSDKDelta {
		responseLimit = syncmessage.MaxResponseBytes
	}
	if reply.err != nil || !reply.verified || len(reply.body) > responseLimit {
		t.Fatalf("message SDK fixture TLS response unavailable: status=%d verified=%t", reply.status, reply.verified)
	}
	if isSend && reply.status == http.StatusOK {
		capture.record(t, f.ctx, expectACK(t, reply))
	}
	mustBridgeWrite(t, fmt.Sprintf("HTTP\t%d\t%s", reply.status, hex.EncodeToString(reply.body)))
}

func (c *messageSDKCapture) record(t *testing.T, parent context.Context, ack protocol.Ack) {
	t.Helper()
	seq, ok := parseMessageSDKSequence(ack.ConversationSeq)
	if !ok || ack.ConversationID != bridgeRoom {
		t.Fatal("message SDK fixture received an ACK outside its conversation")
	}
	if seq <= c.latest {
		return
	}
	if seq != c.latest+1 {
		t.Fatalf("message SDK fixture ACK sequence %d did not follow %d", seq, c.latest)
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	kind, wire, err := c.conn.Read(ctx)
	if err != nil || kind != websocket.MessageText {
		t.Fatalf("message SDK fixture could not collect WSS sequence %d", seq)
	}
	frame, err := protocol.DecodeServerFrame(wire)
	if err != nil || frame.Message == nil {
		t.Fatalf("message SDK fixture WSS sequence %d is not a native message frame", seq)
	}
	msg := frame.Message
	if msg.ConversationSeq != ack.ConversationSeq || msg.ConversationID != ack.ConversationID ||
		msg.ServerMsgID != ack.ServerMsgID || msg.ClientMsgID != ack.ClientMsgID ||
		msg.SenderID != ack.SenderID || msg.ServerTime != ack.ServerTime {
		t.Fatalf("message SDK fixture WSS sequence %d did not match the committed ACK", seq)
	}
	c.frames[seq%messageSDKLimit] = messageSDKFrame{seq: seq, wire: wire}
	c.latest = seq
}

func (c *messageSDKCapture) frame(t *testing.T, rawSeq string) []byte {
	t.Helper()
	seq, ok := parseMessageSDKSequence(rawSeq)
	if !ok {
		t.Fatal("invalid message SDK fixture frame sequence")
	}
	frame := c.frames[seq%messageSDKLimit]
	if frame.seq != seq {
		t.Fatalf("message SDK fixture frame %d is unavailable", seq)
	}
	return frame.wire
}

func parseMessageSDKSequence(raw string) (uint64, bool) {
	seq, err := strconv.ParseUint(raw, 10, 63)
	return seq, err == nil && seq != 0 && strconv.FormatUint(seq, 10) == raw
}
