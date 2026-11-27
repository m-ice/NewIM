//go:build integration

package conversationsynchttp_test

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	bridgeAccount      = "sdk_bridge_alice"
	bridgePeer         = "sdk_bridge_bob"
	bridgeRoom         = "sdk_bridge_group_00"
	bridgeDepartedRoom = "sdk_bridge_departed"
	bridgeBodyLimit    = 32 * 1024
	bridgePageLimit    = 65_536
)

func TestAccountSDKBridge(t *testing.T) {
	phase := "database"
	defer func() {
		if t.Failed() {
			_, _ = fmt.Fprintln(os.Stdout, "FAILED\t"+phase)
		}
	}()
	f := openFixture(t)
	f.requireMigration(18)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	f.ctx = ctx

	settings := map[string]string{
		"NEWIM_CONVERSATION_READ_SYNC":        "1",
		"NEWIM_CONVERSATION_ACCOUNT_SETTINGS": "1",
		"NEWIM_READ_WATERMARK_HTTP":           "1",
	}
	phase = "gateway"
	keys := writeKeys(t, "sdk", "sdk")
	gateway := f.syncGateway(keys, settings)
	legacy := f.readSyncGateway(keys)
	phase = "server"
	server := f.server("1", map[string]string{
		"NEWIM_CONVERSATION_READ_SYNC":        "1",
		"NEWIM_CONVERSATION_ACCOUNT_SETTINGS": "1",
	})
	phase = "accounts"
	provision(t, f, server, bridgeAccount)
	provision(t, f, server, bridgePeer)
	phone := f.admit(bridgeAccount, "phone")
	tablet := f.admit(bridgeAccount, "tablet")
	peer := f.admit(bridgePeer, "phone")
	phase = "groups"
	for i := range 33 {
		id := fmt.Sprintf("sdk_bridge_group_%02d", i)
		createGroup(t, f, gateway, phone, id, nil, true)
	}
	createGroup(t, f, gateway, peer, bridgeDepartedRoom, []string{bridgeAccount}, true)
	phase = "messages"
	for i := range 3 {
		send(t, f, gateway, phone, bridgeRoom, fmt.Sprintf("sdk_bridge_message_%d", i), "fixture message")
	}

	tokens := map[string]string{"phone": phone, "tablet": tablet, "legacy-phone": phone}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 2*bridgeBodyLimit+256)
	mustBridgeWrite(t, "READY")
	for range 64 {
		phase = "transport"
		if !scanner.Scan() {
			t.Fatal("SDK fixture transport ended before shutdown")
		}
		switch line := scanner.Text(); line {
		case "QUIT":
			phase = "shutdown"
			gateway.requireRedacted(t, phone, tablet, peer, f.issuerKey, testDSN, "draft from the other device")
			legacy.requireRedacted(t, phone, tablet, peer, f.issuerKey, testDSN, "draft from the other device")
			mustBridgeWrite(t, "DONE")
			return
		case "RESET":
			// Ordinary membership mutation and an operator-issued reset let the live
			// gateway reject the retained checkpoint with a genuine expiry response.
			phase = "leave"
			leave(t, f, gateway, tablet, bridgeDepartedRoom)
			phase = "backfill"
			runAccountSDKBackfill(t, f, "SYNC_SOURCE_RESET", "reset", "--user", bridgeAccount)
			runAccountSDKBackfill(t, f, "SYNC_SOURCE_COMPLETE", "run", "--limit", "10")
			mustBridgeWrite(t, "RESET_OK")
		case "RESTORE":
			phase = "restore"
			send(t, f, legacy, phone, bridgeRoom, "sdk_bridge_restored", "new message")
			mustBridgeWrite(t, "RESTORE_OK")
		default:
			forwardAccountSDKRequest(t, f, gateway, legacy, tokens, line)
		}
	}
	t.Fatal("SDK fixture transport request limit exceeded")
}

func forwardAccountSDKRequest(t *testing.T, f *fixture, gateway, legacy *process, tokens map[string]string, line string) {
	t.Helper()
	parts := strings.SplitN(line, "\t", 5)
	if len(parts) != 5 || parts[0] != "HTTP" {
		t.Fatal("invalid SDK fixture command")
	}
	member, method, path, encoded := parts[1], parts[2], parts[3], parts[4]
	if member == "legacy-phone" {
		gateway = legacy
	}
	token, ok := tokens[member]
	if !ok || len(encoded) > 2*bridgeBodyLimit || !allowedAccountSDKRoute(method, path) {
		t.Fatal("invalid SDK fixture request")
	}
	var body []byte
	if encoded != "" {
		var err error
		body, err = hex.DecodeString(encoded)
		if err != nil {
			t.Fatal("invalid SDK fixture body encoding")
		}
	}
	reply := f.call(gateway, method, path, token, body, nil)
	if reply.err != nil || !reply.verified || len(reply.body) > bridgePageLimit {
		t.Fatalf("SDK fixture TLS response unavailable: status=%d verified=%t", reply.status, reply.verified)
	}
	mustBridgeWrite(t, fmt.Sprintf("HTTP\t%d\t%s", reply.status, hex.EncodeToString(reply.body)))
}

func allowedAccountSDKRoute(method, path string) bool {
	switch path {
	case bootstrapPath, deltaPath, "/api/v1/sync/messages", "/api/v1/conversations/" + bridgeRoom + "/read",
		"/api/v1/conversations/" + bridgeRoom + "/clear", "/api/v1/conversations/" + bridgeRoom + "/delete":
		return method == http.MethodPost
	case "/api/v1/conversations/" + bridgeRoom + "/settings":
		return method == http.MethodGet || method == http.MethodPatch
	default:
		return false
	}
}

func mustBridgeWrite(t *testing.T, line string) {
	t.Helper()
	if _, err := fmt.Fprintln(os.Stdout, line); err != nil {
		t.Fatal("could not send response over SDK fixture transport")
	}
}

func runAccountSDKBackfill(t *testing.T, f *fixture, want string, args ...string) {
	t.Helper()
	path := os.Getenv("NEWIM_SYNC_BACKFILL_BINARY")
	if !filepath.IsAbs(path) {
		t.Fatal("absolute fixture backfill binary required")
	}
	ctx, cancel := context.WithTimeout(f.ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = []string{
		"NEWIM_AUTH_DSN=" + testDSN,
		"NEWIM_AUTH_ALLOW_LOCAL_SOCKET=1",
		"NEWIM_CONVERSATION_PROJECTION=1",
		"NEWIM_CONVERSATION_READ_SYNC=1",
		"NEWIM_CONVERSATION_ACCOUNT_SETTINGS=1",
	}
	output, err := cmd.Output()
	if err != nil || string(output) != want+"\n" {
		t.Fatalf("SDK fixture backfill %s did not complete", args[0])
	}
}
