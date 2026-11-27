//go:build integration

package friend_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/m-ice/NewIM/server/friend"
	"github.com/m-ice/NewIM/server/user"
)

func TestFriendSecurity(t *testing.T) {
	t.Run("default-off-preserves-profile-http", testDisabled)
	t.Run("credential-request-and-owner-boundaries", testFriendAuthorization)
	t.Run("ip-and-account-budgets-are-independent", testFriendBudgets)
}

func testDisabled(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(11)
	token := f.admit("friend_security_disabled", "mobile")
	gateway := f.gateway("", map[string]string{"NEWIM_USER_PROFILE": "1"})
	reply := f.call(gateway, http.MethodPost, listPath, token, []byte("{}"), nil)
	if reply.err != nil || reply.status != http.StatusNotFound || !reply.verified {
		t.Fatalf("disabled friend route status=%d err=%v", reply.status, reply.err)
	}
	requireProfileView(t, f, gateway, token, "friend_security_disabled")
}

func testFriendAuthorization(t *testing.T) {
	f := openFixture(t)
	alice := f.admit("friend_security_alice", "mobile")
	bob := f.admit("friend_security_bob", "mobile")
	eve := f.admit("friend_security_eve", "mobile")
	gateway := f.gateway("1", nil)
	pending := sendApplication(t, f, gateway, alice, "a01", "friend_security_bob", "synthetic private application")
	actorBody := encode(t, friend.ResponseRequest{ApplicantID: "friend_security_alice", RequestID: "a01", Decision: "accept"})
	empty := []byte("{}")
	f.call(gateway, http.MethodPost, listPath, "", empty, nil).requireError(t, 401, "AUTH_INVALID_TOKEN")
	f.call(gateway, http.MethodPost, listPath, "", empty, http.Header{
		"Authorization": {"Bearer " + bob, "Bearer " + eve},
	}).requireError(t, 401, "AUTH_INVALID_TOKEN")
	f.call(gateway, http.MethodPost, respondPath, eve, actorBody, nil).requireError(t, 404, "FRIEND_NOT_FOUND")
	f.call(gateway, http.MethodPost, respondPath, alice, actorBody, nil).requireError(t, 400, "FRIEND_INVALID_INPUT")
	if got := applicationPage(t, f, gateway, eve, map[string]any{"direction": "incoming"}); len(got.Items) != 0 || got.LatestRevision != 0 {
		t.Fatal("unrelated account received application state")
	}

	tooLong := encode(t, map[string]string{"requestId": "a02", "peerId": "friend_security_bob", "message": strings.Repeat("中", 171)})
	cases := []struct {
		name, method, path string
		body               []byte
		headers            http.Header
		status             int
		code               string
	}{
		{"replayed-key-with-different-message", http.MethodPost, applyPath, []byte(`{"requestId":"a01","peerId":"friend_security_bob","message":"changed text"}`), nil, 409, "FRIEND_APPLICATION_CONFLICT"},
		{"another-key-for-pending-pair", http.MethodPost, applyPath, []byte(`{"requestId":"a02","peerId":"friend_security_bob"}`), nil, 409, "FRIEND_APPLICATION_PENDING"},
		{"peer-missing", http.MethodPost, applyPath, []byte(`{"requestId":"a03","peerId":"friend_security_absent"}`), nil, 404, "FRIEND_NOT_FOUND"},
		{"actor-override", http.MethodPost, applyPath, []byte(`{"requestId":"a03","peerId":"friend_security_bob","applicantId":"friend_security_eve"}`), nil, 400, "FRIEND_INVALID_INPUT"},
		{"multibyte-message-limit", http.MethodPost, applyPath, tooLong, nil, 400, "FRIEND_INVALID_INPUT"},
		{"empty-remark-omitted", http.MethodPost, remarkPath, []byte(`{"peerId":"friend_security_bob"}`), nil, 400, "FRIEND_INVALID_INPUT"},
		{"duplicate-json-field", http.MethodPost, remarkPath, []byte(`{"peerId":"friend_security_bob","remark":"first","remark":"second"}`), nil, 400, "FRIEND_INVALID_INPUT"},
		{"null-remark", http.MethodPost, remarkPath, []byte(`{"peerId":"friend_security_bob","remark":null}`), nil, 400, "FRIEND_INVALID_INPUT"},
		{"foreign-owner-field", http.MethodPost, listPath, []byte(`{"userId":"friend_security_bob"}`), nil, 400, "FRIEND_INVALID_INPUT"},
		{"oversized-page", http.MethodPost, listPath, []byte(`{"limit":101}`), nil, 400, "FRIEND_INVALID_INPUT"},
		{"incomplete-app-cursor", http.MethodPost, applicationsPath, []byte(`{"direction":"incoming","afterApplicantId":"friend_security_bob"}`), nil, 400, "FRIEND_INVALID_INPUT"},
		{"numeric-revision", http.MethodPost, changesPath, []byte(`{"afterRevision":0}`), nil, 400, "FRIEND_INVALID_INPUT"},
		{"noncanonical-revision", http.MethodPost, changesPath, []byte(`{"afterRevision":"00"}`), nil, 400, "FRIEND_INVALID_INPUT"},
		{"query-rejected", http.MethodPost, listPath + "?extra=1", empty, nil, 400, "FRIEND_INVALID_INPUT"},
		{"invalid-method", http.MethodGet, listPath, nil, nil, 405, "HTTP_METHOD_NOT_ALLOWED"},
		{"oversized-body", http.MethodPost, listPath, []byte(strings.Repeat(" ", 16*1024+1)), nil, 413, "HTTP_BODY_TOO_LARGE"},
		{"invalid-media", http.MethodPost, listPath, empty, http.Header{"Content-Type": {"text/plain"}}, 415, "HTTP_UNSUPPORTED_MEDIA_TYPE"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			f.call(gateway, test.method, test.path, alice, test.body, test.headers).requireError(t, test.status, test.code)
		})
	}
	_, err := f.auth.RevokeBearer(f.ctx, alice)
	must(t, err)
	f.call(gateway, http.MethodPost, removePath, alice, []byte(`{"peerId":"friend_security_bob"}`), nil).
		requireError(t, 401, "AUTH_INVALID_TOKEN")
	incoming := applicationPage(t, f, gateway, bob, map[string]any{"direction": "incoming"})
	if len(incoming.Items) != 1 || incoming.Items[0] != pending || incoming.LatestRevision != 1 {
		t.Fatal("a denied request modified the recipient's application or revisions")
	}
	changes := changePage(t, f, gateway, bob, 0, 5)
	if len(changes.Items) != 1 || changes.Items[0].Application == nil || *changes.Items[0].Application != pending {
		t.Fatal("a denied request added a recipient change")
	}
	if got := friendPage(t, f, gateway, bob, map[string]any{}); len(got.Items) != 0 {
		t.Fatal("a denied response created a relationship")
	}
	gateway.requireRedacted(t, alice, bob, eve, "synthetic private application", testDSN)
}

func testFriendBudgets(t *testing.T) {
	f := openFixture(t)
	first := f.admit("friend_budget_owner", "mobile")
	second := f.admit("friend_budget_owner", "desktop")
	other := f.admit("friend_budget_other", "mobile")
	accountLimited := f.gateway("1", map[string]string{
		"NEWIM_USER_PROFILE":                       "1",
		"NEWIM_FRIEND_ACCOUNT_ATTEMPTS_PER_MINUTE": "1",
	})
	waitRateWindow()
	friendPage(t, f, accountLimited, first, map[string]any{})
	f.call(accountLimited, http.MethodPost, listPath, second, []byte("{}"), nil).requireError(t, 429, "FRIEND_RATE_LIMITED")
	friendPage(t, f, accountLimited, other, map[string]any{})
	requireProfileView(t, f, accountLimited, first, "friend_budget_owner")
	requireWebSocket(t, f, accountLimited, first, http.StatusSwitchingProtocols)

	ipLimited := f.gateway("1", map[string]string{
		"NEWIM_FRIEND_IP_ATTEMPTS_PER_MINUTE":       "1",
		"NEWIM_GATEWAY_IP_ATTEMPTS_PER_MINUTE":      "1",
		"NEWIM_USER_PROFILE":                        "1",
		"NEWIM_USER_PROFILE_IP_ATTEMPTS_PER_MINUTE": "1",
	})
	waitRateWindow()
	requireWebSocket(t, f, ipLimited, first, http.StatusSwitchingProtocols)
	requireWebSocket(t, f, ipLimited, other, http.StatusTooManyRequests)
	requireProfileView(t, f, ipLimited, first, "friend_budget_owner")
	f.call(ipLimited, http.MethodGet, "/api/v1/users/me", other, nil, nil).requireError(t, 429, "USER_RATE_LIMITED")
	readResult[friend.EdgePage](t, f.call(ipLimited, http.MethodPost, listPath, first, []byte("{}"),
		http.Header{"X-Forwarded-For": {"192.0.2.1"}}), "page")
	f.call(ipLimited, http.MethodPost, listPath, other, []byte("{}"), http.Header{"X-Forwarded-For": {"192.0.2.2"}}).
		requireError(t, 429, "FRIEND_RATE_LIMITED")
}

func requireProfileView(t *testing.T, f *fixture, gateway *process, token, userID string) {
	t.Helper()
	if got := readResult[user.Profile](t, f.call(gateway, http.MethodGet, "/api/v1/users/me", token, nil, nil), "profile"); got.UserID != userID {
		t.Fatal("friend policy broke the profile route")
	}
}

func requireWebSocket(t *testing.T, f *fixture, gateway *process, raw string, want int) {
	t.Helper()
	conn, reply, err := websocket.Dial(f.ctx, "wss://"+strings.TrimPrefix(gateway.url, "https://")+"/api/v1/gateway",
		&websocket.DialOptions{HTTPClient: f.client, HTTPHeader: http.Header{"Authorization": {"Bearer " + raw}},
			Subprotocols: []string{"newim.v1"}})
	if conn != nil {
		defer conn.CloseNow()
	}
	status := 0
	if reply != nil {
		status = reply.StatusCode
		if err != nil && reply.Body != nil {
			defer reply.Body.Close()
		}
	}
	if status != want || (want == http.StatusSwitchingProtocols) != (conn != nil && err == nil) {
		t.Fatalf("WSS handshake status=%d err=%v want=%d", status, err, want)
	}
}

func waitRateWindow() {
	remaining := time.Until(time.Now().Truncate(time.Minute).Add(time.Minute))
	if remaining < 3*time.Second {
		time.Sleep(remaining + 20*time.Millisecond)
	}
}
