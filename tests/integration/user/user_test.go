//go:build integration

package user_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestUserCheck(t *testing.T) {
	f := openFixture(t)
	if version := f.migrationVersion(); version != 9 {
		t.Fatalf("legacy user fixture migration version=%d want=009", version)
	}
	const existing = "user_fixture_legacy"
	legacy := f.admit(existing, "legacy_device")
	var createdAt time.Time
	must(t, f.db.QueryRow(f.ctx, "SELECT created_at FROM newim.im_users WHERE user_id=$1", existing).Scan(&createdAt))
	f.applyProfileMigration()
	if version := f.migrationVersion(); version != 10 {
		t.Fatalf("upgraded user fixture migration version=%d want=010", version)
	}

	server := f.server("1")
	gateway := f.gateway("1", nil)
	raw := legacy.Token.RawToken()
	f.call(server, http.MethodGet, sessionPath, raw, nil, nil).requireOK(t, false)
	first := f.call(gateway, http.MethodGet, mePath, raw, nil, nil).requireProfile(t, true)
	if first != (profile{UserID: existing}) {
		t.Fatal("account registered before migration did not read as an empty profile")
	}

	want := profile{UserID: existing, Nickname: "搬迁前用户", FaceURL: "https://newim.example.test/avatar", Ex: "public legacy field"}
	changed := f.call(gateway, http.MethodPost, updatePath, raw,
		profileJSON(t, want, false), nil).requireProfile(t, true)
	if changed != want {
		t.Fatal("first profile update did not persist all fields")
	}
	want.FaceURL = ""
	changed = f.call(gateway, http.MethodPost, updatePath, raw, []byte(`{"faceUrl":""}`), nil).requireProfile(t, true)
	if changed != want {
		t.Fatal("explicit empty profile field did not clear while omitted fields remained")
	}
	recheck := f.call(server, http.MethodPost, provisionPath, f.appKey,
		[]byte(`{"userId":"user_fixture_legacy","nickname":"replacement attempt"}`), nil).requireProfile(t, false)
	if recheck != want {
		t.Fatal("provisioning an existing account changed committed profile fields")
	}
	f.call(server, http.MethodGet, sessionPath, raw, nil, nil).requireOK(t, false)
	var retained bool
	must(t, f.db.QueryRow(f.ctx, `SELECT u.created_at=$4 AND s.user_id=$1 AND s.device_id=$2
		FROM newim.im_users AS u JOIN newim.im_sessions AS s ON u.user_id=s.user_id
		WHERE s.session_id=$3`, existing, legacy.Binding.DeviceID, legacy.Binding.SessionID, createdAt).Scan(&retained))
	if !retained {
		t.Fatal("migration or repeated provisioning changed the existing account or session binding")
	}

	created := concurrentProvision(t, f, server, f.server("1"))
	admitted := f.call(server, http.MethodPost, "/api/v1/internal/session/admit", f.appKey,
		[]byte(`{"userId":"user_fixture_created","deviceId":"created_device","platform":"fixture"}`), nil)
	admitted.requireOK(t, false)
	var session struct {
		Version int    `json:"version"`
		Token   string `json:"token"`
	}
	must(t, json.Unmarshal(admitted.body, &session))
	if session.Version != 1 || session.Token == "" {
		t.Fatal("provisioned account could not use the existing device admission route")
	}
	if got := f.call(gateway, http.MethodGet, mePath, session.Token, nil, nil).requireProfile(t, true); got != created {
		t.Fatal("provisioned account's authenticated self view did not return its committed profile")
	}
	_, err := f.auth.RevokeBearer(f.ctx, session.Token)
	must(t, err)
	f.call(gateway, http.MethodPost, updatePath, session.Token, []byte(`{"nickname":"revoked write"}`), nil).
		requireError(t, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	after := f.call(gateway, http.MethodPost, lookupPath, raw, []byte(`{
		"userIds":["user_fixture_created","user_fixture_missing","user_fixture_legacy","user_fixture_created"]
	}`), nil)
	after.requireOK(t, true)
	var listed struct {
		Version  int       `json:"version"`
		Profiles []profile `json:"profiles"`
	}
	decoder := json.NewDecoder(bytes.NewReader(after.body))
	decoder.DisallowUnknownFields()
	must(t, decoder.Decode(&listed))
	if listed.Version != 1 || !slices.Equal(listed.Profiles, []profile{created, want}) {
		t.Fatalf("lookup returned wrong first-occurrence order or visibility: %d profiles", len(listed.Profiles))
	}
	empty := f.call(gateway, http.MethodPost, lookupPath, raw, []byte(`{"userIds":["user_fixture_missing"]}`), nil)
	empty.requireOK(t, true)
	var none struct {
		Version  int       `json:"version"`
		Profiles []profile `json:"profiles"`
	}
	must(t, json.Unmarshal(empty.body, &none))
	if none.Version != 1 || none.Profiles == nil || len(none.Profiles) != 0 {
		t.Fatal("lookup for absent accounts must return an empty list")
	}
	server.requireRedacted(t, f.appKey, raw, session.Token, testDSN, "replacement attempt", want.Ex)
	gateway.requireRedacted(t, f.appKey, raw, session.Token, testDSN, want.Ex)
}

func profileJSON(t *testing.T, value profile, includeUserID bool) []byte {
	t.Helper()
	fields := map[string]string{"nickname": value.Nickname, "faceUrl": value.FaceURL, "ex": value.Ex}
	if includeUserID {
		fields["userId"] = value.UserID
	}
	raw, err := json.Marshal(fields)
	must(t, err)
	return raw
}

func concurrentProvision(t *testing.T, f *fixture, left, right *process) profile {
	t.Helper()
	const n = 12
	const userID = "user_fixture_created"
	candidates := make(map[profile]bool, n)
	replies := make(chan response, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		candidate := profile{
			UserID:   userID,
			Nickname: fmt.Sprintf("candidate-%d", i),
			FaceURL:  fmt.Sprintf("https://newim.example.test/%d", i),
			Ex:       fmt.Sprintf("public candidate %d", i),
		}
		candidates[candidate] = true
		body := profileJSON(t, candidate, true)
		server := left
		if i%2 != 0 {
			server = right
		}
		go func() {
			<-start
			replies <- f.call(server, http.MethodPost, provisionPath, f.appKey, body, nil)
		}()
	}
	close(start)
	persisted := (<-replies).requireProfile(t, false)
	if !candidates[persisted] {
		t.Fatal("profile was not initialized atomically from one of the requests")
	}
	for i := 1; i < n; i++ {
		if got := (<-replies).requireProfile(t, false); got != persisted {
			t.Fatal("concurrent idempotent provision returned multiple profile variants")
		}
	}
	var users, profiles int
	must(t, f.db.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM newim.im_users WHERE user_id=$1),
		(SELECT count(*) FROM newim.im_user_profiles WHERE user_id=$1)`, userID).Scan(&users, &profiles))
	if users != 1 || profiles != 1 {
		t.Fatalf("concurrent provision did not keep one account and profile: accounts=%d profiles=%d", users, profiles)
	}
	return persisted
}

func TestUserSecurity(t *testing.T) {
	t.Run("default-off-preserves-session", testDefaultOff)
	t.Run("credentials-request-limits-and-identity", testAuthorizationAndBounds)
	t.Run("profile-account-ip-budgets-independent-of-wss", testProfileBudgets)
}

func testDefaultOff(t *testing.T) {
	f := openFixture(t)
	if version := f.migrationVersion(); version != 10 {
		t.Fatalf("security fixture migration version=%d want=010", version)
	}
	token := f.admit("user_default_off_fixture", "device_a").Token.RawToken()
	server, gateway := f.server(""), f.gateway("", nil)
	f.call(server, http.MethodGet, sessionPath, token, nil, nil).requireOK(t, false)
	f.call(server, http.MethodPost, provisionPath, f.appKey,
		[]byte(`{"userId":"user_default_off_created"}`), nil).requireError(t, http.StatusNotFound, "HTTP_ROUTE_NOT_FOUND")
	profile := f.call(gateway, http.MethodGet, mePath, token, nil, nil)
	if profile.err != nil || profile.status != http.StatusNotFound {
		t.Fatalf("default-off TLS profile status=%d err=%v", profile.status, profile.err)
	}
	var created int
	must(t, f.db.QueryRow(f.ctx, `SELECT count(*) FROM newim.im_users WHERE user_id='user_default_off_created'`).Scan(&created))
	if created != 0 {
		t.Fatal("disabled provisioning changed persistence")
	}
}

func testAuthorizationAndBounds(t *testing.T) {
	f := openFixture(t)
	const owner = "user_security_owner"
	const peer = "user_security_peer"
	ownerKey := f.admit(owner, "device_a").Token.RawToken()
	peerKey := f.admit(peer, "device_b").Token.RawToken()
	server, gateway := f.server("1"), f.gateway("1", nil)
	f.call(server, http.MethodGet, mePath, ownerKey, nil, nil).requireError(t, http.StatusNotFound, "HTTP_ROUTE_NOT_FOUND")
	result := f.call(gateway, http.MethodPost, provisionPath, f.appKey, []byte(`{"userId":"user_security_not_created"}`), nil)
	if result.err != nil || result.status != http.StatusNotFound {
		t.Fatalf("loopback-only route on TLS gateway status=%d err=%v", result.status, result.err)
	}
	f.call(server, http.MethodPost, provisionPath, ownerKey,
		[]byte(`{"userId":"user_security_not_created"}`), nil).requireError(t, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	f.call(gateway, http.MethodGet, mePath, f.appKey, nil, nil).requireError(t, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	f.call(gateway, http.MethodGet, mePath, "", nil, nil).requireError(t, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	f.call(gateway, http.MethodGet, mePath, "", nil, http.Header{
		"Authorization": {"Bearer " + ownerKey, "Bearer " + ownerKey},
	}).requireError(t, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	before := f.call(gateway, http.MethodPost, updatePath, ownerKey, []byte(`{"nickname":"owner-original"}`), nil).requireProfile(t, true)
	peerBefore := f.call(gateway, http.MethodPost, updatePath, peerKey, []byte(`{"nickname":"peer-original"}`), nil).requireProfile(t, true)

	tests := []struct {
		name   string
		method string
		path   string
		body   []byte
		header http.Header
		status int
		code   string
	}{
		{"cross-account-update", http.MethodPost, updatePath, []byte(`{"userId":"user_security_peer","nickname":"untrusted-target"}`), nil, 400, "USER_INVALID_INPUT"},
		{"duplicate-fields", http.MethodPost, updatePath, []byte(`{"nickname":"first","nickname":"second"}`), nil, 400, "USER_INVALID_INPUT"},
		{"null-field", http.MethodPost, updatePath, []byte(`{"nickname":null}`), nil, 400, "USER_INVALID_INPUT"},
		{"empty-patch", http.MethodPost, updatePath, []byte(`{}`), nil, 400, "USER_INVALID_INPUT"},
		{"multibyte-limit", http.MethodPost, updatePath, []byte(`{"nickname":"` + strings.Repeat("中", 43) + `"}`), nil, 400, "USER_INVALID_INPUT"},
		{"lookup-empty", http.MethodPost, lookupPath, []byte(`{"userIds":[]}`), nil, 400, "USER_INVALID_INPUT"},
		{"query-rejected", http.MethodGet, mePath + "?extra=1", nil, nil, 400, "USER_INVALID_INPUT"},
		{"get-body-rejected", http.MethodGet, mePath, []byte(`{}`), nil, 400, "USER_INVALID_INPUT"},
		{"unsupported-method", http.MethodGet, lookupPath, nil, nil, 405, "HTTP_METHOD_NOT_ALLOWED"},
		{"oversized-request", http.MethodPost, updatePath, []byte(strings.Repeat(" ", 16*1024+1)), nil, 413, "HTTP_BODY_TOO_LARGE"},
		{"unsupported-content-type", http.MethodPost, updatePath, []byte(`{"nickname":"plain"}`), http.Header{"Content-Type": {"text/plain"}}, 415, "HTTP_UNSUPPORTED_MEDIA_TYPE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f.call(gateway, test.method, test.path, ownerKey, test.body, test.header).requireError(t, test.status, test.code)
		})
	}
	ids := make([]string, 51)
	for n := range ids {
		ids[n] = fmt.Sprintf("user_fixture_%d", n)
	}
	body, err := json.Marshal(map[string][]string{"userIds": ids})
	must(t, err)
	f.call(gateway, http.MethodPost, lookupPath, ownerKey, body, nil).requireError(t, 400, "USER_INVALID_INPUT")

	var created int
	must(t, f.db.QueryRow(f.ctx, `SELECT count(*) FROM newim.im_users WHERE user_id='user_security_not_created'`).Scan(&created))
	if created != 0 {
		t.Fatal("wrong credential provisioned an account")
	}
	if got := f.call(gateway, http.MethodGet, mePath, ownerKey, nil, nil).requireProfile(t, true); got != before {
		t.Fatal("denied requests changed the requesting owner's profile")
	}
	if got := f.call(gateway, http.MethodGet, mePath, peerKey, nil, nil).requireProfile(t, true); got != peerBefore {
		t.Fatal("denied requests changed a different account")
	}
	server.requireRedacted(t, f.appKey, ownerKey, peerKey, testDSN)
	gateway.requireRedacted(t, f.appKey, ownerKey, peerKey, testDSN, "untrusted-target", "owner-original")
}

func testProfileBudgets(t *testing.T) {
	f := openFixture(t)
	const account = "user_budget_owner"
	first := f.admit(account, "first_device").Token.RawToken()
	second := f.admit(account, "second_device").Token.RawToken()
	other := f.admit("user_budget_other", "other_device").Token.RawToken()

	accountLimited := f.gateway("1", map[string]string{"NEWIM_USER_PROFILE_ACCOUNT_ATTEMPTS_PER_MINUTE": "1"})
	withinRateWindow()
	f.call(accountLimited, http.MethodGet, mePath, first, nil, nil).requireProfile(t, true)
	f.call(accountLimited, http.MethodGet, mePath, second, nil, nil).requireError(t, http.StatusTooManyRequests, "USER_RATE_LIMITED")
	f.call(accountLimited, http.MethodGet, mePath, other, nil, nil).requireProfile(t, true)
	f.requireWSS(t, accountLimited, first, http.StatusSwitchingProtocols)

	ipLimited := f.gateway("1", map[string]string{
		"NEWIM_USER_PROFILE_IP_ATTEMPTS_PER_MINUTE": "1",
		"NEWIM_GATEWAY_IP_ATTEMPTS_PER_MINUTE":      "1",
	})
	withinRateWindow()
	f.requireWSS(t, ipLimited, first, http.StatusSwitchingProtocols)
	f.requireWSS(t, ipLimited, other, http.StatusTooManyRequests)
	f.call(ipLimited, http.MethodGet, mePath, first, nil, http.Header{"X-Forwarded-For": {"192.0.2.1"}}).requireProfile(t, true)
	f.call(ipLimited, http.MethodGet, mePath, other, nil, http.Header{"X-Forwarded-For": {"192.0.2.2"}}).
		requireError(t, http.StatusTooManyRequests, "USER_RATE_LIMITED")
}

func withinRateWindow() {
	remainder := time.Until(time.Now().Truncate(time.Minute).Add(time.Minute))
	if remainder < 2*time.Second {
		time.Sleep(remainder + 20*time.Millisecond)
	}
}

func (f *fixture) requireWSS(t *testing.T, p *process, token string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	headers := http.Header{"Authorization": {"Bearer " + token}}
	conn, result, err := websocket.Dial(ctx, "wss://"+strings.TrimPrefix(p.url, "https://")+"/api/v1/gateway",
		&websocket.DialOptions{HTTPClient: f.client, HTTPHeader: headers, Subprotocols: []string{"newim.v1"}})
	if conn != nil {
		defer conn.CloseNow()
	}
	if result != nil && result.Body != nil && err != nil {
		defer result.Body.Close()
	}
	if result == nil || result.StatusCode != want || (want == http.StatusSwitchingProtocols) != (err == nil && conn != nil) {
		status := 0
		if result != nil {
			status = result.StatusCode
		}
		t.Fatalf("WSS handshake status=%d error=%v want=%d", status, err, want)
	}
}
