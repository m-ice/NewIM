//go:build integration

package auth_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/auth/session"
)

const (
	admissionRoute  = "/api/v1/internal/session/admit"
	admissionFlag   = "NEWIM_AUTH_DEVICE_ADMISSION"
	accountLimit    = "NEWIM_AUTH_MAX_DEVICES"
	platformLimit   = "NEWIM_AUTH_MAX_DEVICES_PER_PLATFORM"
	singleDevice    = "NEWIM_AUTH_SINGLE_DEVICE"
	legacyPrefix    = "admission_legacy"
	successorDevice = "admission_successor"
)

func TestAuthAdmission(t *testing.T) {
	if os.Getenv("NEWIM_SERVER_BINARY") == "" {
		t.Fatal("NEWIM_SERVER_BINARY is required")
	}
	switch os.Getenv("NEWIM_AUTH_PHASE") {
	case "prepare":
		if !t.Run("upgrade-legacy-and-guard-old-issuer", testAdmissionUpgrade) {
			return
		}
		t.Run("default-quotas-and-deterministic-order", testAdmissionDefaults)
		t.Run("two-process-distinct-and-same-device", testAdmissionConcurrent)
		t.Run("single-device-rollback-and-stale-issuer", testAdmissionRollback)
	case "restart":
		t.Run("restart-preserves-revocation-and-admitted-binding", testAdmissionRestart)
	default:
		t.Fatal("NEWIM_AUTH_PHASE must be prepare or restart")
	}
}

func testAdmissionUpgrade(t *testing.T) {
	f := openFixture(t)
	if got := f.scalarString("SELECT max(version)::text FROM newim_meta.migrations"); got != "7" {
		t.Fatalf("legacy migration head=%s want 7", got)
	}
	legacy := f.seedBinding(legacyPrefix)
	inactive := f.seedBinding("admission_never_admitted")
	f.now.Store(time.Now().Add(-time.Minute).Unix())
	service := f.service(&captureObserver{}, &repeatingReader{pattern: []byte{0x3c}}, nil)
	old := f.issue(service, legacy, time.Hour)
	if old.RawToken() != legacyAdmissionProof() {
		t.Fatal("legacy synthetic token fixture does not match restart proof")
	}
	key := newIssuerKey(t)
	file := writeIssuerKey(t, key, 0o600)
	offEnv := issuerEnvironment(file, "120")
	offEnv[admissionFlag] = ""
	off := startAuthServer(t, offEnv)
	t.Cleanup(func() {
		if off != nil {
			off.stop(t)
		}
	})
	off.waitHealthy(t)
	assertAdmissionBearer(t, off, old.RawToken(), http.StatusOK)
	requireIssuerError(t, callIssuer(t, off, http.MethodPost, admissionRoute, admissionBody(t, legacy.UserID, legacy.DeviceID, "ios"), "Bearer "+key),
		http.StatusNotFound, "HTTP_ROUTE_NOT_FOUND")
	off.stop(t)
	off = nil

	ledgerQuery := "SELECT string_agg(version::text||':'||sha256,',' ORDER BY version) FROM newim_meta.migrations WHERE version<=7"
	ledger := f.scalarString(ledgerQuery)
	original := issuerBindingState(f)
	digest := f.tokenDigestHex(old.TokenID())
	migration := os.Getenv("NEWIM_ADMISSION_MIGRATION_SQL")
	if migration == "" {
		t.Fatal("NEWIM_ADMISSION_MIGRATION_SQL is required")
	}
	commandCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	command := exec.CommandContext(commandCtx, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-d", f.dsn, "-f", migration)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("upgrade legacy admission database: %v output=%q", err, output)
	}
	if got := f.scalarString("SELECT max(version)::text FROM newim_meta.migrations"); got != "8" || f.scalarString(ledgerQuery) != ledger {
		t.Fatalf("admission upgrade changed the historical migration ledger, head=%s", got)
	}
	// Existing row values survive, and migration supplies no unverified platform or admission.
	if f.tokenDigestHex(old.TokenID()) != digest || f.sessionRevoked(legacy) ||
		!f.scalarBool("SELECT platform IS NULL AND last_active_at IS NULL FROM newim.im_devices WHERE user_id=$1 AND device_id=$2", legacy.UserID, legacy.DeviceID) ||
		!f.scalarBool("SELECT admitted_at IS NULL FROM newim.im_sessions WHERE session_id=$1", legacy.SessionID) {
		t.Fatal("migration rewrote the existing credential or supplied new metadata")
	}
	if upgraded := admissionLegacyProjection(f); upgraded != original {
		t.Fatal("migration changed original user, device or session fields")
	}
	expectIssuerConfigFailure(t, admissionEnvironment(file), false, key)
	f.sql(`UPDATE newim.im_devices SET platform='ios',last_active_at=(
		SELECT created_at FROM newim.im_auth_tokens WHERE token_id=$3) WHERE user_id=$1 AND device_id=$2`, legacy.UserID, legacy.DeviceID, old.TokenID())
	f.sql(`UPDATE newim.im_sessions SET admitted_at=(
		SELECT created_at FROM newim.im_auth_tokens WHERE token_id=$2) WHERE session_id=$1`, legacy.SessionID, old.TokenID())

	server := startAuthServer(t, admissionEnvironment(file))
	defer server.stop(t)
	server.waitHealthy(t)
	assertAdmissionBearer(t, server, old.RawToken(), http.StatusOK)
	newToken := requireIssuedToken(t, callIssuer(t, server, http.MethodPost, issuerRoute, issuerRequestBody(t, legacy), "Bearer "+key))
	inspectIssuedToken(t, f, legacy, newToken, 2*time.Minute)
	assertAdmissionBearer(t, server, newToken.Token, http.StatusOK)

	before := f.tokenCount()
	requireIssuerError(t, callIssuer(t, server, http.MethodPost, issuerRoute, issuerRequestBody(t, inactive), "Bearer "+key),
		http.StatusForbidden, "AUTH_SESSION_NOT_ELIGIBLE")
	requireIssuerError(t, callIssuer(t, server, http.MethodPost, admissionRoute, admissionBody(t, legacy.UserID, legacy.DeviceID, "ios"), "Bearer "+newToken.Token),
		http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
	requireIssuerError(t, callIssuer(t, server, http.MethodPost, admissionRoute, admissionBody(t, legacy.UserID, legacy.DeviceID, "android"), "Bearer "+key),
		http.StatusForbidden, "AUTH_SESSION_NOT_ELIGIBLE")
	requireIssuerError(t, callIssuer(t, server, http.MethodPost, admissionRoute, admissionBody(t, "admission_no_user", "device", "ios"), "Bearer "+key),
		http.StatusForbidden, "AUTH_SESSION_NOT_ELIGIBLE")
	requireIssuerError(t, callIssuer(t, server, http.MethodPost, admissionRoute, admissionBody(t, legacy.UserID, "invalid_platform", "IOS"), "Bearer "+key),
		http.StatusBadRequest, "AUTH_INVALID_INPUT")
	pretendedActivity := []byte(fmt.Sprintf(`{"userId":%q,"deviceId":%q,"platform":"ios","lastActiveAt":"2100-01-01T00:00:00Z"}`, legacy.UserID, legacy.DeviceID))
	requireIssuerError(t, callIssuer(t, server, http.MethodPost, admissionRoute, pretendedActivity, "Bearer "+key),
		http.StatusBadRequest, "AUTH_INVALID_INPUT")
	if f.tokenCount() != before || !f.scalarBool("SELECT platform='ios' FROM newim.im_devices WHERE user_id=$1 AND device_id=$2", legacy.UserID, legacy.DeviceID) {
		t.Fatal("rejected admission or legacy issue changed the durable binding")
	}
	assertIssuerRedaction(t, server, key, old.RawToken(), newToken.Token, hashToken(key), hashToken(newToken.Token), localDSN)
}

func testAdmissionDefaults(t *testing.T) {
	f := openFixture(t)
	key := newIssuerKey(t)
	env := admissionEnvironment(writeIssuerKey(t, key, 0o600))
	first, second := admissionPair(t, env)
	const full = "admission_default_account"
	seedAdmissionUser(f, full)
	original := make(map[string]admissionReply)
	for _, platform := range []string{"ios", "android", "web"} {
		size := 3
		if platform == "web" {
			size = 2
		}
		for i := 1; i <= size; i++ {
			device := fmt.Sprintf("%s-%d", platform, i)
			original[device] = admit(t, first, key, full, device, platform)
		}
	}
	expectDeviceCount(t, f, full, 8)
	events, err := f.repo.ListRevocationEvents(ctx, full, 0, 32)
	must(t, err)
	if len(events) != 0 {
		t.Fatal("within-limit devices did not coexist")
	}
	f.sql("UPDATE newim.im_devices SET last_active_at='2025-01-01T12:00:00Z' WHERE user_id=$1 AND device_id='android-1'", full)
	ninth := admit(t, second, key, full, "web-3", "web")
	expectDeviceCount(t, f, full, 8)
	expectAdmissionVictims(t, f, full, original["android-1"].binding())
	assertAdmissionBearer(t, first, original["android-1"].Token, http.StatusUnauthorized)
	for device, accepted := range original {
		if device != "android-1" {
			assertAdmissionBearer(t, second, accepted.Token, http.StatusOK)
		}
	}
	assertAdmissionBearer(t, first, ninth.Token, http.StatusOK)

	const platformAccount = "admission_default_platform"
	seedAdmissionUser(f, platformAccount)
	oldOther := admit(t, first, key, platformAccount, "other-old", "android")
	firstTie := admit(t, first, key, platformAccount, "ios-a", "ios")
	secondTie := admit(t, second, key, platformAccount, "ios-b", "ios")
	admit(t, first, key, platformAccount, "ios-c", "ios")
	f.sql(`UPDATE newim.im_devices SET last_active_at='2025-01-02T00:00:00Z'
		WHERE user_id=$1 AND device_id IN ('ios-a','ios-b')`, platformAccount)
	f.sql("UPDATE newim.im_devices SET last_active_at='2025-01-01T00:00:00Z' WHERE user_id=$1 AND device_id='other-old'", platformAccount)
	f.sql(`UPDATE newim.im_auth_tokens SET created_at=now()-interval '2 minutes', expires_at=now()-interval '1 minute'
		WHERE session_id=$1`, firstTie.SessionID)
	assertAdmissionBearer(t, first, firstTie.Token, http.StatusUnauthorized)
	expectDeviceCount(t, f, platformAccount, 4)
	admit(t, second, key, platformAccount, "ios-d", "ios")
	expectDeviceCount(t, f, platformAccount, 4)
	expectAdmissionVictims(t, f, platformAccount, firstTie.binding())
	assertAdmissionBearer(t, first, oldOther.Token, http.StatusOK)
	assertAdmissionBearer(t, first, secondTie.Token, http.StatusOK)
	assertAdmissionBearer(t, second, firstTie.Token, http.StatusUnauthorized)
}

func testAdmissionConcurrent(t *testing.T) {
	f := openFixture(t)
	key := newIssuerKey(t)
	env := admissionEnvironment(writeIssuerKey(t, key, 0o600))
	env[accountLimit], env[platformLimit] = "2", "2"
	first, second := admissionPair(t, env)

	const distinct = "admission_race_distinct"
	seedAdmissionUser(f, distinct)
	oldFirst := admit(t, first, key, distinct, "old-a", "ios")
	oldSecond := admit(t, second, key, distinct, "old-b", "ios")
	f.sql(`UPDATE newim.im_devices SET last_active_at='2025-01-01T00:00:00Z' WHERE user_id=$1`, distinct)
	left, right := overlappingAdmissions(t, f, first, second, key, distinct, "new-a", "new-b")
	expectDeviceCount(t, f, distinct, 2)
	expectAdmissionVictims(t, f, distinct, oldFirst.binding(), oldSecond.binding())
	assertAdmissionBearer(t, second, oldFirst.Token, http.StatusUnauthorized)
	assertAdmissionBearer(t, first, oldSecond.Token, http.StatusUnauthorized)
	assertAdmissionBearer(t, second, left.Token, http.StatusOK)
	assertAdmissionBearer(t, first, right.Token, http.StatusOK)

	const same = "admission_race_same"
	seedAdmissionUser(f, same)
	left, right = overlappingAdmissions(t, f, first, second, key, same, "same-device", "same-device")
	expectDeviceCount(t, f, same, 1)
	if left.SessionID != right.SessionID || left.Token == right.Token ||
		f.tokenCountForSession(left.SessionID) != 2 ||
		f.scalarString("SELECT count(*)::text FROM newim.im_sessions WHERE user_id=$1", same) != "1" {
		t.Fatal("same-device admission did not preserve one session and issue independent tokens")
	}
	expectAdmissionVictims(t, f, same)
	assertAdmissionBearer(t, first, left.Token, http.StatusOK)
	assertAdmissionBearer(t, second, right.Token, http.StatusOK)
}

func testAdmissionRollback(t *testing.T) {
	f := openFixture(t)
	legacy := newBinding(legacyPrefix)
	key := newIssuerKey(t)
	env := admissionEnvironment(writeIssuerKey(t, key, 0o600))
	env[singleDevice] = "1"
	first, second := admissionPair(t, env)
	assertAdmissionBearer(t, first, legacyAdmissionProof(), http.StatusOK)
	expectDeviceCount(t, f, legacy.UserID, 1)
	before := snapshotAdmissionAccount(f, legacy.UserID)
	tokenCount := f.tokenCountForSession(legacy.SessionID)

	// Reject COMMIT after the transaction has inserted the replacement token
	// and prepared its revocations. The empty reference table exists only here.
	f.sql("CREATE TABLE newim.test_admission_commit(token_id text PRIMARY KEY)")
	f.sql(`ALTER TABLE newim.im_auth_tokens ADD CONSTRAINT test_admission_commit_guard
		FOREIGN KEY (token_id) REFERENCES newim.test_admission_commit(token_id)
		DEFERRABLE INITIALLY DEFERRED NOT VALID`)
	rejected := callIssuer(t, first, http.MethodPost, admissionRoute, admissionBody(t, legacy.UserID, successorDevice, "android"), "Bearer "+key)
	requireIssuerError(t, rejected, http.StatusServiceUnavailable, "AUTH_UNAVAILABLE")
	if snapshotAdmissionAccount(f, legacy.UserID) != before {
		t.Fatal("failed COMMIT changed device, session, token or revocation event state")
	}
	assertAdmissionBearer(t, second, legacyAdmissionProof(), http.StatusOK)
	f.sql("ALTER TABLE newim.im_auth_tokens DROP CONSTRAINT test_admission_commit_guard")
	f.sql("DROP TABLE newim.test_admission_commit")

	// Replacement acquires the account lock and waits for the victim session.
	// The old issuer must queue on the same account, then see the revocation.
	lock := admissionLock(t, f)
	_, err := lock.Exec(ctx, "SELECT session_id FROM newim.im_sessions WHERE session_id=$1 FOR UPDATE", legacy.SessionID)
	must(t, err)
	replacement := asyncAdmission(admissionRequest(t, first, key, legacy.UserID, successorDevice, "android"))
	waitAdmissionLocks(t, f, "im_sessions", 1)
	stale := asyncAdmission(newIssuerRequest(t, second, http.MethodPost, issuerRoute, issuerRequestBody(t, legacy), "Bearer "+key))
	waitAdmissionLocks(t, f, "im_users", 1)
	must(t, lock.Rollback(ctx))
	success := requireAdmission(t, takeAdmission(t, replacement), legacy.UserID, successorDevice)
	requireIssuerError(t, takeAdmission(t, stale), http.StatusForbidden, "AUTH_SESSION_NOT_ELIGIBLE")
	expectDeviceCount(t, f, legacy.UserID, 1)
	if f.tokenCountForSession(legacy.SessionID) != tokenCount ||
		f.scalarString("SELECT count(*)::text FROM newim.im_sessions WHERE user_id=$1 AND device_id=$2", legacy.UserID, successorDevice) != "1" ||
		f.tokenCountForSession(success.SessionID) != 1 {
		t.Fatal("admission retry or stale issuer produced extra durable tokens or sessions")
	}
	inspectIssuedToken(t, f, success.binding(), success.issued(), 2*time.Minute)
	assertAdmissionBearer(t, first, legacyAdmissionProof(), http.StatusUnauthorized)
	assertAdmissionBearer(t, second, success.Token, http.StatusOK)
	events := expectAdmissionVictims(t, f, legacy.UserID, legacy)
	t.Logf("committed synthetic legacy revocation event %d before database restart", events[0].EventID)
	assertAuthRouteRedaction(t, first, "auth_admission", key, success.Token, legacyAdmissionProof(), hashToken(key), hashToken(success.Token), "test_admission_commit_guard", localDSN)
	assertIssuerRedaction(t, second, key, success.Token, legacyAdmissionProof(), hashToken(key), hashToken(success.Token), "test_admission_commit_guard", localDSN)
}

func testAdmissionRestart(t *testing.T) {
	f := openFixture(t)
	legacy := newBinding(legacyPrefix)
	key := newIssuerKey(t)
	env := admissionEnvironment(writeIssuerKey(t, key, 0o600))
	env[singleDevice] = "1"
	server := startAuthServer(t, env)
	defer server.stop(t)
	server.waitHealthy(t)

	expectDeviceCount(t, f, legacy.UserID, 1)
	events := expectAdmissionVictims(t, f, legacy.UserID, legacy)
	t.Logf("read synthetic legacy revocation event %d after database restart", events[0].EventID)
	assertAdmissionBearer(t, server, legacyAdmissionProof(), http.StatusUnauthorized)
	requireIssuerError(t, callIssuer(t, server, http.MethodPost, issuerRoute, issuerRequestBody(t, legacy), "Bearer "+key),
		http.StatusForbidden, "AUTH_SESSION_NOT_ELIGIBLE")
	successor := app.SessionBinding{
		UserID: legacy.UserID, DeviceID: successorDevice,
		SessionID: f.scalarString(`SELECT session_id FROM newim.im_sessions
			WHERE user_id=$1 AND device_id=$2 AND revoked_at IS NULL AND admitted_at IS NOT NULL`, legacy.UserID, successorDevice),
	}
	fresh := requireIssuedToken(t, callIssuer(t, server, http.MethodPost, issuerRoute, issuerRequestBody(t, successor), "Bearer "+key))
	id := inspectIssuedToken(t, f, successor, fresh, 2*time.Minute)
	body, status, _ := doSessionRequest(t, "http://"+server.api, "Bearer "+fresh.Token)
	if status != http.StatusOK {
		t.Fatalf("persisted admitted session was not usable after restart, status=%d", status)
	}
	assertIssuerSessionBinding(t, body, successor, id)
	again := expectAdmissionVictims(t, f, legacy.UserID, legacy)
	if again[0].EventID != events[0].EventID {
		t.Fatal("reissuing token changed the existing revocation event")
	}
}

func overlappingAdmissions(t *testing.T, f *fixture, first, second *runningServer, key, user, left, right string) (admissionReply, admissionReply) {
	t.Helper()
	lock := admissionLock(t, f)
	_, err := lock.Exec(ctx, "SELECT user_id FROM newim.im_users WHERE user_id=$1 FOR UPDATE", user)
	must(t, err)
	firstReply := asyncAdmission(admissionRequest(t, first, key, user, left, "ios"))
	secondReply := asyncAdmission(admissionRequest(t, second, key, user, right, "ios"))
	waitAdmissionLocks(t, f, "im_users", 2)
	must(t, lock.Rollback(ctx))
	return requireAdmission(t, takeAdmission(t, firstReply), user, left), requireAdmission(t, takeAdmission(t, secondReply), user, right)
}

func admissionLock(t *testing.T, f *fixture) pgx.Tx {
	t.Helper()
	conn, err := pgx.Connect(ctx, f.dsn)
	must(t, err)
	t.Cleanup(func() { _ = conn.Close(ctx) })
	tx, err := conn.Begin(ctx)
	must(t, err)
	t.Cleanup(func() { _ = tx.Rollback(ctx) })
	return tx
}

func waitAdmissionLocks(t *testing.T, f *fixture, relation string, want int) {
	t.Helper()
	deadline := time.Now().Add(950 * time.Millisecond)
	for time.Now().Before(deadline) {
		var count int
		must(t, f.db.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname=current_database() AND application_name='newim-auth-http'
			AND wait_event_type='Lock' AND query ILIKE $1 AND cardinality(pg_blocking_pids(pid))>0`,
			"%"+relation+"%").Scan(&count))
		if count >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not observe %d real auth processes blocked on %s", want, relation)
}

func admissionEnvironment(file string) map[string]string {
	env := issuerEnvironment(file, "120")
	env[admissionFlag], env[accountLimit], env[platformLimit], env[singleDevice] = "1", "", "", ""
	return env
}

func admissionPair(t *testing.T, env map[string]string) (*runningServer, *runningServer) {
	t.Helper()
	first := startAuthServer(t, env)
	t.Cleanup(func() { first.stop(t) })
	first.waitHealthy(t)
	second := startAuthServer(t, env)
	t.Cleanup(func() { second.stop(t) })
	second.waitHealthy(t)
	return first, second
}

func admissionBody(t *testing.T, user, device, platform string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]string{"userId": user, "deviceId": device, "platform": platform})
	must(t, err)
	return body
}

func admissionRequest(t *testing.T, server *runningServer, key, user, device, platform string) *http.Request {
	t.Helper()
	return newIssuerRequest(t, server, http.MethodPost, admissionRoute, admissionBody(t, user, device, platform), "Bearer "+key)
}

func admit(t *testing.T, server *runningServer, key, user, device, platform string) admissionReply {
	t.Helper()
	reply := sendIssuerRequest(t, admissionRequest(t, server, key, user, device, platform))
	return requireAdmission(t, reply, user, device)
}

type admissionReply struct {
	Version   int       `json:"version"`
	UserID    string    `json:"userId"`
	DeviceID  string    `json:"deviceId"`
	SessionID string    `json:"sessionId"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expiresAt"`
}

func (r admissionReply) binding() app.SessionBinding {
	return app.SessionBinding{UserID: r.UserID, DeviceID: r.DeviceID, SessionID: r.SessionID}
}

func (r admissionReply) issued() issuedResponse {
	return issuedResponse{Version: r.Version, Token: r.Token, ExpiresAt: r.ExpiresAt}
}

func requireAdmission(t *testing.T, response issuerResponse, user, device string) admissionReply {
	t.Helper()
	if response.status != http.StatusOK || response.headers.Get("Cache-Control") != "no-store" ||
		response.headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("admission status=%d, want noncacheable JSON success", response.status)
	}
	decoder := json.NewDecoder(bytes.NewReader(response.body))
	decoder.DisallowUnknownFields()
	var reply admissionReply
	must(t, decoder.Decode(&reply))
	if reply.Version != 1 || reply.UserID != user || reply.DeviceID != device || !reply.binding().Valid() ||
		reply.ExpiresAt.IsZero() || len(reply.Token) != app.RawTokenLen || !strings.HasPrefix(reply.Token, app.TokenPrefix) {
		t.Fatal("admission returned invalid token or mismatched device identity")
	}
	return reply
}

type admissionCallResult struct {
	reply issuerResponse
	err   error
}

func asyncAdmission(request *http.Request) <-chan admissionCallResult {
	done := make(chan admissionCallResult, 1)
	go func() {
		response, err := (&http.Client{Timeout: 8 * time.Second}).Do(request)
		if err != nil {
			done <- admissionCallResult{err: err}
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		done <- admissionCallResult{issuerResponse{body, response.StatusCode, response.Header}, err}
	}()
	return done
}

func takeAdmission(t *testing.T, done <-chan admissionCallResult) issuerResponse {
	t.Helper()
	result := <-done
	must(t, result.err)
	return result.reply
}

func legacyAdmissionProof() string {
	return app.TokenPrefix + strings.Repeat("3c", 16) + "_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x3c}, 32))
}

func seedAdmissionUser(f *fixture, user string) {
	f.sql("INSERT INTO newim.im_users(user_id) VALUES($1)", user)
}

func expectDeviceCount(t *testing.T, f *fixture, user string, want int) {
	t.Helper()
	var count int
	must(t, f.db.QueryRow(ctx, `SELECT count(DISTINCT device_id) FROM newim.im_sessions
		WHERE user_id=$1 AND revoked_at IS NULL AND admitted_at IS NOT NULL`, user).Scan(&count))
	if count != want {
		t.Fatalf("active devices=%d want %d", count, want)
	}
}

func expectAdmissionVictims(t *testing.T, f *fixture, user string, want ...app.SessionBinding) []app.SessionRevocationEvent {
	t.Helper()
	events, err := f.repo.ListRevocationEvents(ctx, user, 0, 32)
	must(t, err)
	if len(events) != len(want) {
		t.Fatalf("revocation events=%d want %d", len(events), len(want))
	}
	for i, expected := range want {
		event := events[i]
		if event.Binding != expected || event.Reason != "device_limit" || event.RevokedAt.IsZero() ||
			!f.sessionRevoked(expected) ||
			f.scalarBool(`SELECT EXISTS (SELECT 1 FROM newim.im_auth_tokens WHERE session_id=$1 AND revoked_at IS NULL)`, expected.SessionID) {
			t.Fatalf("revocation %d omitted its expected reason, binding or token invalidation", i)
		}
	}
	if len(events) != 0 {
		tail, err := f.repo.ListRevocationEvents(ctx, user, events[len(events)-1].EventID, 32)
		must(t, err)
		if len(tail) != 0 {
			t.Fatal("revocation cursor repeated an already-read event")
		}
	}
	return events
}

func assertAdmissionBearer(t *testing.T, server *runningServer, token string, want int) {
	t.Helper()
	if _, status, _ := doSessionRequest(t, "http://"+server.api, "Bearer "+token); status != want {
		t.Fatalf("session status=%d want %d", status, want)
	}
}

func admissionLegacyProjection(f *fixture) string {
	return f.scalarString(`SELECT
		(SELECT md5(COALESCE(string_agg(t::text,',' ORDER BY user_id),'')) FROM newim.im_users AS t) || '/' ||
		(SELECT md5(COALESCE(string_agg(ROW(user_id,device_id,created_at)::text,',' ORDER BY user_id,device_id),'')) FROM newim.im_devices) || '/' ||
		(SELECT md5(COALESCE(string_agg(ROW(session_id,user_id,device_id,created_at,revoked_at)::text,',' ORDER BY session_id),'')) FROM newim.im_sessions)`)
}

func snapshotAdmissionAccount(f *fixture, user string) string {
	return f.scalarString(`SELECT
		(SELECT md5(COALESCE(string_agg(t::text,',' ORDER BY device_id),'')) FROM newim.im_devices AS t WHERE user_id=$1) || '/' ||
		(SELECT md5(COALESCE(string_agg(t::text,',' ORDER BY session_id),'')) FROM newim.im_sessions AS t WHERE user_id=$1) || '/' ||
		(SELECT md5(COALESCE(string_agg(t::text,',' ORDER BY token_id),''))
			FROM newim.im_auth_tokens AS t JOIN newim.im_sessions AS s USING(session_id) WHERE s.user_id=$1) || '/' ||
		(SELECT md5(COALESCE(string_agg(t::text,',' ORDER BY event_id),'')) FROM newim.im_session_revocation_events AS t WHERE user_id=$1)`, user)
}
