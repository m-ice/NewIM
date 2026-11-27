//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/webhook"
)

const (
	managementWorkerDSN = "host=/var/run/postgresql user=newim_test dbname=newim_test sslmode=disable"
	managementAuthDSN   = "host=/var/run/postgresql user=newim_test dbname=newim_webhook_auth sslmode=disable"
	managementMaster    = "0123456789abcdef0123456789abcdef"
	managementPrefix    = "/api/v1/webhooks/"
	managementURL       = "https://webhook.fixture.invalid/receive"
	managementUpdated   = "https://webhook.fixture.invalid/updated"
)

type managementFixture struct {
	ctx    context.Context
	db     *pgx.Conn
	authDB *pgx.Conn
	token  string
	issuer string
	env    []string
}

func openManagementFixture(t *testing.T) *managementFixture {
	t.Helper()
	ctx := t.Context()
	db, err := pgx.Connect(ctx, managementWorkerDSN)
	managementMust(t, err)
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	authDB, err := pgx.Connect(ctx, managementAuthDSN)
	managementMust(t, err)
	t.Cleanup(func() { _ = authDB.Close(context.Background()) })

	dir := t.TempDir()
	token, issuer := "whm_"+strings.Repeat("a", 64), strings.Repeat("b", 64)
	tokenFile, issuerFile := filepath.Join(dir, "management.key"), filepath.Join(dir, "issuer.key")
	managementMust(t, os.WriteFile(tokenFile, []byte(token+"\n"), 0o600))
	managementMust(t, os.WriteFile(issuerFile, []byte(issuer), 0o600))
	return &managementFixture{ctx: ctx, db: db, authDB: authDB, token: token, issuer: issuer, env: []string{
		"NEWIM_WEBHOOK_DSN=" + managementWorkerDSN,
		"NEWIM_WEBHOOK_ALLOW_LOCAL_SOCKET=1",
		"NEWIM_WEBHOOK_MASTER_KEY_B64=" + base64.StdEncoding.EncodeToString([]byte(managementMaster)),
		webhookManagementTokenFileEnv + "=" + tokenFile,
		"NEWIM_AUTH_DSN=" + managementAuthDSN,
		"NEWIM_AUTH_ALLOW_LOCAL_SOCKET=1",
		"NEWIM_AUTH_ISSUER_TOKEN_FILE=" + issuerFile,
		"NEWIM_AUTH_ISSUER_TTL_SECONDS=120",
		"NEWIM_AUTH_DEVICE_ADMISSION=1",
		"NEWIM_AUTH_ACCOUNT_REVOCATION=1",
		"NEWIM_USER_PROFILE=1",
		"NEWIM_MESSAGE_HTTP=1",
		"NEWIM_PRIVATE_MESSAGE_POLICY=1",
	}}
}

type managementProcess struct {
	cmd    *exec.Cmd
	done   chan error
	url    string
	stdout *bytes.Buffer
	stderr *bytes.Buffer
}

func (f *managementFixture) start(t *testing.T, overrides ...string) *managementProcess {
	t.Helper()
	apiAddr, opsAddr := twoFreeAddrs(t)
	cmd, stdout, stderr := helperCommandEnv(t, append(append([]string(nil), f.env...), overrides...),
		"--api-addr", apiAddr, "--ops-addr", opsAddr)
	managementMust(t, cmd.Start())
	p := &managementProcess{cmd: cmd, done: make(chan error, 1), url: "http://" + apiAddr, stdout: stdout, stderr: stderr}
	go func() { p.done <- cmd.Wait() }()
	t.Cleanup(func() { p.stop(t) })
	managementMust(t, waitForStatus(p.url+"/api/v1/health", http.StatusOK))
	managementMust(t, waitForStatus("http://"+opsAddr+"/ready", http.StatusOK))
	return p
}

func (p *managementProcess) stop(t *testing.T) {
	t.Helper()
	if p.cmd == nil {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-p.done:
		p.cmd = nil
		managementMust(t, err)
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		p.cmd = nil
		t.Fatal("webhook management fixture did not shut down")
	}
}

func (p *managementProcess) requireRedacted(t *testing.T, values ...string) {
	t.Helper()
	p.stop(t)
	output := p.stdout.String() + p.stderr.String()
	for _, value := range values {
		if strings.Contains(output, value) {
			t.Fatal("webhook management process published private configuration")
		}
	}
}

type managementReply struct {
	status int
	header http.Header
	body   []byte
	err    error
}

func (p *managementProcess) request(ctx context.Context, method, path, credential string, body []byte) managementReply {
	req, err := http.NewRequestWithContext(ctx, method, p.url+path, bytes.NewReader(body))
	if err != nil {
		return managementReply{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	if credential != "" {
		req.Header.Set("Authorization", "Bearer "+credential)
	}
	client := &http.Client{Timeout: 4 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return managementReply{err: err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024+1))
	return managementReply{status: resp.StatusCode, header: resp.Header, body: data, err: err}
}

func (f *managementFixture) post(t *testing.T, p *managementProcess, route string, fields map[string]string) managementReply {
	t.Helper()
	body, err := json.Marshal(fields)
	managementMust(t, err)
	return p.request(f.ctx, http.MethodPost, managementPrefix+route, f.token, body)
}

func (r managementReply) require(t *testing.T, status int) {
	t.Helper()
	if r.err != nil || r.status != status {
		t.Fatalf("webhook management response status=%d, want %d; transport error=%v", r.status, status, r.err)
	}
}

func (r managementReply) requireError(t *testing.T, status int, code string) {
	t.Helper()
	r.require(t, status)
	want := "{\"error\":{\"code\":\"" + code + "\"}}"
	if strings.TrimSpace(string(r.body)) != want {
		t.Fatalf("webhook management returned an unexpected error envelope, want %s", code)
	}
}

func managementDecode[T any](t *testing.T, reply managementReply, status int) T {
	t.Helper()
	reply.require(t, status)
	if reply.header.Get("Cache-Control") != "no-store" {
		t.Fatal("webhook management response is cacheable")
	}
	var result T
	decoder := json.NewDecoder(bytes.NewReader(reply.body))
	decoder.DisallowUnknownFields()
	managementMust(t, decoder.Decode(&result))
	return result
}

func managementMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func managementCreate(id, request string) map[string]string {
	return map[string]string{"destinationId": id, "requestId": request, "url": managementURL, "keyId": "initial", "secret": strings.Repeat("1", 64)}
}

func managementEndpoint(t *testing.T, r managementReply, code int, id, state, key string, revision int64) app.EndpointInfo {
	t.Helper()
	got := managementDecode[app.EndpointInfo](t, r, code)
	if got.DestinationID != id || got.Status != state || got.KeyID != key ||
		got.Revision == nil || *got.Revision != revision || got.CreatedAt.IsZero() ||
		got.UpdatedAt.Before(got.CreatedAt) || (got.RevokedAt != nil) != (state == "revoked") {
		t.Fatal("webhook management endpoint result differs from committed state")
	}
	return got
}

func TestWebhookManagement(t *testing.T) {
	f := openManagementFixture(t)
	p := f.start(t)
	// All ten previously supported trusted routes must still coexist with management.
	for _, path := range []string{
		"/api/v1/session/tokens/current", "/api/v1/internal/session/token", "/api/v1/internal/session/admit",
		"/api/v1/internal/session/revoke-account", "/api/v1/internal/users/provision", "/api/v1/messages",
		"/api/v1/conversations/direct", "/api/v1/relationships/block", "/api/v1/relationships/unblock",
	} {
		p.request(f.ctx, http.MethodGet, path, "", nil).require(t, http.StatusMethodNotAllowed)
	}
	p.request(f.ctx, http.MethodGet, "/api/v1/session", "", nil).require(t, http.StatusUnauthorized)

	const a, b = "management_a", "management_b"
	managementEndpoint(t, f.post(t, p, "endpoints/create", managementCreate(a, "create_a")), 201, a, "active", "initial", 1)
	managementEndpoint(t, f.post(t, p, "endpoints/create", managementCreate(b, "create_b")), 201, b, "active", "initial", 1)
	page := managementDecode[app.EndpointPage](t, p.request(f.ctx, http.MethodPost, managementPrefix+"endpoints/list", f.token, []byte(`{"limit":1}`)), 200)
	if len(page.Items) != 1 || page.Items[0].DestinationID != a || page.NextAfterID != a {
		t.Fatal("webhook endpoint first page did not expose its continuation")
	}
	page = managementDecode[app.EndpointPage](t, p.request(f.ctx, http.MethodPost, managementPrefix+"endpoints/list", f.token, []byte(`{"limit":1,"afterId":"management_a"}`)), 200)
	if len(page.Items) != 1 || page.Items[0].DestinationID != b || page.NextAfterID != "" {
		t.Fatal("webhook endpoint continuation did not finish at the second endpoint")
	}

	f.seedDelivery(t, a, "delivery_a", 1, false)
	f.seedDelivery(t, a, "delivery_b", 1, true)
	managementEndpoint(t, f.post(t, p, "endpoints/update", map[string]string{
		"destinationId": a, "requestId": "update_a", "expectedRevision": "1", "url": managementUpdated,
	}), 200, a, "active", "initial", 2)
	managementEndpoint(t, f.post(t, p, "endpoints/rotate", map[string]string{
		"destinationId": a, "requestId": "rotate_a", "expectedRevision": "2", "keyId": "replacement", "secret": strings.Repeat("2", 64),
	}), 200, a, "active", "replacement", 3)
	managementEndpoint(t, f.post(t, p, "endpoints/get", map[string]string{"destinationId": a}), 200, a, "active", "replacement", 3)
	f.checkRevision(t, a, 1, managementURL, "initial", "1")
	f.checkRevision(t, a, 2, managementUpdated, "initial", "1")
	f.checkRevision(t, a, 3, managementUpdated, "replacement", "2")

	first := managementDecode[app.DeliveryPage](t, p.request(f.ctx, http.MethodPost, managementPrefix+"deliveries/list", f.token,
		[]byte(`{"destinationId":"management_a","limit":1}`)), 200)
	if len(first.Items) != 1 || first.Items[0].DeliveryID != "delivery_a" || first.NextAfterID != "delivery_a" ||
		first.Items[0].EndpointRevision == nil || *first.Items[0].EndpointRevision != 1 {
		t.Fatal("delivery pagination changed the frozen endpoint revision")
	}
	last := managementDecode[app.DeliveryPage](t, p.request(f.ctx, http.MethodPost, managementPrefix+"deliveries/list", f.token,
		[]byte(`{"destinationId":"management_a","limit":1,"afterId":"delivery_a"}`)), 200)
	if len(last.Items) != 1 || last.Items[0].DeliveryID != "delivery_b" || last.NextAfterID != "" {
		t.Fatal("delivery pagination did not return its second and final item")
	}
	f.post(t, p, "deliveries/replay", map[string]string{"requestId": "stale_replay", "destinationId": a, "deliveryId": "delivery_a"}).
		requireError(t, 409, string(app.CodeManagementNotReplayable))

	managementEndpoint(t, f.post(t, p, "endpoints/revoke", map[string]string{
		"destinationId": a, "requestId": "revoke_a", "expectedRevision": "3",
	}), 200, a, "revoked", "replacement", 3)
	f.requireCancelled(t, a, "delivery_b")
	f.post(t, p, "endpoints/create", managementCreate(a, "recreate_a")).requireError(t, 409, string(app.CodeManagementConflict))
	managementEndpoint(t, f.post(t, p, "endpoints/get", map[string]string{"destinationId": a}), 200, a, "revoked", "replacement", 3)
	all := managementDecode[app.EndpointPage](t, f.post(t, p, "endpoints/list", map[string]string{}), 200)
	if len(all.Items) != 2 || all.Items[0].DestinationID != a || all.Items[0].Status != "revoked" {
		t.Fatal("revoking an endpoint removed its history from the directory")
	}

	const audit = `SELECT COALESCE(string_agg(actor||':'||action||':'||revision,',' ORDER BY audit_id),'')
FROM newim.im_webhook_management_audit WHERE destination_id=$1`
	var history string
	managementMust(t, f.db.QueryRow(f.ctx, audit, a).Scan(&history))
	if history != "deployment:create:1,deployment:update:2,deployment:rotate:3,deployment:revoke:3" {
		t.Fatal("webhook endpoint audit does not match successful writes")
	}
	managementMust(t, f.authDB.QueryRow(f.ctx, audit, a).Scan(&history))
	if history != "" {
		t.Fatal("webhook management wrote to the auth database")
	}
	p.requireRedacted(t, f.token, f.issuer, managementMaster, managementURL, managementUpdated, strings.Repeat("1", 64), strings.Repeat("2", 64))
}

func (f *managementFixture) checkRevision(t *testing.T, id string, revision int64, url, key, hexDigit string) {
	t.Helper()
	material := app.SecretMaterial{DestinationID: id, Revision: revision}
	managementMust(t, f.db.QueryRow(f.ctx, `SELECT url,key_id,secret_nonce,secret_ciphertext
FROM newim.im_webhook_endpoint_revisions WHERE destination_id=$1 AND revision=$2`, id, revision).
		Scan(&material.URL, &material.KeyID, &material.Nonce, &material.Ciphertext))
	resolver, err := app.NewLocalSecretResolver([]byte(managementMaster))
	managementMust(t, err)
	secret, err := resolver.Resolve(f.ctx, material)
	managementMust(t, err)
	expected, err := hex.DecodeString(strings.Repeat(hexDigit, 64))
	managementMust(t, err)
	if material.URL != url || material.KeyID != key || !bytes.Equal(secret, expected) {
		t.Fatal("frozen endpoint revision did not retain its signing material")
	}
}

func (f *managementFixture) seedDelivery(t *testing.T, destination, delivery string, revision int64, pending bool) {
	t.Helper()
	tx, err := f.db.Begin(f.ctx)
	managementMust(t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, `
INSERT INTO newim.im_users(user_id) VALUES ('management_fixture_author') ON CONFLICT DO NOTHING;
INSERT INTO newim.im_conversations(conversation_id,kind) VALUES ('management_fixture_room','group') ON CONFLICT DO NOTHING;
INSERT INTO newim.im_conversation_members(conversation_id,user_id)
VALUES ('management_fixture_room','management_fixture_author') ON CONFLICT DO NOTHING;`)
	managementMust(t, err)
	event := "event_" + delivery
	_, err = tx.Exec(f.ctx, `SELECT newim.persist_message(
'management_fixture_author',$1,'management_fixture_room',$2,'text',1,0,$3,$4)`,
		"client_"+delivery, "message_"+delivery, []byte(`{"text":"webhook fixture"}`), event)
	managementMust(t, err)
	_, err = tx.Exec(f.ctx, `UPDATE newim.im_outbox_events SET webhook_fanout_at=clock_timestamp() WHERE event_id=$1`, event)
	managementMust(t, err)
	if pending {
		_, err = tx.Exec(f.ctx, `
INSERT INTO newim.im_webhook_deliveries(delivery_id,event_id,destination_id,endpoint_revision,next_attempt_at)
VALUES ($1,$2,$3,$4,clock_timestamp()+interval '1 hour')`, delivery, event, destination, revision)
	} else {
		_, err = tx.Exec(f.ctx, `
INSERT INTO newim.im_webhook_deliveries(delivery_id,event_id,destination_id,endpoint_revision,
status,attempts,last_http_status,last_error_code,next_attempt_at,completed_at)
VALUES ($1,$2,$3,$4,'dead_letter',3,410,'WEBHOOK_HTTP_PERMANENT',NULL,clock_timestamp())`,
			delivery, event, destination, revision)
	}
	managementMust(t, err)
	managementMust(t, tx.Commit(f.ctx))
}

func (f *managementFixture) requireCancelled(t *testing.T, destination, delivery string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var status string
		var attempts int
		managementMust(t, f.db.QueryRow(ctx, `SELECT status,attempts FROM newim.im_webhook_deliveries
WHERE destination_id=$1 AND delivery_id=$2`, destination, delivery).Scan(&status, &attempts))
		if status == "cancelled" && attempts == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("worker did not cancel the revoked endpoint's scheduled delivery")
		case <-ticker.C:
		}
	}
}

func TestWebhookManagementSecurity(t *testing.T) {
	f := openManagementFixture(t)
	t.Run("deployment-credential-and-listener", func(t *testing.T) {
		off := f.start(t, webhookManagementTokenFileEnv+"=")
		off.request(f.ctx, http.MethodPost, managementPrefix+"endpoints/list", f.token, []byte("{}")).
			requireError(t, http.StatusNotFound, "HTTP_ROUTE_NOT_FOUND")
		off.stop(t)

		apiAddr, opsAddr := twoFreeAddrs(t)
		tokenFile := filepath.Join(t.TempDir(), "invalid-permissions")
		managementMust(t, os.WriteFile(tokenFile, []byte(f.token), 0o644))
		managementStartupRejected(t, apiAddr, opsAddr,
			append(append([]string(nil), f.env...), webhookManagementTokenFileEnv+"="+tokenFile), app.CodeInvalidConfig)
		managementStartupRejected(t, apiAddr, opsAddr,
			append(append([]string(nil), f.env...), "NEWIM_WEBHOOK_DSN="), app.CodeInvalidConfig)
		_, port, err := net.SplitHostPort(apiAddr)
		managementMust(t, err)
		webhookOnly := filteredEnv(f.env,
			"NEWIM_AUTH_DSN", "NEWIM_AUTH_ALLOW_LOCAL_SOCKET", "NEWIM_AUTH_ISSUER_TOKEN_FILE",
			"NEWIM_AUTH_ISSUER_TTL_SECONDS", "NEWIM_AUTH_DEVICE_ADMISSION", "NEWIM_AUTH_ACCOUNT_REVOCATION",
			"NEWIM_USER_PROFILE", "NEWIM_MESSAGE_HTTP", "NEWIM_PRIVATE_MESSAGE_POLICY")
		managementStartupRejected(t, net.JoinHostPort("0.0.0.0", port), opsAddr, webhookOnly, app.CodeInvalidConfig)

		on := f.start(t)
		for _, credential := range []string{"", f.issuer} {
			rejected := on.request(f.ctx, http.MethodPost, managementPrefix+"endpoints/list", credential, []byte("{}"))
			rejected.requireError(t, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
			if rejected.header.Get("WWW-Authenticate") != `Bearer realm="newim-webhook-management"` {
				t.Fatal("webhook management omitted its own authentication challenge")
			}
		}
		on.request(f.ctx, http.MethodPost, "/api/v1/internal/session/token", f.token, []byte("{}")).
			requireError(t, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
		session := f.admitSession(t, on)
		on.request(f.ctx, http.MethodGet, "/api/v1/session", session, nil).require(t, http.StatusOK)
		on.request(f.ctx, http.MethodPost, managementPrefix+"endpoints/list", session, []byte("{}")).
			requireError(t, http.StatusUnauthorized, "AUTH_INVALID_TOKEN")
		on.requireRedacted(t, f.token, f.issuer, session)
	})
	t.Run("write-rollback-and-replay", func(t *testing.T) {
		testManagementWrites(t, f)
	})
}

func (f *managementFixture) admitSession(t *testing.T, p *managementProcess) string {
	t.Helper()
	_, err := f.authDB.Exec(f.ctx, "INSERT INTO newim.im_users(user_id) VALUES ('management_session_user')")
	managementMust(t, err)
	reply := p.request(f.ctx, http.MethodPost, "/api/v1/internal/session/admit", f.issuer,
		[]byte(`{"userId":"management_session_user","deviceId":"management_session_device","platform":"ios"}`))
	reply.require(t, http.StatusOK)
	var issued struct {
		Token string `json:"token"`
	}
	managementMust(t, json.Unmarshal(reply.body, &issued))
	if issued.Token == "" {
		t.Fatal("fixture admission did not return a session token")
	}
	return issued.Token
}

func managementStartupRejected(t *testing.T, apiAddr, opsAddr string, env []string, code app.Code) {
	t.Helper()
	cmd, stdout, stderr := helperCommandEnv(t, env, "--api-addr", apiAddr, "--ops-addr", opsAddr)
	managementMust(t, cmd.Start())
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	select {
	case err := <-wait:
		exit, ok := err.(*exec.ExitError)
		if !ok || exit.ExitCode() != 2 || strings.TrimSpace(stderr.String()) != string(code) ||
			strings.Contains(stdout.String(), "listening") {
			t.Fatal("invalid webhook management configuration did not fail startup")
		}
	case <-time.After(7 * time.Second):
		_ = cmd.Process.Kill()
		<-wait
		t.Fatal("invalid webhook management configuration did not terminate the fixture")
	}
}

func (f *managementFixture) installWriteFixtures(t *testing.T) {
	t.Helper()
	_, err := f.db.Exec(f.ctx, `
CREATE FUNCTION newim.fixture_hold_management_replay() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.next_attempt_at := clock_timestamp() + interval '1 hour';
  RETURN NEW;
END $$;
CREATE TRIGGER fixture_hold_management_replay BEFORE UPDATE OF status ON newim.im_webhook_deliveries
FOR EACH ROW WHEN (OLD.delivery_id='management_dead_letter' AND OLD.status='dead_letter' AND NEW.status='pending')
EXECUTE FUNCTION newim.fixture_hold_management_replay();
CREATE FUNCTION newim.fixture_refuse_management_audit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'management fixture refused audit at commit';
END $$;
CREATE CONSTRAINT TRIGGER fixture_refuse_management_audit AFTER INSERT ON newim.im_webhook_management_audit
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.request_id IN ('reject_create','reject_replay'))
EXECUTE FUNCTION newim.fixture_refuse_management_audit();`)
	managementMust(t, err)
}

func testManagementWrites(t *testing.T, f *managementFixture) {
	p := f.start(t)
	f.installWriteFixtures(t)
	const id, delivery = "management_atomic", "management_dead_letter"
	f.post(t, p, "endpoints/create", managementCreate(id, "reject_create")).
		requireError(t, 503, string(app.CodeStorageUnavailable))
	f.post(t, p, "endpoints/get", map[string]string{"destinationId": id}).
		requireError(t, 404, string(app.CodeManagementNotFound))
	managementEndpoint(t, f.post(t, p, "endpoints/create", managementCreate(id, "create_atomic")), 201, id, "active", "initial", 1)

	change := func(request, url string) map[string]string {
		return map[string]string{"destinationId": id, "requestId": request, "expectedRevision": "1", "url": url}
	}
	_, result := f.compete(t, p, "endpoints/update", [2]map[string]string{
		change("update_first", managementUpdated), change("update_second", managementUpdated+"/other"),
	}, app.CodeManagementConflict)
	managementEndpoint(t, result, 200, id, "active", "initial", 2)
	f.seedDelivery(t, id, delivery, 2, false)
	const another = "management_other_dead_letter"
	f.seedDelivery(t, id, another, 2, false)
	var completed time.Time
	managementMust(t, f.db.QueryRow(f.ctx, "SELECT completed_at FROM newim.im_webhook_deliveries WHERE delivery_id=$1", delivery).Scan(&completed))
	replay := func(request, delivery string) map[string]string {
		return map[string]string{"requestId": request, "destinationId": id, "deliveryId": delivery}
	}
	f.post(t, p, "deliveries/replay", replay("reject_replay", delivery)).
		requireError(t, 503, string(app.CodeStorageUnavailable))
	lookup := func(p *managementProcess) app.DeliveryInfo {
		t.Helper()
		page := managementDecode[app.DeliveryPage](t, f.post(t, p, "deliveries/list", map[string]string{
			"destinationId": id, "deliveryId": delivery,
		}), 200)
		if len(page.Items) != 1 || page.NextAfterID != "" || page.Items[0].DeliveryID != delivery {
			t.Fatal("webhook delivery identity lookup returned a different record")
		}
		return page.Items[0]
	}
	original := lookup(p)
	if original.Status != "dead_letter" || original.Attempts != 3 || original.CompletedAt == nil ||
		!original.CompletedAt.Equal(completed) || original.LastErrorCode != app.CodeHTTPPermanent {
		t.Fatal("failed replay audit did not roll back the delivery")
	}
	f.requireReplayCapacity(t, p, id, delivery)

	requests := [2]map[string]string{replay("replay_first", delivery), replay("replay_second", delivery)}
	winner, result := f.compete(t, p, "deliveries/replay", requests, app.CodeManagementNotReplayable)
	receipt := managementDecode[app.ReplayReceipt](t, result, 200)
	if receipt.RequestID != requests[winner]["requestId"] || receipt.DestinationID != id ||
		receipt.DeliveryID != delivery || receipt.Revision != 2 || receipt.ReplayedAt.IsZero() {
		t.Fatal("webhook replay returned the wrong durable receipt")
	}
	after := lookup(p)
	if after.Status != "pending" || after.Attempts != 0 || after.CompletedAt != nil || after.LastHTTPStatus != nil ||
		after.LastErrorCode != "" || after.EndpointRevision == nil || *after.EndpointRevision != 2 || after.EventID != original.EventID {
		t.Fatal("webhook replay changed delivery identity or retained the old attempt outcome")
	}
	f.post(t, p, "deliveries/replay", replay(receipt.RequestID, another)).
		requireError(t, 409, string(app.CodeManagementConflict))
	f.requireReplayAudit(t, receipt, completed)
	p.requireRedacted(t, f.token, f.issuer, "management fixture refused audit at commit")

	restarted := f.start(t)
	repeated := f.post(t, restarted, "deliveries/replay", requests[winner])
	repeated.require(t, 200)
	if !bytes.Equal(repeated.body, result.body) {
		t.Fatal("restarting webhook management changed the committed replay receipt")
	}
	// Model the terminal state after the worker exhausts the manual replay's attempts.
	_, err := f.db.Exec(f.ctx, `UPDATE newim.im_webhook_deliveries
SET status='dead_letter',attempts=8,next_attempt_at=NULL,completed_at=clock_timestamp(),
last_error_code='WEBHOOK_DELIVERY_DEAD_LETTER' WHERE delivery_id=$1 AND status='pending'`, delivery)
	managementMust(t, err)
	if terminal := lookup(restarted); terminal.Status != "dead_letter" || terminal.Attempts != 8 {
		t.Fatal("fixture did not restore an otherwise replayable terminal state")
	}
	f.post(t, restarted, "deliveries/replay", replay("third_replay", delivery)).
		requireError(t, 409, string(app.CodeManagementNotReplayable))
	f.requireReplayAudit(t, receipt, completed)
	restarted.requireRedacted(t, f.token, f.issuer, managementMaster, managementURL)
}

func (f *managementFixture) requireReplayCapacity(t *testing.T, p *managementProcess, destination, delivery string) {
	t.Helper()
	const first, second = "management_capacity_first", "management_capacity_second"
	managementEndpoint(t, f.post(t, p, "endpoints/create", managementCreate(first, "create_capacity_first")), 201, first, "active", "initial", 1)
	managementEndpoint(t, f.post(t, p, "endpoints/create", managementCreate(second, "create_capacity_second")), 201, second, "active", "initial", 1)
	_, err := f.db.Exec(f.ctx, `
INSERT INTO newim.im_conversations(conversation_id,kind) VALUES ('management_capacity_room','group');
INSERT INTO newim.im_conversation_members(conversation_id,user_id)
VALUES ('management_capacity_room','management_fixture_author');`)
	managementMust(t, err)
	_, err = f.db.Exec(f.ctx, `WITH messages AS (
  INSERT INTO newim.im_messages(server_msg_id,sender_id,client_msg_id,conversation_id,conversation_seq,
  protocol_version,schema_version,message_type,server_time_ms,payload_bytes)
  SELECT 'message_capacity_'||n,'management_fixture_author','client_capacity_'||n,'management_capacity_room',
  n,1,1,'text',0,'{"text":"webhook capacity fixture"}'::bytea FROM generate_series(1,$1::integer) n
  RETURNING server_msg_id
)
INSERT INTO newim.im_outbox_events(event_id,server_msg_id,webhook_fanout_at)
SELECT 'event_'||server_msg_id,server_msg_id,clock_timestamp() FROM messages`, webhookDestinationQueue)
	managementMust(t, err)

	fill := func(id string, revision int64, count int) {
		t.Helper()
		_, err := f.db.Exec(f.ctx, `INSERT INTO newim.im_webhook_deliveries
(delivery_id,event_id,destination_id,endpoint_revision,next_attempt_at)
SELECT $1||'_capacity_'||n,'event_message_capacity_'||n,$1,$2,clock_timestamp()+interval '1 hour'
FROM generate_series(1,$3::integer) n`, id, revision, count)
		managementMust(t, err)
	}
	reject := func(requestID string) {
		t.Helper()
		f.post(t, p, "deliveries/replay", map[string]string{
			"requestId": requestID, "destinationId": destination, "deliveryId": delivery,
		}).requireError(t, http.StatusServiceUnavailable, string(app.CodeManagementCapacity))
	}

	fill(destination, 2, webhookDestinationQueue)
	reject("full_destination")
	_, err = f.db.Exec(f.ctx, "DELETE FROM newim.im_webhook_deliveries WHERE destination_id=$1 AND status='pending'", destination)
	managementMust(t, err)
	fill(first, 1, webhookDestinationQueue)
	fill(second, 1, webhookHighWater-webhookDestinationQueue)
	reject("full_global")
	_, err = f.db.Exec(f.ctx, "DELETE FROM newim.im_webhook_deliveries WHERE destination_id IN ($1,$2)", first, second)
	managementMust(t, err)
}

func (f *managementFixture) compete(t *testing.T, p *managementProcess, route string, intents [2]map[string]string, losingCode app.Code) (int, managementReply) {
	t.Helper()
	var replies [2]chan managementReply
	start := make(chan struct{})
	for i, intent := range intents {
		body, err := json.Marshal(intent)
		managementMust(t, err)
		replies[i] = make(chan managementReply, 1)
		go func(out chan<- managementReply, data []byte) {
			<-start
			out <- p.request(f.ctx, http.MethodPost, managementPrefix+route, f.token, data)
		}(replies[i], body)
	}
	close(start)

	winner := -1
	var success managementReply
	for i, out := range replies {
		reply := <-out
		if reply.err == nil && reply.status == http.StatusOK {
			if winner >= 0 {
				t.Fatal("both concurrent webhook writes succeeded")
			}
			winner, success = i, reply
		} else {
			reply.requireError(t, http.StatusConflict, string(losingCode))
		}
	}
	if winner == -1 {
		t.Fatal("neither concurrent webhook write succeeded")
	}
	return winner, success
}

func (f *managementFixture) requireReplayAudit(t *testing.T, receipt app.ReplayReceipt, originalCompletion time.Time) {
	t.Helper()
	var deliveries, revisions, writes, replays, refused int
	managementMust(t, f.db.QueryRow(f.ctx, `SELECT
(SELECT count(*) FROM newim.im_webhook_deliveries WHERE destination_id=$1),
(SELECT count(*) FROM newim.im_webhook_endpoint_revisions WHERE destination_id=$1),
(SELECT count(*) FROM newim.im_webhook_management_audit WHERE destination_id=$1 AND action IN ('create','update')),
(SELECT count(*) FROM newim.im_webhook_management_audit WHERE destination_id=$1 AND action='replay'),
(SELECT count(*) FROM newim.im_webhook_management_audit WHERE request_id IN ('reject_create','reject_replay'))`,
		receipt.DestinationID).Scan(&deliveries, &revisions, &writes, &replays, &refused))
	if deliveries != 2 || revisions != 2 || writes != 2 || replays != 1 || refused != 0 {
		t.Fatal("webhook replay or failed audit changed durable record counts")
	}

	var actor, state, code string
	var attempts, httpStatus int
	var completed, recorded time.Time
	managementMust(t, f.db.QueryRow(f.ctx, `SELECT actor,previous_status,previous_attempts,previous_http_status,
previous_error_code,previous_completed_at,recorded_at
FROM newim.im_webhook_management_audit WHERE action='replay' AND request_id=$1
AND destination_id=$2 AND delivery_id=$3 AND revision=$4`, receipt.RequestID,
		receipt.DestinationID, receipt.DeliveryID, receipt.Revision).
		Scan(&actor, &state, &attempts, &httpStatus, &code, &completed, &recorded))
	if actor != "deployment" || state != "dead_letter" || attempts != 3 || httpStatus != 410 ||
		code != string(app.CodeHTTPPermanent) || !completed.Equal(originalCompletion) || !recorded.Equal(receipt.ReplayedAt) {
		t.Fatal("webhook replay audit did not preserve the prior terminal state")
	}
}
