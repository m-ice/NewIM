//go:build integration

package mediahttp_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	auth "github.com/m-ice/NewIM/server/auth/session"
	authstore "github.com/m-ice/NewIM/server/storage/authsession"
	"github.com/m-ice/NewIM/server/storage/conversationsync"
)

const (
	testDSN      = "host=/var/run/postgresql user=newim_test dbname=newim_test sslmode=disable"
	grantPath    = "/api/v1/media/uploads/grant"
	completePath = "/api/v1/media/uploads/complete"
	downloadPath = "/api/v1/media/download"
	sendPath     = "/api/v1/messages"
	deltaPath    = "/api/v1/sync/messages"
	uploadHeader = "X-NewIM-Upload-Token"
	jsonType     = "application/json; charset=utf-8"
	objectType   = "application/octet-stream"
)

type fixture struct {
	t       *testing.T
	ctx     context.Context
	db      *pgx.Conn
	auth    *auth.Service
	client  *http.Client
	cert    string
	key     string
	syncKey string
	objects *s3Fixture
}

type authObservation struct{}

func (authObservation) Observe(auth.Observation) {}

func openFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := t.Context()
	db, err := pgx.Connect(ctx, testDSN)
	must(t, err)
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	var migration int
	must(t, db.QueryRow(ctx, "SELECT max(version) FROM newim_meta.migrations").Scan(&migration))
	if migration != 14 {
		t.Fatalf("fixture requires migration 14, got %d", migration)
	}
	repository, err := authstore.Open(ctx, authstore.Config{
		DSN: testDSN, AllowLocalSocket: true, AdmissionRequired: true,
		MaxConnections: 3, ApplicationName: "newim-media-http-fixture",
	})
	must(t, err)
	t.Cleanup(repository.Close)
	service, err := auth.NewService(repository, auth.Config{
		Clock: auth.ClockFunc(time.Now), Observer: authObservation{},
	})
	must(t, err)

	dir := t.TempDir()
	cert, key, roots := fixtureCertificate(t, dir)
	syncKey := filepath.Join(dir, "sync-keys.json")
	secret := sha256.Sum256([]byte("synthetic media HTTP integration cursor key"))
	must(t, os.WriteFile(syncKey, encode(t, map[string]any{"activeKeyId": "fixture",
		"keys": map[string]string{"fixture": hex.EncodeToString(secret[:])}}), 0o600))
	transport := &http.Transport{
		TLSClientConfig:       &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}},
		ExpectContinueTimeout: time.Second,
	}
	t.Cleanup(transport.CloseIdleConnections)
	return &fixture{t: t, ctx: ctx, db: db, auth: service, cert: cert, key: key, syncKey: syncKey,
		objects: readS3Fixture(t), client: &http.Client{Transport: transport, Timeout: 40 * time.Second}}
}

func fixtureCertificate(t *testing.T, dir string) (string, string, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must(t, err)
	now := time.Now()
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		BasicConstraintsValid: true, IsCA: true,
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	must(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	must(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	must(t, os.WriteFile(certPath, certPEM, 0o600))
	must(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600))
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("could not load the TLS fixture certificate")
	}
	return certPath, keyPath, roots
}

type gateway struct {
	log byteLog
	url string
}

type byteLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *byteLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *byteLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func (f *fixture) gatewayCommand(ctx context.Context, root string, overrides map[string]string) *exec.Cmd {
	f.t.Helper()
	binary := os.Getenv("NEWIM_GATEWAY_BINARY")
	if !filepath.IsAbs(binary) {
		f.t.Fatal("absolute gateway fixture binary path required")
	}
	settings := map[string]string{
		"NEWIM_AUTH_DSN": testDSN, "NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1", "NEWIM_AUTH_DEVICE_ADMISSION": "1",
		"NEWIM_GATEWAY_ADDR": "127.0.0.1:0", "NEWIM_GATEWAY_TLS_CERT_FILE": f.cert, "NEWIM_GATEWAY_TLS_KEY_FILE": f.key,
		"NEWIM_TLS_PRIVATE_MESSAGES": "1", "NEWIM_PRIVATE_MESSAGE_POLICY": "1", "NEWIM_GROUP_HTTP": "1",
		"NEWIM_CONVERSATION_PROJECTION": "1", "NEWIM_CONVERSATION_SYNC": "1", "NEWIM_CONVERSATION_SYNC_KEYS_FILE": f.syncKey,
	}
	if root != "" {
		settings["NEWIM_MEDIA_HTTP"] = "1"
		if f.objects == nil {
			settings["NEWIM_MEDIA_LOCAL_ROOT"] = root
		} else {
			settings["NEWIM_MEDIA_STORAGE"] = "s3"
			settings["NEWIM_MEDIA_S3_BUCKET"] = f.objects.Bucket
			settings["NEWIM_MEDIA_S3_REGION"] = f.objects.Region
			settings["NEWIM_MEDIA_S3_ENDPOINT"] = f.objects.Endpoint
			settings["NEWIM_MEDIA_S3_CA_FILE"] = f.objects.CAFile
			settings["NEWIM_MEDIA_S3_STAGING_ROOT"] = filepath.Join(f.t.TempDir(), "staging")
			settings["AWS_ACCESS_KEY_ID"] = f.objects.AccessKey
			settings["AWS_SECRET_ACCESS_KEY"] = f.objects.SecretKey
		}
	}
	for k, v := range overrides {
		settings[k] = v
	}

	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = os.Environ()
	for k, v := range settings {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	return cmd
}

func (f *fixture) gateway(root string, overrides map[string]string) *gateway {
	f.t.Helper()
	g := &gateway{}
	cmd := f.gatewayCommand(context.Background(), root, overrides)
	cmd.Stdout, cmd.Stderr = &g.log, &g.log
	must(f.t, cmd.Start())
	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(done)
	}()
	f.t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(4 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		for _, line := range strings.Split(g.log.String(), "\n") {
			address, ok := strings.CutPrefix(line, "newim-gateway listening on ")
			host, port, err := net.SplitHostPort(address)
			if ok && err == nil && host == "127.0.0.1" && port != "0" {
				g.url = "https://" + address
				return g
			}
		}
		select {
		case <-done:
			f.t.Fatalf("gateway failed to start: %v; %s", waitErr, g.log.String())
		case <-deadline.C:
			f.t.Fatalf("gateway startup deadline: %s", g.log.String())
		case <-tick.C:
		}
	}
}

func (g *gateway) requireRedacted(t *testing.T, values ...string) {
	t.Helper()
	log := g.log.String()
	for _, value := range values {
		if value != "" && strings.Contains(log, value) {
			t.Fatalf("gateway log contains a sensitive value of length %d", len(value))
		}
	}
}

func (f *fixture) newAccount(user string) string {
	f.t.Helper()
	tx, err := f.db.Begin(f.ctx)
	must(f.t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, "INSERT INTO newim.im_users (user_id) VALUES ($1)", user)
	must(f.t, err)
	must(f.t, conversationsync.InitializeSourceAccount(f.ctx, tx, user))
	must(f.t, tx.Commit(f.ctx))
	return f.admit(user, "phone")
}

func (f *fixture) admit(user, device string) string {
	f.t.Helper()
	result, err := f.auth.Admit(f.ctx, auth.AdmissionRequest{
		UserID: user, DeviceID: device, Platform: "fixture", TTL: time.Hour,
		Limits: auth.DeviceLimits{Account: 8, Platform: 3},
	})
	must(f.t, err)
	return result.Token.RawToken()
}

func (f *fixture) direct(g *gateway, token, peer string) string {
	f.t.Helper()
	var result struct {
		ConversationID string `json:"conversationId"`
	}
	decode(f.t, f.post(g, "/api/v1/conversations/direct", token, map[string]string{"peerUserId": peer}), &result)
	if result.ConversationID == "" {
		f.t.Fatal("direct conversation ID missing")
	}
	return result.ConversationID
}

type reply struct {
	status int
	header http.Header
	body   []byte
	err    error
}

func (f *fixture) request(g *gateway, path, token string, body io.Reader, contentType string) *http.Request {
	f.t.Helper()
	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, g.url+path, body)
	must(f.t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", contentType)
	return req
}

func (f *fixture) post(g *gateway, path, token string, value any) reply {
	f.t.Helper()
	return f.execute(f.request(g, path, token, bytes.NewReader(encode(f.t, value)), jsonType))
}

func (f *fixture) execute(req *http.Request) reply {
	res, err := f.client.Do(req)
	if err != nil {
		return reply{err: err}
	}
	defer res.Body.Close()
	if res.TLS == nil || len(res.TLS.VerifiedChains) == 0 {
		return reply{status: res.StatusCode, err: io.ErrUnexpectedEOF}
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, 256*1024+1))
	return reply{status: res.StatusCode, header: res.Header, body: raw, err: err}
}

func (r reply) require(t *testing.T, status int) {
	t.Helper()
	if r.status != status || r.err != nil {
		t.Fatalf("TLS HTTP status=%d err=%v want=%d", r.status, r.err, status)
	}
	if r.header.Get("Cache-Control") != "no-store" || r.header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("TLS HTTP response omitted cache or MIME protection")
	}
}

func (r reply) requireError(t *testing.T, status int, code string) {
	t.Helper()
	r.require(t, status)
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	d := json.NewDecoder(bytes.NewReader(r.body))
	d.DisallowUnknownFields()
	must(t, d.Decode(&envelope))
	if envelope.Error.Code != code {
		t.Fatalf("TLS HTTP error code=%q want=%q", envelope.Error.Code, code)
	}
}

func encode(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	must(t, err)
	return raw
}

func decode(t *testing.T, r reply, target any) {
	t.Helper()
	r.require(t, http.StatusOK)
	d := json.NewDecoder(bytes.NewReader(r.body))
	d.DisallowUnknownFields()
	must(t, d.Decode(target))
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) message(g *gateway, token, conversation, id, kind string, payload []byte) reply {
	f.t.Helper()
	wire, err := protocol.EncodeSend(protocol.Send{
		ProtocolVersion: 1, ClientMsgID: id, ConversationID: conversation, Version: 1, Type: kind, Payload: payload,
	})
	must(f.t, err)
	return f.execute(f.request(g, sendPath, token, bytes.NewReader(wire), jsonType))
}

func ack(t *testing.T, r reply) protocol.Ack {
	t.Helper()
	r.require(t, http.StatusOK)
	frame, err := protocol.DecodeServerFrame(r.body)
	must(t, err)
	if frame.Ack == nil {
		t.Fatal("send did not return a committed ACK")
	}
	return *frame.Ack
}
