//go:build integration

package tlsmessage_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	auth "github.com/m-ice/NewIM/server/auth/session"
	authstore "github.com/m-ice/NewIM/server/storage/authsession"
)

const testDSN = "host=/var/run/postgresql user=newim_test dbname=newim_test sslmode=disable"

type fixture struct {
	t       *testing.T
	ctx     context.Context
	db      *pgx.Conn
	auth    *auth.Service
	client  *http.Client
	tlsCert string
	tlsKey  string
}

type authObservation struct{}

func (authObservation) Observe(auth.Observation) {}

func openFixture(t *testing.T) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	db, err := pgx.Connect(ctx, testDSN)
	must(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = db.Close(closeCtx)
	})
	repository, err := authstore.Open(ctx, authstore.Config{
		DSN: testDSN, AllowLocalSocket: true, AdmissionRequired: true,
		MaxConnections: 3, ApplicationName: "newim-tls-message-fixture",
	})
	must(t, err)
	t.Cleanup(repository.Close)
	service, err := auth.NewService(repository, auth.Config{
		Clock: auth.ClockFunc(time.Now), Observer: authObservation{},
	})
	must(t, err)

	dir := t.TempDir()
	cert, privateKey, roots := fixtureCertificate(t, dir)
	transport := &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: roots, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"},
	}}
	t.Cleanup(transport.CloseIdleConnections)
	return &fixture{
		t: t, ctx: ctx, db: db, auth: service,
		client:  &http.Client{Transport: transport, Timeout: 5 * time.Second},
		tlsCert: cert, tlsKey: privateKey,
	}
}

func fixtureCertificate(t *testing.T, dir string) (string, string, *x509.CertPool) {
	t.Helper()
	now := time.Now()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must(t, err)
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
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	privateKey := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	must(t, os.WriteFile(certPath, certificate, 0o600))
	must(t, os.WriteFile(keyPath, privateKey, 0o600))
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		t.Fatal("could not load message TLS fixture certificate")
	}
	return certPath, keyPath, roots
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) admit(userID, deviceID string) string {
	f.t.Helper()
	f.sql("INSERT INTO newim.im_users (user_id) VALUES($1) ON CONFLICT DO NOTHING", userID)
	result, err := f.auth.Admit(f.ctx, auth.AdmissionRequest{
		UserID: userID, DeviceID: deviceID, Platform: "fixture", TTL: time.Hour,
		Limits: auth.DeviceLimits{Account: 8, Platform: 3},
	})
	must(f.t, err)
	return result.Token.RawToken()
}

func (f *fixture) sql(query string, args ...any) {
	f.t.Helper()
	_, err := f.db.Exec(f.ctx, query, args...)
	must(f.t, err)
}

func (f *fixture) requireMigration(want int) {
	f.t.Helper()
	var result int
	must(f.t, f.db.QueryRow(f.ctx, "SELECT max(version) FROM newim_meta.migrations").Scan(&result))
	if result != want {
		f.t.Fatalf("fixture migration version=%d want=%d", result, want)
	}
}

type processLog struct {
	mu sync.Mutex
	bytes.Buffer
}

func (l *processLog) Write(raw []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.Buffer.Write(raw)
}

func (l *processLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.Buffer.String()
}

type process struct {
	cmd     *exec.Cmd
	done    chan struct{}
	waitErr error
	log     processLog
	url     string
}

func startProcess(t *testing.T, binary, enabled string, settings map[string]string, args ...string) *process {
	t.Helper()
	if !filepath.IsAbs(binary) {
		t.Fatal("absolute fixture binary path required")
	}
	p := &process{cmd: exec.Command(binary, args...), done: make(chan struct{})}
	p.cmd.Env = append(os.Environ(), "NEWIM_AUTH_DSN="+testDSN,
		"NEWIM_AUTH_ALLOW_LOCAL_SOCKET=1", "NEWIM_AUTH_DEVICE_ADMISSION=1",
		"NEWIM_TLS_PRIVATE_MESSAGES="+enabled)
	for key, value := range settings {
		p.cmd.Env = append(p.cmd.Env, key+"="+value)
	}
	p.cmd.Stdout, p.cmd.Stderr = &p.log, &p.log
	must(t, p.cmd.Start())
	go func() {
		p.waitErr = p.cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		select {
		case <-p.done:
			return
		default:
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
		}
		select {
		case <-p.done:
		case <-time.After(4 * time.Second):
			_ = p.cmd.Process.Kill()
			select {
			case <-p.done:
			case <-time.After(4 * time.Second):
			}
		}
	})
	return p
}

func waitForStartup(t *testing.T, p *process, ready func() bool) {
	t.Helper()
	timeout := time.NewTimer(8 * time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-p.done:
			t.Fatalf("fixture process failed to start: %v; %s", p.waitErr, p.log.String())
		case <-timeout.C:
			t.Fatalf("fixture startup deadline: %s", p.log.String())
		case <-tick.C:
		}
	}
}

func (f *fixture) gateway(enabled string, settings map[string]string) *process {
	f.t.Helper()
	env := map[string]string{
		"NEWIM_GATEWAY_ADDR": "127.0.0.1:0", "NEWIM_GATEWAY_TLS_CERT_FILE": f.tlsCert,
		"NEWIM_GATEWAY_TLS_KEY_FILE": f.tlsKey, "NEWIM_PRIVATE_MESSAGE_POLICY": enabled,
	}
	for key, value := range settings {
		env[key] = value
	}
	p := startProcess(f.t, os.Getenv("NEWIM_GATEWAY_BINARY"), enabled, env)
	const prefix = "newim-gateway listening on "
	waitForStartup(f.t, p, func() bool {
		for _, line := range strings.Split(p.log.String(), "\n") {
			if address, ok := strings.CutPrefix(line, prefix); ok {
				host, port, err := net.SplitHostPort(address)
				n, parseErr := strconv.Atoi(port)
				if err == nil && host == "127.0.0.1" && parseErr == nil && n > 0 {
					p.url = "https://" + address
					return true
				}
			}
		}
		return false
	})
	return p
}

func (f *fixture) messageLoopback(overrides map[string]string) (*process, string) {
	f.t.Helper()
	api, err := net.Listen("tcp4", "127.0.0.1:0")
	must(f.t, err)
	defer api.Close()
	ops, err := net.Listen("tcp4", "127.0.0.1:0")
	must(f.t, err)
	defer ops.Close()
	apiAddr, opsAddr := api.Addr().String(), ops.Addr().String()
	must(f.t, api.Close())
	must(f.t, ops.Close())

	issuerFile := filepath.Join(f.t.TempDir(), "issuer-key")
	var key [32]byte
	_, err = rand.Read(key[:])
	must(f.t, err)
	credential := hex.EncodeToString(key[:])
	must(f.t, os.WriteFile(issuerFile, []byte(credential), 0o600))
	clear(key[:])
	settings := map[string]string{
		"NEWIM_MESSAGE_HTTP":            "1",
		"NEWIM_PRIVATE_MESSAGE_POLICY":  "1",
		"NEWIM_AUTH_ISSUER_TOKEN_FILE":  issuerFile,
		"NEWIM_AUTH_ISSUER_TTL_SECONDS": "120",
	}
	for key, value := range overrides {
		settings[key] = value
	}
	p := startProcess(f.t, os.Getenv("NEWIM_SERVER_BINARY"), "", settings, "--api-addr", apiAddr, "--ops-addr", opsAddr)
	p.url = "http://" + apiAddr
	waitForStartup(f.t, p, func() bool {
		r := f.call(p, http.MethodGet, "/api/v1/health", "", nil, nil)
		return r.err == nil && r.status == http.StatusOK
	})
	return p, credential
}

type response struct {
	status   int
	headers  http.Header
	body     []byte
	verified bool
	err      error
}

func (f *fixture) call(p *process, method, path, token string, body []byte, extra http.Header) response {
	request, err := http.NewRequestWithContext(f.ctx, method, p.url+path, bytes.NewReader(body))
	if err != nil {
		return response{err: err}
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	for name, values := range extra {
		request.Header[http.CanonicalHeaderKey(name)] = values
	}
	result, err := f.client.Do(request)
	if err != nil {
		return response{err: err}
	}
	defer result.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(result.Body, 512*1024))
	return response{
		status: result.StatusCode, headers: result.Header, body: raw, err: err,
		verified: result.TLS != nil && len(result.TLS.VerifiedChains) > 0,
	}
}

func (r response) requireError(t *testing.T, status int, code string) {
	t.Helper()
	if r.err != nil || r.status != status {
		t.Fatalf("TLS message API status=%d err=%v want=%d (%s)", r.status, r.err, status, code)
	}
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	decoder := json.NewDecoder(bytes.NewReader(r.body))
	decoder.DisallowUnknownFields()
	must(t, decoder.Decode(&envelope))
	if envelope.Error.Code != code {
		t.Fatalf("TLS message API error code=%q want=%q", envelope.Error.Code, code)
	}
	if r.headers.Get("Cache-Control") != "no-store" {
		t.Fatal("TLS message API error must disable caching")
	}
	if r.headers.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("TLS message API error %s missing nosniff response header", code)
	}
}

func (p *process) requireRedacted(t *testing.T, values ...string) {
	t.Helper()
	log := p.log.String()
	for _, value := range values {
		if value != "" && strings.Contains(log, value) {
			t.Fatalf("fixture log contains a sensitive value of length %d", len(value))
		}
	}
}
