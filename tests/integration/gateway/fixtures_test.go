//go:build integration

package gateway_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
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

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	auth "github.com/m-ice/NewIM/server/auth/session"
	authstore "github.com/m-ice/NewIM/server/storage/authsession"
)

const (
	gatewayDSN         = "host=/var/run/postgresql user=newim_test dbname=newim_test sslmode=disable"
	gatewaySubprotocol = "newim.v1"
	gatewayRoute       = "/api/v1/gateway"
	ioDeadline         = 8 * time.Second
)

type fixture struct {
	t        *testing.T
	ctx      context.Context
	db       *pgx.Conn
	auth     *auth.Service
	certFile string
	keyFile  string
	roots    *x509.CertPool
}

type authObserver struct{}

func (authObserver) Observe(auth.Observation) {}

func openFixture(t *testing.T) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	db, err := pgx.Connect(ctx, gatewayDSN)
	must(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = db.Close(closeCtx)
	})
	repo, err := authstore.Open(ctx, authstore.Config{
		DSN: gatewayDSN, AllowLocalSocket: true, AdmissionRequired: true,
		MaxConnections: 3, ApplicationName: "newim-gateway-fixture",
	})
	must(t, err)
	t.Cleanup(repo.Close)
	service, err := auth.NewService(repo, auth.Config{
		Clock: auth.ClockFunc(time.Now), Observer: authObserver{},
	})
	must(t, err)
	certFile, keyFile, roots := makeCertificate(t)
	return &fixture{t: t, ctx: ctx, db: db, auth: service, certFile: certFile, keyFile: keyFile, roots: roots}
}

func makeCertificate(t *testing.T) (string, string, *x509.CertPool) {
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
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "tls-cert.pem"), filepath.Join(dir, "tls-key.pem")
	must(t, os.WriteFile(certFile, certPEM, 0o600))
	must(t, os.WriteFile(keyFile, keyPEM, 0o600))
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("could not load test TLS root")
	}
	return certFile, keyFile, roots
}

func (f *fixture) admit(userID, deviceID string, accountLimit int) auth.AdmissionResult {
	f.t.Helper()
	f.sql("INSERT INTO newim.im_users (user_id) VALUES($1) ON CONFLICT DO NOTHING", userID)
	result, err := f.auth.Admit(f.ctx, auth.AdmissionRequest{
		UserID: userID, DeviceID: deviceID, Platform: "fixture", TTL: time.Hour,
		Limits: auth.DeviceLimits{Account: accountLimit, Platform: accountLimit},
	})
	must(f.t, err)
	return result
}

func (f *fixture) conversation(id string, users ...string) {
	f.t.Helper()
	f.sql("INSERT INTO newim.im_conversations (conversation_id,kind) VALUES($1,'group')", id)
	for _, user := range users {
		f.sql("INSERT INTO newim.im_conversation_members (conversation_id,user_id) VALUES($1,$2)", id, user)
	}
}

func (f *fixture) sql(statement string, args ...any) {
	f.t.Helper()
	_, err := f.db.Exec(f.ctx, statement, args...)
	must(f.t, err)
}

func (f *fixture) persist(tx pgx.Tx, conversationID, senderID, clientID, body string) {
	f.t.Helper()
	payload, err := json.Marshal(struct {
		Text string `json:"text"`
	}{Text: body})
	must(f.t, err)
	sql := `SELECT newim.persist_message($1,$2,$3,$4,'text',1,$5,$6,$7)`
	args := []any{senderID, clientID, conversationID, clientID + "_server",
		time.Now().UnixMilli(), payload, clientID + "_event"}
	if tx != nil {
		_, err = tx.Exec(f.ctx, sql, args...)
	} else {
		_, err = f.db.Exec(f.ctx, sql, args...)
	}
	must(f.t, err)
}

type concurrentLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *concurrentLog) Write(data []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(data)
}

func (l *concurrentLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

type gatewayProcess struct {
	command *exec.Cmd
	done    chan struct{}
	waitErr error
	log     concurrentLog
	address string
}

func (f *fixture) gateway(settings map[string]string) *gatewayProcess {
	f.t.Helper()
	path := os.Getenv("NEWIM_GATEWAY_BINARY")
	if !filepath.IsAbs(path) {
		f.t.Fatal("absolute NEWIM_GATEWAY_BINARY required")
	}
	process := &gatewayProcess{command: exec.Command(path), done: make(chan struct{})}
	process.command.Env = append(os.Environ(),
		"NEWIM_GATEWAY_ADDR=127.0.0.1:0", "NEWIM_GATEWAY_TLS_CERT_FILE="+f.certFile,
		"NEWIM_GATEWAY_TLS_KEY_FILE="+f.keyFile, "NEWIM_AUTH_DSN="+gatewayDSN,
		"NEWIM_AUTH_ALLOW_LOCAL_SOCKET=1", "NEWIM_AUTH_DEVICE_ADMISSION=1")
	for key, value := range settings {
		process.command.Env = append(process.command.Env, key+"="+value)
	}
	process.command.Stdout, process.command.Stderr = &process.log, &process.log
	must(f.t, process.command.Start())
	go func() {
		process.waitErr = process.command.Wait()
		close(process.done)
	}()
	f.t.Cleanup(process.kill)
	const prefix = "newim-gateway listening on "
	deadline := time.NewTimer(ioDeadline)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, line := range strings.Split(process.log.String(), "\n") {
			if address, found := strings.CutPrefix(line, prefix); found {
				host, port, err := net.SplitHostPort(address)
				n, parseErr := strconv.Atoi(port)
				if err == nil && host == "127.0.0.1" && parseErr == nil && n > 0 && n <= 65535 {
					process.address = address
					return process
				}
			}
		}
		select {
		case <-process.done:
			f.t.Fatalf("gateway exited before readiness: %v; %s", process.waitErr, process.log.String())
		case <-deadline.C:
			f.t.Fatalf("gateway readiness deadline: %s", process.log.String())
		case <-ticker.C:
		}
	}
}

func (p *gatewayProcess) kill() {
	select {
	case <-p.done:
		return
	default:
		_ = p.command.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(3 * time.Second):
		}
	}
}

func (p *gatewayProcess) signalDrain(t *testing.T) {
	t.Helper()
	must(t, p.command.Process.Signal(syscall.SIGTERM))
}

func (p *gatewayProcess) requireExit(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		if p.waitErr != nil {
			t.Fatalf("gateway drain exit: %v; %s", p.waitErr, p.log.String())
		}
	case <-time.After(ioDeadline):
		t.Fatalf("gateway did not exit after drain: %s", p.log.String())
	}
}

type keyTransport struct {
	transport *http.Transport
	forcedKey string
	key       string
}

func (k *keyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if k.forcedKey != "" {
		request = request.Clone(request.Context())
		request.Header.Set("Sec-WebSocket-Key", k.forcedKey)
	}
	k.key = request.Header.Get("Sec-WebSocket-Key")
	return k.transport.RoundTrip(request)
}

type dialResult struct {
	conn     *websocket.Conn
	response *http.Response
	key      string
	err      error
}

func (f *fixture) dial(server *gatewayProcess, route, rawToken string, extra http.Header, key string) dialResult {
	f.t.Helper()
	return f.dialWithReadBuffer(server, route, rawToken, extra, key, 0)
}

func (f *fixture) dialWithReadBuffer(server *gatewayProcess, route, rawToken string, extra http.Header, key string, readBuffer int) dialResult {
	f.t.Helper()
	headers := make(http.Header)
	for field, values := range extra {
		headers[field] = values
	}
	if rawToken != "" {
		headers.Set("Authorization", "Bearer "+rawToken)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: f.roots, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"},
	}}
	if readBuffer > 0 {
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			tcp, ok := conn.(*net.TCPConn)
			if !ok {
				_ = conn.Close()
				return nil, errors.New("expected TCP transport")
			}
			if err = tcp.SetReadBuffer(readBuffer); err != nil {
				_ = tcp.Close()
				return nil, err
			}
			return tcp, nil
		}
	}
	f.t.Cleanup(transport.CloseIdleConnections)
	recording := &keyTransport{transport: transport, forcedKey: key}
	ctx, cancel := context.WithTimeout(f.ctx, ioDeadline)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, "wss://"+server.address+route, &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: recording}, HTTPHeader: headers,
		Subprotocols: []string{gatewaySubprotocol},
	})
	if conn != nil {
		conn.SetReadLimit(protocol.MaxFrameBytes)
		f.t.Cleanup(func() { _ = conn.CloseNow() })
	}
	if response != nil && response.Body != nil && err != nil {
		_ = response.Body.Close()
	}
	return dialResult{conn: conn, response: response, key: recording.key, err: err}
}

func (r dialResult) connected(t *testing.T) *websocket.Conn {
	t.Helper()
	if r.err != nil || r.conn == nil || r.response == nil || r.response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected verified WSS upgrade: HTTP %d, error %v", r.status(), r.err)
	}
	if r.conn.Subprotocol() != gatewaySubprotocol || r.response.TLS == nil || len(r.response.TLS.VerifiedChains) == 0 {
		t.Fatal("gateway did not negotiate the declared subprotocol over verified TLS")
	}
	return r.conn
}

func (r dialResult) status() int {
	if r.response == nil {
		return 0
	}
	return r.response.StatusCode
}

func (r dialResult) rejected(t *testing.T, status int) {
	t.Helper()
	if r.conn != nil || r.err == nil || r.status() != status {
		t.Fatalf("WSS rejection status=%d, error %v, want HTTP %d", r.status(), r.err, status)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func writeSubscription(t *testing.T, conn *websocket.Conn, conversationID string, after int64) {
	t.Helper()
	payload, err := json.Marshal(struct {
		ConversationID string `json:"conversationId"`
		AfterSeq       string `json:"afterSeq"`
	}{conversationID, strconv.FormatInt(after, 10)})
	must(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), ioDeadline)
	defer cancel()
	must(t, conn.Write(ctx, websocket.MessageText, payload))
}

type receivedFrame struct {
	kind websocket.MessageType
	data []byte
	err  error
}

func readFrame(conn *websocket.Conn) receivedFrame {
	return readFrameWithin(conn, ioDeadline)
}

func readFrameWithin(conn *websocket.Conn, timeout time.Duration) receivedFrame {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	kind, raw, err := conn.Read(ctx)
	return receivedFrame{kind, raw, err}
}

func readAsync(conn *websocket.Conn, deadline time.Duration) <-chan receivedFrame {
	result := make(chan receivedFrame, 1)
	go func() { result <- readFrameWithin(conn, deadline) }()
	return result
}

func expectMessage(t *testing.T, result receivedFrame, conversationID, clientID, senderID, body string, seq int64) {
	t.Helper()
	if result.err != nil || result.kind != websocket.MessageText {
		t.Fatalf("expected text message, frame type=%d error=%v", result.kind, result.err)
	}
	frame, err := protocol.DecodeServerFrame(result.data)
	must(t, err)
	msg := frame.Message
	if msg == nil {
		t.Fatal("expected a persisted message frame")
	}
	if msg.ConversationID != conversationID || msg.ClientMsgID != clientID || msg.ServerMsgID != clientID+"_server" ||
		msg.SenderID != senderID || msg.ConversationSeq != strconv.FormatInt(seq, 10) {
		t.Fatal("gateway message does not match the committed identity or sequence")
	}
	var decoded struct {
		Text string `json:"text"`
	}
	must(t, json.Unmarshal(msg.Payload, &decoded))
	if decoded.Text != body {
		t.Fatal("gateway message payload differs from stored fixture")
	}
}

func expectClose(t *testing.T, conn *websocket.Conn, code websocket.StatusCode, reason string) {
	t.Helper()
	result := readFrame(conn)
	var closeErr websocket.CloseError
	if !errors.As(result.err, &closeErr) || closeErr.Code != code || closeErr.Reason != reason {
		t.Fatalf("gateway close error=%v, want code %d reason %s", result.err, code, reason)
	}
}

func (p *gatewayProcess) requireRedacted(t *testing.T, boundaries ...string) {
	t.Helper()
	log := p.log.String()
	for _, value := range boundaries {
		if value != "" && strings.Contains(log, value) {
			t.Fatalf("gateway log contains a fixture value of length %d", len(value))
		}
	}
}
