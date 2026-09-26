//go:build integration

package messagehttp_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

const localDSN = "host=/var/run/postgresql user=newim_test dbname=newim_test sslmode=disable"

var ctx = context.Background()

type fixture struct {
	t    *testing.T
	db   *pgx.Conn
	dsn  string
	name string
}

type seedIdentity struct {
	UserID    string
	DeviceID  string
	SessionID string
	TokenID   string
	RawToken  string
}

type seedOptions struct {
	Expired bool
	Revoked bool
}

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

type runningServer struct {
	cmd     *exec.Cmd
	stdout  synchronizedBuffer
	stderr  synchronizedBuffer
	api     string
	ops     string
	done    chan error
	stopped bool
	failed  error
}

type processResult struct {
	code   int
	stdout string
	stderr string
}

type httpResult struct {
	body    []byte
	status  int
	headers http.Header
	err     error
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func openFixture(t *testing.T) *fixture {
	t.Helper()
	name := "newim_test"
	if phase := os.Getenv("NEWIM_MESSAGE_HTTP_PHASE"); phase == "identity" {
		name = "newim_test_identity"
	}
	return openFixtureDSN(t, fmt.Sprintf("host=/var/run/postgresql user=newim_test dbname=%s sslmode=disable application_name=nim_message_http_test", name), name)
}

func openFixtureDSN(t *testing.T, dsn, name string) *fixture {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	must(t, err)
	f := &fixture{t: t, db: conn, dsn: dsn, name: name}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return f
}

func (f *fixture) exec(statement string, args ...any) {
	f.t.Helper()
	_, err := f.db.Exec(ctx, statement, args...)
	must(f.t, err)
}

func (f *fixture) scalarInt(statement string, args ...any) int64 {
	f.t.Helper()
	var value int64
	must(f.t, f.db.QueryRow(ctx, statement, args...).Scan(&value))
	return value
}

func (f *fixture) scalarString(statement string, args ...any) string {
	f.t.Helper()
	var value string
	must(f.t, f.db.QueryRow(ctx, statement, args...).Scan(&value))
	return value
}

func (f *fixture) seedIdentity(prefix string, options seedOptions) seedIdentity {
	f.t.Helper()
	identity := seedIdentity{
		UserID:    prefix + "_user",
		DeviceID:  prefix + "_device",
		SessionID: prefix + "_session",
	}
	hash := sha256.Sum256([]byte(prefix))
	identity.TokenID = hex.EncodeToString(hash[:16])
	secretHash := sha256.Sum256([]byte(prefix + "_secret"))
	identity.RawToken = "n1_" + identity.TokenID + "_" + base64.RawURLEncoding.EncodeToString(secretHash[:])
	digest := sha256.Sum256([]byte(identity.RawToken))
	createdAt := time.Now().UTC().Add(-2 * time.Hour)
	expiresAt := time.Now().UTC().Add(time.Hour)
	if options.Expired {
		expiresAt = createdAt.Add(time.Second)
	}
	f.exec("INSERT INTO newim.im_users(user_id) VALUES($1)", identity.UserID)
	f.exec("INSERT INTO newim.im_devices(user_id,device_id) VALUES($1,$2)", identity.UserID, identity.DeviceID)
	f.exec("INSERT INTO newim.im_sessions(session_id,user_id,device_id) VALUES($1,$2,$3)", identity.SessionID, identity.UserID, identity.DeviceID)
	f.exec("INSERT INTO newim.im_auth_tokens(token_id,token_digest,session_id,created_at,expires_at,revoked_at) VALUES($1,$2,$3,$4,$5,$6)",
		identity.TokenID, digest[:], identity.SessionID, createdAt, expiresAt, nullableTime(options.Revoked, time.Now().UTC()))
	return identity
}

func nullableTime(present bool, value time.Time) any {
	if !present {
		return nil
	}
	return value
}

func (f *fixture) seedConversation(prefix string, users ...string) string {
	f.t.Helper()
	conversationID := prefix + "_conversation"
	f.exec("INSERT INTO newim.im_conversations(conversation_id) VALUES($1)", conversationID)
	for _, userID := range users {
		f.exec("INSERT INTO newim.im_conversation_members(conversation_id,user_id) VALUES($1,$2)", conversationID, userID)
	}
	return conversationID
}

func (f *fixture) removeMembership(conversationID, userID string) {
	f.t.Helper()
	f.exec("DELETE FROM newim.im_conversation_members WHERE conversation_id=$1 AND user_id=$2", conversationID, userID)
}

func textBody(t *testing.T, clientMsgID, conversationID, text string) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"text": text})
	must(t, err)
	wire, err := protocol.EncodeSend(protocol.Send{
		ProtocolVersion: 1,
		ClientMsgID:     clientMsgID,
		ConversationID:  conversationID,
		Version:         1,
		Type:            "text",
		Payload:         payload,
	})
	must(t, err)
	return wire
}

func decodeACK(t *testing.T, body []byte) protocol.Ack {
	t.Helper()
	frame, err := protocol.DecodeServerFrame(body)
	must(t, err)
	if frame.Ack == nil || frame.Error != nil || frame.Message != nil {
		t.Fatalf("expected send_ack, got %+v body=%q", frame, body)
	}
	return *frame.Ack
}

func errorCode(t *testing.T, body []byte) string {
	t.Helper()
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	must(t, json.Unmarshal(body, &envelope))
	if envelope.Error.Code == "" {
		t.Fatalf("missing stable error code: %q", body)
	}
	return envelope.Error.Code
}

func assertError(t *testing.T, result httpResult, status int, code string) {
	t.Helper()
	if result.err != nil {
		t.Fatalf("request error: %v", result.err)
	}
	if result.status != status || errorCode(t, result.body) != code {
		t.Fatalf("status=%d code=%s body=%q want status=%d code=%s", result.status, errorCode(t, result.body), result.body, status, code)
	}
	requireCommonHeaders(t, result.headers)
}

func requireCommonHeaders(t *testing.T, headers http.Header) {
	t.Helper()
	if headers.Get("Cache-Control") != "no-store" || headers.Get("X-Content-Type-Options") != "nosniff" ||
		headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("common headers=%v", headers)
	}
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 8 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: true,
			DialContext:       (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
		},
	}
}

func doRequest(t *testing.T, method, url string, body []byte, token string, mutate func(*http.Request)) httpResult {
	t.Helper()
	result := doRequestErr(method, url, body, token, mutate)
	if result.err != nil {
		t.Fatalf("HTTP request failed: %v", result.err)
	}
	return result
}

func doRequestErr(method, url string, body []byte, token string, mutate func(*http.Request)) httpResult {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		return httpResult{err: err}
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if mutate != nil {
		mutate(request)
	}
	response, err := newHTTPClient().Do(request)
	if err != nil {
		return httpResult{err: err}
	}
	defer response.Body.Close()
	payload, readErr := io.ReadAll(response.Body)
	return httpResult{body: payload, status: response.StatusCode, headers: response.Header, err: readErr}
}

func messageURL(server *runningServer) string {
	return "http://" + server.api + "/api/v1/messages"
}

func healthURL(server *runningServer) string {
	return "http://" + server.api + "/api/v1/health"
}

func readyURL(server *runningServer) string {
	return "http://" + server.ops + "/ready"
}

func metricsURL(server *runningServer) string {
	return "http://" + server.ops + "/metrics"
}

func mergeEnv(overrides map[string]string) []string {
	values := make(map[string]string)
	order := make([]string, 0)
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if _, exists := values[key]; !exists {
			order = append(order, key)
		}
		values[key] = value
	}
	for key, value := range overrides {
		if _, exists := values[key]; !exists {
			order = append(order, key)
		}
		values[key] = value
	}
	result := make([]string, 0, len(values))
	for _, key := range order {
		result = append(result, key+"="+values[key])
	}
	return result
}

func startMessageServer(t *testing.T, env map[string]string) *runningServer {
	t.Helper()
	apiAddr, opsAddr := twoFreeAddrs(t)
	return startProcess(t, os.Getenv("NEWIM_SERVER_BINARY"), apiAddr, opsAddr, env)
}

func startLossWrapper(t *testing.T, env map[string]string) *runningServer {
	t.Helper()
	binary := strings.TrimSpace(os.Getenv("NEWIM_LOSS_WRAPPER_BINARY"))
	if binary == "" {
		t.Fatal("NEWIM_LOSS_WRAPPER_BINARY is required")
	}
	apiAddr, opsAddr := twoFreeAddrs(t)
	return startProcess(t, binary, apiAddr, opsAddr, env)
}

func startProcess(t *testing.T, binary, apiAddr, opsAddr string, env map[string]string) *runningServer {
	t.Helper()
	if strings.TrimSpace(binary) == "" {
		t.Fatal("server binary is required")
	}
	cmd := exec.Command(binary, "--api-addr", apiAddr, "--ops-addr", opsAddr)
	cmd.Env = mergeEnv(env)
	server := &runningServer{cmd: cmd, api: apiAddr, ops: opsAddr, done: make(chan error, 1)}
	cmd.Stdout = &server.stdout
	cmd.Stderr = &server.stderr
	must(t, cmd.Start())
	go func() { server.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if !server.stopped {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			select {
			case <-server.done:
			case <-time.After(2 * time.Second):
			}
		}
	})
	return server
}

func (s *runningServer) waitHealthy(t *testing.T) {
	t.Helper()
	if err := waitForHTTPStatus(healthURL(s), http.StatusOK, 8*time.Second); err != nil {
		t.Fatalf("health failed: %v stderr=%q", err, s.stderr.String())
	}
}

func (s *runningServer) waitReady(t *testing.T) {
	t.Helper()
	if err := waitForHTTPStatus(readyURL(s), http.StatusOK, 8*time.Second); err != nil {
		t.Fatalf("readiness failed: %v stderr=%q", err, s.stderr.String())
	}
}

func (s *runningServer) signalSIGTERM(t *testing.T) {
	t.Helper()
	if s.stopped {
		return
	}
	must(t, s.cmd.Process.Signal(syscall.SIGTERM))
}

func (s *runningServer) waitExit(t *testing.T, timeout time.Duration) error {
	t.Helper()
	if s.stopped {
		return s.failed
	}
	select {
	case err := <-s.done:
		s.stopped = true
		s.failed = err
		return err
	case <-time.After(timeout):
		_ = s.cmd.Process.Kill()
		<-s.done
		s.stopped = true
		s.failed = errors.New("shutdown deadline exceeded")
		return s.failed
	}
}

func (s *runningServer) pollExit() bool {
	if s == nil || s.stopped {
		return s != nil
	}
	select {
	case err := <-s.done:
		s.stopped = true
		s.failed = err
		return true
	default:
		return false
	}
}

func (s *runningServer) stop(t *testing.T) {
	t.Helper()
	if s == nil || s.stopped {
		return
	}
	s.signalSIGTERM(t)
	if err := s.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("SIGTERM exit: %v stderr=%q", err, s.stderr.String())
	}
}

func (s *runningServer) logs() string {
	return s.stdout.String() + s.stderr.String()
}

func waitForHTTPStatus(url string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 250 * time.Millisecond}
	var last error
	for time.Now().Before(deadline) {
		response, err := client.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == want {
				return nil
			}
			last = errors.New(response.Status)
		} else {
			last = err
		}
		time.Sleep(25 * time.Millisecond)
	}
	if last == nil {
		last = errors.New("deadline exceeded")
	}
	return last
}

func waitForReadyNotReady(t *testing.T, server *runningServer, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 250 * time.Millisecond}
	for time.Now().Before(deadline) {
		response, err := client.Get(readyURL(server))
		if err == nil {
			body, _ := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusServiceUnavailable && strings.Contains(string(body), "SERVER_NOT_READY") {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("readiness was not observed false before listener close; stderr=%q", server.stderr.String())
}

func activeConnections(t *testing.T, f *fixture, application string) int64 {
	t.Helper()
	return f.scalarInt("SELECT count(*) FROM pg_stat_activity WHERE application_name=$1", application)
}

func waitConnectionRange(t *testing.T, f *fixture, application string, minimum, maximum int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var count int64
	for time.Now().Before(deadline) {
		count = activeConnections(t, f, application)
		if count >= minimum && count <= maximum {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s connections=%d want [%d,%d]", application, count, minimum, maximum)
}

func waitNoConnections(t *testing.T, f *fixture, applications ...string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, application := range applications {
			if activeConnections(t, f, application) != 0 {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, application := range applications {
		if count := activeConnections(t, f, application); count != 0 {
			t.Fatalf("%s connections remained open: %d", application, count)
		}
	}
}

func waitBackendPID(t *testing.T, f *fixture, application, queryFragment string, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var pid int
		err := f.db.QueryRow(ctx, `SELECT pid FROM pg_stat_activity
			WHERE application_name=$1 AND pid<>pg_backend_pid()
			  AND wait_event_type='Lock' AND query ILIKE $2
			ORDER BY query_start ASC LIMIT 1`, application, "%"+queryFragment+"%").Scan(&pid)
		if err == nil {
			return pid
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s backend did not block on %q", application, queryFragment)
	return 0
}

func terminateBackend(t *testing.T, f *fixture, pid int) {
	t.Helper()
	_, err := f.db.Exec(ctx, "SELECT pg_terminate_backend($1)", pid)
	must(t, err)
}

func twoFreeAddrs(t *testing.T) (string, string) {
	t.Helper()
	for {
		a, b := freeAddr(t), freeAddr(t)
		if a != b {
			return a, b
		}
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	address := listener.Addr().String()
	must(t, listener.Close())
	return address
}

func runServerExpectExit(t *testing.T, args []string, env map[string]string, timeout time.Duration) processResult {
	t.Helper()
	command := exec.Command(os.Getenv("NEWIM_SERVER_BINARY"), args...)
	command.Env = mergeEnv(env)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	done := make(chan error, 1)
	must(t, command.Start())
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		code := 0
		if err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("server wait failed: %v", err)
			}
			code = exitErr.ExitCode()
		}
		return processResult{code: code, stdout: stdout.String(), stderr: stderr.String()}
	case <-time.After(timeout):
		_ = command.Process.Kill()
		<-done
		t.Fatalf("startup failure command deadline exceeded; stdout=%q stderr=%q", stdout.String(), stderr.String())
		return processResult{}
	}
}

func assertPortReusable(t *testing.T, address string) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("address %s remained bound: %v", address, err)
	}
	must(t, listener.Close())
}

func rawChunkedRequest(t *testing.T, server *runningServer, token string, contentType string, body []byte) httpResult {
	t.Helper()
	connection, err := net.DialTimeout("tcp", server.api, 3*time.Second)
	must(t, err)
	defer connection.Close()
	must(t, connection.SetDeadline(time.Now().Add(8*time.Second)))
	request := fmt.Sprintf("POST /api/v1/messages HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: %s\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n%x\r\n", server.api, token, contentType, len(body))
	_, err = io.WriteString(connection, request)
	must(t, err)
	_, err = connection.Write(body)
	must(t, err)
	_, err = io.WriteString(connection, "\r\n0\r\n\r\n")
	must(t, err)
	response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodPost})
	must(t, err)
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	must(t, err)
	return httpResult{body: payload, status: response.StatusCode, headers: response.Header}
}

func rawFixedLengthRequest(t *testing.T, server *runningServer, token string, contentType string, contentLength int) httpResult {
	t.Helper()
	connection, err := net.DialTimeout("tcp", server.api, 3*time.Second)
	must(t, err)
	defer connection.Close()
	must(t, connection.SetDeadline(time.Now().Add(8*time.Second)))
	request := fmt.Sprintf("POST /api/v1/messages HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n",
		server.api, token, contentType, contentLength)
	_, err = io.WriteString(connection, request)
	must(t, err)
	response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodPost})
	must(t, err)
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	must(t, err)
	return httpResult{body: payload, status: response.StatusCode, headers: response.Header}
}
