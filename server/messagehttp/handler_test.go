package messagehttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	app "github.com/m-ice/NewIM/server/auth/session"
	messageservice "github.com/m-ice/NewIM/server/message"
)

type authenticatorFunc func(context.Context, string) (app.BearerSession, error)

func (f authenticatorFunc) AuthenticateBearer(ctx context.Context, token string) (app.BearerSession, error) {
	return f(ctx, token)
}

type senderFunc func(context.Context, app.ConnectionIdentity, protocol.Send) (protocol.ServerFrame, error)

func (f senderFunc) Send(ctx context.Context, identity app.ConnectionIdentity, request protocol.Send) (protocol.ServerFrame, error) {
	return f(ctx, identity, request)
}

type noopObserver struct{}

func (noopObserver) Observe(app.Observation) {}

type memoryAuthStore struct {
	mu      sync.Mutex
	binding app.SessionBinding
	tokenID string
	digest  [32]byte
	expires time.Time
}

func (s *memoryAuthStore) Issue(_ context.Context, binding app.SessionBinding, generate func() (app.IssuedToken, error)) (app.IssuedToken, error) {
	issued, err := generate()
	if err != nil {
		return app.IssuedToken{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.binding = binding
	s.tokenID = issued.TokenID()
	s.digest = issued.Digest()
	s.expires = issued.ExpiresAt()
	return issued, nil
}

func (s *memoryAuthStore) Authenticate(_ context.Context, tokenID string, verify func(app.TokenSnapshot) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tokenID != s.tokenID {
		return app.Fail(app.AuthTokenUnknown)
	}
	snapshot, err := app.NewTokenSnapshot(s.tokenID, s.digest[:], s.binding, s.expires, nil, nil)
	if err != nil {
		return err
	}
	return verify(snapshot)
}

func (*memoryAuthStore) RevokeBearer(context.Context, string, func(app.TokenSnapshot) error, time.Time) (app.RevocationOutcome, error) {
	return "", app.Fail(app.AuthStorageUnavailable)
}

func (s *memoryAuthStore) LookupSession(context.Context, string) (app.SessionSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return app.NewSessionSnapshot(s.binding, nil)
}

func (*memoryAuthStore) RevokeSession(context.Context, app.SessionBinding, time.Time) (app.RevocationOutcome, error) {
	return "", app.Fail(app.AuthStorageUnavailable)
}

func (*memoryAuthStore) RevokeToken(context.Context, string, string, time.Time) (app.RevocationOutcome, error) {
	return "", app.Fail(app.AuthStorageUnavailable)
}

func newAuthFixture(t *testing.T) (*app.Service, app.IssuedToken) {
	t.Helper()
	store := &memoryAuthStore{}
	service, err := app.NewService(store, app.Config{
		Clock:    app.ClockFunc(func() time.Time { return time.Unix(1_800_000_000, 0).UTC() }),
		Observer: noopObserver{},
		Entropy:  bytes.NewReader(make([]byte, app.TokenIDHexLen/2+app.TokenSecretLen)),
	})
	if err != nil {
		t.Fatal(err)
	}
	token, err := service.Issue(context.Background(), app.IssueRequest{
		Binding: app.SessionBinding{UserID: "user_1", DeviceID: "device_1", SessionID: "session_1"},
		TTL:     time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service, token
}

func validSendBody(t *testing.T) []byte {
	t.Helper()
	body, err := protocol.EncodeSend(protocol.Send{
		ProtocolVersion: 1,
		ClientMsgID:     "client_1",
		ConversationID:  "conversation_1",
		Version:         1,
		Type:            "text",
		Payload:         json.RawMessage(`{"text":"hello"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func newMessageRequest(t *testing.T, rawToken string, body []byte) *http.Request {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, Route, bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+rawToken)
	request.Header.Set("Content-Type", "application/json")
	return request
}

func mustHandler(t *testing.T, authenticator Authenticator, sender Sender, maxBodyBytes int64) *Handler {
	t.Helper()
	handler, err := NewHandler(authenticator, sender, maxBodyBytes)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func serve(handler http.Handler, request *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func decodeCode(t *testing.T, body []byte) Code {
	t.Helper()
	var envelope struct {
		Error struct {
			Code Code `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode error response: %v body=%q", err, body)
	}
	return envelope.Error.Code
}

func requireCommonHeaders(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Header().Get("Cache-Control") != "no-store" ||
		recorder.Header().Get("X-Content-Type-Options") != "nosniff" ||
		recorder.Header().Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("unexpected common headers: %v", recorder.Header())
	}
}

func TestNewHandlerRejectsInvalidConfiguration(t *testing.T) {
	service, _ := newAuthFixture(t)
	sender := senderFunc(func(context.Context, app.ConnectionIdentity, protocol.Send) (protocol.ServerFrame, error) {
		return protocol.ServerFrame{}, nil
	})
	if _, err := NewHandler(nil, sender, 1); err == nil {
		t.Fatal("nil authenticator accepted")
	}
	if _, err := NewHandler(service, nil, 1); err == nil {
		t.Fatal("nil sender accepted")
	}
	if _, err := NewHandler(service, sender, 0); err == nil {
		t.Fatal("zero body limit accepted")
	}
}

func TestHandlerSuccessUsesTokenScopedIdentityAndProtocolACK(t *testing.T) {
	service, token := newAuthFixture(t)
	body := validSendBody(t)
	var capturedIdentity app.ConnectionIdentity
	var capturedRequest protocol.Send
	sender := senderFunc(func(_ context.Context, identity app.ConnectionIdentity, request protocol.Send) (protocol.ServerFrame, error) {
		capturedIdentity = identity
		capturedRequest = request
		return protocol.ServerFrame{Ack: &protocol.Ack{
			ClientMsgID:     request.ClientMsgID,
			ConversationID:  request.ConversationID,
			SenderID:        identity.UserID(),
			ServerMsgID:     "server_1",
			ConversationSeq: "7",
			ServerTime:      "1800000000000",
		}}, nil
	})
	handler := mustHandler(t, service, sender, int64(len(body)))

	recorder := serve(handler, newMessageRequest(t, token.RawToken(), body))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	requireCommonHeaders(t, recorder)
	if recorder.Header().Get("WWW-Authenticate") != "" || recorder.Header().Get("Retry-After") != "" {
		t.Fatalf("unexpected headers: %v", recorder.Header())
	}
	if capturedIdentity.UserID() != "user_1" || capturedIdentity.DeviceID() != "device_1" ||
		capturedIdentity.SessionID() != "session_1" || capturedIdentity.ConnectionID() != token.TokenID() ||
		capturedIdentity.TokenID() != token.TokenID() {
		t.Fatalf("identity=%+v tokenID=%s", capturedIdentity, token.TokenID())
	}
	if capturedRequest.ClientMsgID != "client_1" || capturedRequest.ConversationID != "conversation_1" || capturedRequest.Type != "text" {
		t.Fatalf("request=%+v", capturedRequest)
	}
	frame, err := protocol.DecodeServerFrame(recorder.Body.Bytes())
	if err != nil || frame.Ack == nil || frame.Error != nil || frame.Message != nil {
		t.Fatalf("frame=%+v error=%v body=%q", frame, err, recorder.Body.String())
	}
	if frame.Ack.ServerMsgID != "server_1" || frame.Ack.ConversationSeq != "7" || !strings.Contains(recorder.Body.String(), "SERVER_PERSISTED") {
		t.Fatalf("ack=%+v body=%q", frame.Ack, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), token.RawToken()) {
		t.Fatal("raw token reflected in success response")
	}
}

func TestHandlerStrictHeaders(t *testing.T) {
	service, token := newAuthFixture(t)
	body := validSendBody(t)
	sender := senderFunc(func(_ context.Context, identity app.ConnectionIdentity, request protocol.Send) (protocol.ServerFrame, error) {
		return protocol.ServerFrame{Ack: &protocol.Ack{
			ClientMsgID: request.ClientMsgID, ConversationID: request.ConversationID, SenderID: identity.UserID(),
			ServerMsgID: "server_1", ConversationSeq: "1", ServerTime: "1800000000000",
		}}, nil
	})
	handler := mustHandler(t, service, sender, int64(len(body)))

	tests := []struct {
		name   string
		mutate func(*http.Request)
		status int
		code   Code
	}{
		{name: "missing-content-type", mutate: func(r *http.Request) { r.Header.Del("Content-Type") }, status: http.StatusUnsupportedMediaType, code: CodeUnsupportedMediaType},
		{name: "duplicate-content-type", mutate: func(r *http.Request) { r.Header.Add("Content-Type", "application/json") }, status: http.StatusUnsupportedMediaType, code: CodeUnsupportedMediaType},
		{name: "wrong-charset", mutate: func(r *http.Request) { r.Header.Set("Content-Type", "application/json; charset=iso-8859-1") }, status: http.StatusUnsupportedMediaType, code: CodeUnsupportedMediaType},
		{name: "unknown-parameter", mutate: func(r *http.Request) { r.Header.Set("Content-Type", "application/json; profile=v1") }, status: http.StatusUnsupportedMediaType, code: CodeUnsupportedMediaType},
		{name: "gzip", mutate: func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }, status: http.StatusUnsupportedMediaType, code: CodeUnsupportedMediaType},
		{name: "duplicate-encoding", mutate: func(r *http.Request) {
			r.Header.Set("Content-Encoding", "identity")
			r.Header.Add("Content-Encoding", "identity")
		}, status: http.StatusUnsupportedMediaType, code: CodeUnsupportedMediaType},
		{name: "duplicate-authorization", mutate: func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+token.RawToken()) }, status: http.StatusUnauthorized, code: CodeInvalidToken},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := newMessageRequest(t, token.RawToken(), body)
			test.mutate(request)
			recorder := serve(handler, request)
			if recorder.Code != test.status || decodeCode(t, recorder.Body.Bytes()) != test.code {
				t.Fatalf("status=%d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
			}
			requireCommonHeaders(t, recorder)
			if test.status == http.StatusUnauthorized && recorder.Header().Get("WWW-Authenticate") != bearerChallenge {
				t.Fatalf("missing bearer challenge: %v", recorder.Header())
			}
		})
	}

	for name, contentType := range map[string]string{
		"utf8-case": "application/json; charset=UTF-8",
		"charset":   "application/json; charset=utf-8",
	} {
		t.Run(name, func(t *testing.T) {
			request := newMessageRequest(t, token.RawToken(), body)
			request.Header.Set("Content-Type", contentType)
			request.Header.Set("Content-Encoding", "identity")
			recorder := serve(handler, request)
			if recorder.Code != http.StatusOK {
				t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestHandlerMethodRouteAndQueryPrecedence(t *testing.T) {
	service, token := newAuthFixture(t)
	body := validSendBody(t)
	handler := mustHandler(t, service, senderFunc(func(_ context.Context, identity app.ConnectionIdentity, request protocol.Send) (protocol.ServerFrame, error) {
		return protocol.ServerFrame{Ack: &protocol.Ack{
			ClientMsgID: request.ClientMsgID, ConversationID: request.ConversationID, SenderID: identity.UserID(),
			ServerMsgID: "server_1", ConversationSeq: "1", ServerTime: "1",
		}}, nil
	}), int64(len(body)))

	t.Run("wrong-method", func(t *testing.T) {
		request := newMessageRequest(t, token.RawToken(), body)
		request.Method = http.MethodGet
		request.URL.RawQuery = "invalid=true"
		recorder := serve(handler, request)
		if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != http.MethodPost || decodeCode(t, recorder.Body.Bytes()) != CodeMethodNotAllowed {
			t.Fatalf("status=%d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
		}
	})

	t.Run("route", func(t *testing.T) {
		request := newMessageRequest(t, token.RawToken(), body)
		request.URL.Path = Route + "/"
		recorder := serve(handler, request)
		if recorder.Code != http.StatusNotFound || decodeCode(t, recorder.Body.Bytes()) != CodeRouteNotFound {
			t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
		}
	})

	for name, mutate := range map[string]func(*http.Request){
		"raw-query": func(r *http.Request) { r.URL.RawQuery = "x=1" },
		"force-query": func(r *http.Request) {
			r.URL.RawQuery = ""
			r.URL.ForceQuery = true
		},
		"fragment": func(r *http.Request) { r.URL.Fragment = "secret" },
	} {
		t.Run(name, func(t *testing.T) {
			request := newMessageRequest(t, token.RawToken(), body)
			request.Header.Set("Content-Type", "text/plain")
			request.ContentLength = int64(len(body)) + 1
			mutate(request)
			recorder := serve(handler, request)
			if recorder.Code != http.StatusBadRequest || decodeCode(t, recorder.Body.Bytes()) != CodeInvalidQuery {
				t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestHandlerBodyBoundaries(t *testing.T) {
	service, token := newAuthFixture(t)
	body := validSendBody(t)
	successSender := senderFunc(func(_ context.Context, identity app.ConnectionIdentity, request protocol.Send) (protocol.ServerFrame, error) {
		return protocol.ServerFrame{Ack: &protocol.Ack{
			ClientMsgID: request.ClientMsgID, ConversationID: request.ConversationID, SenderID: identity.UserID(),
			ServerMsgID: "server_1", ConversationSeq: "1", ServerTime: "1",
		}}, nil
	})
	handler := mustHandler(t, service, successSender, int64(len(body)))

	t.Run("known-over-limit-precedes-media-type", func(t *testing.T) {
		oversized := append(append([]byte(nil), body...), ' ')
		request := newMessageRequest(t, token.RawToken(), oversized)
		request.Header.Set("Content-Type", "text/plain")
		request.Header.Del("Authorization")
		recorder := serve(handler, request)
		if recorder.Code != http.StatusRequestEntityTooLarge || decodeCode(t, recorder.Body.Bytes()) != CodeBodyTooLarge {
			t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("unknown-over-limit-after-valid-headers", func(t *testing.T) {
		oversized := append(append([]byte(nil), body...), ' ')
		request := newMessageRequest(t, token.RawToken(), oversized)
		request.Header.Del("Authorization")
		request.ContentLength = -1
		request.TransferEncoding = []string{"chunked"}
		recorder := serve(handler, request)
		if recorder.Code != http.StatusRequestEntityTooLarge || decodeCode(t, recorder.Body.Bytes()) != CodeBodyTooLarge {
			t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("unknown-invalid-header-does-not-read", func(t *testing.T) {
		counter := &countingReader{}
		request := httptest.NewRequest(http.MethodPost, Route, counter)
		request.Header.Set("Authorization", "Bearer "+token.RawToken())
		request.Header.Set("Content-Type", "text/plain")
		request.ContentLength = -1
		request.TransferEncoding = []string{"chunked"}
		recorder := serve(handler, request)
		if recorder.Code != http.StatusUnsupportedMediaType || decodeCode(t, recorder.Body.Bytes()) != CodeUnsupportedMediaType || counter.reads != 0 {
			t.Fatalf("status=%d reads=%d body=%q", recorder.Code, counter.reads, recorder.Body.String())
		}
	})

	t.Run("unknown-valid-boundary", func(t *testing.T) {
		request := newMessageRequest(t, token.RawToken(), body)
		request.ContentLength = -1
		request.TransferEncoding = []string{"chunked"}
		recorder := serve(handler, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("protocol-too-large", func(t *testing.T) {
		large := bytes.Repeat([]byte{' '}, protocol.MaxFrameBytes+1)
		largeHandler := mustHandler(t, service, successSender, int64(len(large)))
		recorder := serve(largeHandler, newMessageRequest(t, token.RawToken(), large))
		if recorder.Code != http.StatusRequestEntityTooLarge || decodeCode(t, recorder.Body.Bytes()) != CodeBodyTooLarge {
			t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
		}
	})
}

type countingReader struct {
	reads int
}

func (r *countingReader) Read([]byte) (int, error) {
	r.reads++
	return 0, io.EOF
}

func TestHandlerProtocolValidation(t *testing.T) {
	service, token := newAuthFixture(t)
	senderCalls := 0
	handler := mustHandler(t, service, senderFunc(func(context.Context, app.ConnectionIdentity, protocol.Send) (protocol.ServerFrame, error) {
		senderCalls++
		return protocol.ServerFrame{}, nil
	}), 4096)

	base := `{"protocolVersion":1,"kind":"send","body":{"clientMsgId":"client_1","conversationId":"conversation_1","version":1,"type":"text","payload":{"text":"hello"}}}`
	baseWithForgedIdentity := `{"protocolVersion":1,"kind":"send","body":{"clientMsgId":"client_1","conversationId":"conversation_1","version":1,"type":"text","payload":{"text":"hello"},"senderId":"mallory"}}`
	unknownType := `{"protocolVersion":1,"kind":"send","body":{"clientMsgId":"client_1","conversationId":"conversation_1","version":1,"type":"video","payload":{"uri":"https://example.invalid/secret"}}}`
	invalidMessage := `{"protocolVersion":1,"kind":"send","body":{"clientMsgId":"client_1","conversationId":"conversation_1","version":1,"type":"text","payload":{"text":""}}}`
	unsupportedVersion := `{"protocolVersion":2,"kind":"send","body":{"clientMsgId":"client_1","conversationId":"conversation_1","version":1,"type":"text","payload":{"text":"hello"}}}`
	unsupportedFrame := `{"protocolVersion":1,"kind":"ping","body":{"clientMsgId":"client_1","conversationId":"conversation_1","version":1,"type":"text","payload":{"text":"hello"}}}`
	tooDeep := `{"protocolVersion":1,"kind":"send","body":{"clientMsgId":"client_1","conversationId":"conversation_1","version":1,"type":"text","payload":{"text":"hello","extra":` +
		strings.Repeat("[", protocol.MaxDepth+2) + `0` + strings.Repeat("]", protocol.MaxDepth+2) + `}}}`
	mediaPayload, err := protocol.EncodeMediaPayload(protocol.MediaMetadata{
		MediaKey: "media_key", Kind: "image", ContentType: "image/jpeg", Size: "1",
		SHA256: strings.Repeat("a", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	mediaWire, err := protocol.EncodeSend(protocol.Send{
		ProtocolVersion: 1, ClientMsgID: "client_media", ConversationID: "conversation_1",
		Version: 1, Type: "media", Payload: mediaPayload,
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		body string
		code Code
	}{
		{name: "invalid-json", body: `{`, code: CodeInvalidInput},
		{name: "invalid-message", body: invalidMessage, code: CodeInvalidInput},
		{name: "too-deep", body: tooDeep, code: CodeInvalidInput},
		{name: "unsupported-version", body: unsupportedVersion, code: CodeInvalidInput},
		{name: "unsupported-frame", body: unsupportedFrame, code: CodeInvalidInput},
		{name: "forged-sender", body: baseWithForgedIdentity, code: CodeInvalidInput},
		{name: "media-not-enabled", body: string(mediaWire), code: CodeInvalidInput},
		{name: "unsupported-type", body: unknownType, code: CodeInvalidInput},
		{name: "trailing-json", body: base + `{}`, code: CodeInvalidInput},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := serve(handler, newMessageRequest(t, token.RawToken(), []byte(test.body)))
			if recorder.Code != http.StatusBadRequest || decodeCode(t, recorder.Body.Bytes()) != test.code {
				t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
			}
			if strings.Contains(recorder.Body.String(), "secret") || strings.Contains(recorder.Body.String(), token.RawToken()) {
				t.Fatalf("sensitive input reflected: %q", recorder.Body.String())
			}
		})
	}
	if senderCalls != 0 {
		t.Fatalf("sender called %d times for invalid protocol input", senderCalls)
	}
}

func TestHandlerAuthenticationErrorMapping(t *testing.T) {
	body := validSendBody(t)
	tests := []struct {
		name   string
		err    error
		status int
		code   Code
	}{
		{name: "malformed", err: app.Fail(app.AuthTokenMalformed), status: http.StatusUnauthorized, code: CodeInvalidToken},
		{name: "expired", err: app.Fail(app.AuthTokenExpired), status: http.StatusUnauthorized, code: CodeInvalidToken},
		{name: "session-revoked", err: app.Fail(app.AuthSessionRevoked), status: http.StatusUnauthorized, code: CodeInvalidToken},
		{name: "binding-mismatch", err: app.Fail(app.AuthForbidden), status: http.StatusUnauthorized, code: CodeInvalidToken},
		{name: "storage", err: app.Fail(app.AuthStorageUnavailable), status: http.StatusServiceUnavailable, code: CodeAuthUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := mustHandler(t, authenticatorFunc(func(context.Context, string) (app.BearerSession, error) {
				return app.BearerSession{}, test.err
			}), senderFunc(func(context.Context, app.ConnectionIdentity, protocol.Send) (protocol.ServerFrame, error) {
				t.Fatal("sender called after authentication failure")
				return protocol.ServerFrame{}, nil
			}), int64(len(body)))
			recorder := serve(handler, newMessageRequest(t, "n1_0123456789abcdef0123456789abcdef_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", body))
			if recorder.Code != test.status || decodeCode(t, recorder.Body.Bytes()) != test.code {
				t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
			}
			requireCommonHeaders(t, recorder)
			if test.status == http.StatusUnauthorized && recorder.Header().Get("WWW-Authenticate") != bearerChallenge {
				t.Fatalf("missing bearer challenge: %v", recorder.Header())
			}
		})
	}
}

func TestHandlerSendErrorMapping(t *testing.T) {
	service, token := newAuthFixture(t)
	body := validSendBody(t)
	tests := []struct {
		name       string
		err        error
		status     int
		code       Code
		retryAfter bool
	}{
		{name: "invalid-input", err: messageservice.Fail(messageservice.SendInvalidInput), status: http.StatusBadRequest, code: CodeInvalidInput},
		{name: "unauthorized", err: messageservice.Fail(messageservice.SendUnauthorized), status: http.StatusForbidden, code: CodeUnauthorized},
		{name: "missing", err: messageservice.Fail(messageservice.SendConversationMissing), status: http.StatusNotFound, code: CodeConversationNotFound},
		{name: "conflict", err: messageservice.Fail(messageservice.SendIDConflict), status: http.StatusConflict, code: CodeIDConflict},
		{name: "sequence", err: messageservice.Fail(messageservice.SendSequenceExhausted), status: http.StatusConflict, code: CodeSequenceExhausted},
		{name: "storage", err: messageservice.Fail(messageservice.SendStorageUnavailable), status: http.StatusServiceUnavailable, code: CodeTemporaryUnavailable, retryAfter: true},
		{name: "lock", err: messageservice.Fail(messageservice.SendLockUnavailable), status: http.StatusServiceUnavailable, code: CodeTemporaryUnavailable, retryAfter: true},
		{name: "unknown-code", err: messageservice.Fail(messageservice.SendUnknown), status: http.StatusInternalServerError, code: CodeSendUnknown},
		{name: "unknown-error", err: errors.New("sql sentinel DSN secret"), status: http.StatusInternalServerError, code: CodeSendUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := mustHandler(t, service, senderFunc(func(context.Context, app.ConnectionIdentity, protocol.Send) (protocol.ServerFrame, error) {
				return protocol.ServerFrame{}, test.err
			}), int64(len(body)))
			recorder := serve(handler, newMessageRequest(t, token.RawToken(), body))
			if recorder.Code != test.status || decodeCode(t, recorder.Body.Bytes()) != test.code {
				t.Fatalf("status=%d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
			}
			requireCommonHeaders(t, recorder)
			if got := recorder.Header().Get("Retry-After"); got != "" && got != "1" {
				t.Fatalf("retry-after=%q", got)
			}
			if test.retryAfter && recorder.Header().Get("Retry-After") != "1" {
				t.Fatalf("missing retry-after: %v", recorder.Header())
			}
			if strings.Contains(recorder.Body.String(), "sentinel") || strings.Contains(recorder.Body.String(), token.RawToken()) {
				t.Fatalf("sensitive error reflected: %q", recorder.Body.String())
			}
		})
	}
}
