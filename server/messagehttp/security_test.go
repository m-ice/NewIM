package messagehttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	app "github.com/m-ice/NewIM/server/auth/session"
)

type typedNilAuthenticator struct{}

func (*typedNilAuthenticator) AuthenticateBearer(context.Context, string) (app.BearerSession, error) {
	return app.BearerSession{}, nil
}

type typedNilSender struct{}

func (*typedNilSender) Send(context.Context, app.ConnectionIdentity, protocol.Send) (protocol.ServerFrame, error) {
	return protocol.ServerFrame{}, nil
}

func TestNewHandlerRejectsTypedNilDependencies(t *testing.T) {
	var authenticator *typedNilAuthenticator
	var sender *typedNilSender
	service, _ := newAuthFixture(t)
	if _, err := NewHandler(authenticator, sender, 1); err == nil {
		t.Fatal("typed nil dependencies accepted")
	}
	if _, err := NewHandler(service, sender, 1); err == nil {
		t.Fatal("typed nil sender accepted")
	}
}

func TestHandlerSafeNilRequestAndHandler(t *testing.T) {
	service, token := newAuthFixture(t)
	body := validSendBody(t)
	handler := mustHandler(t, service, senderFunc(func(context.Context, app.ConnectionIdentity, protocol.Send) (protocol.ServerFrame, error) {
		return protocol.ServerFrame{}, nil
	}), int64(len(body)))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, nil)
	if recorder.Code != http.StatusBadRequest || decodeCode(t, recorder.Body.Bytes()) != CodeInvalidInput {
		t.Fatalf("nil request status=%d body=%q", recorder.Code, recorder.Body.String())
	}

	var nilHandler *Handler
	recorder = serve(nilHandler, newMessageRequest(t, token.RawToken(), body))
	if recorder.Code != http.StatusInternalServerError || decodeCode(t, recorder.Body.Bytes()) != CodeInternalError {
		t.Fatalf("nil handler status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestHandlerRecoversAndRedactsPanics(t *testing.T) {
	body := validSendBody(t)
	rawToken := "n1_0123456789abcdef0123456789abcdef_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	panicSentinel := "token=secret body=sensitive sql=select dsn=postgres"

	t.Run("authenticator", func(t *testing.T) {
		handler := mustHandler(t, authenticatorFunc(func(context.Context, string) (app.BearerSession, error) {
			panic(panicSentinel)
		}), senderFunc(func(context.Context, app.ConnectionIdentity, protocol.Send) (protocol.ServerFrame, error) {
			t.Fatal("sender called after authenticator panic")
			return protocol.ServerFrame{}, nil
		}), int64(len(body)))
		recorder := serve(handler, newMessageRequest(t, rawToken, body))
		if recorder.Code != http.StatusInternalServerError || decodeCode(t, recorder.Body.Bytes()) != CodeInternalError ||
			strings.Contains(recorder.Body.String(), panicSentinel) || strings.Contains(recorder.Body.String(), rawToken) {
			t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("sender", func(t *testing.T) {
		service, token := newAuthFixture(t)
		handler := mustHandler(t, service, senderFunc(func(context.Context, app.ConnectionIdentity, protocol.Send) (protocol.ServerFrame, error) {
			panic(panicSentinel)
		}), int64(len(body)))
		recorder := serve(handler, newMessageRequest(t, token.RawToken(), body))
		if recorder.Code != http.StatusInternalServerError || decodeCode(t, recorder.Body.Bytes()) != CodeInternalError ||
			strings.Contains(recorder.Body.String(), panicSentinel) || strings.Contains(recorder.Body.String(), token.RawToken()) {
			t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
		}
	})
}

func TestHandlerRejectsNonACKSuccessFrames(t *testing.T) {
	service, token := newAuthFixture(t)
	body := validSendBody(t)
	handler := mustHandler(t, service, senderFunc(func(context.Context, app.ConnectionIdentity, protocol.Send) (protocol.ServerFrame, error) {
		return protocol.ServerFrame{Error: &protocol.SendError{
			ClientMsgID: "client_1", ConversationID: "conversation_1", Code: "SERVER_TEMPORARY_UNAVAILABLE",
		}}, nil
	}), int64(len(body)))
	recorder := serve(handler, newMessageRequest(t, token.RawToken(), body))
	if recorder.Code != http.StatusInternalServerError || decodeCode(t, recorder.Body.Bytes()) != CodeSendUnknown {
		t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestHandlerRejectsMiscorrelatedACK(t *testing.T) {
	service, token := newAuthFixture(t)
	body := validSendBody(t)
	handler := mustHandler(t, service, senderFunc(func(context.Context, app.ConnectionIdentity, protocol.Send) (protocol.ServerFrame, error) {
		return protocol.ServerFrame{Ack: &protocol.Ack{
			ClientMsgID: "other_client", ConversationID: "conversation_1", SenderID: "user_1",
			ServerMsgID: "server_1", ConversationSeq: "1", ServerTime: "1",
		}}, nil
	}), int64(len(body)))
	recorder := serve(handler, newMessageRequest(t, token.RawToken(), body))
	if recorder.Code != http.StatusInternalServerError || decodeCode(t, recorder.Body.Bytes()) != CodeSendUnknown {
		t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}
