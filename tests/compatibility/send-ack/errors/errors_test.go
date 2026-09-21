package errors_test

import (
	p "github.com/m-ice/NewIM/core/protocol/go"
	c "github.com/m-ice/NewIM/tests/compatibility/send-ack/common"
	"testing"
)

func TestSharedErrors(t *testing.T) {
	for _, f := range c.Frames(t, "errors") {
		t.Run(f.ID, func(t *testing.T) {
			var e error
			switch f.Direction {
			case "send":
				_, e = p.DecodeSend(c.Wire(t, f))
			case "server":
				_, e = p.DecodeServerFrame(c.Wire(t, f))
			default:
				t.Fatal("bad direction")
			}
			c.Error(t, e, f.Error)
		})
	}
}
func TestCorrelationAndConflicts(t *testing.T) {
	request := c.Request(`{"x":1}`)
	ack := c.Ack()
	message := c.Message(`{"x":1.00}`)
	a, m := p.ServerFrame{Ack: &ack}, p.ServerFrame{Message: &message}
	for _, f := range []p.ServerFrame{a, m} {
		c.Error(t, p.Correlate(request, "u1", f), nil)
	}
	c.Error(t, p.CompareResults(a, m), nil)
	c.Error(t, p.CompareResults(m, a), nil)
	c.Error(t, p.CompareResults(a, a), nil)
	for _, change := range []func(*p.Ack){func(a *p.Ack) { a.ClientMsgID = "wrong" }, func(a *p.Ack) { a.ConversationID = "wrong" }, func(a *p.Ack) { a.SenderID = "wrong" }} {
		b := ack
		change(&b)
		c.Code(t, p.Correlate(request, "u1", p.ServerFrame{Ack: &b}), "SEND_CORRELATION_MISMATCH")
		c.Code(t, p.CompareResults(a, p.ServerFrame{Ack: &b}), "SEND_CORRELATION_MISMATCH")
	}
	for _, change := range []func(*p.Ack){func(a *p.Ack) { a.ServerMsgID = "other" }, func(a *p.Ack) { a.ConversationSeq = "2" }, func(a *p.Ack) { a.ServerTime = "2" }} {
		b := ack
		change(&b)
		c.Code(t, p.CompareResults(a, p.ServerFrame{Ack: &b}), "SEND_RESULT_CONFLICT")
	}
	message.Payload = []byte(`{"x":2}`)
	c.Code(t, p.Correlate(request, "u1", m), "SEND_CORRELATION_MISMATCH")
	message.Payload = []byte(`{"x":1}`)
	message.Type = "other"
	c.Code(t, p.Correlate(request, "u1", m), "SEND_CORRELATION_MISMATCH")
	failure := p.ServerFrame{Error: &p.SendError{ClientMsgID: "c1", ConversationID: "room1", Code: "FUTURE_CODE"}}
	c.Error(t, p.Correlate(request, "u1", failure), nil)
	c.Code(t, p.CompareResults(a, failure), "PROTOCOL_INVALID_MESSAGE")
	failure.Error.ClientMsgID = "wrong"
	c.Code(t, p.Correlate(request, "u1", failure), "SEND_CORRELATION_MISMATCH")
	c.Code(t, p.Correlate(request, "bad sender", a), "PROTOCOL_INVALID_MESSAGE")
}
func TestDisposition(t *testing.T) {
	for code, want := range map[string]p.RetryDisposition{"SERVER_TEMPORARY_UNAVAILABLE": p.RetrySameIntent, "AUTH_REQUIRED": p.AuthRecovery, "AUTH_TOKEN_EXPIRED": p.AuthRecovery, "SEND_ID_CONFLICT": p.StopAutomaticRetry, "MESSAGE_PERMISSION_DENIED": p.StopAutomaticRetry, "MESSAGE_UNSUPPORTED_SCHEMA": p.StopAutomaticRetry, "MESSAGE_TOO_LARGE": p.StopAutomaticRetry, "PROTOCOL_INVALID_MESSAGE": p.StopAutomaticRetry, "PROTOCOL_UNSUPPORTED_VERSION": p.StopAutomaticRetry, "FUTURE_UNKNOWN": p.StopAutomaticRetry} {
		if p.Disposition(code) != want {
			t.Fatal(code)
		}
	}
}
func TestEncoderInvalidObjects(t *testing.T) {
	a := c.Ack()
	_, e := p.EncodeServerFrame(p.ServerFrame{})
	c.Code(t, e, "PROTOCOL_INVALID_MESSAGE")
	_, e = p.EncodeServerFrame(p.ServerFrame{Ack: &a, Message: func() *p.Message { m := c.Message(`{}`); return &m }()})
	c.Code(t, e, "PROTOCOL_INVALID_MESSAGE")
	a.ConversationSeq = "01"
	_, e = p.EncodeServerFrame(p.ServerFrame{Ack: &a})
	c.Code(t, e, "PROTOCOL_INVALID_MESSAGE")
	s := c.Request(`{},"injected":true`)
	_, e = p.EncodeSend(s)
	c.Code(t, e, "PROTOCOL_INVALID_JSON")
	s = c.Request(`{"x":1,"x":2}`)
	s.ClientMsgID = ""
	_, e = p.EncodeSend(s)
	c.Code(t, e, "PROTOCOL_INVALID_JSON")
}
