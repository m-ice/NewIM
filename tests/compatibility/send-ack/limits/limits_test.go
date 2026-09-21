package limits_test

import (
	"bytes"
	"encoding/json"
	p "github.com/m-ice/NewIM/core/protocol/go"
	c "github.com/m-ice/NewIM/tests/compatibility/send-ack/common"
	"strings"
	"testing"
)

func sizedBody(body []byte, n int) []byte {
	return append(append(bytes.Clone(body[:len(body)-1]), bytes.Repeat([]byte(" "), n-len(body))...), '}')
}
func bodyOf(t *testing.T, wire []byte) []byte {
	t.Helper()
	var f map[string]json.RawMessage
	if e := json.Unmarshal(wire, &f); e != nil {
		t.Fatal(e)
	}
	return f["body"]
}
func TestBodyAndMetadataLimits(t *testing.T) {
	s, e := p.EncodeSend(c.Request(`{}`))
	c.Error(t, e, nil)
	ack := c.Ack()
	a, e := p.EncodeServerFrame(p.ServerFrame{Ack: &ack})
	c.Error(t, e, nil)
	er, e := p.EncodeServerFrame(p.ServerFrame{Error: &p.SendError{ClientMsgID: "c1", ConversationID: "room1", Code: "FUTURE"}})
	c.Error(t, e, nil)
	m, e := p.Encode(c.Message(`{}`))
	c.Error(t, e, nil)
	for _, v := range []struct {
		kind  string
		body  []byte
		limit int
		send  bool
	}{{"send", bodyOf(t, s), p.MaxSendBytes, true}, {"send_ack", bodyOf(t, a), p.MaxResultBytes, false}, {"send_error", bodyOf(t, er), p.MaxResultBytes, false}, {"message", m, p.MaxBytes, false}} {
		t.Run(v.kind, func(t *testing.T) {
			decode := func(w []byte) error {
				if v.send {
					_, e := p.DecodeSend(w)
					return e
				}
				_, e := p.DecodeServerFrame(w)
				return e
			}
			body := sizedBody(v.body, v.limit)
			wire := c.Wrap(v.kind, body)
			c.Error(t, decode(wire), nil)
			c.Code(t, decode(c.Wrap(v.kind, sizedBody(v.body, v.limit+1))), "MESSAGE_TOO_LARGE")
			overhead := len(wire) - len(body)
			if overhead > p.MaxMetadataBytes {
				t.Fatal("bad fixture")
			}
			wire = append(bytes.Repeat([]byte(" "), p.MaxMetadataBytes-overhead), wire...)
			if len(wire) != v.limit+p.MaxMetadataBytes {
				t.Fatal("bad boundary")
			}
			c.Error(t, decode(wire), nil)
			c.Code(t, decode(append([]byte(" "), wire...)), "MESSAGE_TOO_LARGE")
			t.Logf("body=%d metadata=%d", v.limit, p.MaxMetadataBytes)
		})
	}
	// Unknown kind still receives common body and metadata checks.
	c.Code(t, func() error { _, e := p.DecodeSend(c.Wrap("future", sizedBody([]byte(`{}`), p.MaxBytes+1))); return e }(), "MESSAGE_TOO_LARGE")
}
func TestMaxPersistedEncodeWrap(t *testing.T) {
	m := c.Message(`{"data":""}`)
	raw, e := p.Encode(m)
	c.Error(t, e, nil)
	m.Payload = []byte(`{"data":"` + strings.Repeat("x", p.MaxBytes-len(raw)) + `"}`)
	raw, e = p.Encode(m)
	c.Error(t, e, nil)
	if len(raw) != p.MaxBytes {
		t.Fatalf("body %d", len(raw))
	}
	wire, e := p.EncodeServerFrame(p.ServerFrame{Message: &m})
	c.Error(t, e, nil)
	if len(wire) != p.MaxBytes+46 {
		t.Fatalf("wrapper %d", len(wire))
	}
	decoded, e := p.DecodeServerFrame(wire)
	c.Error(t, e, nil)
	if !bytes.Equal(m.Payload, decoded.Message.Payload) {
		t.Fatal("payload changed")
	}
	m.Payload = append(m.Payload[:len(m.Payload)-2], []byte(`x"}`)...)
	_, e = p.EncodeServerFrame(p.ServerFrame{Message: &m})
	c.Code(t, e, "MESSAGE_TOO_LARGE")
}
func nested(depth int) string {
	return `{"x":` + strings.Repeat("[", depth) + `0` + strings.Repeat("]", depth) + `}`
}
func TestDepthBoundaries(t *testing.T) {
	for _, n := range []int{30, 31} {
		s := c.Request(nested(n))
		_, e := p.EncodeSend(s)
		if n == 30 {
			c.Error(t, e, nil)
		} else {
			c.Code(t, e, "PROTOCOL_NESTING_EXCEEDED")
		}
		m := c.Message(nested(n))
		raw, e := p.Encode(m)
		if n == 30 {
			c.Error(t, e, nil)
			_, e = p.DecodeServerFrame(c.Wrap("message", raw))
			c.Error(t, e, nil)
			_, e = p.EncodeServerFrame(p.ServerFrame{Message: &m})
			c.Error(t, e, nil)
		} else {
			c.Code(t, e, "PROTOCOL_NESTING_EXCEEDED")
		}
	}
}
func TestNumberAndTextBoundaries(t *testing.T) {
	for _, n := range []int{128, 129} {
		s := c.Request(`{"n":` + strings.Repeat("1", n) + `}`)
		wire, e := p.EncodeSend(s)
		if n == 128 {
			c.Error(t, e, nil)
			round, e := p.DecodeSend(wire)
			c.Error(t, e, nil)
			if !bytes.Equal(s.Payload, round.Payload) {
				t.Fatal("number rounded")
			}
		} else {
			c.Code(t, e, "PROTOCOL_INVALID_JSON")
		}
	}
	for _, n := range []int{p.MaxTextBytes, p.MaxTextBytes + 1} {
		s := c.Request(`{"text":"` + strings.Repeat("a", n) + `"}`)
		s.Type = "text"
		_, e := p.EncodeSend(s)
		if n == p.MaxTextBytes {
			c.Error(t, e, nil)
		} else {
			c.Code(t, e, "PROTOCOL_INVALID_MESSAGE")
		}
	}
}
func TestSendOutputBoundaryAndOwnership(t *testing.T) {
	s := c.Request(`{"data":""}`)
	wire, e := p.EncodeSend(s)
	c.Error(t, e, nil)
	padding := p.MaxSendBytes - len(bodyOf(t, wire))
	s.Payload = []byte(`{"data":"` + strings.Repeat("x", padding) + `"}`)
	wire, e = p.EncodeSend(s)
	c.Error(t, e, nil)
	if len(bodyOf(t, wire)) != p.MaxSendBytes {
		t.Fatal("not exact")
	}
	saved := bytes.Clone(wire)
	decoded, e := p.DecodeSend(wire)
	c.Error(t, e, nil)
	decoded.Payload[0] = '['
	if !bytes.Equal(saved, wire) {
		t.Fatal("decoded payload aliases input")
	}
	s.Payload = append(s.Payload[:len(s.Payload)-2], []byte(`x"}`)...)
	_, e = p.EncodeSend(s)
	c.Code(t, e, "MESSAGE_TOO_LARGE")
	s = c.Request(`{"x":1,"x":2}`)
	s.ClientMsgID = strings.Repeat("x", p.MaxSendBytes+1)
	_, e = p.EncodeSend(s)
	c.Code(t, e, "MESSAGE_TOO_LARGE")
}
func TestHugeExponentBoundedComparison(t *testing.T) {
	pairs := [][2]string{{"1e" + strings.Repeat("9", 125), "10e" + strings.Repeat("9", 124) + "8"}, {"100e-" + strings.Repeat("9", 123), "1e-" + strings.Repeat("9", 122) + "7"}, {"0e" + strings.Repeat("9", 125), "-0"}, {"0.1e" + strings.Repeat("9", 123) + "0", "1e" + strings.Repeat("9", 122) + "89"}}
	for i, pair := range pairs {
		a, b := c.Request(`{"n":`+pair[0]+`}`), c.Request(`{"n":`+pair[1]+`}`)
		equal, e := p.SameIntent(a, b)
		c.Error(t, e, nil)
		if !equal {
			t.Fatalf("pair %d unequal", i)
		}
	}
}
func TestCombinedBoundsPrecedence(t *testing.T) {
	wire := bytes.Repeat([]byte("x"), p.MaxFrameBytes+1)
	_, e := p.DecodeSend(wire)
	c.Code(t, e, "MESSAGE_TOO_LARGE")
	body := sizedBody([]byte(`{"version":null}`), p.MaxSendBytes+1)
	_, e = p.DecodeSend(c.Wrap("send", body))
	c.Code(t, e, "MESSAGE_TOO_LARGE")
	wire = c.Wrap("send", []byte(`{"senderId":null}`))
	wire = append(bytes.Repeat([]byte(" "), 513), wire...)
	_, e = p.DecodeSend(wire)
	c.Code(t, e, "PROTOCOL_INVALID_MESSAGE")
}

func TestSendLeavesAuthoritativeMetadataBudget(t *testing.T) {
	s := c.Request(`{"data":""}`)
	s.ClientMsgID = strings.Repeat("c", 128)
	s.ConversationID = strings.Repeat("r", 128)
	s.Type = strings.Repeat("t", 64)
	s.Version = 2147483647
	wire, e := p.EncodeSend(s)
	c.Error(t, e, nil)
	s.Payload = []byte(`{"data":"` + strings.Repeat("x", p.MaxSendBytes-len(bodyOf(t, wire))) + `"}`)
	_, e = p.EncodeSend(s)
	c.Error(t, e, nil)
	m := c.Message(string(s.Payload))
	m.ClientMsgID = s.ClientMsgID
	m.ConversationID = s.ConversationID
	m.Type = s.Type
	m.Version = s.Version
	m.SenderID = strings.Repeat("u", 128)
	m.ServerMsgID = strings.Repeat("s", 128)
	m.ConversationSeq = "9223372036854775807"
	m.ServerTime = "9223372036854775807"
	wire, e = p.Encode(m)
	c.Error(t, e, nil)
	if len(wire) > p.MaxBytes {
		t.Fatal("accepted send cannot fit authoritative envelope")
	}
	c.Error(t, p.Correlate(s, m.SenderID, p.ServerFrame{Message: &m}), nil)
}

func TestLargePersistedPayloadIsCorrelationMismatch(t *testing.T) {
	s := c.Request(`{}`)
	m := c.Message(`{"data":"` + strings.Repeat("x", p.MaxSendBytes) + `"}`)
	c.Code(t, p.Correlate(s, "u1", p.ServerFrame{Message: &m}), "SEND_CORRELATION_MISMATCH")
}

func TestEncodedSizePrecedesMalformedPayload(t *testing.T) {
	s := c.Request(`{"x":1,"x":2}`)
	s.ClientMsgID = strings.Repeat("\x00", 20000)
	_, e := p.EncodeSend(s)
	c.Code(t, e, "MESSAGE_TOO_LARGE")
}
