package golden_test

import (
	"bytes"
	p "github.com/m-ice/NewIM/core/protocol/go"
	c "github.com/m-ice/NewIM/tests/compatibility/send-ack/common"
	"reflect"
	"testing"
)

func TestSharedFrames(t *testing.T) {
	for _, f := range c.Frames(t, "frames") {
		t.Run(f.ID, func(t *testing.T) {
			wire := c.Wire(t, f)
			original := bytes.Clone(wire)
			switch f.Direction {
			case "send":
				s, e := p.DecodeSend(wire)
				c.Error(t, e, f.Error)
				if e != nil {
					return
				}
				saved := bytes.Clone(s.Payload)
				encoded, e := p.EncodeSend(s)
				c.Error(t, e, nil)
				round, e := p.DecodeSend(encoded)
				c.Error(t, e, nil)
				if !bytes.Equal(c.Compact(t, s.Payload), c.Compact(t, round.Payload)) || !bytes.Equal(saved, s.Payload) {
					t.Fatal("payload changed")
				}
				s.Payload = nil
				round.Payload = nil
				if !reflect.DeepEqual(s, round) {
					t.Fatal("send fields changed")
				}
			case "server":
				s, e := p.DecodeServerFrame(wire)
				c.Error(t, e, f.Error)
				if e != nil {
					return
				}
				encoded, e := p.EncodeServerFrame(s)
				c.Error(t, e, nil)
				round, e := p.DecodeServerFrame(encoded)
				c.Error(t, e, nil)
				if !reflect.DeepEqual(s, round) {
					t.Fatal("server fields changed")
				}
			default:
				t.Fatal("invalid direction")
			}
			if !bytes.Equal(wire, original) {
				t.Fatal("input modified")
			}
		})
	}
}
func TestSharedIntent(t *testing.T) {
	for _, f := range c.Intents(t) {
		t.Run(f.ID, func(t *testing.T) {
			a, b := c.Request(f.Left), c.Request(f.Right)
			equal, e := p.SameIntent(a, b)
			c.Error(t, e, f.Error)
			if e == nil && equal != f.Equal {
				t.Fatalf("equal=%v", equal)
			}
			if string(a.Payload) != f.Left || string(b.Payload) != f.Right {
				t.Fatal("input modified")
			}
			if e == nil {
				reverse, e := p.SameIntent(b, a)
				c.Error(t, e, nil)
				if reverse != equal {
					t.Fatal("asymmetric equality")
				}
			}
		})
	}
}
func TestIntentFields(t *testing.T) {
	a := c.Request(`{"x":1}`)
	b := a
	b.ClientMsgID = "another"
	equal, e := p.SameIntent(a, b)
	c.Error(t, e, nil)
	if !equal {
		t.Fatal("client ID is separate key")
	}
	for _, change := range []func(*p.Send){func(s *p.Send) { s.ConversationID = "other" }, func(s *p.Send) { s.Version = 2 }, func(s *p.Send) { s.Type = "other" }} {
		b = a
		change(&b)
		equal, e = p.SameIntent(a, b)
		c.Error(t, e, nil)
		if equal {
			t.Fatal("semantic field ignored")
		}
	}
}
