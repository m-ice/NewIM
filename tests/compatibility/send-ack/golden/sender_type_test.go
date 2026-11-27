package golden_test

import (
	"bytes"
	"testing"

	p "github.com/m-ice/NewIM/core/protocol/go"
	c "github.com/m-ice/NewIM/tests/compatibility/send-ack/common"
)

func TestHumanSenderTypePreservesLegacyFrameBytes(t *testing.T) {
	for _, test := range c.Frames(t, "frames") {
		if test.ID != "ack" && test.ID != "message" {
			continue
		}
		t.Run(test.ID, func(t *testing.T) {
			wire := c.Wire(t, test)
			human := bytes.Replace(wire, []byte(`"senderId":`), []byte(`"senderType":"HUMAN","senderId":`), 1)
			frame, err := p.DecodeServerFrame(human)
			c.Error(t, err, nil)
			if frame.Ack != nil && frame.Ack.SenderType != p.SenderHuman ||
				frame.Message != nil && frame.Message.SenderType != p.SenderHuman {
				t.Fatal("explicit human sender type was not decoded")
			}
			encoded, err := p.EncodeServerFrame(frame)
			c.Error(t, err, nil)
			if !bytes.Equal(encoded, wire) {
				t.Fatalf("human sender changed legacy wire: %s", encoded)
			}
		})
	}
}

func TestSenderTypeCountsAgainstMessageByteLimit(t *testing.T) {
	message := c.Message(`{"n":""}`)
	message.SenderType = p.SenderAIAgent
	wire, err := p.Encode(message)
	c.Error(t, err, nil)
	payload := append([]byte(`{"n":"`), bytes.Repeat([]byte{'a'}, p.MaxBytes-len(wire))...)
	message.Payload = append(payload, '"', '}')
	wire, err = p.Encode(message)
	c.Error(t, err, nil)
	if len(wire) != p.MaxBytes {
		t.Fatalf("message length = %d", len(wire))
	}
	message.Payload = append(append(message.Payload[:len(message.Payload)-2], 'a'), '"', '}')
	_, err = p.Encode(message)
	if err != p.TooLarge {
		t.Fatalf("over-limit message error = %v", err)
	}
}
