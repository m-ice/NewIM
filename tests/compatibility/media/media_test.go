package media_test

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

type fixtureSet struct {
	Golden  protocol.MediaMetadata `json:"golden"`
	Invalid []struct {
		Name    string `json:"name"`
		Payload string `json:"payload"`
		Error   string `json:"error"`
	} `json:"invalid"`
}

func loadFixtures(t *testing.T) fixtureSet {
	t.Helper()
	raw, err := os.ReadFile("fixtures/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures fixtureSet
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	return fixtures
}

func mediaMessage(t *testing.T, payload []byte) protocol.Message {
	t.Helper()
	return protocol.Message{
		ProtocolVersion: 1, Version: 1, ClientMsgID: "client_media", ServerMsgID: "server_media",
		ConversationID: "conversation_media", ConversationSeq: "1", SenderID: "user_media",
		Type: "media", ServerTime: "1800000000000", Payload: payload,
	}
}

func TestGoldenAndRoundTrip(t *testing.T) {
	fixtures := loadFixtures(t)
	payload, err := protocol.EncodeMediaPayload(fixtures.Golden)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := protocol.DecodeMediaPayload(payload)
	if err != nil || metadata != fixtures.Golden {
		t.Fatalf("metadata = %+v, err = %v", metadata, err)
	}
	wire, err := protocol.Encode(mediaMessage(t, payload))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := protocol.Decode(wire)
	if err != nil || !decoded.Supported() {
		t.Fatalf("decode = %+v, err = %v", decoded, err)
	}
	if !bytes.Equal(payload, decoded.Payload) {
		t.Fatalf("payload changed: %s", decoded.Payload)
	}
	sendWire, err := protocol.EncodeSend(protocol.Send{
		ProtocolVersion: 1, ClientMsgID: "client_media", ConversationID: "conversation_media",
		Version: 1, Type: "media", Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	send, err := protocol.DecodeSend(sendWire)
	if err != nil || !send.Supported() || !bytes.Equal(send.Payload, payload) {
		t.Fatalf("send = %+v, err = %v", send, err)
	}
}

func TestRejectsClosedMetadata(t *testing.T) {
	fixtures := loadFixtures(t)
	for _, testCase := range fixtures.Invalid {
		t.Run(testCase.Name, func(t *testing.T) {
			_, err := protocol.Encode(mediaMessage(t, []byte(testCase.Payload)))
			if err == nil || err.Error() != testCase.Error {
				t.Fatalf("Encode error = %v want %s", err, testCase.Error)
			}
			_, err = protocol.EncodeSend(protocol.Send{
				ProtocolVersion: 1, ClientMsgID: "client_media", ConversationID: "conversation_media",
				Version: 1, Type: "media", Payload: []byte(testCase.Payload),
			})
			if err == nil || err.Error() != testCase.Error {
				t.Fatalf("EncodeSend error = %v want %s", err, testCase.Error)
			}
		})
	}
}

func TestMediaBounds(t *testing.T) {
	fixtures := loadFixtures(t)
	first := strings.Repeat("a", 63)
	second := strings.Repeat("b", 64)
	mime128 := first + "/" + second
	mime129 := strings.Repeat("a", 64) + "/" + second
	if len(mime128) != 128 || len(mime129) != 129 {
		t.Fatal("MIME boundary construction failed")
	}
	for _, testCase := range []struct {
		name       string
		content    string
		size       string
		valid      bool
		payloadLen int
	}{
		{"mime_128", mime128, "1", true, 0},
		{"mime_129", mime129, "1", false, 0},
		{"size_max", "image/jpeg", "104857600", true, 0},
		{"size_first_invalid", "image/jpeg", "104857601", false, 0},
		{"payload_4096", "image/jpeg", "1", true, 4096},
		{"payload_4097", "image/jpeg", "1", false, 4097},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			metadata := fixtures.Golden
			metadata.ContentType = testCase.content
			metadata.Size = testCase.size
			raw, err := json.Marshal(metadata)
			if err != nil {
				t.Fatal(err)
			}
			if testCase.payloadLen != 0 {
				padding := testCase.payloadLen - len(raw)
				if padding < 0 {
					t.Fatal("payload boundary too small")
				}
				raw = []byte(strings.Replace(string(raw), "{", "{"+strings.Repeat(" ", padding), 1))
			}
			_, err = protocol.Encode(mediaMessage(t, raw))
			if testCase.valid && err != nil {
				t.Fatalf("valid boundary rejected: %v", err)
			}
			if !testCase.valid && (err == nil || err.Error() != string(protocol.TooLarge) && err.Error() != string(protocol.InvalidMessage)) {
				t.Fatalf("invalid boundary error = %v", err)
			}
		})
	}
}

func TestTextAndAckCompatibility(t *testing.T) {
	text := protocol.Send{
		ProtocolVersion: 1, ClientMsgID: "client_text", ConversationID: "conversation_text",
		Version: 1, Type: "text", Payload: json.RawMessage(`{"text":"hello","future":1}`),
	}
	wire, err := protocol.EncodeSend(text)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := protocol.DecodeSend(wire)
	if err != nil || !decoded.Supported() {
		t.Fatalf("text send = %+v, err = %v", decoded, err)
	}
	ack := protocol.ServerFrame{Ack: &protocol.Ack{
		ClientMsgID: "client_text", ConversationID: "conversation_text", SenderID: "user_text",
		ServerMsgID: "server_text", ConversationSeq: "1", ServerTime: "1800000000000",
	}}
	frame, err := protocol.EncodeServerFrame(ack)
	if err != nil {
		t.Fatal(err)
	}
	decodedFrame, err := protocol.DecodeServerFrame(frame)
	if err != nil || decodedFrame.Ack == nil || decodedFrame.Ack.ServerMsgID != "server_text" {
		t.Fatalf("ACK compatibility failed: %+v, %v", decodedFrame, err)
	}
}
