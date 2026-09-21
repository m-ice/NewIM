package compatibility_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"testing"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

type fixture struct {
	Name      string  `json:"name"`
	Group     string  `json:"group"`
	Wire      string  `json:"wire"`
	WireHex   string  `json:"wireHex"`
	Error     *string `json:"error"`
	Supported bool    `json:"supported"`
}

func fixtures(t *testing.T) []fixture {
	t.Helper()
	wire, err := os.ReadFile("../../core/protocol/fixtures/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []fixture
	if err = json.Unmarshal(wire, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatal("empty shared fixture set")
	}
	return cases
}

func compact(t *testing.T, raw []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := json.Compact(&out, raw); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func checkGroup(t *testing.T, group string) {
	t.Helper()
	count := 0
	for _, c := range fixtures(t) {
		if c.Group != group {
			continue
		}
		count++
		t.Run(c.Name, func(t *testing.T) {
			wire := []byte(c.Wire)
			if c.WireHex != "" {
				var err error
				wire, err = hex.DecodeString(c.WireHex)
				if err != nil {
					t.Fatal(err)
				}
			}
			m, err := protocol.Decode(wire)
			if c.Error != nil {
				if err == nil || err.Error() != *c.Error {
					t.Fatalf("expected %s, got %v", *c.Error, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if m.Supported() != c.Supported {
				t.Fatalf("supported = %v", m.Supported())
			}
			encoded, err := protocol.Encode(m)
			if err != nil {
				t.Fatal(err)
			}
			round, err := protocol.Decode(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(compact(t, m.Payload), compact(t, round.Payload)) {
				t.Fatal("opaque payload tokens changed")
			}
			m.Payload = nil
			round.Payload = nil
			if !reflect.DeepEqual(m, round) {
				t.Fatal("envelope changed during round trip")
			}
		})
	}
	if count == 0 {
		t.Fatalf("no fixtures selected for %s", group)
	}
}

func TestGolden(t *testing.T)        { checkGroup(t, "golden") }
func TestUnknownFields(t *testing.T) { checkGroup(t, "unknown_fields") }
func TestUnknownType(t *testing.T)   { checkGroup(t, "unknown_type") }
func TestLimits(t *testing.T) {
	checkGroup(t, "limits")
	raw, err := os.ReadFile("../../core/protocol/schema/message-v1.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties map[string]struct {
			Pattern string `json:"pattern"`
		} `json:"properties"`
		Profile struct {
			MaxBytes int `json:"maxBytes"`
			Depth    int `json:"maxContainerDepth"`
			Number   int `json:"maxNumberTokenBytes"`
		} `json:"x-wire-profile"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Profile.MaxBytes != protocol.MaxBytes || schema.Profile.Depth != protocol.MaxDepth || schema.Profile.Number != 128 {
		t.Fatal("schema wire limits disagree")
	}
	for _, field := range []string{"conversationSeq", "serverTime"} {
		pattern, err := regexp.Compile(schema.Properties[field].Pattern)
		if err != nil {
			t.Fatal(err)
		}
		for _, valid := range []string{"0", "9", "10", "9007199254740993", "9223372036854775807"} {
			if !pattern.MatchString(valid) {
				t.Fatalf("schema rejects %s", valid)
			}
		}
		for _, invalid := range []string{"", "01", "-1", "1e2", "9223372036854775808", "9999999999999999999"} {
			if pattern.MatchString(invalid) {
				t.Fatalf("schema accepts %s", invalid)
			}
		}
	}
}

func TestEncodeRejectsInvalidPublicObjects(t *testing.T) {
	base, err := protocol.Decode([]byte(fixtures(t)[0].Wire))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*protocol.Message){
		"missing_identity":  func(m *protocol.Message) { m.SenderID = "" },
		"sequence_overflow": func(m *protocol.Message) { m.ConversationSeq = "9223372036854775808" },
		"future_protocol":   func(m *protocol.Message) { m.ProtocolVersion = 2 },
		"zero_schema":       func(m *protocol.Message) { m.Version = 0 },
		"empty_text":        func(m *protocol.Message) { m.Payload = json.RawMessage(`{"text":""}`) },
		"duplicate_payload": func(m *protocol.Message) { m.Payload = json.RawMessage(`{"text":"a","text":"b"}`) },
		"null_payload":      func(m *protocol.Message) { m.Payload = json.RawMessage(`null`) },
		"oversize_payload":  func(m *protocol.Message) { m.Payload = bytes.Repeat([]byte(" "), protocol.MaxBytes+1) },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			m := base
			change(&m)
			if _, err := protocol.Encode(m); err == nil {
				t.Fatal("invalid outbound message accepted")
			}
			if protocol.Validate(m) == nil {
				t.Fatal("validation disagrees")
			}
		})
	}
}

func FuzzDecode(f *testing.F) {
	f.Add([]byte(`{"protocolVersion":1,"version":1,"clientMsgId":"a","serverMsgId":"b","conversationId":"c","conversationSeq":"0","senderId":"d","type":"future","serverTime":"0","payload":{}}`))
	f.Add([]byte(`{"x":1,"x":2}`))
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, wire []byte) {
		m, err := protocol.Decode(wire)
		if err != nil {
			return
		}
		out, err := protocol.Encode(m)
		if err != nil {
			t.Fatalf("valid decoded value cannot encode: %v", err)
		}
		if _, err = protocol.Decode(out); err != nil {
			t.Fatalf("encoded value cannot decode: %v", err)
		}
	})
}
