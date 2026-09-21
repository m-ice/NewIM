// Package common reads shared wire strings without coercing payload numbers.
package common

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	p "github.com/m-ice/NewIM/core/protocol/go"
)

type FrameCase struct {
	ID        string  `json:"id"`
	Wire      string  `json:"wire"`
	WireHex   string  `json:"wireHex"`
	Direction string  `json:"direction"`
	Error     *string `json:"error"`
}
type IntentCase struct {
	ID    string  `json:"id"`
	Left  string  `json:"left"`
	Right string  `json:"right"`
	Equal bool    `json:"equal"`
	Error *string `json:"error"`
}

func read(t *testing.T, name string, out any) {
	t.Helper()
	raw, e := os.ReadFile("../fixtures/" + name + ".json")
	if e != nil {
		t.Fatal(e)
	}
	var entries []map[string]json.RawMessage
	if e = json.Unmarshal(raw, &entries); e != nil {
		t.Fatal(e)
	}
	if len(entries) == 0 {
		t.Fatal("empty fixtures")
	}
	seen := map[string]bool{}
	for _, entry := range entries {
		var id string
		if json.Unmarshal(entry["id"], &id) != nil || id == "" || seen[id] {
			t.Fatal("invalid/repeated fixture id")
		}
		seen[id] = true
		if _, ok := entry["error"]; !ok {
			t.Fatal("missing explicit expectation")
		}
		if name == "intents" {
			var value bool
			if json.Unmarshal(entry["equal"], &value) != nil {
				t.Fatal("missing equality expectation")
			}
		}
	}
	if e = json.Unmarshal(raw, out); e != nil {
		t.Fatal(e)
	}
	t.Logf("%s: %d shared scenarios", name, len(entries))
}
func Frames(t *testing.T, name string) []FrameCase {
	t.Helper()
	var v []FrameCase
	read(t, name, &v)
	return v
}
func Intents(t *testing.T) []IntentCase {
	t.Helper()
	var v []IntentCase
	read(t, "intents", &v)
	return v
}
func Wire(t *testing.T, c FrameCase) []byte {
	t.Helper()
	if c.WireHex != "" {
		if c.Wire != "" {
			t.Fatal("two wire representations")
		}
		v, e := hex.DecodeString(c.WireHex)
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	if c.Wire == "" {
		t.Fatal("no wire")
	}
	return []byte(c.Wire)
}
func Error(t *testing.T, e error, want *string) {
	t.Helper()
	if want == nil {
		if e != nil {
			t.Fatal(e)
		}
	} else if e == nil || e.Error() != *want {
		t.Fatalf("want %s got %v", *want, e)
	}
}
func Code(t *testing.T, e error, want string) { t.Helper(); Error(t, e, &want) }
func Request(payload string) p.Send {
	return p.Send{ProtocolVersion: 1, ClientMsgID: "c1", ConversationID: "room1", Version: 1, Type: "future", Payload: json.RawMessage(payload)}
}
func Ack() p.Ack {
	return p.Ack{ClientMsgID: "c1", ConversationID: "room1", SenderID: "u1", ServerMsgID: "s1", ConversationSeq: "1", ServerTime: "1"}
}
func Message(payload string) p.Message {
	return p.Message{ProtocolVersion: 1, Version: 1, ClientMsgID: "c1", ServerMsgID: "s1", ConversationID: "room1", ConversationSeq: "1", SenderID: "u1", Type: "future", ServerTime: "1", Payload: json.RawMessage(payload)}
}
func Compact(t *testing.T, raw []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	if e := json.Compact(&b, raw); e != nil {
		t.Fatal(e)
	}
	return b.Bytes()
}
func Wrap(kind string, body []byte) []byte {
	return []byte(`{"protocolVersion":1,"kind":"` + kind + `","body":` + string(body) + `}`)
}
