// Package protocol validates persisted message envelopes. 解码不代表发送或持久化确认。
package protocol

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"unicode/utf8"
)

const (
	MaxBytes     = 65536
	MaxDepth     = 32
	MaxTextBytes = 16384
)

// ErrorCode is stable and never includes message content. 错误码不含消息原文。
type ErrorCode string

const (
	InvalidJSON        ErrorCode = "PROTOCOL_INVALID_JSON"
	InvalidMessage     ErrorCode = "PROTOCOL_INVALID_MESSAGE"
	UnsupportedVersion ErrorCode = "PROTOCOL_UNSUPPORTED_VERSION"
	TooLarge           ErrorCode = "MESSAGE_TOO_LARGE"
	TooDeep            ErrorCode = "PROTOCOL_NESTING_EXCEEDED"
)

func (e ErrorCode) Error() string { return string(e) }

// Message represents a persisted envelope, never a send request. 表示已持久化消息的协议形状。
type Message struct {
	ProtocolVersion uint32          `json:"protocolVersion"`
	Version         uint32          `json:"version"`
	ClientMsgID     string          `json:"clientMsgId"`
	ServerMsgID     string          `json:"serverMsgId"`
	ConversationID  string          `json:"conversationId"`
	ConversationSeq string          `json:"conversationSeq"`
	SenderID        string          `json:"senderId"`
	Type            string          `json:"type"`
	ServerTime      string          `json:"serverTime"`
	Payload         json.RawMessage `json:"payload"`
}

// Supported reports implemented payload semantics, not permission to deliver. 是否支持该负载语义。
func (m Message) Supported() bool {
	return m.ProtocolVersion == 1 && m.Type == "text" && m.Version == 1
}

func identifier(s string, limit int, messageType bool) bool {
	if len(s) == 0 || len(s) > limit {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if messageType {
			if (c >= 'a' && c <= 'z') || (i > 0 && (c == '_' || c >= '0' && c <= '9')) {
				continue
			}
		} else if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

func decimal(s, maximum string) bool {
	if len(s) == 0 || len(s) > len(maximum) || len(s) > 1 && s[0] == '0' {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) < len(maximum) || s <= maximum
}

func shape(m Message) bool {
	return m.ProtocolVersion > 0 && m.ProtocolVersion <= 2147483647 &&
		m.Version > 0 && m.Version <= 2147483647 &&
		identifier(m.ClientMsgID, 128, false) && identifier(m.ServerMsgID, 128, false) &&
		identifier(m.ConversationID, 128, false) && identifier(m.SenderID, 128, false) &&
		identifier(m.Type, 64, true) && decimal(m.ConversationSeq, "9223372036854775807") &&
		decimal(m.ServerTime, "9223372036854775807")
}

// Decode rejects ambiguous JSON before interpreting fields. 先消除重复键等解析歧义，再校验语义。
func Decode(wire []byte) (Message, error) {
	var m Message
	if err := strictJSON(wire); err != nil {
		return m, err
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(wire, &fields) != nil || fields == nil {
		return m, InvalidMessage
	}
	strings := map[string]*string{
		"clientMsgId": &m.ClientMsgID, "serverMsgId": &m.ServerMsgID,
		"conversationId": &m.ConversationID, "conversationSeq": &m.ConversationSeq,
		"senderId": &m.SenderID, "type": &m.Type, "serverTime": &m.ServerTime,
	}
	for key, dest := range strings {
		raw, ok := fields[key]
		if !ok || bytes.Equal(raw, []byte("null")) || json.Unmarshal(raw, dest) != nil {
			return Message{}, InvalidMessage
		}
	}
	for key, dest := range map[string]*uint32{"protocolVersion": &m.ProtocolVersion, "version": &m.Version} {
		raw, ok := fields[key]
		if !ok || !decimal(string(raw), "2147483647") || string(raw) == "0" {
			return Message{}, InvalidMessage
		}
		value, _ := strconv.ParseUint(string(raw), 10, 32)
		*dest = uint32(value)
	}
	var payload map[string]json.RawMessage
	raw, ok := fields["payload"]
	if !ok || json.Unmarshal(raw, &payload) != nil || payload == nil || !shape(m) {
		return Message{}, InvalidMessage
	}
	if m.ProtocolVersion != 1 {
		return Message{}, UnsupportedVersion
	}
	if m.Supported() {
		var text string
		if json.Unmarshal(payload["text"], &text) != nil || len(text) == 0 || len(text) > MaxTextBytes {
			return Message{}, InvalidMessage
		}
	}
	m.Payload = append(json.RawMessage(nil), raw...)
	return m, nil
}

// Encode validates outbound objects and preserves opaque payload numbers. 编码同样校验并保留未知负载数字。
func Encode(m Message) ([]byte, error) {
	// Bound allocations before JSON serialization, including invalid public structs.
	if len(m.Payload) > MaxBytes {
		return nil, TooLarge
	}
	if !shape(m) {
		return nil, InvalidMessage
	}
	if err := strictJSON(m.Payload); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if encoder.Encode(m) != nil {
		return nil, InvalidJSON
	}
	wire := bytes.TrimSuffix(out.Bytes(), []byte("\n"))
	if _, err := Decode(wire); err != nil {
		return nil, err
	}
	return wire, nil
}

// Validate applies the same rules as Encode. 出站校验与编码使用同一契约。
func Validate(m Message) error { _, err := Encode(m); return err }

func strictJSON(wire []byte) error {
	if len(wire) > MaxBytes {
		return TooLarge
	}
	if !utf8.Valid(wire) || !pairedSurrogates(wire) {
		return InvalidJSON
	}
	d := json.NewDecoder(bytes.NewReader(wire))
	d.UseNumber()
	if err := value(d, 0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return InvalidJSON
	}
	return nil
}

func value(d *json.Decoder, depth int) error {
	token, err := d.Token()
	if err != nil {
		return InvalidJSON
	}
	if number, ok := token.(json.Number); ok && len(number) > 128 {
		return InvalidJSON
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delim != '{' && delim != '[' {
		return InvalidJSON
	}
	depth++
	if depth > MaxDepth {
		return TooDeep
	}
	if delim == '{' {
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return InvalidJSON
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return InvalidJSON
			}
			seen[name] = true
			if err := value(d, depth); err != nil {
				return err
			}
		}
	} else {
		for d.More() {
			if err := value(d, depth); err != nil {
				return err
			}
		}
	}
	close, err := d.Token()
	if err != nil || delim == '{' && close != json.Delim('}') || delim == '[' && close != json.Delim(']') {
		return InvalidJSON
	}
	return nil
}

func hex4(b []byte) (uint16, bool) {
	if len(b) < 4 {
		return 0, false
	}
	var n uint16
	for _, c := range b[:4] {
		n <<= 4
		switch {
		case c >= '0' && c <= '9':
			n += uint16(c - '0')
		case c >= 'a' && c <= 'f':
			n += uint16(c - 'a' + 10)
		case c >= 'A' && c <= 'F':
			n += uint16(c - 'A' + 10)
		default:
			return 0, false
		}
	}
	return n, true
}

// encoding/json replaces isolated surrogates; the wire contract rejects them.
func pairedSurrogates(b []byte) bool {
	inString := false
	for i := 0; i < len(b); i++ {
		if b[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || b[i] != '\\' {
			continue
		}
		i++
		if i >= len(b) {
			return false
		}
		if b[i] != 'u' {
			continue
		}
		n, ok := hex4(b[i+1:])
		if !ok {
			return false
		}
		i += 4
		if n >= 0xDC00 && n <= 0xDFFF {
			return false
		}
		if n >= 0xD800 && n <= 0xDBFF {
			if i+6 >= len(b) || b[i+1] != '\\' || b[i+2] != 'u' {
				return false
			}
			low, ok := hex4(b[i+3:])
			if !ok || low < 0xDC00 || low > 0xDFFF {
				return false
			}
			i += 6
		}
	}
	return !inString
}
