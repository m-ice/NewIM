package protocol

import (
	"bytes"
	"encoding/json"
	"strconv"
)

const (
	MaxFrameBytes    = 66048
	MaxSendBytes     = 61440
	MaxResultBytes   = 4096
	MaxMetadataBytes = 512
)

// SendCode adds frame errors without changing persisted errors. 新帧错误不改变旧错误契约。
type SendCode string

const (
	UnsupportedFrame    SendCode = "PROTOCOL_UNSUPPORTED_FRAME"
	CorrelationMismatch SendCode = "SEND_CORRELATION_MISMATCH"
	ResultConflict      SendCode = "SEND_RESULT_CONFLICT"
)

func (e SendCode) Error() string { return string(e) }

// Send carries caller intent, never sender authority. 发送意图不接受客户端身份声明。
type Send struct {
	ProtocolVersion uint32
	ClientMsgID     string
	ConversationID  string
	Version         uint32
	Type            string
	Payload         json.RawMessage
}

// Ack is a result shape; producers must already have committed. 仅已提交结果可产生此确认。
type Ack struct {
	ClientMsgID     string `json:"clientMsgId"`
	ConversationID  string `json:"conversationId"`
	SenderID        string `json:"senderId"`
	ServerMsgID     string `json:"serverMsgId"`
	ConversationSeq string `json:"conversationSeq"`
	ServerTime      string `json:"serverTime"`
}

// SendError carries a correlated stable code without diagnostics. 关联错误不携带敏感诊断。
type SendError struct {
	ClientMsgID    string `json:"clientMsgId"`
	ConversationID string `json:"conversationId"`
	Code           string `json:"code"`
}

// ServerFrame requires exactly one variant. 编码时要求恰好一个结果分支。
type ServerFrame struct {
	Ack     *Ack
	Error   *SendError
	Message *Message
}

// Supported indicates understood payload semantics, never permission. 语义支持并非发送授权。
func (s Send) Supported() bool { return s.ProtocolVersion == 1 && s.Type == "text" && s.Version == 1 }

type frameParts struct {
	version uint32
	kind    string
	raw     json.RawMessage
	fields  map[string]json.RawMessage
}

func objectFields(raw []byte) (map[string]json.RawMessage, error) {
	var f map[string]json.RawMessage
	if json.Unmarshal(raw, &f) != nil || f == nil {
		return nil, InvalidMessage
	}
	return f, nil
}
func stringField(f map[string]json.RawMessage, k string) (string, error) {
	var s string
	r, ok := f[k]
	if !ok || bytes.Equal(r, []byte("null")) || json.Unmarshal(r, &s) != nil {
		return "", InvalidMessage
	}
	return s, nil
}
func versionField(f map[string]json.RawMessage, k string) (uint32, error) {
	r := string(f[k])
	if !decimal(r, "2147483647") || r == "0" {
		return 0, InvalidMessage
	}
	n, _ := strconv.ParseUint(r, 10, 32)
	return uint32(n), nil
}
func forbidden(f map[string]json.RawMessage) bool {
	for _, k := range []string{"senderId", "serverMsgId", "conversationSeq", "serverTime", "status"} {
		if _, ok := f[k]; ok {
			return true
		}
	}
	return false
}
func parseFrame(wire []byte) (frameParts, error) {
	var p frameParts
	if err := strictJSONLimits(wire, MaxFrameBytes, MaxDepth+1); err != nil {
		return p, err
	}
	f, err := objectFields(wire)
	if err != nil {
		return p, err
	}
	if p.version, err = versionField(f, "protocolVersion"); err != nil {
		return p, err
	}
	if p.kind, err = stringField(f, "kind"); err != nil {
		return p, err
	}
	p.raw = f["body"]
	if p.fields, err = objectFields(p.raw); err != nil {
		return p, err
	}
	if p.kind == "send" && (forbidden(f) || forbidden(p.fields)) {
		return p, InvalidMessage
	}
	limit := MaxBytes
	if p.kind == "send" {
		limit = MaxSendBytes
	}
	if p.kind == "send_ack" || p.kind == "send_error" {
		limit = MaxResultBytes
	}
	if len(wire)-len(p.raw) > MaxMetadataBytes || len(p.raw) > limit {
		return p, TooLarge
	}
	if err = strictJSONLimits(p.raw, limit, MaxDepth); err != nil {
		return p, err
	}
	return p, nil
}
func sendShape(p frameParts) (Send, error) {
	s := Send{ProtocolVersion: p.version}
	var err error
	if s.ClientMsgID, err = stringField(p.fields, "clientMsgId"); err != nil {
		return s, err
	}
	if s.ConversationID, err = stringField(p.fields, "conversationId"); err != nil {
		return s, err
	}
	if s.Version, err = versionField(p.fields, "version"); err != nil {
		return s, err
	}
	if s.Type, err = stringField(p.fields, "type"); err != nil {
		return s, err
	}
	if !identifier(s.ClientMsgID, 128, false) || !identifier(s.ConversationID, 128, false) || !identifier(s.Type, 64, true) {
		return s, InvalidMessage
	}
	raw := p.fields["payload"]
	if _, err = objectFields(raw); err != nil {
		return s, err
	}
	s.Payload = append(json.RawMessage(nil), raw...)
	return s, nil
}
func ackShape(f map[string]json.RawMessage) (Ack, error) {
	var a Ack
	for k, d := range map[string]*string{"clientMsgId": &a.ClientMsgID, "conversationId": &a.ConversationID, "senderId": &a.SenderID, "serverMsgId": &a.ServerMsgID, "conversationSeq": &a.ConversationSeq, "serverTime": &a.ServerTime} {
		v, e := stringField(f, k)
		if e != nil {
			return a, e
		}
		*d = v
	}
	status, e := stringField(f, "status")
	if e != nil || status != "SERVER_PERSISTED" {
		return a, InvalidMessage
	}
	if !identifier(a.ClientMsgID, 128, false) || !identifier(a.ConversationID, 128, false) || !identifier(a.SenderID, 128, false) || !identifier(a.ServerMsgID, 128, false) || !decimal(a.ConversationSeq, "9223372036854775807") || !decimal(a.ServerTime, "9223372036854775807") {
		return a, InvalidMessage
	}
	return a, nil
}
func validCode(s string) bool {
	if len(s) == 0 || len(s) > 64 || s[0] < 'A' || s[0] > 'Z' {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
			return false
		}
	}
	return true
}
func errorShape(f map[string]json.RawMessage) (SendError, error) {
	var e SendError
	var err error
	if e.ClientMsgID, err = stringField(f, "clientMsgId"); err != nil {
		return e, err
	}
	if e.ConversationID, err = stringField(f, "conversationId"); err != nil {
		return e, err
	}
	if e.Code, err = stringField(f, "code"); err != nil {
		return e, err
	}
	if !identifier(e.ClientMsgID, 128, false) || !identifier(e.ConversationID, 128, false) || !validCode(e.Code) {
		return e, InvalidMessage
	}
	return e, nil
}
func bodyShape(p frameParts) error {
	switch p.kind {
	case "send":
		_, e := sendShape(p)
		return e
	case "send_ack":
		_, e := ackShape(p.fields)
		return e
	case "send_error":
		_, e := errorShape(p.fields)
		return e
	case "message":
		_, e := decodeMessage(p.raw, false)
		return e
	}
	return nil
}
func sendSemantics(s Send) error {
	if s.Supported() {
		f, _ := objectFields(s.Payload)
		text, e := stringField(f, "text")
		if e != nil || len(text) == 0 || len(text) > MaxTextBytes {
			return InvalidMessage
		}
	}
	return nil
}

// DecodeSend accepts only client-direction frames. 只接受客户端发送方向。
func DecodeSend(wire []byte) (Send, error) {
	p, e := parseFrame(wire)
	if e != nil {
		return Send{}, e
	}
	if e = bodyShape(p); e != nil {
		return Send{}, e
	}
	if p.version != 1 {
		return Send{}, UnsupportedVersion
	}
	if p.kind != "send" {
		return Send{}, UnsupportedFrame
	}
	s, e := sendShape(p)
	if e == nil {
		e = sendSemantics(s)
	}
	if e != nil {
		return Send{}, e
	}
	return s, nil
}

// DecodeServerFrame requires explicit server framing. 服务端帧不猜测裸消息格式。
func DecodeServerFrame(wire []byte) (ServerFrame, error) {
	var f ServerFrame
	p, e := parseFrame(wire)
	if e != nil {
		return f, e
	}
	if e = bodyShape(p); e != nil {
		return f, e
	}
	if p.version != 1 {
		return f, UnsupportedVersion
	}
	switch p.kind {
	case "send_ack":
		a, e := ackShape(p.fields)
		f.Ack = &a
		return f, e
	case "send_error":
		v, e := errorShape(p.fields)
		f.Error = &v
		return f, e
	case "message":
		m, e := Decode(p.raw)
		if e != nil {
			return f, e
		}
		f.Message = &m
		return f, nil
	default:
		return f, UnsupportedFrame
	}
}
func lowerBound(limit int, sizes ...int) error {
	total := 0
	for _, n := range sizes {
		if n > limit-total {
			return TooLarge
		}
		total += n
	}
	return nil
}
func wrap(version uint32, kind string, body []byte) []byte {
	return []byte(`{"protocolVersion":` + strconv.FormatUint(uint64(version), 10) + `,"kind":"` + kind + `","body":` + string(body) + `}`)
}

// Match JSON wire escaping rather than HTML/script escaping, including invalid public
// identifiers, so size-versus-shape errors agree with the Rust encoder.
func quotedWireString(out []byte, value string) []byte {
	out = append(out, '"')
	const hex = "0123456789abcdef"
	for i := 0; i < len(value); i++ {
		ch := value[i]
		switch ch {
		case '"', '\\':
			out = append(out, '\\', ch)
		case '\b':
			out = append(out, '\\', 'b')
		case '\f':
			out = append(out, '\\', 'f')
		case '\n':
			out = append(out, '\\', 'n')
		case '\r':
			out = append(out, '\\', 'r')
		case '\t':
			out = append(out, '\\', 't')
		default:
			if ch < 32 {
				out = append(out, '\\', 'u', '0', '0', hex[ch>>4], hex[ch&15])
			} else {
				out = append(out, ch)
			}
		}
	}
	return append(out, '"')
}
func stringObject(pairs ...[2]string) []byte {
	out := []byte{'{'}
	for i, pair := range pairs {
		if i > 0 {
			out = append(out, ',')
		}
		out = quotedWireString(out, pair[0])
		out = append(out, ':')
		out = quotedWireString(out, pair[1])
	}
	return append(out, '}')
}

// EncodeSend validates public fields and keeps payload tokens. 公有字段编码仍须验证。
func EncodeSend(s Send) ([]byte, error) {
	if e := lowerBound(MaxSendBytes, len(s.ClientMsgID), len(s.ConversationID), len(s.Type), len(s.Payload)); e != nil {
		return nil, e
	}
	// Validate the independent fragment before concatenation; no injected frame fields.
	if s.Payload != nil && !json.Valid(s.Payload) {
		return nil, InvalidJSON
	}
	meta := stringObject([2]string{"clientMsgId", s.ClientMsgID}, [2]string{"conversationId", s.ConversationID}, [2]string{"type", s.Type})
	body := append(meta[:len(meta)-1], []byte(`,"version":`+strconv.FormatUint(uint64(s.Version), 10)+`,"payload":`)...)

	if s.Payload == nil {
		body = append(body, []byte("null")...)
	} else {
		body = append(body, s.Payload...)
	}
	body = append(body, '}')
	wire := wrap(s.ProtocolVersion, "send", body)
	if _, e := DecodeSend(wire); e != nil {
		return nil, e
	}
	return wire, nil
}

// EncodeServerFrame validates exactly one result. 输出只允许一个合法服务端结果。
func EncodeServerFrame(f ServerFrame) ([]byte, error) {
	n := 0
	if f.Ack != nil {
		n++
	}
	if f.Error != nil {
		n++
	}
	if f.Message != nil {
		n++
	}
	if n != 1 {
		return nil, InvalidMessage
	}
	var body []byte
	var e error
	kind := ""
	if a := f.Ack; a != nil {
		kind = "send_ack"
		if e = lowerBound(MaxResultBytes, len(a.ClientMsgID), len(a.ConversationID), len(a.SenderID), len(a.ServerMsgID), len(a.ConversationSeq), len(a.ServerTime)); e != nil {
			return nil, e
		}
		body = stringObject([2]string{"status", "SERVER_PERSISTED"}, [2]string{"clientMsgId", a.ClientMsgID}, [2]string{"conversationId", a.ConversationID}, [2]string{"senderId", a.SenderID}, [2]string{"serverMsgId", a.ServerMsgID}, [2]string{"conversationSeq", a.ConversationSeq}, [2]string{"serverTime", a.ServerTime})
	}
	if v := f.Error; v != nil {
		kind = "send_error"
		if e = lowerBound(MaxResultBytes, len(v.ClientMsgID), len(v.ConversationID), len(v.Code)); e != nil {
			return nil, e
		}
		body = stringObject([2]string{"clientMsgId", v.ClientMsgID}, [2]string{"conversationId", v.ConversationID}, [2]string{"code", v.Code})
	}
	if f.Message != nil {
		kind = "message"
		body, e = Encode(*f.Message)
	}
	if e != nil {
		return nil, e
	}
	wire := wrap(1, kind, body)
	if _, e = DecodeServerFrame(wire); e != nil {
		return nil, e
	}
	return wire, nil
}

// Disposition is a local conservative retry decision. 未知错误保守停止自动重试。
type RetryDisposition string

const (
	RetrySameIntent    RetryDisposition = "RetrySameIntent"
	AuthRecovery       RetryDisposition = "AuthRecovery"
	StopAutomaticRetry RetryDisposition = "StopAutomaticRetry"
)

func Disposition(code string) RetryDisposition {
	switch code {
	case "SERVER_TEMPORARY_UNAVAILABLE":
		return RetrySameIntent
	case "AUTH_REQUIRED", "AUTH_TOKEN_EXPIRED":
		return AuthRecovery
	default:
		return StopAutomaticRetry
	}
}
