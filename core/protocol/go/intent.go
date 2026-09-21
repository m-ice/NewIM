package protocol

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// SameIntent compares validated v1 intent; caller checks sender/client key. 意图比较不替代幂等键检查。
func SameIntent(a, b Send) (bool, error) {
	if _, e := EncodeSend(a); e != nil {
		return false, e
	}
	if _, e := EncodeSend(b); e != nil {
		return false, e
	}
	if a.ProtocolVersion != b.ProtocolVersion || a.ConversationID != b.ConversationID || a.Version != b.Version || a.Type != b.Type {
		return false, nil
	}
	return equalPayload(a.Payload, b.Payload), nil
}
func equalPayload(a, b []byte) bool {
	decode := func(raw []byte) any {
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		var v any
		_ = d.Decode(&v)
		return v
	}
	return equalValue(decode(a), decode(b))
}

func equalValue(a, b any) bool {
	switch x := a.(type) {
	case nil:
		return b == nil
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	case string:
		y, ok := b.(string)
		return ok && x == y
	case json.Number:
		y, ok := b.(json.Number)
		return ok && canonicalNumber(string(x)) == canonicalNumber(string(y))
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !equalValue(x[i], y[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, ok := y[k]
			if !ok || !equalValue(v, w) {
				return false
			}
		}
		return true
	}
	return false
}

// Signed decimal arithmetic touches only token-sized digit strings, never exponent-sized buffers.
func signed(s string) (bool, string) {
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimLeft(s, "+-")
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return false, "0"
	}
	return neg, s
}
func addMagnitude(a, b string) string {
	n := len(a)
	if len(b) > n {
		n = len(b)
	}
	out := make([]byte, n+1)
	carry := 0
	for i := 0; i < n; i++ {
		v := carry
		if i < len(a) {
			v += int(a[len(a)-1-i] - '0')
		}
		if i < len(b) {
			v += int(b[len(b)-1-i] - '0')
		}
		out[n-i] = byte(v%10) + '0'
		carry = v / 10
	}
	out[0] = byte(carry) + '0'
	if carry == 0 {
		return string(out[1:])
	}
	return string(out)
}
func subtractMagnitude(a, b string) string {
	out := make([]byte, len(a))
	borrow := 0
	for i := 0; i < len(a); i++ {
		v := int(a[len(a)-1-i]-'0') - borrow
		if i < len(b) {
			v -= int(b[len(b)-1-i] - '0')
		}
		borrow = 0
		if v < 0 {
			v += 10
			borrow = 1
		}
		out[len(a)-1-i] = byte(v) + '0'
	}
	r := strings.TrimLeft(string(out), "0")
	if r == "" {
		return "0"
	}
	return r
}
func addExponent(a string, delta int) string {
	an, av := signed(a)
	bn, bv := signed(strconv.Itoa(delta))
	negative := an
	var v string
	if an == bn {
		v = addMagnitude(av, bv)
	} else if len(av) > len(bv) || len(av) == len(bv) && av >= bv {
		v = subtractMagnitude(av, bv)
	} else {
		v = subtractMagnitude(bv, av)
		negative = bn
	}
	if negative && v != "0" {
		return "-" + v
	}
	return v
}
func canonicalNumber(token string) string {
	negative := strings.HasPrefix(token, "-")
	token = strings.TrimPrefix(token, "-")
	exponent := "0"
	if i := strings.IndexAny(token, "eE"); i >= 0 {
		exponent = token[i+1:]
		token = token[:i]
	}
	fraction := 0
	if i := strings.IndexByte(token, '.'); i >= 0 {
		fraction = len(token) - i - 1
		token = token[:i] + token[i+1:]
	}
	token = strings.TrimLeft(token, "0")
	if token == "" {
		return "0"
	}
	coefficient := strings.TrimRight(token, "0")
	exponent = addExponent(exponent, len(token)-len(coefficient)-fraction)
	sign := ""
	if negative {
		sign = "-"
	}
	return sign + coefficient + "e" + exponent
}

// Correlate checks identity and intent against trusted caller context. 调用方须保证账户和连接代次可信。
func Correlate(request Send, trustedSender string, result ServerFrame) error {
	if _, e := EncodeSend(request); e != nil {
		return e
	}
	if !identifier(trustedSender, 128, false) {
		return InvalidMessage
	}
	if _, e := EncodeServerFrame(result); e != nil {
		return e
	}
	if v := result.Error; v != nil {
		if v.ClientMsgID != request.ClientMsgID || v.ConversationID != request.ConversationID {
			return CorrelationMismatch
		}
		return nil
	}
	a := resultAck(result)
	if a.ClientMsgID != request.ClientMsgID || a.ConversationID != request.ConversationID || a.SenderID != trustedSender {
		return CorrelationMismatch
	}
	if m := result.Message; m != nil {
		if request.ProtocolVersion != m.ProtocolVersion || request.Version != m.Version || request.Type != m.Type || !equalPayload(request.Payload, m.Payload) {
			return CorrelationMismatch
		}
	}
	return nil
}
func resultAck(f ServerFrame) Ack {
	if f.Ack != nil {
		return *f.Ack
	}
	m := f.Message
	return Ack{ClientMsgID: m.ClientMsgID, ConversationID: m.ConversationID, SenderID: m.SenderID, ServerMsgID: m.ServerMsgID, ConversationSeq: m.ConversationSeq, ServerTime: m.ServerTime}
}

// CompareResults detects contradictory persisted results without mutating state. 同键结果冲突不得静默覆盖。
func CompareResults(first, second ServerFrame) error {
	if _, e := EncodeServerFrame(first); e != nil {
		return e
	}
	if _, e := EncodeServerFrame(second); e != nil {
		return e
	}
	if first.Error != nil || second.Error != nil {
		return InvalidMessage
	}
	a, b := resultAck(first), resultAck(second)
	if a.ClientMsgID != b.ClientMsgID || a.ConversationID != b.ConversationID || a.SenderID != b.SenderID {
		return CorrelationMismatch
	}
	if a.ServerMsgID != b.ServerMsgID || a.ConversationSeq != b.ConversationSeq || a.ServerTime != b.ServerTime {
		return ResultConflict
	}
	return nil
}
