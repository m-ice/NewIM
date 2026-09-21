// This executable exposes the real codec to the database roundtrip test.
// 测试桥接只转换为不丢失精度的字节载体，不实现数据库适配器。
package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

type row struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	SchemaVersion   uint32 `json:"schema_version"`
	ClientID        string `json:"client_id"`
	ServerID        string `json:"server_id"`
	ConversationID  string `json:"conversation_id"`
	Seq             string `json:"seq"`
	SenderID        string `json:"sender_id"`
	Type            string `json:"type"`
	Time            string `json:"time"`
	Payload         string `json:"payload_base64"`
}

func convert(mode string, input []byte) ([]byte, error) {
	if mode == "decode" {
		m, err := protocol.Decode(input)
		if err != nil {
			return nil, err
		}
		return json.Marshal(row{m.ProtocolVersion, m.Version, m.ClientMsgID, m.ServerMsgID,
			m.ConversationID, m.ConversationSeq, m.SenderID, m.Type, m.ServerTime,
			base64.StdEncoding.EncodeToString(m.Payload)})
	}
	if mode == "encode" {
		var r row
		if err := json.Unmarshal(input, &r); err != nil {
			return nil, err
		}
		payload, err := base64.StdEncoding.DecodeString(r.Payload)
		if err != nil {
			return nil, err
		}
		return protocol.Encode(protocol.Message{ProtocolVersion: r.ProtocolVersion, Version: r.SchemaVersion,
			ClientMsgID: r.ClientID, ServerMsgID: r.ServerID, ConversationID: r.ConversationID,
			ConversationSeq: r.Seq, SenderID: r.SenderID, Type: r.Type, ServerTime: r.Time,
			Payload: payload})
	}
	return nil, fmt.Errorf("invalid codec test mode")
}

func main() {
	if len(os.Args) != 2 {
		os.Exit(2)
	}
	input, err := io.ReadAll(io.LimitReader(os.Stdin, 131073))
	if err != nil || len(input) > 131072 {
		os.Exit(2)
	}
	output, err := convert(os.Args[1], input)
	if err != nil {
		// No input or parser details in logs. 不输出正文或身份字段。
		fmt.Fprintln(os.Stderr, "codec test conversion failed")
		os.Exit(1)
	}
	if _, err := os.Stdout.Write(output); err != nil {
		os.Exit(1)
	}
}
