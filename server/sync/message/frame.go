package message

import (
	"strconv"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

// EncodeServerFrame encodes a persisted message in the delivery wire format.
func EncodeServerFrame(item Item) ([]byte, error) {
	msg := protocol.Message{
		ProtocolVersion: item.ProtocolVersion, Version: item.SchemaVersion,
		ClientMsgID: item.ClientMsgID, ServerMsgID: item.ServerMsgID,
		ConversationID: item.ConversationID, ConversationSeq: strconv.FormatInt(item.ConversationSeq, 10),
		SenderID: item.SenderID, Type: item.MessageType, ServerTime: strconv.FormatInt(item.ServerTime, 10),
		Payload: item.Payload,
	}
	return protocol.EncodeServerFrame(protocol.ServerFrame{Message: &msg})
}
