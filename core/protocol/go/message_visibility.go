package protocol

import (
	"bytes"
	"encoding/json"
)

const VisibilityRequestLimit = 1024

type VisibilityAction string

const (
	Recall      VisibilityAction = "recall"
	DeleteForMe VisibilityAction = "deleteForMe"
	AdminRemove VisibilityAction = "adminRemove"
)

func (a VisibilityAction) Valid() bool {
	return a == Recall || a == DeleteForMe || a == AdminRemove
}

type VisibilityRequest struct {
	ConversationID string           `json:"conversationId"`
	ServerMsgID    string           `json:"serverMsgId"`
	RequestID      string           `json:"requestId"`
	Action         VisibilityAction `json:"action"`
}

func (request VisibilityRequest) Valid() bool {
	return identifier(request.ConversationID, 128, false) &&
		identifier(request.ServerMsgID, 128, false) && identifier(request.RequestID, 128, false) &&
		request.Action.Valid()
}

func DecodeVisibilityRequest(wire []byte) (VisibilityRequest, error) {
	if err := strictJSONLimits(wire, VisibilityRequestLimit, MaxDepth); err != nil {
		return VisibilityRequest{}, err
	}
	fields, err := objectFields(wire)
	if err != nil || len(fields) != 4 {
		return VisibilityRequest{}, InvalidMessage
	}
	var request VisibilityRequest
	var action string
	for key, field := range map[string]*string{
		"conversationId": &request.ConversationID, "serverMsgId": &request.ServerMsgID,
		"requestId": &request.RequestID, "action": &action,
	} {
		if *field, err = stringField(fields, key); err != nil {
			return VisibilityRequest{}, err
		}
	}
	request.Action = VisibilityAction(action)
	if !request.Valid() {
		return VisibilityRequest{}, InvalidMessage
	}
	return request, nil
}

type VisibilityEvent struct {
	ConversationID  string           `json:"conversationId"`
	ServerMsgID     string           `json:"serverMsgId"`
	ConversationSeq int64            `json:"conversationSeq,string"`
	Revision        int64            `json:"visibilityRevision,string"`
	Action          VisibilityAction `json:"action"`
	CountAsUnread   bool             `json:"countAsUnread"`
}

const HiddenMessageType = "message_hidden"

func EncodeHiddenPayload(action VisibilityAction) ([]byte, error) {
	if !action.Valid() {
		return nil, InvalidMessage
	}
	return json.Marshal(struct {
		Action        VisibilityAction `json:"action"`
		CountAsUnread bool             `json:"countAsUnread"`
	}{Action: action})
}

func validateHiddenPayload(raw []byte) error {
	fields, err := objectFields(raw)
	if err != nil || len(fields) != 2 || !bytes.Equal(bytes.TrimSpace(fields["countAsUnread"]), []byte("false")) {
		return InvalidMessage
	}
	action, err := stringField(fields, "action")
	if err != nil || !VisibilityAction(action).Valid() {
		return InvalidMessage
	}
	return nil
}
