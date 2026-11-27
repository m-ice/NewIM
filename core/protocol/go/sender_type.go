package protocol

import "encoding/json"

// SenderType is server-assigned metadata on a persisted result.
type SenderType string

const (
	SenderHuman   SenderType = "HUMAN"
	SenderBot     SenderType = "BOT"
	SenderSystem  SenderType = "SYSTEM"
	SenderAIAgent SenderType = "AI_AGENT"
)

// Valid reports whether a sender type is one of the four assigned classifications.
func (s SenderType) Valid() bool {
	switch s {
	case SenderHuman, SenderBot, SenderSystem, SenderAIAgent:
		return true
	default:
		return false
	}
}

// IsZero keeps both absent and human types out of the v1 encoding.
func (s SenderType) IsZero() bool { return s == "" || s == SenderHuman }

func senderTypeField(fields map[string]json.RawMessage) (SenderType, error) {
	if _, ok := fields["senderType"]; !ok {
		return "", nil
	}
	value, err := stringField(fields, "senderType")
	senderType := SenderType(value)
	if err != nil || !senderType.Valid() {
		return "", InvalidMessage
	}
	return senderType, nil
}
