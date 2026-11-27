package conversation

import "unicode/utf8"

const MaxAccountDraftBytes = 4096

const (
	AccountSettingsInvalid  Code = "SYNC_INVALID_INPUT"
	AccountSettingsNotFound Code = "SYNC_CONVERSATION_NOT_FOUND"
	AccountStateConflict    Code = "SYNC_ACCOUNT_STATE_CONFLICT"
	ConversationDeleted     Code = "SYNC_CONVERSATION_DELETED"
)

// AccountSettings is the current account's state for one conversation.
// AccountSettings 只包含当前账号自己的会话设置。
type AccountSettings struct {
	Version         uint64 `json:"settingsVersion,string"`
	Pinned          bool   `json:"pinned"`
	Muted           bool   `json:"muted"`
	Draft           string `json:"draft"`
	ClearThroughSeq uint64 `json:"clearThroughSeq,string"`
	Deleted         bool   `json:"deleted"`
}

type AccountSettingsAction uint8

const (
	ChangeAccountSettings AccountSettingsAction = iota + 1
	ClearAccountHistory
	DeleteAccountConversation
)

// AccountSettingsMutation carries a version precondition and an explicit change.
// AccountSettingsMutation 携带设置版本和明确的变更。
type AccountSettingsMutation struct {
	Action          AccountSettingsAction
	ExpectedVersion uint64
	Pinned          *bool
	Muted           *bool
	Draft           *string
}

func (m AccountSettingsMutation) Valid() bool {
	if m.ExpectedVersion > MaxSequence {
		return false
	}
	hasSettings := m.Pinned != nil || m.Muted != nil || m.Draft != nil
	switch m.Action {
	case ChangeAccountSettings:
		return hasSettings && (m.Draft == nil || ValidAccountDraft(*m.Draft))
	case ClearAccountHistory, DeleteAccountConversation:
		return !hasSettings
	default:
		return false
	}
}

func ValidAccountDraft(draft string) bool {
	return len(draft) <= MaxAccountDraftBytes && utf8.ValidString(draft)
}
