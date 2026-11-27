package conversationhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/read"
	"github.com/m-ice/NewIM/server/sync/conversation"
	syncmessage "github.com/m-ice/NewIM/server/sync/message"
	"github.com/m-ice/NewIM/server/userhttp"
)

const accountSettingsBodyLimit = 32 * 1024

type AccountSettingsStore interface {
	AccountSettings(context.Context, session.BearerSession, string) (conversation.AccountSettings, error)
	MutateAccountSettings(context.Context, session.BearerSession, string, conversation.AccountSettingsMutation) (conversation.AccountSettings, error)
}

func NewWithAccountSettings(service *conversation.Service, messages *syncmessage.Service, auth userhttp.Authenticator, budget userhttp.Budget, store AccountSettingsStore) (*Handler, error) {
	if store == nil {
		return nil, conversation.Fail(conversation.StorageUnavailable)
	}
	handler, err := NewWithMessages(service, messages, auth, budget)
	if err != nil {
		return nil, err
	}
	handler.settings = store
	return handler, nil
}

func accountSettingsRoute(path string) (string, conversation.AccountSettingsAction, bool) {
	rest, ok := strings.CutPrefix(path, "/api/v1/conversations/")
	if !ok {
		return "", 0, false
	}
	id, action, ok := strings.Cut(rest, "/")
	if !ok || id == "" {
		return "", 0, false
	}
	switch action {
	case "settings":
		return id, conversation.ChangeAccountSettings, true
	case "clear":
		return id, conversation.ClearAccountHistory, true
	case "delete":
		return id, conversation.DeleteAccountConversation, true
	default:
		return "", 0, false
	}
}

func (h *Handler) serveAccountSettings(w http.ResponseWriter, r *http.Request, token string, identity session.BearerSession, id string, action conversation.AccountSettingsAction) {
	if !read.ValidConversationID(id) {
		writeError(w, http.StatusBadRequest, invalidInput)
		return
	}
	var state conversation.AccountSettings
	var err error
	if r.Method == http.MethodGet {
		if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			writeError(w, http.StatusBadRequest, invalidInput)
			return
		}
		state, err = h.settings.AccountSettings(r.Context(), identity, id)
	} else {
		body, ok := readLimitedBody(w, r, accountSettingsBodyLimit)
		if !ok {
			return
		}
		mutation, ok := decodeAccountSettingsMutation(body, action)
		if !ok {
			writeError(w, http.StatusBadRequest, invalidInput)
			return
		}
		state, err = h.settings.MutateAccountSettings(r.Context(), identity, id, mutation)
	}
	if err != nil {
		var authErr *session.Error
		if errors.As(err, &authErr) {
			status, code := bearerhttp.MapAuthenticationError(err)
			writeAuthError(w, status, code)
		} else {
			writeSyncError(w, string(conversation.ErrorCode(err)))
		}
		return
	}
	payload, err := json.Marshal(state)
	if err != nil || len(payload) > conversation.MaxResponseBytes {
		writeSyncError(w, string(conversation.StorageUnavailable))
		return
	}
	h.writeVerifiedPage(w, r.Context(), token, identity, payload)
}

func decodeAccountSettingsMutation(body []byte, action conversation.AccountSettingsAction) (conversation.AccountSettingsMutation, bool) {
	var wire struct {
		ExpectedVersion jsontext.Value `json:"expectedVersion"`
		Pinned          jsontext.Value `json:"pinned"`
		Muted           jsontext.Value `json:"muted"`
		Draft           jsontext.Value `json:"draft"`
	}
	var mutation conversation.AccountSettingsMutation
	if len(body) == 0 || body[0] != '{' || jsonv2.Unmarshal(body, &wire, jsonv2.RejectUnknownMembers(true)) != nil {
		return mutation, false
	}
	var version string
	if !decodeString(wire.ExpectedVersion, &version) {
		return mutation, false
	}
	seq, err := strconv.ParseUint(version, 10, 63)
	if err != nil || strconv.FormatUint(seq, 10) != version {
		return mutation, false
	}
	mutation.Action = action
	mutation.ExpectedVersion = seq
	var ok bool
	if mutation.Pinned, ok = decodeOptionalBool(wire.Pinned); !ok {
		return mutation, false
	}
	if mutation.Muted, ok = decodeOptionalBool(wire.Muted); !ok {
		return mutation, false
	}
	if len(wire.Draft) > 0 {
		var draft string
		if wire.Draft[0] != '"' || jsonv2.Unmarshal(wire.Draft, &draft) != nil {
			return mutation, false
		}
		mutation.Draft = &draft
	}
	return mutation, mutation.Valid()
}

func decodeOptionalBool(raw jsontext.Value) (*bool, bool) {
	switch string(bytes.TrimSpace(raw)) {
	case "":
		return nil, true
	case "true":
		value := true
		return &value, true
	case "false":
		value := false
		return &value, true
	default:
		return nil, false
	}
}
