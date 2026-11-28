package conversationhttp

import (
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"net/http"
	"strconv"

	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/auth/session"
	syncmessage "github.com/m-ice/NewIM/server/sync/message"
	"github.com/m-ice/NewIM/server/user"
)

func (h *Handler) readVisibility(w http.ResponseWriter, ctx context.Context, token string, bearer session.BearerSession, body []byte) {
	var wire struct {
		ConversationID string         `json:"conversationId"`
		After          jsontext.Value `json:"afterRevision"`
		Through        jsontext.Value `json:"throughRevision"`
		Limit          jsontext.Value `json:"limit"`
	}
	if len(body) == 0 || body[0] != '{' ||
		jsonv2.Unmarshal(body, &wire, jsonv2.RejectUnknownMembers(true)) != nil ||
		!user.ValidID(wire.ConversationID) {
		writeError(w, http.StatusBadRequest, invalidInput)
		return
	}
	after, afterOK := decodeVisibilityRevision(wire.After)
	through, throughOK := decodeVisibilityRevision(wire.Through)
	if !afterOK || !throughOK {
		writeSyncError(w, string(syncmessage.InvalidCursor))
		return
	}
	limit, ok := decodeLimit(wire.Limit)
	if !ok {
		writeError(w, http.StatusBadRequest, invalidInput)
		return
	}
	identity, err := session.NewConnectionIdentity(
		bearer.UserID(), bearer.DeviceID(), bearer.SessionID(), bearer.TokenID(), bearer.TokenID(),
	)
	if err != nil {
		writeAuthError(w, http.StatusServiceUnavailable, bearerhttp.CodeUnavailable)
		return
	}
	page, err := h.messages.ReadVisibility(ctx, identity, syncmessage.VisibilityRequest{
		ConversationID: wire.ConversationID, AfterRevision: after, ThroughRevision: through, Limit: limit,
	})
	if err != nil {
		writeSyncError(w, string(syncmessage.ErrorCode(err)))
		return
	}
	payload, err := json.Marshal(page)
	if err != nil || len(payload) > syncmessage.MaxResponseBytes {
		writeSyncError(w, string(syncmessage.StorageUnavailable))
		return
	}
	h.writeVerifiedPage(w, ctx, token, bearer, payload)
}

func decodeVisibilityRevision(raw jsontext.Value) (int64, bool) {
	var wire string
	if !decodeString(raw, &wire) {
		return 0, false
	}
	n, err := strconv.ParseInt(wire, 10, 64)
	return n, err == nil && n >= 0 && strconv.FormatInt(n, 10) == wire
}
