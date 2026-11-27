package conversationhttp

import (
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"net/http"
	"strconv"

	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/auth/session"
	syncmessage "github.com/m-ice/NewIM/server/sync/message"
	"github.com/m-ice/NewIM/server/user"
)

const messagesRoute = "/api/v1/sync/messages"

func (h *Handler) readMessages(w http.ResponseWriter, ctx context.Context, token string, bearer session.BearerSession, body []byte) {
	var wire struct {
		ConversationID string         `json:"conversationId"`
		AfterSeq       jsontext.Value `json:"afterSeq"`
		Limit          jsontext.Value `json:"limit"`
	}
	var after string
	if len(body) == 0 || body[0] != '{' ||
		jsonv2.Unmarshal(body, &wire, jsonv2.RejectUnknownMembers(true)) != nil ||
		!user.ValidID(wire.ConversationID) || !decodeString(wire.AfterSeq, &after) {
		writeError(w, http.StatusBadRequest, invalidInput)
		return
	}
	limit, ok := decodeLimit(wire.Limit)
	if !ok {
		writeError(w, http.StatusBadRequest, invalidInput)
		return
	}
	seq, err := strconv.ParseInt(after, 10, 64)
	if err != nil || seq < 0 || strconv.FormatInt(seq, 10) != after {
		writeSyncError(w, string(syncmessage.InvalidCursor))
		return
	}
	identity, err := session.NewConnectionIdentity(
		bearer.UserID(), bearer.DeviceID(), bearer.SessionID(), bearer.TokenID(), bearer.TokenID(),
	)
	if err != nil {
		writeAuthError(w, http.StatusServiceUnavailable, bearerhttp.CodeUnavailable)
		return
	}
	page, err := h.messages.ReadAfterSeq(ctx, identity, syncmessage.ReadRequest{
		ConversationID: wire.ConversationID, HasAfterSeq: true, AfterSeq: seq, Limit: limit,
	})
	if err != nil {
		writeSyncError(w, string(syncmessage.ErrorCode(err)))
		return
	}
	payload, err := encodeMessagePage(page)
	if err != nil {
		writeSyncError(w, string(syncmessage.ErrorCode(err)))
		return
	}
	h.writeVerifiedPage(w, ctx, token, bearer, payload)
}

func encodeMessagePage(page syncmessage.Page) ([]byte, error) {
	body := fmt.Appendf(nil, `{"historyAfterSeq":"%d","latestSeq":"%d","items":[`, page.HistoryAfterSeq, page.LatestSeq)
	footer := messagePageFooter(page.HasNext, 0)
	for i, item := range page.Items {
		frame, err := syncmessage.EncodeServerFrame(item)
		if err != nil {
			return nil, syncmessage.Fail(syncmessage.StorageUnavailable)
		}
		nextFooter := messagePageFooter(page.HasNext || i+1 < len(page.Items), item.ConversationSeq)
		size := len(body) + len(frame) + len(nextFooter)
		if i > 0 {
			size++
		}
		if size > syncmessage.MaxResponseBytes {
			if i == 0 {
				return nil, syncmessage.Fail(syncmessage.LimitExceeded)
			}
			break
		}
		if i > 0 {
			body = append(body, ',')
		}
		body = append(body, frame...)
		footer = nextFooter
	}
	return append(body, footer...), nil
}

func messagePageFooter(hasNext bool, next int64) []byte {
	footer := strconv.AppendBool([]byte(`],"hasNext":`), hasNext)
	footer = append(footer, `,"nextAfterSeq":`...)
	if next == 0 {
		return append(footer, "null}"...)
	}
	footer = append(footer, '"')
	footer = strconv.AppendInt(footer, next, 10)
	return append(footer, '"', '}')
}
