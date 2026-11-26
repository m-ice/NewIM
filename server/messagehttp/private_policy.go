package messagehttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

const (
	DirectConversationRoute = "/api/v1/conversations/direct"
	BlockPeerRoute          = "/api/v1/relationships/block"
	UnblockPeerRoute        = "/api/v1/relationships/unblock"
	PrivatePolicyBodyLimit  = 512
)

// PrivatePolicyStore commits a relationship change or direct conversation for a trusted actor.
type PrivatePolicyStore interface {
	CreateDirect(ctx context.Context, actorID, peerID string) (string, error)
	SetBlocked(ctx context.Context, actorID, peerID string, blocked bool) error
}

// PrivatePolicyHandler uses the existing Bearer session boundary for relationship routes.
type PrivatePolicyHandler struct {
	authenticator Authenticator
	store         PrivatePolicyStore
}

func NewPrivatePolicyHandler(authenticator Authenticator, store PrivatePolicyStore) (*PrivatePolicyHandler, error) {
	if isNil(authenticator) || isNil(store) {
		return nil, errInvalidConfiguration
	}
	return &PrivatePolicyHandler{authenticator: authenticator, store: store}, nil
}

func (h *PrivatePolicyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r == nil || r.URL == nil {
		writeError(w, http.StatusBadRequest, CodeInvalidInput, false)
		return
	}
	route := r.URL.EscapedPath()
	switch route {
	case DirectConversationRoute, BlockPeerRoute, UnblockPeerRoute:
	default:
		writeError(w, http.StatusNotFound, CodeRouteNotFound, false)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, CodeMethodNotAllowed, false)
		return
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.Fragment != "" || r.URL.RawFragment != "" {
		writeError(w, http.StatusBadRequest, CodeInvalidQuery, false)
		return
	}
	if r.ContentLength > PrivatePolicyBodyLimit {
		writeError(w, http.StatusRequestEntityTooLarge, CodeBodyTooLarge, false)
		return
	}
	if !validContentType(r.Header) || !validContentEncoding(r.Header) {
		writeError(w, http.StatusUnsupportedMediaType, CodeUnsupportedMediaType, false)
		return
	}
	body, err := readBoundedBody(w, r, PrivatePolicyBodyLimit)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, CodeBodyTooLarge, false)
		} else {
			writeError(w, http.StatusBadRequest, CodeInvalidInput, false)
		}
		return
	}
	ctx := r.Context()
	session, ok := authenticateSession(w, ctx, r.Header, h.authenticator)
	if !ok {
		return
	}
	peerID, ok := decodePeer(body)
	if !ok {
		writeError(w, http.StatusBadRequest, CodeInvalidInput, false)
		return
	}
	if route == DirectConversationRoute {
		conversationID, err := h.store.CreateDirect(ctx, session.UserID(), peerID)
		if err != nil {
			writePrivatePolicyError(w, err)
			return
		}
		setCommonHeaders(w)
		_ = json.NewEncoder(w).Encode(struct {
			ConversationID string `json:"conversationId"`
		}{ConversationID: conversationID})
		return
	}
	if err := h.store.SetBlocked(ctx, session.UserID(), peerID, route == BlockPeerRoute); err != nil {
		writePrivatePolicyError(w, err)
		return
	}
	setCommonHeaders(w)
	w.WriteHeader(http.StatusNoContent)
}

func writePrivatePolicyError(w http.ResponseWriter, err error) {
	status, code, retry := mapSendError(err)
	writeError(w, status, code, retry)
}

// A single-member object excludes duplicate keys and any client-supplied actor.
func decodePeer(body []byte) (string, bool) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return "", false
	}
	key, err := decoder.Token()
	if err != nil || key != "peerUserId" {
		return "", false
	}
	var peer string
	if err := decoder.Decode(&peer); err != nil || peer == "" {
		return "", false
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return "", false
	}
	_, err = decoder.Token()
	return peer, errors.Is(err, io.EOF)
}
