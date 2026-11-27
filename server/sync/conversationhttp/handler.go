// Package conversationhttp serves the authenticated conversation read protocol.
package conversationhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/netip"
	"strings"

	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/sync/conversation"
	syncmessage "github.com/m-ice/NewIM/server/sync/message"
	"github.com/m-ice/NewIM/server/userhttp"
)

const (
	bootstrapRoute = "/api/v1/sync/bootstrap"
	deltaRoute     = "/api/v1/sync/delta"
	bodyLimit      = 4096
	invalidInput   = "SYNC_INVALID_INPUT"
	rateLimited    = "SYNC_RATE_LIMITED"
)

type Handler struct {
	service  *conversation.Service
	messages *syncmessage.Service
	auth     userhttp.Authenticator
	budget   userhttp.Budget
}

func New(service *conversation.Service, auth userhttp.Authenticator, budget userhttp.Budget) (*Handler, error) {
	if service == nil || auth == nil || budget == nil {
		return nil, conversation.Fail(conversation.StorageUnavailable)
	}
	return &Handler{service: service, auth: auth, budget: budget}, nil
}

func NewWithMessages(service *conversation.Service, messages *syncmessage.Service, auth userhttp.Authenticator, budget userhttp.Budget) (*Handler, error) {
	if messages == nil {
		return nil, conversation.Fail(conversation.StorageUnavailable)
	}
	handler, err := New(service, auth, budget)
	if err != nil {
		return nil, err
	}
	handler.messages = messages
	return handler, nil
}

func ContainsRoute(path string) bool {
	return path == bootstrapRoute || path == deltaRoute || path == messagesRoute
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.EscapedPath()
	switch {
	case !ContainsRoute(path) || (path == messagesRoute && h.messages == nil):
		writeError(w, http.StatusNotFound, "HTTP_NOT_FOUND")
		return
	case r.Method != http.MethodPost:
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "HTTP_METHOD_NOT_ALLOWED")
		return
	case r.TLS == nil || r.URL.RawQuery != "" || r.URL.ForceQuery:
		writeError(w, http.StatusBadRequest, invalidInput)
		return
	}
	addr, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		writeError(w, http.StatusBadRequest, invalidInput)
		return
	}
	if !h.budget.AllowAddress(addr.Addr().Unmap()) {
		writeError(w, http.StatusTooManyRequests, rateLimited)
		return
	}
	token, ok := bearerhttp.ParseBearerHeader(r.Header.Values("Authorization"))
	if !ok {
		writeAuthError(w, http.StatusUnauthorized, bearerhttp.CodeInvalidToken)
		return
	}
	identity, err := h.auth.AuthenticateBearer(r.Context(), token)
	if err != nil {
		status, code := bearerhttp.MapAuthenticationError(err)
		writeAuthError(w, status, code)
		return
	}
	if !h.budget.AllowAccount(identity.UserID()) {
		writeError(w, http.StatusTooManyRequests, rateLimited)
		return
	}
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	if path == messagesRoute {
		h.readMessages(w, r.Context(), identity, body)
		return
	}
	request, ok := decodeRequest(path, body)
	if !ok {
		writeError(w, http.StatusBadRequest, invalidInput)
		return
	}
	page, err := h.read(r.Context(), path, conversation.Principal{UserID: identity.UserID()}, request)
	if err != nil {
		writeSyncError(w, string(conversation.ErrorCode(err)))
		return
	}
	payload, err := json.Marshal(page)
	if err != nil || len(payload) > conversation.MaxResponseBytes {
		writeSyncError(w, string(conversation.StorageUnavailable))
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

type readRequest struct {
	cursor     string
	checkpoint string
	limit      int
}

func decodeRequest(path string, body []byte) (readRequest, bool) {
	var wire struct {
		Cursor     jsontext.Value `json:"cursor"`
		Checkpoint jsontext.Value `json:"checkpoint"`
		Limit      jsontext.Value `json:"limit"`
	}
	if len(body) == 0 || body[0] != '{' || jsonv2.Unmarshal(body, &wire, jsonv2.RejectUnknownMembers(true)) != nil {
		return readRequest{}, false
	}
	var request readRequest
	if len(wire.Cursor) != 0 {
		ok := len(wire.Checkpoint) == 0 && len(wire.Limit) == 0 && decodeString(wire.Cursor, &request.cursor)
		return request, ok
	}
	if path == deltaRoute {
		if !decodeString(wire.Checkpoint, &request.checkpoint) {
			return readRequest{}, false
		}
	} else if len(wire.Checkpoint) != 0 {
		return readRequest{}, false
	}
	var ok bool
	request.limit, ok = decodeLimit(wire.Limit)
	if !ok {
		return readRequest{}, false
	}
	return request, true
}

func decodeString(value jsontext.Value, dest *string) bool {
	return len(value) > 0 && value[0] == '"' && jsonv2.Unmarshal(value, dest) == nil && *dest != ""
}

func decodeLimit(value jsontext.Value) (int, bool) {
	var limit int
	if len(value) == 0 {
		return 0, true
	}
	if bytes.Equal(bytes.TrimSpace(value), []byte("null")) || jsonv2.Unmarshal(value, &limit) != nil {
		return 0, false
	}
	return limit, true
}

func (h *Handler) read(ctx context.Context, path string, principal conversation.Principal, req readRequest) (conversation.Page, error) {
	if path == bootstrapRoute {
		if req.cursor != "" {
			return h.service.ContinueBootstrap(ctx, principal, req.cursor)
		}
		return h.service.BeginBootstrap(ctx, principal, req.limit)
	}
	if req.cursor != "" {
		return h.service.ContinueDelta(ctx, principal, req.cursor)
	}
	return h.service.BeginDelta(ctx, principal, req.checkpoint, req.limit)
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r.ContentLength > bodyLimit {
		writeError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
		return nil, false
	}
	if !jsonContentType(r.Header) {
		writeError(w, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
		return nil, false
	}
	if r.Body == nil {
		writeError(w, http.StatusBadRequest, invalidInput)
		return nil, false
	}
	reader := http.MaxBytesReader(w, r.Body, bodyLimit)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err == nil {
		return bytes.Trim(body, " \t\r\n"), true
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
	} else {
		writeError(w, http.StatusBadRequest, invalidInput)
	}
	return nil, false
}

func jsonContentType(headers http.Header) bool {
	values := headers.Values("Content-Type")
	if len(values) != 1 {
		return false
	}
	media, params, err := mime.ParseMediaType(values[0])
	charset, hasCharset := params["charset"]
	if err != nil || media != "application/json" ||
		(len(params) != 0 && (len(params) != 1 || !hasCharset || !strings.EqualFold(charset, "utf-8"))) {
		return false
	}
	encodings := headers.Values("Content-Encoding")
	return len(encodings) == 0 || (len(encodings) == 1 && strings.EqualFold(strings.TrimSpace(encodings[0]), "identity"))
}

func writeSyncError(w http.ResponseWriter, code string) {
	status := http.StatusServiceUnavailable
	switch conversation.Code(code) {
	case conversation.InvalidCursor, conversation.LimitExceeded:
		status = http.StatusBadRequest
	case conversation.Forbidden:
		status = http.StatusForbidden
	case conversation.CursorExpired:
		status = http.StatusConflict
	}
	writeError(w, status, code)
}

func writeAuthError(w http.ResponseWriter, status int, code bearerhttp.Code) {
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	writeError(w, status, string(code))
}

func writeError(w http.ResponseWriter, status int, code string) {
	body, _ := json.Marshal(map[string]any{"error": map[string]string{"code": code}})
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
