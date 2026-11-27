// Package readhttp serves conversation read state over TLS.
package readhttp

import (
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/read"
	"github.com/m-ice/NewIM/server/userhttp"
)

const (
	routePrefix     = "/api/v1/conversations/"
	watermarkSuffix = "/read"
	receiptSuffix   = "/read-receipt"
	bodyLimit       = 4096
)

type Handler struct {
	service *read.Service
	auth    bearerhttp.Authenticator
	budget  userhttp.Budget
}

func New(service *read.Service, auth bearerhttp.Authenticator, budget userhttp.Budget) (*Handler, error) {
	if service == nil || auth == nil || budget == nil {
		return nil, read.Fail(read.StorageUnavailable)
	}
	return &Handler{service: service, auth: auth, budget: budget}, nil
}

func ContainsRoute(path string) bool {
	_, _, ok := parseRoute(path)
	return ok
}

func parseRoute(path string) (conversationID, serverMsgID string, ok bool) {
	remainder, ok := strings.CutPrefix(path, routePrefix)
	if !ok {
		return "", "", false
	}
	if id, watermark := strings.CutSuffix(remainder, watermarkSuffix); watermark && pathSegment(id) {
		return id, "", true
	}
	path, ok = strings.CutSuffix(remainder, receiptSuffix)
	if !ok {
		return "", "", false
	}
	conversationID, serverMsgID, ok = strings.Cut(path, "/messages/")
	return conversationID, serverMsgID, ok && pathSegment(conversationID) && pathSegment(serverMsgID)
}

func pathSegment(value string) bool {
	return value != "" && !strings.Contains(value, "/")
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	id, msgID, routed := parseRoute(r.URL.EscapedPath())
	isReceipt := msgID != ""
	switch {
	case !routed:
		writeError(w, http.StatusNotFound, "HTTP_ROUTE_NOT_FOUND")
		return
	case r.Method != http.MethodGet && (isReceipt || r.Method != http.MethodPost):
		allow := "GET, POST"
		if isReceipt {
			allow = http.MethodGet
		}
		w.Header().Set("Allow", allow)
		writeError(w, http.StatusMethodNotAllowed, "HTTP_METHOD_NOT_ALLOWED")
		return
	case r.TLS == nil || r.URL.RawQuery != "" || r.URL.ForceQuery || !read.ValidConversationID(id) ||
		(isReceipt && !read.ValidMessageID(msgID)):
		writeError(w, http.StatusBadRequest, string(read.InvalidInput))
		return
	}
	addr, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		writeError(w, http.StatusBadRequest, string(read.InvalidInput))
		return
	}
	if !h.budget.AllowAddress(addr.Addr().Unmap()) {
		writeError(w, http.StatusTooManyRequests, "READ_RATE_LIMITED")
		return
	}
	token, ok := bearerhttp.ParseBearerHeader(r.Header.Values("Authorization"))
	if !ok {
		writeError(w, http.StatusUnauthorized, string(bearerhttp.CodeInvalidToken))
		return
	}
	bearer, err := h.auth.AuthenticateBearer(r.Context(), token)
	if err != nil {
		status, code := bearerhttp.MapAuthenticationError(err)
		writeError(w, status, string(code))
		return
	}
	if !h.budget.AllowAccount(bearer.UserID()) {
		writeError(w, http.StatusTooManyRequests, "READ_RATE_LIMITED")
		return
	}
	if r.Method == http.MethodGet && (r.ContentLength != 0 || len(r.TransferEncoding) != 0) {
		writeError(w, http.StatusBadRequest, string(read.InvalidInput))
		return
	}

	if isReceipt {
		receipt, err := h.service.GetReceipt(r.Context(), bearer, id, msgID)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, receiptResponse{ServerMsgID: receipt.ServerMsgID, Read: receipt.Read})
		return
	}

	var state read.State
	if r.Method == http.MethodGet {
		state, err = h.service.Get(r.Context(), bearer, id)
	} else {
		seq, ok := readSequence(w, r)
		if !ok {
			return
		}
		state, err = h.service.Advance(r.Context(), bearer, id, seq)
	}
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stateResponse{
		ConversationID: state.ConversationID, LastReadSeq: state.LastReadSeq,
		HistoryAfterSeq: state.HistoryAfterSeq, LatestSeq: state.LatestSeq, UpdatedAt: state.UpdatedAt,
	})
}

type receiptResponse struct {
	ServerMsgID string `json:"serverMsgId"`
	Read        bool   `json:"read"`
}

type stateResponse struct {
	ConversationID  string     `json:"conversationId"`
	LastReadSeq     int64      `json:"lastReadSeq,string"`
	HistoryAfterSeq int64      `json:"historyAfterSeq,string"`
	LatestSeq       int64      `json:"latestSeq,string"`
	UpdatedAt       *time.Time `json:"updatedAt"`
}

func readSequence(w http.ResponseWriter, r *http.Request) (int64, bool) {
	if r.ContentLength > bodyLimit {
		writeError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
		return 0, false
	}
	if !validJSONContent(r.Header) {
		writeError(w, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
		return 0, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, bodyLimit))
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
		return 0, false
	}
	var request struct {
		LastReadSeq string `json:"lastReadSeq"`
	}
	if err != nil || jsonv2.Unmarshal(body, &request, jsonv2.RejectUnknownMembers(true)) != nil {
		writeError(w, http.StatusBadRequest, string(read.InvalidInput))
		return 0, false
	}
	seq, err := strconv.ParseInt(request.LastReadSeq, 10, 64)
	if err != nil || seq < 0 || strconv.FormatInt(seq, 10) != request.LastReadSeq {
		writeError(w, http.StatusBadRequest, string(read.InvalidInput))
		return 0, false
	}
	return seq, true
}

func validJSONContent(header http.Header) bool {
	types, encodings := header.Values("Content-Type"), header.Values("Content-Encoding")
	if len(types) != 1 || len(encodings) > 1 || (len(encodings) == 1 && !strings.EqualFold(encodings[0], "identity")) {
		return false
	}
	mediaType, params, err := mime.ParseMediaType(types[0])
	return err == nil && mediaType == "application/json" &&
		(len(params) == 0 || (len(params) == 1 && strings.EqualFold(params["charset"], "utf-8")))
}

func writeServiceError(w http.ResponseWriter, err error) {
	code := read.ErrorCode(err)
	status := http.StatusServiceUnavailable
	switch code {
	case read.InvalidInput:
		status = http.StatusBadRequest
	case read.NotFound:
		status = http.StatusNotFound
	case read.AuthInvalidToken:
		status = http.StatusUnauthorized
	}
	writeError(w, status, string(code))
}

func writeError(w http.ResponseWriter, status int, code string) {
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="newim-session"`)
	}
	type responseError struct {
		Code string `json:"code"`
	}
	writeJSON(w, status, struct {
		Error responseError `json:"error"`
	}{Error: responseError{Code: code}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		status = http.StatusServiceUnavailable
		body = []byte(`{"error":{"code":"READ_STORAGE_UNAVAILABLE"}}`)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}
