// Package mediahttp serves authenticated uploads and downloads over TLS.
package mediahttp

import (
	"encoding/hex"
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
	"github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/media"
	"github.com/m-ice/NewIM/server/userhttp"
)

const (
	grantRoute    = "/api/v1/media/uploads/grant"
	completeRoute = "/api/v1/media/uploads/complete"
	downloadRoute = "/api/v1/media/download"
	uploadHeader  = "X-NewIM-Upload-Token"
	jsonLimit     = 4096

	ObjectTimeout = 2 * time.Minute
	writeTimeout  = ObjectTimeout + 15*time.Second
)

type Handler struct {
	service *media.Service
	auth    bearerhttp.Authenticator
	budget  userhttp.Budget
}

func New(service *media.Service, auth bearerhttp.Authenticator, budget userhttp.Budget) (*Handler, error) {
	if service == nil || auth == nil || budget == nil {
		return nil, media.Fail(media.MediaStorageUnavailable)
	}
	return &Handler{service: service, auth: auth, budget: budget}, nil
}

func ContainsRoute(path string) bool {
	return path == grantRoute || path == completeRoute || path == downloadRoute
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	path := r.URL.EscapedPath()
	switch {
	case !ContainsRoute(path):
		writeError(w, http.StatusNotFound, "HTTP_ROUTE_NOT_FOUND")
		return
	case r.Method != http.MethodPost:
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "HTTP_METHOD_NOT_ALLOWED")
		return
	case r.TLS == nil || r.URL.RawQuery != "" || r.URL.ForceQuery:
		writeMediaError(w, media.Fail(media.MediaInvalidInput))
		return
	}

	addr, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		writeMediaError(w, media.Fail(media.MediaInvalidInput))
		return
	}
	if !h.budget.AllowAddress(addr.Addr().Unmap()) {
		writeError(w, http.StatusTooManyRequests, "MEDIA_RATE_LIMITED")
		return
	}
	identity, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	if !h.budget.AllowAccount(identity.UserID()) {
		writeError(w, http.StatusTooManyRequests, "MEDIA_RATE_LIMITED")
		return
	}

	switch path {
	case grantRoute:
		h.grant(w, r, identity)
	case completeRoute:
		h.complete(w, r, identity)
	case downloadRoute:
		h.download(w, r, identity)
	}
}

func (h *Handler) authenticate(w http.ResponseWriter, r *http.Request) (session.ConnectionIdentity, bool) {
	token, ok := bearerhttp.ParseBearerHeader(r.Header.Values("Authorization"))
	if !ok {
		writeError(w, http.StatusUnauthorized, string(bearerhttp.CodeInvalidToken))
		return session.ConnectionIdentity{}, false
	}
	bearer, err := h.auth.AuthenticateBearer(r.Context(), token)
	if err != nil {
		status, code := bearerhttp.MapAuthenticationError(err)
		writeError(w, status, string(code))
		return session.ConnectionIdentity{}, false
	}
	identity, err := session.NewConnectionIdentity(bearer.UserID(), bearer.DeviceID(), bearer.SessionID(), bearer.TokenID(), bearer.TokenID())
	if err != nil || bearer.ExpiresAt().IsZero() {
		writeError(w, http.StatusServiceUnavailable, string(bearerhttp.CodeUnavailable))
		return session.ConnectionIdentity{}, false
	}
	return identity, true
}

type grantRequest struct {
	ConversationID string `json:"conversationId"`
	Kind           string `json:"kind"`
	ContentType    string `json:"contentType"`
	Size           string `json:"size"`
	SHA256         string `json:"sha256"`
}

type grantResponse struct {
	UploadToken string    `json:"uploadToken"`
	MediaKey    string    `json:"mediaKey"`
	ExpiresAt   time.Time `json:"expiresAt"`
}

func (h *Handler) grant(w http.ResponseWriter, r *http.Request, identity session.ConnectionIdentity) {
	var request grantRequest
	if !readJSON(w, r, &request) {
		return
	}
	size, err := strconv.ParseInt(request.Size, 10, 64)
	if err != nil || strconv.FormatInt(size, 10) != request.Size {
		writeMediaError(w, media.Fail(media.MediaInvalidInput))
		return
	}
	grant, err := h.service.BeginUpload(r.Context(), identity, media.BeginUploadRequest{
		ConversationID: request.ConversationID, Kind: request.Kind,
		ContentType: request.ContentType, Size: size, SHA256: request.SHA256,
	})
	if err != nil {
		writeMediaError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, grantResponse{UploadToken: grant.RawToken, MediaKey: grant.MediaKey, ExpiresAt: grant.ExpiresAt})
}

type completeResponse struct {
	MediaKey    string `json:"mediaKey"`
	Kind        string `json:"kind"`
	ContentType string `json:"contentType"`
	Size        string `json:"size"`
	SHA256      string `json:"sha256"`
}

func (h *Handler) complete(w http.ResponseWriter, r *http.Request, identity session.ConnectionIdentity) {
	if r.ContentLength > media.MaxMediaSize {
		w.Header().Set("Connection", "close")
		writeError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
		return
	}
	if !validContent(w, r, "application/octet-stream") {
		return
	}
	tokens := r.Header.Values(uploadHeader)
	if len(tokens) != 1 || tokens[0] == "" {
		writeMediaError(w, media.Fail(media.MediaInvalidToken))
		return
	}
	controller := http.NewResponseController(w)
	now := time.Now()
	if controller.SetReadDeadline(now.Add(ObjectTimeout)) != nil ||
		controller.SetWriteDeadline(now.Add(writeTimeout)) != nil {
		writeMediaError(w, media.Fail(media.MediaStorageUnavailable))
		return
	}
	body := &uploadReader{reader: http.MaxBytesReader(w, r.Body, media.MaxMediaSize)}
	ready, err := h.service.CompleteUpload(r.Context(), identity, tokens[0], body)
	if body.tooLarge || err != nil {
		w.Header().Set("Connection", "close")
		if body.tooLarge {
			writeError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
		} else {
			writeMediaError(w, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, completeResponse{
		MediaKey: ready.MediaKey, Kind: ready.Kind, ContentType: ready.ContentType,
		Size: strconv.FormatInt(ready.DeclaredSize, 10), SHA256: hex.EncodeToString(ready.SHA256[:]),
	})
}

func (h *Handler) download(w http.ResponseWriter, r *http.Request, identity session.ConnectionIdentity) {
	var request struct {
		MediaKey string `json:"mediaKey"`
	}
	if !readJSON(w, r, &request) {
		return
	}
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		writeMediaError(w, media.Fail(media.MediaStorageUnavailable))
		return
	}
	download, err := h.service.OpenPrivateDownload(r.Context(), identity, request.MediaKey)
	if err != nil {
		writeMediaError(w, err)
		return
	}
	defer download.Body.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment")
	w.Header().Set("Content-Length", strconv.FormatInt(download.Size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.CopyN(w, download.Body, download.Size)
}

// Keep the transport limit visible even when the object store redacts its read error.
type uploadReader struct {
	reader   io.Reader
	tooLarge bool
}

func (r *uploadReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	var limit *http.MaxBytesError
	if errors.As(err, &limit) {
		r.tooLarge = true
	}
	return n, err
}

func validContent(w http.ResponseWriter, r *http.Request, want string) bool {
	types := r.Header.Values("Content-Type")
	encodings := r.Header.Values("Content-Encoding")
	if len(types) == 1 && (len(encodings) == 0 || (len(encodings) == 1 && strings.EqualFold(encodings[0], "identity"))) {
		got, params, err := mime.ParseMediaType(types[0])
		if err == nil && got == want &&
			(len(params) == 0 || (want == "application/json" && len(params) == 1 && strings.EqualFold(params["charset"], "utf-8"))) {
			return true
		}
	}
	writeError(w, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
	return false
}

func readJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.ContentLength > jsonLimit {
		writeError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
		return false
	}
	if !validContent(w, r, "application/json") {
		return false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, jsonLimit))
	var limit *http.MaxBytesError
	if errors.As(err, &limit) {
		writeError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
		return false
	}
	if err != nil || jsonv2.Unmarshal(body, dst, jsonv2.RejectUnknownMembers(true)) != nil {
		writeMediaError(w, media.Fail(media.MediaInvalidInput))
		return false
	}
	return true
}

func writeMediaError(w http.ResponseWriter, err error) {
	code := media.ErrorCode(err)
	status := http.StatusServiceUnavailable
	switch code {
	case media.MediaInvalidInput, media.MediaInvalidToken:
		status = http.StatusBadRequest
	case media.MediaUnauthorized:
		status = http.StatusForbidden
	case media.MediaNotFound:
		status = http.StatusNotFound
	case media.MediaConflict:
		status = http.StatusConflict
	case media.MediaExpired:
		status = http.StatusGone
	default:
		code = media.MediaStorageUnavailable
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
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
