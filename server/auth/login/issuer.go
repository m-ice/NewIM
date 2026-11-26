// Package login exposes the trusted application boundary for session issuance.
package login

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	app "github.com/m-ice/NewIM/server/auth/session"
)

const (
	// IssuerRoute is the exact loopback path for trusted application issuance.
	IssuerRoute = "/api/v1/internal/session/token"
	// IssuerBodyLimit is the maximum number of request body bytes.
	IssuerBodyLimit = 1024

	codeSessionNotEligible = "AUTH_SESSION_NOT_ELIGIBLE"
)

// IssuerHandler authenticates a trusted application before issuing a session token.
type IssuerHandler struct {
	issue  func(context.Context, app.IssueRequest) (app.IssuedToken, error)
	digest [sha256.Size]byte
	ttl    time.Duration
}

// NewIssuerHandler reads and verifies the application credential at startup.
func NewIssuerHandler(service *app.Service, absoluteTokenFile string, ttl time.Duration) (*IssuerHandler, error) {
	if service == nil || ttl < app.MinTTL || ttl > app.MaxTTL || ttl%time.Second != 0 {
		return nil, errInvalidIssuerConfig
	}
	digest, err := loadIssuerDigest(absoluteTokenFile)
	if err != nil {
		return nil, err
	}
	return &IssuerHandler{issue: service.Issue, digest: digest, ttl: ttl}, nil
}

func (h *IssuerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.issue == nil {
		writeIssuerError(w, http.StatusServiceUnavailable, string(bearerhttp.CodeUnavailable))
		return
	}
	if r == nil {
		writeIssuerError(w, http.StatusBadRequest, string(app.AuthInvalidInput))
		return
	}
	if r.URL == nil || r.URL.EscapedPath() != IssuerRoute {
		writeIssuerError(w, http.StatusNotFound, string(bearerhttp.CodeRouteNotFound))
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeIssuerError(w, http.StatusMethodNotAllowed, string(bearerhttp.CodeMethodNotAllowed))
		return
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.Fragment != "" || r.URL.RawFragment != "" {
		writeIssuerError(w, http.StatusBadRequest, "HTTP_INVALID_QUERY")
		return
	}
	if r.ContentLength > IssuerBodyLimit {
		writeIssuerError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
		return
	}
	if !validIssuerContentType(r.Header) {
		writeIssuerError(w, http.StatusUnsupportedMediaType, "HTTP_UNSUPPORTED_MEDIA_TYPE")
		return
	}
	if !h.authorized(r.Header) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="newim-issuer"`)
		writeIssuerError(w, http.StatusUnauthorized, string(bearerhttp.CodeInvalidToken))
		return
	}
	if r.Body == nil {
		writeIssuerError(w, http.StatusBadRequest, string(app.AuthInvalidInput))
		return
	}
	body := http.MaxBytesReader(w, r.Body, IssuerBodyLimit)
	defer body.Close()
	raw, err := io.ReadAll(body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeIssuerError(w, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
		} else {
			writeIssuerError(w, http.StatusBadRequest, string(app.AuthInvalidInput))
		}
		return
	}
	binding, valid := decodeIssueBinding(raw)
	if !valid {
		writeIssuerError(w, http.StatusBadRequest, string(app.AuthInvalidInput))
		return
	}
	issued, err := h.issue(r.Context(), app.IssueRequest{Binding: binding, TTL: h.ttl})
	if err != nil {
		switch app.ErrorCode(err) {
		case app.AuthInvalidInput:
			writeIssuerError(w, http.StatusBadRequest, string(app.AuthInvalidInput))
		case app.AuthForbidden, app.AuthSessionNotFound, app.AuthSessionRevoked:
			writeIssuerError(w, http.StatusForbidden, codeSessionNotEligible)
		default:
			writeIssuerError(w, http.StatusServiceUnavailable, string(bearerhttp.CodeUnavailable))
		}
		return
	}
	writeIssuedToken(w, issued)
}

func (h *IssuerHandler) authorized(header http.Header) bool {
	token, ok := bearerhttp.ParseBearerHeader(header.Values("Authorization"))
	if !ok || !validIssuerCredential([]byte(token)) {
		return false
	}
	digest := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(digest[:], h.digest[:]) == 1
}

func validIssuerContentType(header http.Header) bool {
	types := header.Values("Content-Type")
	if len(types) != 1 {
		return false
	}
	media, params, err := mime.ParseMediaType(types[0])
	if err != nil || media != "application/json" {
		return false
	}
	for key, value := range params {
		if key != "charset" || !strings.EqualFold(value, "utf-8") {
			return false
		}
	}
	encodings := header.Values("Content-Encoding")
	if len(encodings) == 0 {
		return true
	}
	if len(encodings) != 1 {
		return false
	}
	encoding := strings.TrimSpace(encodings[0])
	return encoding == "" || strings.EqualFold(encoding, "identity")
}

func decodeIssueBinding(raw []byte) (app.SessionBinding, bool) {
	if !utf8.Valid(raw) {
		return app.SessionBinding{}, false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return app.SessionBinding{}, false
	}
	var binding app.SessionBinding
	fields := map[string]*string{
		"userId":    &binding.UserID,
		"deviceId":  &binding.DeviceID,
		"sessionId": &binding.SessionID,
	}
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		target := fields[key]
		if err != nil || !ok || target == nil {
			return app.SessionBinding{}, false
		}
		token, err = decoder.Token()
		value, ok := token.(string)
		if err != nil || !ok {
			return app.SessionBinding{}, false
		}
		*target = value
		delete(fields, key)
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || len(fields) != 0 || !binding.Valid() {
		return app.SessionBinding{}, false
	}
	_, err = decoder.Token()
	return binding, err == io.EOF
}

func writeIssuedToken(w http.ResponseWriter, token app.IssuedToken) {
	if token.RawToken() == "" || token.ExpiresAt().IsZero() {
		writeIssuerError(w, http.StatusServiceUnavailable, string(bearerhttp.CodeUnavailable))
		return
	}
	body, err := json.Marshal(struct {
		Version   int       `json:"version"`
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expiresAt"`
	}{Version: bearerhttp.Version, Token: token.RawToken(), ExpiresAt: token.ExpiresAt().UTC()})
	if err != nil {
		writeIssuerError(w, http.StatusServiceUnavailable, string(bearerhttp.CodeUnavailable))
		return
	}
	writeIssuerJSON(w, http.StatusOK, append(body, '\n'))
}

func writeIssuerError(w http.ResponseWriter, status int, code string) {
	writeIssuerJSON(w, status, []byte(`{"error":{"code":"`+code+`"}}`+"\n"))
}

func writeIssuerJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
