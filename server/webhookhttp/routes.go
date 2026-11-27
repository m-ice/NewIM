// Package webhookhttp adapts webhook management to trusted process API routes.
package webhookhttp

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

	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/credentialfile"
	app "github.com/m-ice/NewIM/server/webhook"
)

const (
	pathPrefix = "/api/v1/webhooks/"
	bodyLimit  = 8192

	tokenPrefix = "whm_"
	tokenLength = len(tokenPrefix) + 2*sha256.Size
)

type handler struct {
	store  app.ManagementStore
	digest [sha256.Size]byte
}

type operation func(context.Context, []byte) (any, error)

// NewRoutes reads the operator credential and returns the exact management routes.
// The process composer mounts them only on its numeric loopback listener.
func NewRoutes(store app.ManagementStore, absoluteTokenFile string) ([]api.APIRoute, error) {
	if store == nil {
		return nil, app.Fail(app.CodeInvalidConfig)
	}
	raw, err := credentialfile.Read(absoluteTokenFile, tokenLength, tokenLength+1)
	if err != nil {
		return nil, app.Fail(app.CodeInvalidConfig)
	}
	defer clear(raw)
	token := bytes.TrimSuffix(raw, []byte{'\n'})
	if !validToken(token) {
		return nil, app.Fail(app.CodeInvalidConfig)
	}
	h := handler{store: store, digest: sha256.Sum256(token)}
	return []api.APIRoute{
		h.route("webhook_endpoint_create", "endpoints/create", http.StatusCreated, h.changeEndpoint(app.CreateEndpoint)),
		h.route("webhook_endpoint_get", "endpoints/get", http.StatusOK, h.getEndpoint),
		h.route("webhook_endpoint_list", "endpoints/list", http.StatusOK, h.listEndpoints),
		h.route("webhook_endpoint_update", "endpoints/update", http.StatusOK, h.changeEndpoint(app.UpdateEndpoint)),
		h.route("webhook_endpoint_revoke", "endpoints/revoke", http.StatusOK, h.changeEndpoint(app.RevokeEndpoint)),
		h.route("webhook_endpoint_rotate", "endpoints/rotate", http.StatusOK, h.changeEndpoint(app.RotateEndpoint)),
		h.route("webhook_delivery_list", "deliveries/list", http.StatusOK, h.listDeliveries),
		h.route("webhook_delivery_replay", "deliveries/replay", http.StatusOK, h.replayDelivery),
	}, nil
}

func (h *handler) route(name, suffix string, success int, run operation) api.APIRoute {
	path := pathPrefix + suffix
	return api.APIRoute{
		Name:           name,
		Path:           path,
		AllowedMethods: []string{http.MethodPost},
		MaxBodyBytes:   bodyLimit,
		RejectQuery:    true,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, ok := h.readRequest(w, r, path)
			if !ok {
				return
			}
			defer clear(raw)
			result, err := run(r.Context(), raw)
			if err != nil {
				writeManagementError(w, err)
				return
			}
			body, err := json.Marshal(result)
			if err != nil {
				writeManagementError(w, err)
				return
			}
			writeJSON(w, success, append(body, '\n'))
		}),
	}
}

func (h *handler) readRequest(w http.ResponseWriter, r *http.Request, path string) ([]byte, bool) {
	switch {
	case r.URL == nil || r.URL.EscapedPath() != path:
		writeError(w, http.StatusNotFound, api.HTTPRouteNotFound)
	case r.Method != http.MethodPost:
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, api.HTTPMethodNotAllowed)
	case r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.Fragment != "" || r.URL.RawFragment != "":
		writeError(w, http.StatusBadRequest, api.HTTPInvalidQuery)
	case r.ContentLength > bodyLimit:
		writeError(w, http.StatusRequestEntityTooLarge, api.HTTPBodyTooLarge)
	case !validJSONHeaders(r.Header):
		writeError(w, http.StatusUnsupportedMediaType, api.HTTPUnsupportedMediaType)
	case !h.authorized(r.Header):
		w.Header().Set("WWW-Authenticate", `Bearer realm="newim-webhook-management"`)
		writeError(w, http.StatusUnauthorized, string(bearerhttp.CodeInvalidToken))
	case r.Body == nil:
		writeError(w, http.StatusBadRequest, string(app.CodeManagementInput))
	default:
		body := http.MaxBytesReader(w, r.Body, bodyLimit)
		defer body.Close()
		raw, err := io.ReadAll(body)
		if err == nil {
			return raw, true
		}
		clear(raw)
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, api.HTTPBodyTooLarge)
		} else {
			writeError(w, http.StatusBadRequest, string(app.CodeManagementInput))
		}
	}
	return nil, false
}

func (h *handler) authorized(header http.Header) bool {
	token, ok := bearerhttp.ParseBearerHeader(header.Values("Authorization"))
	if !ok || !validToken([]byte(token)) {
		return false
	}
	got := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(got[:], h.digest[:]) == 1
}

func validToken(raw []byte) bool {
	return len(raw) == tokenLength &&
		bytes.HasPrefix(raw, []byte(tokenPrefix)) &&
		lowerHex(raw[len(tokenPrefix):])
}

func validJSONHeaders(header http.Header) bool {
	values := header.Values("Content-Type")
	if len(values) != 1 {
		return false
	}
	media, params, err := mime.ParseMediaType(values[0])
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

func writeManagementError(w http.ResponseWriter, err error) {
	code := app.ErrorCode(err)
	status := http.StatusServiceUnavailable
	switch code {
	case app.CodeManagementInput:
		status = http.StatusBadRequest
	case app.CodeManagementNotFound:
		status = http.StatusNotFound
	case app.CodeManagementConflict, app.CodeManagementNotReplayable:
		status = http.StatusConflict
	case app.CodeManagementCapacity:
	default:
		code = app.CodeStorageUnavailable
	}
	writeError(w, status, string(code))
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, []byte(`{"error":{"code":"`+code+`"}}`+"\n"))
}

func writeJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
