package messagehttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	"github.com/m-ice/NewIM/server/auth/session"
)

const VisibilityRoute = "/api/v1/messages/visibility"

type VisibilityStore interface {
	SupportsVisibility() bool
	ChangeMessageVisibility(context.Context, session.BearerSession, protocol.VisibilityRequest) (protocol.VisibilityEvent, error)
}

type VisibilityHandler struct {
	authenticator Authenticator
	store         VisibilityStore
}

func NewVisibilityHandler(authenticator Authenticator, store VisibilityStore) (*VisibilityHandler, error) {
	if isNil(authenticator) || isNil(store) || !store.SupportsVisibility() {
		return nil, errInvalidConfiguration
	}
	return &VisibilityHandler{authenticator: authenticator, store: store}, nil
}

func (h *VisibilityHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r == nil || r.URL == nil || r.URL.EscapedPath() != VisibilityRoute {
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
	if r.ContentLength > protocol.VisibilityRequestLimit {
		writeError(w, http.StatusRequestEntityTooLarge, CodeBodyTooLarge, false)
		return
	}
	if !validContentType(r.Header) || !validContentEncoding(r.Header) {
		writeError(w, http.StatusUnsupportedMediaType, CodeUnsupportedMediaType, false)
		return
	}
	body, err := readBoundedBody(w, r, protocol.VisibilityRequestLimit)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, CodeBodyTooLarge, false)
		} else {
			writeError(w, http.StatusBadRequest, CodeInvalidInput, false)
		}
		return
	}
	ctx := r.Context()
	bearer, ok := authenticateSession(w, ctx, r.Header, h.authenticator)
	if !ok {
		return
	}
	request, err := protocol.DecodeVisibilityRequest(body)
	if err != nil {
		status, code := mapProtocolError(err)
		writeError(w, status, code, false)
		return
	}
	event, err := h.store.ChangeMessageVisibility(ctx, bearer, request)
	if err != nil {
		var authErr *session.Error
		if errors.As(err, &authErr) {
			status, code := bearerhttp.MapAuthenticationError(err)
			if status == http.StatusUnauthorized {
				writeUnauthorized(w)
			} else {
				writeError(w, status, Code(code), false)
			}
			return
		}
		status, code, retry := mapSendError(err)
		writeError(w, status, code, retry)
		return
	}
	setCommonHeaders(w)
	_ = json.NewEncoder(w).Encode(event)
}
