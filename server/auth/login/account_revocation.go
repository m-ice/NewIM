package login

import (
	"context"
	"net/http"

	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	app "github.com/m-ice/NewIM/server/auth/session"
)

// AccountRevocationRoute accepts verified actions from the internal application.
const AccountRevocationRoute = "/api/v1/internal/session/revoke-account"

type AccountRevocationHandler struct {
	issuer *IssuerHandler
	revoke func(context.Context, app.AccountRevocationRequest) (app.AccountRevocationResult, error)
}

func NewAccountRevocationHandler(issuer *IssuerHandler, service *app.Service) (*AccountRevocationHandler, error) {
	if issuer == nil || service == nil {
		return nil, errInvalidIssuerConfig
	}
	return &AccountRevocationHandler{issuer: issuer, revoke: service.RevokeAccount}, nil
}

func (h *AccountRevocationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.revoke == nil {
		writeIssuerError(w, http.StatusServiceUnavailable, string(bearerhttp.CodeUnavailable))
		return
	}
	raw, ok := h.issuer.readTrustedRequest(w, r, AccountRevocationRoute)
	if !ok {
		return
	}
	var request app.AccountRevocationRequest
	if !decodeIdentityFields(raw, map[string]*string{
		"userId": &request.UserID, "requestId": &request.RequestID, "reason": &request.Reason,
	}) {
		writeIssuerError(w, http.StatusBadRequest, string(app.AuthInvalidInput))
		return
	}
	result, err := h.revoke(r.Context(), request)
	if err != nil {
		writeSessionActionError(w, err)
		return
	}
	writeSessionResponse(w, struct {
		Version         int   `json:"version"`
		RevokedSessions int64 `json:"revokedSessions"`
	}{Version: bearerhttp.Version, RevokedSessions: result.RevokedSessions})
}
