package login

import (
	"context"
	"net/http"

	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	app "github.com/m-ice/NewIM/server/auth/session"
)

// AdmissionRoute admits a verified device for an existing account.
const AdmissionRoute = "/api/v1/internal/session/admit"

// AdmissionHandler shares the issuer credential and bounded HTTP contract.
type AdmissionHandler struct {
	issuer *IssuerHandler
	admit  func(context.Context, app.AdmissionRequest) (app.AdmissionResult, error)
	limits app.DeviceLimits
}

func NewAdmissionHandler(issuer *IssuerHandler, service *app.Service, limits app.DeviceLimits) (*AdmissionHandler, error) {
	if issuer == nil || service == nil || limits.Account < 1 || limits.Platform < 1 {
		return nil, errInvalidIssuerConfig
	}
	return &AdmissionHandler{issuer: issuer, admit: service.Admit, limits: limits}, nil
}

func (h *AdmissionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.admit == nil {
		writeIssuerError(w, http.StatusServiceUnavailable, string(bearerhttp.CodeUnavailable))
		return
	}
	raw, ok := h.issuer.readTrustedRequest(w, r, AdmissionRoute)
	if !ok {
		return
	}
	request := app.AdmissionRequest{TTL: h.issuer.ttl, Limits: h.limits}
	if !decodeIdentityFields(raw, map[string]*string{
		"userId": &request.UserID, "deviceId": &request.DeviceID, "platform": &request.Platform,
	}) {
		writeIssuerError(w, http.StatusBadRequest, string(app.AuthInvalidInput))
		return
	}
	result, err := h.admit(r.Context(), request)
	if err != nil {
		writeSessionActionError(w, err)
		return
	}
	token := result.Token
	if !result.Binding.Valid() || token.RawToken() == "" || token.ExpiresAt().IsZero() {
		writeIssuerError(w, http.StatusServiceUnavailable, string(bearerhttp.CodeUnavailable))
		return
	}
	writeSessionResponse(w, struct {
		issuedTokenResponse
		UserID    string `json:"userId"`
		DeviceID  string `json:"deviceId"`
		SessionID string `json:"sessionId"`
	}{
		issuedTokenResponse: issuedTokenResponse{
			Version: bearerhttp.Version, Token: token.RawToken(), ExpiresAt: token.ExpiresAt().UTC(),
		},
		UserID: result.Binding.UserID, DeviceID: result.Binding.DeviceID, SessionID: result.Binding.SessionID,
	})
}
