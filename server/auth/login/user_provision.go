package login

import (
	"net/http"

	"github.com/m-ice/NewIM/server/user"
	"github.com/m-ice/NewIM/server/userhttp"
)

const (
	UserProvisionRoute     = "/api/v1/internal/users/provision"
	UserProvisionBodyLimit = userhttp.BodyLimit
)

// UserProvisionHandler lets the trusted application provision a user and profile.
// UserProvisionHandler 仅向可信应用开放账号与初始资料预配。
type UserProvisionHandler struct {
	issuer  *IssuerHandler
	service *user.Service
}

func NewUserProvisionHandler(issuer *IssuerHandler, store user.Store) (*UserProvisionHandler, error) {
	if issuer == nil {
		return nil, errInvalidIssuerConfig
	}
	service, err := user.NewService(store)
	if err != nil {
		return nil, err
	}
	return &UserProvisionHandler{issuer: issuer, service: service}, nil
}

func (h *UserProvisionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.service == nil {
		userhttp.WriteServiceError(w, user.Fail(user.StorageUnavailable))
		return
	}
	raw, ok := h.issuer.readTrustedRequestWithLimit(w, r, UserProvisionRoute, UserProvisionBodyLimit, string(user.InvalidInput))
	if !ok {
		return
	}
	id, patch, ok := userhttp.DecodeProvision(raw)
	if !ok {
		userhttp.WriteServiceError(w, user.Fail(user.InvalidInput))
		return
	}
	profile, err := h.service.Provision(r.Context(), id, patch)
	if err != nil {
		userhttp.WriteServiceError(w, err)
		return
	}
	userhttp.WriteProfile(w, profile)
}
