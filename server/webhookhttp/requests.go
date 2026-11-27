package webhookhttp

import (
	"context"
	"encoding/hex"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"strconv"

	app "github.com/m-ice/NewIM/server/webhook"
)

const defaultPageLimit = 50

type mutationIDs struct {
	RequestID     string `json:"requestId"`
	DestinationID string `json:"destinationId"`
}

type versionedMutation struct {
	mutationIDs
	ExpectedRevision string `json:"expectedRevision"`
}

type signingInput struct {
	KeyID  string `json:"keyId"`
	Secret string `json:"secret"`
}

type pageInput struct {
	AfterID jsontext.Value `json:"afterId"`
	Limit   jsontext.Value `json:"limit"`
}

func (h *handler) changeEndpoint(action app.EndpointOperation) operation {
	return func(ctx context.Context, raw []byte) (any, error) {
		mutation, ok := decodeEndpointMutation(raw, action)
		defer clear(mutation.Secret)
		if !ok {
			return nil, app.Fail(app.CodeManagementInput)
		}
		return h.store.ChangeEndpoint(ctx, mutation)
	}
}

func decodeEndpointMutation(raw []byte, action app.EndpointOperation) (app.EndpointMutation, bool) {
	mutation := app.EndpointMutation{Operation: action}
	var ids mutationIDs
	var revision string
	var signing signingInput
	switch action {
	case app.CreateEndpoint:
		var input struct {
			mutationIDs
			signingInput
			URL string `json:"url"`
		}
		if !decodeObject(raw, &input) {
			return mutation, false
		}
		ids, signing, mutation.URL = input.mutationIDs, input.signingInput, input.URL
	case app.UpdateEndpoint:
		var input struct {
			versionedMutation
			URL string `json:"url"`
		}
		if !decodeObject(raw, &input) {
			return mutation, false
		}
		ids, revision, mutation.URL = input.mutationIDs, input.ExpectedRevision, input.URL
	case app.RevokeEndpoint:
		var input versionedMutation
		if !decodeObject(raw, &input) {
			return mutation, false
		}
		ids, revision = input.mutationIDs, input.ExpectedRevision
	case app.RotateEndpoint:
		var input struct {
			versionedMutation
			signingInput
		}
		if !decodeObject(raw, &input) {
			return mutation, false
		}
		ids, revision, signing = input.mutationIDs, input.ExpectedRevision, input.signingInput
	}
	mutation.RequestID, mutation.DestinationID = ids.RequestID, ids.DestinationID
	if action != app.CreateEndpoint {
		parsed, err := strconv.ParseInt(revision, 10, 64)
		if err != nil || parsed < 1 || strconv.FormatInt(parsed, 10) != revision {
			return mutation, false
		}
		mutation.ExpectedRevision = parsed
	}
	if action == app.CreateEndpoint || action == app.RotateEndpoint {
		if len(signing.Secret) != 2*app.ManagementSecretSize || !lowerHex([]byte(signing.Secret)) {
			return mutation, false
		}
		secret, err := hex.DecodeString(signing.Secret)
		if err != nil {
			clear(secret)
			return mutation, false
		}
		mutation.KeyID, mutation.Secret = signing.KeyID, secret
	}
	return mutation, mutation.Valid()
}

func (h *handler) getEndpoint(ctx context.Context, raw []byte) (any, error) {
	var input struct {
		DestinationID string `json:"destinationId"`
	}
	if !decodeObject(raw, &input) || !app.ValidManagementID(input.DestinationID) {
		return nil, app.Fail(app.CodeManagementInput)
	}
	return h.store.GetEndpoint(ctx, input.DestinationID)
}

func (h *handler) listEndpoints(ctx context.Context, raw []byte) (any, error) {
	var input pageInput
	if !decodeObject(raw, &input) {
		return nil, app.Fail(app.CodeManagementInput)
	}
	after, limit, ok := input.position()
	if !ok {
		return nil, app.Fail(app.CodeManagementInput)
	}
	page, err := h.store.ListEndpoints(ctx, after, limit)
	if page.Items == nil {
		page.Items = []app.EndpointInfo{}
	}
	return page, err
}

func (h *handler) listDeliveries(ctx context.Context, raw []byte) (any, error) {
	var input struct {
		pageInput
		DestinationID string         `json:"destinationId"`
		DeliveryID    jsontext.Value `json:"deliveryId"`
	}
	if !decodeObject(raw, &input) {
		return nil, app.Fail(app.CodeManagementInput)
	}
	after, limit, pageOK := input.position()
	delivery, deliveryOK := optionalID(input.DeliveryID)
	query := app.DeliveryQuery{DestinationID: input.DestinationID, DeliveryID: delivery, AfterID: after, Limit: limit}
	if !pageOK || !deliveryOK || !query.Valid() {
		return nil, app.Fail(app.CodeManagementInput)
	}
	page, err := h.store.ListDeliveries(ctx, query)
	if page.Items == nil {
		page.Items = []app.DeliveryInfo{}
	}
	return page, err
}

func (h *handler) replayDelivery(ctx context.Context, raw []byte) (any, error) {
	var input struct {
		mutationIDs
		DeliveryID string `json:"deliveryId"`
	}
	if !decodeObject(raw, &input) {
		return nil, app.Fail(app.CodeManagementInput)
	}
	request := app.ReplayRequest{RequestID: input.RequestID, DestinationID: input.DestinationID, DeliveryID: input.DeliveryID}
	if !request.Valid() {
		return nil, app.Fail(app.CodeManagementInput)
	}
	return h.store.ReplayDelivery(ctx, request)
}

func (input pageInput) position() (string, int, bool) {
	after, ok := optionalID(input.AfterID)
	limit := defaultPageLimit
	if input.Limit != nil && (input.Limit.Kind() != '0' || jsonv2.Unmarshal(input.Limit, &limit) != nil) {
		return "", 0, false
	}
	return after, limit, ok && app.ValidManagementPage(after, limit)
}

func optionalID(raw jsontext.Value) (string, bool) {
	if raw == nil {
		return "", true
	}
	var id string
	ok := raw.Kind() == '"' && jsonv2.Unmarshal(raw, &id) == nil && app.ValidManagementID(id)
	return id, ok
}

func decodeObject(raw []byte, target any) bool {
	// The v2 decoder also rejects duplicate names, trailing data, and invalid Unicode.
	return jsontext.Value(raw).Kind() == '{' &&
		jsonv2.Unmarshal(raw, target, jsonv2.RejectUnknownMembers(true)) == nil
}

func lowerHex(raw []byte) bool {
	for _, ch := range raw {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return false
		}
	}
	return true
}
