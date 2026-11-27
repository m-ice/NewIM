package webhook

import (
	"context"
	"crypto/rand"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	CodeManagementInput         Code = "WEBHOOK_MANAGEMENT_INVALID_INPUT"
	CodeManagementNotFound      Code = "WEBHOOK_MANAGEMENT_NOT_FOUND"
	CodeManagementConflict      Code = "WEBHOOK_MANAGEMENT_CONFLICT"
	CodeManagementCapacity      Code = "WEBHOOK_MANAGEMENT_CAPACITY"
	CodeManagementNotReplayable Code = "WEBHOOK_MANAGEMENT_NOT_REPLAYABLE"

	MaxManagementPage    = 100
	ManagementSecretSize = 32
)

type EndpointOperation string

const (
	CreateEndpoint EndpointOperation = "create"
	UpdateEndpoint EndpointOperation = "update"
	RotateEndpoint EndpointOperation = "rotate"
	RevokeEndpoint EndpointOperation = "revoke"
)

type EndpointMutation struct {
	Operation        EndpointOperation
	RequestID        string
	DestinationID    string
	ExpectedRevision int64
	URL              string
	KeyID            string
	Secret           []byte
}

func (m EndpointMutation) Valid() bool {
	if !ValidManagementID(m.RequestID) || !ValidManagementID(m.DestinationID) {
		return false
	}
	switch m.Operation {
	case CreateEndpoint:
		return m.ExpectedRevision == 0 && validManagementURL(m.URL) && len(m.KeyID) <= 64 && ValidManagementID(m.KeyID) && len(m.Secret) == ManagementSecretSize
	case UpdateEndpoint:
		return m.ExpectedRevision > 0 && validManagementURL(m.URL) && m.KeyID == "" && len(m.Secret) == 0
	case RotateEndpoint:
		return m.ExpectedRevision > 0 && m.URL == "" && len(m.KeyID) <= 64 && ValidManagementID(m.KeyID) && len(m.Secret) == ManagementSecretSize
	case RevokeEndpoint:
		return m.ExpectedRevision > 0 && m.URL == "" && m.KeyID == "" && len(m.Secret) == 0
	default:
		return false
	}
}

type EndpointInfo struct {
	DestinationID string     `json:"destinationId"`
	Status        string     `json:"status"`
	Revision      *int64     `json:"revision,string"`
	KeyID         string     `json:"keyId,omitempty"`
	CreatedAt     time.Time  `json:"createdAt"`
	UpdatedAt     time.Time  `json:"updatedAt"`
	RevokedAt     *time.Time `json:"revokedAt,omitempty"`
}

type EndpointPage struct {
	Items       []EndpointInfo `json:"items"`
	NextAfterID string         `json:"nextAfterId,omitempty"`
}

type DeliveryQuery struct {
	DestinationID string
	DeliveryID    string
	AfterID       string
	Limit         int
}

func (q DeliveryQuery) Valid() bool {
	return ValidManagementID(q.DestinationID) && ValidManagementPage(q.AfterID, q.Limit) &&
		(q.DeliveryID == "" || q.AfterID == "" && ValidManagementID(q.DeliveryID))
}

type DeliveryInfo struct {
	DeliveryID       string     `json:"deliveryId"`
	EventID          string     `json:"eventId"`
	DestinationID    string     `json:"destinationId"`
	EndpointRevision *int64     `json:"endpointRevision,string"`
	Status           string     `json:"status"`
	Attempts         int        `json:"attempts"`
	LastHTTPStatus   *int       `json:"lastHttpStatus,omitempty"`
	LastErrorCode    Code       `json:"lastErrorCode,omitempty"`
	NextAttemptAt    *time.Time `json:"nextAttemptAt,omitempty"`
	CompletedAt      *time.Time `json:"completedAt,omitempty"`
	UpdatedAt        time.Time  `json:"updatedAt"`
}

type DeliveryPage struct {
	Items       []DeliveryInfo `json:"items"`
	NextAfterID string         `json:"nextAfterId,omitempty"`
}

type ReplayRequest struct {
	RequestID     string
	DestinationID string
	DeliveryID    string
}

func (r ReplayRequest) Valid() bool {
	return ValidManagementID(r.RequestID) && ValidManagementID(r.DestinationID) && ValidManagementID(r.DeliveryID)
}

type ReplayReceipt struct {
	RequestID     string    `json:"requestId"`
	DestinationID string    `json:"destinationId"`
	DeliveryID    string    `json:"deliveryId"`
	Revision      int64     `json:"revision,string"`
	ReplayedAt    time.Time `json:"replayedAt"`
}

// ManagementStore owns revision changes and their transactionally committed audit.
// ManagementStore 负责修订变更与同事务审计。
type ManagementStore interface {
	ChangeEndpoint(context.Context, EndpointMutation) (EndpointInfo, error)
	GetEndpoint(context.Context, string) (EndpointInfo, error)
	ListEndpoints(context.Context, string, int) (EndpointPage, error)
	ListDeliveries(context.Context, DeliveryQuery) (DeliveryPage, error)
	ReplayDelivery(context.Context, ReplayRequest) (ReplayReceipt, error)
}

func ValidManagementPage(afterID string, limit int) bool {
	return (afterID == "" || ValidManagementID(afterID)) && limit >= 1 && limit <= MaxManagementPage
}

func ValidManagementID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := range len(value) {
		b := value[i]
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-') {
			return false
		}
	}
	return true
}

func validManagementURL(value string) bool {
	if len(value) == 0 || len(value) > 2048 || strings.IndexFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) >= 0 {
		return false
	}
	target, err := parseEndpoint(value, false)
	if err != nil {
		return false
	}
	host, port := target.Hostname(), target.Port()
	if port != "" {
		if number, err := strconv.ParseUint(port, 10, 16); err != nil || number == 0 {
			return false
		}
	}
	if ip := net.ParseIP(host); ip != nil {
		return (Policy{}).allowsAddress(ip)
	}
	return !strings.Contains(host, "%")
}

// SealRevision wraps a secret with a fresh nonce bound to one immutable endpoint revision.
// SealRevision 使用新 nonce 封存密钥，并绑定不可变目标修订。
func (r *LocalSecretResolver) SealRevision(material SecretMaterial, secret []byte) (SecretMaterial, error) {
	if r == nil || r.aead == nil {
		return SecretMaterial{}, Fail(CodeInvalidConfig)
	}
	material.Nonce = make([]byte, r.aead.NonceSize())
	if _, err := rand.Read(material.Nonce); err != nil {
		return SecretMaterial{}, Fail(CodeSecretUnavailable)
	}
	ciphertext, err := SealSecret(r.aead, material, secret)
	if err != nil {
		return SecretMaterial{}, err
	}
	material.Ciphertext = ciphertext
	return material, nil
}
