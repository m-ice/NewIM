// Package friend implements applications and owner-specific friend records.
package friend

import (
	"context"
	"errors"

	"github.com/m-ice/NewIM/server/user"
)

const (
	DefaultLimit = 50
	MaxLimit     = 100
	MaxTextBytes = 512
)

type Code string

const (
	InvalidInput       Code = "FRIEND_INVALID_INPUT"
	NotFound           Code = "FRIEND_NOT_FOUND"
	Pending            Code = "FRIEND_APPLICATION_PENDING"
	Conflict           Code = "FRIEND_APPLICATION_CONFLICT"
	StorageUnavailable Code = "FRIEND_STORAGE_UNAVAILABLE"
)

func (e Code) Error() string { return string(e) }

func ErrorCode(err error) Code {
	var code Code
	if errors.As(err, &code) {
		return code
	}
	return StorageUnavailable
}

type State string

const (
	StatePending  State = "pending"
	StateAccepted State = "accepted"
	StateRejected State = "rejected"
)

type Application struct {
	RequestID   string `json:"requestId"`
	ApplicantID string `json:"applicantId"`
	RecipientID string `json:"recipientId"`
	Message     string `json:"message"`
	State       State  `json:"state"`
}

type Edge struct {
	Profile user.Profile `json:"profile"`
	Remark  string       `json:"remark"`
}

type ApplyRequest struct {
	RequestID string `json:"requestId"`
	PeerID    string `json:"peerId"`
	Message   string `json:"message"`
}

type ResponseRequest struct {
	ApplicantID string `json:"applicantId"`
	RequestID   string `json:"requestId"`
	Decision    string `json:"decision"`
}

type ListRequest struct {
	AfterPeerID    string `json:"afterPeerId"`
	Limit          int    `json:"limit"`
	ExcludeBlocked bool   `json:"excludeBlocked"`
}

type ApplicationListRequest struct {
	Direction        string `json:"direction"`
	AfterApplicantID string `json:"afterApplicantId"`
	AfterRequestID   string `json:"afterRequestId"`
	Limit            int    `json:"limit"`
}

type EdgePage struct {
	Items          []Edge `json:"items"`
	LatestRevision int64  `json:"latestRevision,string"`
	HasMore        bool   `json:"hasMore"`
}

type ApplicationPage struct {
	Items          []Application `json:"items"`
	LatestRevision int64         `json:"latestRevision,string"`
	HasMore        bool          `json:"hasMore"`
}

type ChangeKind string

const (
	ApplicationChanged ChangeKind = "application_changed"
	FriendUpserted     ChangeKind = "friend_upserted"
	FriendRemoved      ChangeKind = "friend_removed"
)

type Change struct {
	Revision    int64        `json:"revision,string"`
	Kind        ChangeKind   `json:"kind"`
	PeerID      string       `json:"peerId,omitempty"`
	Application *Application `json:"application,omitempty"`
	Edge        *Edge        `json:"friend,omitempty"`
}

type ChangePage struct {
	Items          []Change `json:"items"`
	LatestRevision int64    `json:"latestRevision,string"`
	HasMore        bool     `json:"hasMore"`
}

type Store interface {
	Apply(context.Context, string, ApplyRequest) (Application, error)
	Respond(context.Context, string, ResponseRequest) (Application, error)
	Remark(context.Context, string, string, string) (Edge, error)
	Remove(context.Context, string, string) (bool, error)
	List(context.Context, string, ListRequest) (EdgePage, error)
	ListApplications(context.Context, string, ApplicationListRequest) (ApplicationPage, error)
	Changes(context.Context, string, int64, int) (ChangePage, error)
}
