// Package group defines the small-group application and storage contract.
package group

import (
	"context"
	"errors"
	"time"
)

const (
	DefaultMemberLimit = 1000
	MaxMemberLimit     = 1000
	DefaultPageSize    = 50
	MaxPageSize        = 100
)

type Code string

const (
	InvalidInput       Code = "GROUP_INVALID_INPUT"
	Forbidden          Code = "GROUP_FORBIDDEN"
	NotFound           Code = "GROUP_NOT_FOUND"
	Conflict           Code = "GROUP_CONFLICT"
	CapacityExceeded   Code = "GROUP_CAPACITY_EXCEEDED"
	StorageUnavailable Code = "GROUP_STORAGE_UNAVAILABLE"
)

func (e Code) Error() string { return string(e) }

func ErrorCode(err error) Code {
	var code Code
	if errors.As(err, &code) {
		return code
	}
	return StorageUnavailable
}

type Role string

const (
	Owner  Role = "owner"
	Admin  Role = "admin"
	Member Role = "member"
)

type JoinMode int

const (
	ApproveApplications JoinMode = iota
	ApproveOrdinaryInvites
	OpenJoining
)

type Profile struct {
	Name         string `json:"name"`
	FaceURL      string `json:"faceUrl"`
	Description  string `json:"description"`
	Notification string `json:"notification"`
	Ex           string `json:"ex"`
}

type Group struct {
	GroupID          string   `json:"groupId"`
	OwnerID          string   `json:"ownerId"`
	Profile          Profile  `json:"profile"`
	JoinMode         JoinMode `json:"joinMode"`
	MemberLimit      int      `json:"memberLimit"`
	MembersCanInvite bool     `json:"membersCanInvite"`
	HistoryEnabled   bool     `json:"historyEnabled"`
	Muted            bool     `json:"muted"`
}

type Membership struct {
	UserID             string     `json:"userId"`
	Role               Role       `json:"role"`
	MuteUntil          *time.Time `json:"muteUntil,omitempty"`
	HistoryAfterSeq    int64      `json:"historyAfterSeq,string"`
	DepartedThroughSeq *int64     `json:"departedThroughSeq,string,omitempty"`
}

type View struct {
	Group      Group      `json:"group"`
	Membership Membership `json:"membership"`
}

type CreateRequest struct {
	GroupID          string   `json:"groupId"`
	Profile          Profile  `json:"profile"`
	JoinMode         JoinMode `json:"joinMode"`
	MemberLimit      int      `json:"memberLimit"`
	MembersCanInvite *bool    `json:"membersCanInvite"`
	HistoryEnabled   *bool    `json:"historyEnabled"`
	MemberIDs        []string `json:"memberIds"`
}

type UpdateRequest struct {
	GroupID          string    `json:"groupId"`
	Name             *string   `json:"name"`
	FaceURL          *string   `json:"faceUrl"`
	Description      *string   `json:"description"`
	Notification     *string   `json:"notification"`
	Ex               *string   `json:"ex"`
	JoinMode         *JoinMode `json:"joinMode"`
	MembersCanInvite *bool     `json:"membersCanInvite"`
	HistoryEnabled   *bool     `json:"historyEnabled"`
}

type RequestKind string

const (
	Join   RequestKind = "join"
	Invite RequestKind = "invite"
)

type RequestState string

const (
	Pending  RequestState = "pending"
	Accepted RequestState = "accepted"
	Rejected RequestState = "rejected"
)

type JoinRequest struct {
	GroupID   string      `json:"groupId"`
	RequestID string      `json:"requestId"`
	Kind      RequestKind `json:"kind"`
	UserID    string      `json:"userId"`
	Message   string      `json:"message"`
}

type Entry struct {
	JoinRequest
	ActorID     string       `json:"actorId"`
	State       RequestState `json:"state"`
	RespondedBy string       `json:"respondedBy,omitempty"`
}

type ResponseRequest struct {
	GroupID   string       `json:"groupId"`
	ActorID   string       `json:"actorId"`
	RequestID string       `json:"requestId"`
	Decision  RequestState `json:"decision"`
}

type MemberAction string

const (
	Leave         MemberAction = "leave"
	Remove        MemberAction = "remove"
	Promote       MemberAction = "promote"
	Demote        MemberAction = "demote"
	TransferOwner MemberAction = "transfer_owner"
	MuteMember    MemberAction = "mute"
	UnmuteMember  MemberAction = "unmute"
)

type MemberRequest struct {
	GroupID     string       `json:"groupId"`
	UserID      string       `json:"userId"`
	Action      MemberAction `json:"action"`
	MuteSeconds int64        `json:"muteSeconds"`
}

type MemberListRequest struct {
	GroupID     string `json:"groupId"`
	AfterUserID string `json:"afterUserId"`
	Limit       int    `json:"limit"`
}

type EntryListRequest struct {
	GroupID        string `json:"groupId"`
	AfterActorID   string `json:"afterActorId"`
	AfterRequestID string `json:"afterRequestId"`
	Limit          int    `json:"limit"`
}

type Page[T any] struct {
	Items   []T  `json:"items"`
	HasMore bool `json:"hasMore"`
}

type ChangeKind string

const (
	GroupChanged   ChangeKind = "group_changed"
	GroupRemoved   ChangeKind = "group_removed"
	RequestChanged ChangeKind = "request_changed"
)

type Change struct {
	Revision   int64       `json:"revision,string"`
	GroupID    string      `json:"groupId"`
	Kind       ChangeKind  `json:"kind"`
	Membership *Membership `json:"membership,omitempty"`
	Entry      *Entry      `json:"request,omitempty"`
}

type ChangePage struct {
	Page[Change]
	LatestRevision int64 `json:"latestRevision,string"`
}

type Store interface {
	Create(context.Context, string, CreateRequest) (View, error)
	Get(context.Context, string, string) (View, error)
	Update(context.Context, string, UpdateRequest) (View, error)
	RequestEntry(context.Context, string, JoinRequest) (Entry, error)
	Respond(context.Context, string, ResponseRequest) (Entry, error)
	ChangeMember(context.Context, string, MemberRequest) (Membership, error)
	SetMuted(context.Context, string, string, bool) (View, error)
	Dissolve(context.Context, string, string) (bool, error)
	ListMembers(context.Context, string, MemberListRequest) (Page[Membership], error)
	ListEntries(context.Context, string, EntryListRequest) (Page[Entry], error)
	Changes(context.Context, string, int64, int) (ChangePage, error)
}
