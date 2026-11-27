package group

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/group"
	"github.com/m-ice/NewIM/server/user"
)

func (r *Repository) RequestEntry(ctx context.Context, actor string, request app.JoinRequest) (app.Entry, error) {
	if ctx == nil || !user.ValidID(actor) || !user.ValidID(request.GroupID) || !user.ValidID(request.RequestID) ||
		len(request.Message) > 512 || !utf8.ValidString(request.Message) || strings.IndexByte(request.Message, 0) >= 0 {
		return app.Entry{}, app.InvalidInput
	}
	switch request.Kind {
	case app.Join:
		if request.UserID != "" && request.UserID != actor {
			return app.Entry{}, app.InvalidInput
		}
		request.UserID = actor
	case app.Invite:
		if !user.ValidID(request.UserID) || request.UserID == actor {
			return app.Entry{}, app.InvalidInput
		}
	default:
		return app.Entry{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.beginWrite(ctx)
	if err != nil {
		return app.Entry{}, err
	}
	defer rollback(tx)
	state, err := lockGroup(ctx, tx, request.GroupID)
	if err != nil {
		return app.Entry{}, err
	}
	entry, err := loadEntry(ctx, tx, request.GroupID, actor, request.RequestID)
	if err == nil {
		if entry.JoinRequest != request {
			return app.Entry{}, app.Conflict
		}
		return committed(ctx, tx, entry)
	}
	if err != app.NotFound {
		return app.Entry{}, err
	}
	if state.dissolvedAt != nil {
		return app.Entry{}, app.NotFound
	}

	direct := state.group.JoinMode == app.OpenJoining
	if request.Kind == app.Invite {
		inviter, err := activeMember(ctx, tx, state, actor)
		if err != nil {
			return app.Entry{}, err
		}
		if !manager(inviter.Role) && !state.group.MembersCanInvite {
			return app.Entry{}, app.Forbidden
		}
		direct = state.group.JoinMode != app.ApproveOrdinaryInvites || manager(inviter.Role)
	}
	users := []string{actor}
	if request.UserID != actor {
		users = append(users, request.UserID)
	}
	if err = requireUsers(ctx, tx, users); err != nil {
		return app.Entry{}, err
	}
	if err = checkEntryAvailable(ctx, tx, request.GroupID, request.UserID); err != nil {
		return app.Entry{}, err
	}
	if direct {
		if _, err = addMember(ctx, tx, state, request.UserID); err != nil {
			return app.Entry{}, err
		}
	}
	entry, err = scanEntry(tx.QueryRow(ctx, `INSERT INTO newim.im_group_requests
		(group_id,actor_id,request_id,user_id,kind,state,message,responded_at,responded_by)
		VALUES ($1,$2,$3,$4,$5,CASE WHEN $6 THEN 'accepted' ELSE 'pending' END,$7,
			CASE WHEN $6 THEN clock_timestamp() END,CASE WHEN $6 THEN $2::newim.identifier END)
		ON CONFLICT DO NOTHING RETURNING `+entryColumns, request.GroupID, actor, request.RequestID,
		request.UserID, request.Kind, direct, request.Message))
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Entry{}, app.Conflict
	}
	if err != nil {
		return app.Entry{}, storageError(err)
	}
	if err = recordEntryChanges(ctx, tx, entry, direct); err != nil {
		return app.Entry{}, err
	}
	return committed(ctx, tx, entry)
}

func (r *Repository) Respond(ctx context.Context, actor string, request app.ResponseRequest) (app.Entry, error) {
	if ctx == nil || !user.ValidID(actor) || !user.ValidID(request.GroupID) || !user.ValidID(request.ActorID) ||
		!user.ValidID(request.RequestID) || (request.Decision != app.Accepted && request.Decision != app.Rejected) {
		return app.Entry{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.beginWrite(ctx)
	if err != nil {
		return app.Entry{}, err
	}
	defer rollback(tx)
	state, err := lockGroup(ctx, tx, request.GroupID)
	if err != nil {
		return app.Entry{}, err
	}
	approver, err := activeMember(ctx, tx, state, actor)
	if err != nil {
		return app.Entry{}, err
	}
	if !manager(approver.Role) {
		return app.Entry{}, app.Forbidden
	}
	entry, err := loadEntry(ctx, tx, request.GroupID, request.ActorID, request.RequestID)
	if err != nil {
		return app.Entry{}, err
	}
	if entry.State == request.Decision {
		return committed(ctx, tx, entry)
	}
	if entry.State != app.Pending {
		return app.Entry{}, app.Conflict
	}
	admitted := request.Decision == app.Accepted
	if admitted {
		if _, err = addMember(ctx, tx, state, entry.UserID); err != nil {
			return app.Entry{}, err
		}
	}
	entry, err = scanEntry(tx.QueryRow(ctx, `UPDATE newim.im_group_requests
		SET state=$4,responded_at=clock_timestamp(),responded_by=$5
		WHERE group_id=$1 AND actor_id=$2 AND request_id=$3 AND state='pending'
		RETURNING `+entryColumns, request.GroupID, request.ActorID, request.RequestID, request.Decision, actor))
	if err != nil {
		return app.Entry{}, storageError(err)
	}
	if err = recordEntryChanges(ctx, tx, entry, admitted); err != nil {
		return app.Entry{}, err
	}
	return committed(ctx, tx, entry)
}

func loadEntry(ctx context.Context, tx pgx.Tx, groupID, actor, requestID string) (app.Entry, error) {
	entry, err := scanEntry(tx.QueryRow(ctx, "SELECT "+entryColumns+` FROM newim.im_group_requests
		WHERE group_id=$1 AND actor_id=$2 AND request_id=$3`, groupID, actor, requestID))
	return entry, storageError(err)
}

func checkEntryAvailable(ctx context.Context, tx pgx.Tx, groupID, target string) error {
	var exists bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM newim.im_conversation_members
		WHERE conversation_id=$1 AND user_id=$2 AND departed_at IS NULL
	) OR EXISTS (
		SELECT 1 FROM newim.im_group_requests
		WHERE group_id=$1 AND user_id=$2 AND state='pending'
	)`, groupID, target).Scan(&exists)
	if err != nil {
		return app.StorageUnavailable
	}
	if exists {
		return app.Conflict
	}
	return nil
}

func addMember(ctx context.Context, tx pgx.Tx, state groupRow, userID string) (app.Membership, error) {
	var count int
	err := tx.QueryRow(ctx, `SELECT count(*) FROM newim.im_conversation_members
		WHERE conversation_id=$1 AND departed_at IS NULL AND group_role IS NOT NULL`,
		state.group.GroupID).Scan(&count)
	if err != nil {
		return app.Membership{}, app.StorageUnavailable
	}
	if count >= state.group.MemberLimit {
		return app.Membership{}, app.CapacityExceeded
	}
	var historyAfter int64
	if !state.group.HistoryEnabled {
		historyAfter = state.head
	}
	member, err := scanMember(tx.QueryRow(ctx, `INSERT INTO newim.im_conversation_members AS member
		(conversation_id,user_id,group_role,history_after_seq) VALUES ($1,$2,'member',$3)
		ON CONFLICT (conversation_id,user_id) DO UPDATE
		SET group_role='member',mute_until=NULL,departed_at=NULL,departed_through_seq=NULL,
			history_after_seq=GREATEST(member.history_after_seq,excluded.history_after_seq)
		WHERE member.departed_at IS NOT NULL
		RETURNING `+memberColumns, state.group.GroupID, userID, historyAfter))
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Membership{}, app.Conflict
	}
	return member, storageError(err)
}

func recordEntryChanges(ctx context.Context, tx pgx.Tx, entry app.Entry, admitted bool) error {
	var changes []accountChange
	if admitted {
		var err error
		changes, err = groupChanges(ctx, tx, entry.GroupID)
		if err != nil {
			return err
		}
	}
	return recordChanges(ctx, tx, entry.GroupID, append(changes, entryChanges(entry)...))
}
