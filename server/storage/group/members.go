package group

import (
	"context"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/group"
	"github.com/m-ice/NewIM/server/user"
)

func (r *Repository) ChangeMember(ctx context.Context, actor string, request app.MemberRequest) (app.Membership, error) {
	if !validActor(ctx, actor, request.GroupID) {
		return app.Membership{}, app.InvalidInput
	}
	if request.Action == app.Leave {
		if request.UserID != "" && request.UserID != actor {
			return app.Membership{}, app.InvalidInput
		}
		request.UserID = actor
	} else if !user.ValidID(request.UserID) {
		return app.Membership{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.beginWrite(ctx)
	if err != nil {
		return app.Membership{}, err
	}
	defer rollback(tx)
	record, err := lockGroup(ctx, tx, request.GroupID)
	if err != nil {
		return app.Membership{}, err
	}
	if record.dissolvedAt != nil {
		return app.Membership{}, app.NotFound
	}
	acting, err := loadMember(ctx, tx, request.GroupID, actor)
	if err == app.NotFound {
		return app.Membership{}, app.Forbidden
	}
	if err != nil {
		return app.Membership{}, err
	}
	if acting.DepartedThroughSeq != nil {
		if request.Action == app.Leave {
			return committed(ctx, tx, acting)
		}
		return app.Membership{}, app.Forbidden
	}
	if err = authorizeMemberAction(acting, request); err != nil {
		return app.Membership{}, err
	}
	target, err := loadMember(ctx, tx, request.GroupID, request.UserID)
	if err != nil {
		return app.Membership{}, err
	}
	if !canManageTarget(acting, target, request.Action) {
		return app.Membership{}, app.Forbidden
	}
	if target.DepartedThroughSeq != nil {
		if request.Action == app.Remove {
			return committed(ctx, tx, target)
		}
		return app.Membership{}, app.NotFound
	}
	changed, err := changeMember(ctx, tx, record, request, target)
	if err != nil {
		return app.Membership{}, err
	}
	if changed {
		target, err = loadMember(ctx, tx, request.GroupID, request.UserID)
		if err != nil {
			return app.Membership{}, err
		}
		changes, err := groupChanges(ctx, tx, request.GroupID)
		if err != nil {
			return app.Membership{}, err
		}
		if target.DepartedThroughSeq != nil {
			changes = append(changes, memberChange(target, app.GroupRemoved))
		}
		if err = recordChanges(ctx, tx, request.GroupID, changes); err != nil {
			return app.Membership{}, err
		}
	}
	return committed(ctx, tx, target)
}

func authorizeMemberAction(acting app.Membership, request app.MemberRequest) error {
	switch request.Action {
	case app.Leave:
		if acting.Role == app.Owner {
			return app.Forbidden
		}
	case app.Promote, app.Demote, app.TransferOwner:
		if acting.Role != app.Owner || acting.UserID == request.UserID {
			return app.Forbidden
		}
	case app.Remove, app.MuteMember, app.UnmuteMember:
		if !manager(acting.Role) || acting.UserID == request.UserID {
			return app.Forbidden
		}
		if request.Action == app.MuteMember && request.MuteSeconds < 1 {
			return app.InvalidInput
		}
	default:
		return app.InvalidInput
	}
	return nil
}

func canManageTarget(acting, target app.Membership, action app.MemberAction) bool {
	if action == app.Leave {
		return true
	}
	return target.Role != app.Owner && (acting.Role == app.Owner || target.Role == app.Member)
}

func changeMember(ctx context.Context, tx pgx.Tx, record groupRow, request app.MemberRequest, target app.Membership) (bool, error) {
	groupID, userID := request.GroupID, request.UserID
	var tagCount int64
	switch request.Action {
	case app.Leave, app.Remove:
		tag, err := tx.Exec(ctx, `UPDATE newim.im_conversation_members
			SET departed_at=clock_timestamp(),departed_through_seq=$3
			WHERE conversation_id=$1 AND user_id=$2 AND departed_at IS NULL`, groupID, userID, record.head)
		if err != nil {
			return false, app.StorageUnavailable
		}
		tagCount = tag.RowsAffected()
	case app.Promote, app.Demote:
		role := app.Member
		if request.Action == app.Promote {
			role = app.Admin
		}
		if target.Role == role {
			return false, nil
		}
		tag, err := tx.Exec(ctx, `UPDATE newim.im_conversation_members SET group_role=$3
			WHERE conversation_id=$1 AND user_id=$2 AND departed_at IS NULL`, groupID, userID, role)
		if err != nil {
			return false, app.StorageUnavailable
		}
		tagCount = tag.RowsAffected()
	case app.TransferOwner:
		if err := transferOwner(ctx, tx, record, target); err != nil {
			return false, err
		}
		return true, nil
	case app.MuteMember:
		tag, err := tx.Exec(ctx, `UPDATE newim.im_conversation_members
			SET mute_until=clock_timestamp()+($3::bigint * interval '1 second')
			WHERE conversation_id=$1 AND user_id=$2 AND departed_at IS NULL`, groupID, userID, request.MuteSeconds)
		if err != nil {
			return false, app.StorageUnavailable
		}
		tagCount = tag.RowsAffected()
	case app.UnmuteMember:
		if target.MuteUntil == nil {
			return false, nil
		}
		tag, err := tx.Exec(ctx, `UPDATE newim.im_conversation_members SET mute_until=NULL
			WHERE conversation_id=$1 AND user_id=$2 AND departed_at IS NULL`, groupID, userID)
		if err != nil {
			return false, app.StorageUnavailable
		}
		tagCount = tag.RowsAffected()
	default:
		return false, app.InvalidInput
	}
	if tagCount != 1 {
		return false, app.StorageUnavailable
	}
	return true, nil
}

func transferOwner(ctx context.Context, tx pgx.Tx, record groupRow, successor app.Membership) error {
	// Free the unique active-owner slot before promoting the successor.
	tag, err := tx.Exec(ctx, `UPDATE newim.im_conversation_members SET group_role=$3
		WHERE conversation_id=$1 AND user_id=$2 AND group_role='owner' AND departed_at IS NULL`,
		record.group.GroupID, record.group.OwnerID, successor.Role)
	if err != nil || tag.RowsAffected() != 1 {
		return app.StorageUnavailable
	}
	tag, err = tx.Exec(ctx, `UPDATE newim.im_conversation_members SET group_role='owner',mute_until=NULL
		WHERE conversation_id=$1 AND user_id=$2 AND departed_at IS NULL`, record.group.GroupID, successor.UserID)
	if err != nil || tag.RowsAffected() != 1 {
		return app.StorageUnavailable
	}
	return nil
}
