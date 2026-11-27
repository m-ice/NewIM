package group

import (
	"context"
	"slices"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/group"
	"github.com/m-ice/NewIM/server/user"
)

func (r *Repository) Create(ctx context.Context, actor string, request app.CreateRequest) (app.View, error) {
	if !validActor(ctx, actor, request.GroupID) || request.Profile.Name == "" ||
		request.JoinMode < app.ApproveApplications || request.JoinMode > app.OpenJoining ||
		request.MemberLimit < 0 || request.MemberLimit > app.MaxMemberLimit || len(request.MemberIDs) >= app.MaxMemberLimit {
		return app.View{}, app.InvalidInput
	}
	limit := request.MemberLimit
	if limit == 0 {
		limit = app.DefaultMemberLimit
	}
	ids := make([]string, 0, len(request.MemberIDs)+1)
	ids = append(ids, actor)
	for _, id := range request.MemberIDs {
		if !user.ValidID(id) {
			return app.View{}, app.InvalidInput
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.beginWrite(ctx)
	if err != nil {
		return app.View{}, err
	}
	defer rollback(tx)
	tag, err := tx.Exec(ctx, `INSERT INTO newim.im_conversations(conversation_id,kind)
		VALUES ($1,'group') ON CONFLICT (conversation_id) DO NOTHING`, request.GroupID)
	if err != nil {
		return app.View{}, app.StorageUnavailable
	}
	if tag.RowsAffected() == 0 {
		existing, err := lockGroup(ctx, tx, request.GroupID)
		if err == app.NotFound || (err == nil && (existing.dissolvedAt != nil || existing.group.OwnerID != actor)) {
			return app.View{}, app.Conflict
		}
		if err != nil {
			return app.View{}, err
		}
		return commitView(ctx, tx, existing.group, actor)
	}
	if len(ids) > limit {
		return app.View{}, app.CapacityExceeded
	}
	if err = requireUsers(ctx, tx, ids); err != nil {
		return app.View{}, err
	}
	profile := request.Profile
	if _, err = tx.Exec(ctx, `INSERT INTO newim.im_groups
		(group_id,name,face_url,description,notification,ex,join_mode,member_limit,members_can_invite,history_enabled)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,COALESCE($9,true),COALESCE($10,true))`,
		request.GroupID, profile.Name, profile.FaceURL, profile.Description, profile.Notification, profile.Ex,
		request.JoinMode, limit, request.MembersCanInvite, request.HistoryEnabled); err != nil {
		return app.View{}, app.StorageUnavailable
	}
	if _, err = tx.Exec(ctx, `INSERT INTO newim.im_conversation_members(conversation_id,user_id,group_role)
		SELECT $1,id,CASE WHEN id=$2 THEN 'owner' ELSE 'member' END
		FROM unnest($3::text[]) AS ids(id) ORDER BY id COLLATE "C"`, request.GroupID, actor, ids); err != nil {
		return app.View{}, app.StorageUnavailable
	}
	saved, err := loadGroup(ctx, tx, request.GroupID)
	if err != nil {
		return app.View{}, err
	}
	if err = recordCurrentGroup(ctx, tx, request.GroupID); err != nil {
		return app.View{}, err
	}
	if err = r.recordConversationSource(ctx, tx, request.GroupID, nil); err != nil {
		return app.View{}, err
	}
	return commitView(ctx, tx, saved.group, actor)
}

func (r *Repository) Update(ctx context.Context, actor string, request app.UpdateRequest) (app.View, error) {
	if !validActor(ctx, actor, request.GroupID) || (request.Name != nil && *request.Name == "") ||
		(request.JoinMode != nil && (*request.JoinMode < app.ApproveApplications || *request.JoinMode > app.OpenJoining)) {
		return app.View{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.beginWrite(ctx)
	if err != nil {
		return app.View{}, err
	}
	defer rollback(tx)
	existing, actorMember, err := lockManagedGroup(ctx, tx, request.GroupID, actor)
	if err != nil {
		return app.View{}, err
	}
	if !manager(actorMember.Role) {
		return app.View{}, app.Forbidden
	}
	if _, err = tx.Exec(ctx, `UPDATE newim.im_groups SET name=COALESCE($2,name),face_url=COALESCE($3,face_url),
		description=COALESCE($4,description),notification=COALESCE($5,notification),ex=COALESCE($6,ex),
		join_mode=COALESCE($7,join_mode),members_can_invite=COALESCE($8,members_can_invite),
		history_enabled=COALESCE($9,history_enabled) WHERE group_id=$1`,
		request.GroupID, request.Name, request.FaceURL, request.Description, request.Notification, request.Ex,
		request.JoinMode, request.MembersCanInvite, request.HistoryEnabled); err != nil {
		return app.View{}, app.StorageUnavailable
	}
	changed, err := loadGroup(ctx, tx, request.GroupID)
	if err != nil {
		return app.View{}, err
	}
	if changed.group != existing.group {
		if err = recordCurrentGroup(ctx, tx, request.GroupID); err != nil {
			return app.View{}, err
		}
	}
	return committed(ctx, tx, app.View{Group: changed.group, Membership: actorMember})
}

func (r *Repository) SetMuted(ctx context.Context, actor, groupID string, muted bool) (app.View, error) {
	if !validActor(ctx, actor, groupID) {
		return app.View{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.beginWrite(ctx)
	if err != nil {
		return app.View{}, err
	}
	defer rollback(tx)
	record, actorMember, err := lockManagedGroup(ctx, tx, groupID, actor)
	if err != nil {
		return app.View{}, err
	}
	if !manager(actorMember.Role) {
		return app.View{}, app.Forbidden
	}
	if record.group.Muted != muted {
		if _, err = tx.Exec(ctx, "UPDATE newim.im_groups SET muted=$2 WHERE group_id=$1", groupID, muted); err != nil {
			return app.View{}, app.StorageUnavailable
		}
		record.group.Muted = muted
		if err = recordCurrentGroup(ctx, tx, groupID); err != nil {
			return app.View{}, err
		}
	}
	return committed(ctx, tx, app.View{Group: record.group, Membership: actorMember})
}

func (r *Repository) Dissolve(ctx context.Context, actor, groupID string) (bool, error) {
	if !validActor(ctx, actor, groupID) {
		return false, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.beginWrite(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	record, err := lockGroup(ctx, tx, groupID)
	if err != nil {
		return false, err
	}
	if record.group.OwnerID != actor {
		return false, app.Forbidden
	}
	if record.dissolvedAt != nil {
		return committed(ctx, tx, true)
	}
	if _, err = tx.Exec(ctx, "UPDATE newim.im_groups SET dissolved_at=clock_timestamp() WHERE group_id=$1", groupID); err != nil {
		return false, app.StorageUnavailable
	}
	rows, err := tx.Query(ctx, `UPDATE newim.im_conversation_members
		SET departed_at=statement_timestamp(),departed_through_seq=$2
		WHERE conversation_id=$1 AND group_role IS NOT NULL AND departed_at IS NULL RETURNING `+memberColumns,
		groupID, record.head)
	if err != nil {
		return false, app.StorageUnavailable
	}
	members, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (app.Membership, error) {
		return scanMember(row)
	})
	if err != nil || len(members) == 0 || len(members) > app.MaxMemberLimit {
		return false, app.StorageUnavailable
	}
	changes := make([]accountChange, 0, len(members))
	for _, member := range members {
		changes = append(changes, memberChange(member, app.GroupRemoved))
	}
	if err = recordChanges(ctx, tx, groupID, changes); err != nil {
		return false, err
	}
	if err = r.recordConversationSource(ctx, tx, groupID, members); err != nil {
		return false, err
	}
	return committed(ctx, tx, true)
}

func validActor(ctx context.Context, actor, groupID string) bool {
	return ctx != nil && user.ValidID(actor) && user.ValidID(groupID)
}

func lockManagedGroup(ctx context.Context, tx pgx.Tx, groupID, actor string) (groupRow, app.Membership, error) {
	record, err := lockGroup(ctx, tx, groupID)
	if err != nil {
		return groupRow{}, app.Membership{}, err
	}
	member, err := activeMember(ctx, tx, record, actor)
	return record, member, err
}

func commitView(ctx context.Context, tx pgx.Tx, group app.Group, actor string) (app.View, error) {
	member, err := loadMember(ctx, tx, group.GroupID, actor)
	if err != nil {
		return app.View{}, err
	}
	return committed(ctx, tx, app.View{Group: group, Membership: member})
}

func recordCurrentGroup(ctx context.Context, tx pgx.Tx, groupID string) error {
	changes, err := groupChanges(ctx, tx, groupID)
	if err != nil {
		return err
	}
	return recordChanges(ctx, tx, groupID, changes)
}
