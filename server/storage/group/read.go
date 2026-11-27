package group

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/group"
	"github.com/m-ice/NewIM/server/user"
)

func (r *Repository) Get(ctx context.Context, actor, groupID string) (app.View, error) {
	if ctx == nil || !user.ValidID(actor) || !user.ValidID(groupID) {
		return app.View{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return app.View{}, err
	}
	defer rollback(tx)
	view, err := activeView(ctx, tx, actor, groupID)
	if err != nil {
		return app.View{}, err
	}
	if err = commit(ctx, tx); err != nil {
		return app.View{}, err
	}
	return view, nil
}

func (r *Repository) ListMembers(ctx context.Context, actor string, request app.MemberListRequest) (app.Page[app.Membership], error) {
	limit := readPageLimit(request.Limit)
	if ctx == nil || !user.ValidID(actor) || !user.ValidID(request.GroupID) ||
		!validReadCursor(request.AfterUserID) || limit < 1 || limit > app.MaxPageSize {
		return app.Page[app.Membership]{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return app.Page[app.Membership]{}, err
	}
	defer rollback(tx)
	if _, err = activeView(ctx, tx, actor, request.GroupID); err != nil {
		return app.Page[app.Membership]{}, err
	}
	rows, err := tx.Query(ctx, "SELECT "+memberColumns+`
		FROM newim.im_conversation_members
		WHERE conversation_id=$1 AND group_role IS NOT NULL AND departed_at IS NULL
			AND user_id > $2::text
		ORDER BY user_id LIMIT $3`, request.GroupID, request.AfterUserID, limit+1)
	if err != nil {
		return app.Page[app.Membership]{}, app.StorageUnavailable
	}
	return collectPage(ctx, tx, rows, limit, scanMember)
}

func (r *Repository) ListEntries(ctx context.Context, actor string, request app.EntryListRequest) (app.Page[app.Entry], error) {
	limit := readPageLimit(request.Limit)
	if ctx == nil || !user.ValidID(actor) || !user.ValidID(request.GroupID) ||
		!validReadCursor(request.AfterActorID) || !validReadCursor(request.AfterRequestID) ||
		(request.AfterActorID == "") != (request.AfterRequestID == "") || limit < 1 || limit > app.MaxPageSize {
		return app.Page[app.Entry]{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return app.Page[app.Entry]{}, err
	}
	defer rollback(tx)
	rows, err := tx.Query(ctx, "SELECT "+entryColumns+`
		FROM newim.im_group_requests
		WHERE group_id=$1 AND (actor_id, request_id) > ($3::text, $4::text)
			AND (actor_id=$2 OR user_id=$2 OR EXISTS (
				SELECT 1 FROM newim.im_groups g
				JOIN newim.im_conversation_members m ON m.conversation_id=g.group_id
				WHERE g.group_id=$1 AND g.dissolved_at IS NULL AND m.user_id=$2
					AND m.departed_at IS NULL AND m.group_role IN ('owner','admin')
			))
		ORDER BY actor_id, request_id LIMIT $5`, request.GroupID, actor, request.AfterActorID, request.AfterRequestID, limit+1)
	if err != nil {
		return app.Page[app.Entry]{}, app.StorageUnavailable
	}
	return collectPage(ctx, tx, rows, limit, scanEntry)
}

func (r *Repository) Changes(ctx context.Context, actor string, after int64, limit int) (app.ChangePage, error) {
	limit = readPageLimit(limit)
	if ctx == nil || !user.ValidID(actor) || after < 0 || limit < 1 || limit > app.MaxPageSize {
		return app.ChangePage{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return app.ChangePage{}, err
	}
	defer rollback(tx)
	var head int64
	err = tx.QueryRow(ctx, `SELECT COALESCE((
		SELECT latest_revision FROM newim.im_group_sync_accounts WHERE owner_id=$1
	), 0)`, actor).Scan(&head)
	if err != nil {
		return app.ChangePage{}, app.StorageUnavailable
	}
	if after > head {
		return app.ChangePage{}, app.InvalidInput
	}
	rows, err := tx.Query(ctx, `SELECT revision, group_id, kind, payload FROM newim.im_group_sync_changes
		WHERE owner_id=$1 AND revision>$2 AND revision<=$3 ORDER BY revision LIMIT $4`, actor, after, head, limit+1)
	if err != nil {
		return app.ChangePage{}, app.StorageUnavailable
	}
	defer rows.Close()
	items := make([]app.Change, 0, limit+1)
	next := after
	for rows.Next() {
		change, err := scanReadChange(rows, actor)
		if err != nil || change.Revision != next+1 {
			return app.ChangePage{}, app.StorageUnavailable
		}
		items = append(items, change)
		next = change.Revision
	}
	if rows.Err() != nil || (len(items) <= limit && next != head) || commit(ctx, tx) != nil {
		return app.ChangePage{}, app.StorageUnavailable
	}
	return app.ChangePage{
		Page:           app.Page[app.Change]{Items: items[:min(len(items), limit)], HasMore: len(items) > limit},
		LatestRevision: head,
	}, nil
}

func activeView(ctx context.Context, tx pgx.Tx, actor, groupID string) (app.View, error) {
	row, err := loadGroup(ctx, tx, groupID)
	if err != nil {
		return app.View{}, err
	}
	member, err := activeMember(ctx, tx, row, actor)
	if err != nil {
		return app.View{}, err
	}
	return app.View{Group: row.group, Membership: member}, nil
}

func collectPage[T any](ctx context.Context, tx pgx.Tx, rows pgx.Rows, limit int, scan func(pgx.Row) (T, error)) (app.Page[T], error) {
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (T, error) { return scan(row) })
	if err != nil || commit(ctx, tx) != nil {
		return app.Page[T]{}, app.StorageUnavailable
	}
	return app.Page[T]{Items: items[:min(len(items), limit)], HasMore: len(items) > limit}, nil
}

func scanReadChange(row pgx.Row, actor string) (app.Change, error) {
	var change app.Change
	var body []byte
	if err := row.Scan(&change.Revision, &change.GroupID, &change.Kind, &body); err != nil {
		return app.Change{}, app.StorageUnavailable
	}
	var payload changePayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return app.Change{}, app.StorageUnavailable
	}
	switch change.Kind {
	case app.GroupChanged, app.GroupRemoved:
		member := payload.Membership
		if member == nil || member.UserID != actor || payload.Entry != nil ||
			(member.DepartedThroughSeq != nil) != (change.Kind == app.GroupRemoved) {
			return app.Change{}, app.StorageUnavailable
		}
		change.Membership = member
	case app.RequestChanged:
		entry := payload.Entry
		if entry == nil || entry.GroupID != change.GroupID || payload.Membership != nil ||
			(actor != entry.ActorID && actor != entry.UserID) {
			return app.Change{}, app.StorageUnavailable
		}
		change.Entry = entry
	default:
		return app.Change{}, app.StorageUnavailable
	}
	return change, nil
}

func readPageLimit(limit int) int {
	if limit == 0 {
		return app.DefaultPageSize
	}
	return limit
}

func validReadCursor(id string) bool {
	return id == "" || user.ValidID(id)
}
