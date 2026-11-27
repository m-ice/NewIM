package group

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/group"
)

type changePayload struct {
	Membership *app.Membership `json:"membership,omitempty"`
	Entry      *app.Entry      `json:"request,omitempty"`
}

type accountChange struct {
	owner   string
	kind    app.ChangeKind
	payload changePayload
}

func groupChanges(ctx context.Context, tx pgx.Tx, groupID string) ([]accountChange, error) {
	rows, err := tx.Query(ctx, "SELECT "+memberColumns+` FROM newim.im_conversation_members
		WHERE conversation_id=$1 AND group_role IS NOT NULL AND departed_at IS NULL LIMIT $2`,
		groupID, app.MaxMemberLimit+1)
	if err != nil {
		return nil, app.StorageUnavailable
	}
	defer rows.Close()
	var changes []accountChange
	for rows.Next() {
		member, err := scanMember(rows)
		if err != nil {
			return nil, app.StorageUnavailable
		}
		changes = append(changes, memberChange(member, app.GroupChanged))
	}
	if rows.Err() != nil || len(changes) > app.MaxMemberLimit {
		return nil, app.StorageUnavailable
	}
	return changes, nil
}

func memberChange(member app.Membership, kind app.ChangeKind) accountChange {
	return accountChange{owner: member.UserID, kind: kind, payload: changePayload{Membership: &member}}
}

func entryChanges(entry app.Entry) []accountChange {
	change := accountChange{owner: entry.ActorID, kind: app.RequestChanged, payload: changePayload{Entry: &entry}}
	changes := []accountChange{change}
	if entry.UserID != entry.ActorID {
		change.owner = entry.UserID
		changes = append(changes, change)
	}
	return changes
}

// Acquire account heads in the same order even when separate groups overlap.
func recordChanges(ctx context.Context, tx pgx.Tx, groupID string, changes []accountChange) error {
	if len(changes) == 0 {
		return nil
	}
	slices.SortStableFunc(changes, func(a, b accountChange) int {
		return strings.Compare(a.owner, b.owner)
	})
	owners := make([]string, 0, len(changes))
	kinds := make([]string, 0, len(changes))
	bodies := make([]string, 0, len(changes))
	for _, change := range changes {
		body, err := json.Marshal(change.payload)
		if err != nil {
			return app.StorageUnavailable
		}
		owners = append(owners, change.owner)
		kinds = append(kinds, string(change.kind))
		bodies = append(bodies, string(body))
	}
	accounts := slices.Compact(slices.Clone(owners))
	if _, err := tx.Exec(ctx, `INSERT INTO newim.im_group_sync_accounts(owner_id)
		SELECT id FROM unnest($1::text[]) AS ids(id) ORDER BY id COLLATE "C"
		ON CONFLICT (owner_id) DO NOTHING`, accounts); err != nil {
		return app.StorageUnavailable
	}
	rows, err := tx.Query(ctx, `SELECT owner_id FROM newim.im_group_sync_accounts
		WHERE owner_id=ANY($1::text[]) ORDER BY owner_id FOR UPDATE`, accounts)
	if err != nil {
		return app.StorageUnavailable
	}
	locked, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || !slices.Equal(locked, accounts) {
		return app.StorageUnavailable
	}
	// The heads stay locked while each account gets its contiguous revision range.
	tag, err := tx.Exec(ctx, `WITH numbered AS (
		SELECT owner_id,kind,payload,
			row_number() OVER (PARTITION BY owner_id ORDER BY ordinal) AS position
		FROM unnest($1::text[],$2::text[],$3::text[]) WITH ORDINALITY AS pending(owner_id,kind,payload,ordinal)
	), totals AS (
		SELECT owner_id,count(*) AS amount FROM numbered GROUP BY owner_id
	), advanced AS (
		UPDATE newim.im_group_sync_accounts AS a SET latest_revision=a.latest_revision+totals.amount
		FROM totals WHERE a.owner_id=totals.owner_id AND a.latest_revision<=9223372036854775807-totals.amount
		RETURNING a.owner_id,a.latest_revision,totals.amount
	)
	INSERT INTO newim.im_group_sync_changes(owner_id,revision,group_id,kind,payload)
	SELECT n.owner_id,a.latest_revision-a.amount+n.position,$4,n.kind,n.payload::jsonb
	FROM numbered n JOIN advanced a ON a.owner_id=n.owner_id`, owners, kinds, bodies, groupID)
	if err != nil || tag.RowsAffected() != int64(len(changes)) {
		return app.StorageUnavailable
	}
	return nil
}
