package friend

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/friend"
	"github.com/m-ice/NewIM/server/user"
)

// List reads one page of edges owned by the authenticated account.
// List 分页读取当前已核验账号的好友记录。
func (r *Repository) List(ctx context.Context, actor string, request app.ListRequest) (app.EdgePage, error) {
	limit := pageLimit(request.Limit)
	if ctx == nil || !user.ValidID(actor) || limit < 1 || limit > app.MaxLimit || !validCursorID(request.AfterPeerID) {
		return app.EdgePage{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, head, err := r.readHead(ctx, actor)
	if err != nil {
		return app.EdgePage{}, err
	}
	defer rollback(tx)
	rows, err := tx.Query(ctx, edgeQuery+` AND e.peer_id>$2::text
		AND (NOT $3::boolean OR NOT EXISTS (
			SELECT 1 FROM newim.im_blocks WHERE user_id=e.owner_id AND blocked_user_id=e.peer_id
		))
		ORDER BY e.peer_id LIMIT $4`, actor, request.AfterPeerID, request.ExcludeBlocked, limit+1)
	if err != nil {
		return app.EdgePage{}, app.StorageUnavailable
	}
	defer rows.Close()
	items := make([]app.Edge, 0, limit+1)
	for rows.Next() {
		edge, err := scanEdge(rows)
		if err != nil {
			return app.EdgePage{}, app.StorageUnavailable
		}
		items = append(items, edge)
	}
	if rows.Err() != nil || tx.Commit(ctx) != nil {
		return app.EdgePage{}, app.StorageUnavailable
	}
	return app.EdgePage{Items: items[:min(len(items), limit)], HasMore: len(items) > limit, LatestRevision: head}, nil
}

// ListApplications reads received or sent applications for the caller.
// ListApplications 分页读取当前账号收到或发出的申请。
func (r *Repository) ListApplications(ctx context.Context, actor string, request app.ApplicationListRequest) (app.ApplicationPage, error) {
	limit := pageLimit(request.Limit)
	if ctx == nil || !user.ValidID(actor) || limit < 1 || limit > app.MaxLimit ||
		!validCursorID(request.AfterApplicantID) || !validCursorID(request.AfterRequestID) ||
		(request.AfterApplicantID == "") != (request.AfterRequestID == "") {
		return app.ApplicationPage{}, app.InvalidInput
	}
	ownerColumn := "applicant_id"
	switch request.Direction {
	case "incoming":
		ownerColumn = "recipient_id"
	case "outgoing":
	default:
		return app.ApplicationPage{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, head, err := r.readHead(ctx, actor)
	if err != nil {
		return app.ApplicationPage{}, err
	}
	defer rollback(tx)
	rows, err := tx.Query(ctx, "SELECT "+applicationFields+" FROM newim.im_friend_applications WHERE "+ownerColumn+
		`=$1 AND (applicant_id,request_id)>($2::text,$3::text) ORDER BY applicant_id,request_id LIMIT $4`,
		actor, request.AfterApplicantID, request.AfterRequestID, limit+1)
	if err != nil {
		return app.ApplicationPage{}, app.StorageUnavailable
	}
	defer rows.Close()
	items := make([]app.Application, 0, limit+1)
	for rows.Next() {
		application, err := scanApplication(rows)
		if err != nil {
			return app.ApplicationPage{}, app.StorageUnavailable
		}
		items = append(items, application)
	}
	if rows.Err() != nil || tx.Commit(ctx) != nil {
		return app.ApplicationPage{}, app.StorageUnavailable
	}
	return app.ApplicationPage{Items: items[:min(len(items), limit)], HasMore: len(items) > limit, LatestRevision: head}, nil
}

// Changes reads immutable, committed revisions owned by the caller.
// Changes 按修订顺序读取当前账号已提交的不可变变更。
func (r *Repository) Changes(ctx context.Context, actor string, after int64, limit int) (app.ChangePage, error) {
	limit = pageLimit(limit)
	if ctx == nil || !user.ValidID(actor) || after < 0 || limit < 1 || limit > app.MaxLimit {
		return app.ChangePage{}, app.InvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, head, err := r.readHead(ctx, actor)
	if err != nil {
		return app.ChangePage{}, err
	}
	defer rollback(tx)
	if after > head {
		return app.ChangePage{}, app.InvalidInput
	}
	rows, err := tx.Query(ctx, `SELECT revision,kind,peer_id,payload FROM newim.im_friend_sync_changes
		WHERE owner_id=$1 AND revision>$2 AND revision<=$3 ORDER BY revision LIMIT $4`, actor, after, head, limit+1)
	if err != nil {
		return app.ChangePage{}, app.StorageUnavailable
	}
	defer rows.Close()
	items := make([]app.Change, 0, limit+1)
	next := after
	for rows.Next() {
		change, err := scanChange(rows)
		if err != nil || change.Revision != next+1 {
			return app.ChangePage{}, app.StorageUnavailable
		}
		items = append(items, change)
		next = change.Revision
	}
	if rows.Err() != nil || (len(items) <= limit && next != head) || tx.Commit(ctx) != nil {
		return app.ChangePage{}, app.StorageUnavailable
	}
	return app.ChangePage{Items: items[:min(len(items), limit)], HasMore: len(items) > limit, LatestRevision: head}, nil
}

func (r *Repository) readHead(ctx context.Context, actor string) (pgx.Tx, int64, error) {
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, 0, err
	}
	var head int64
	if err = tx.QueryRow(ctx, `SELECT COALESCE((
		SELECT latest_revision FROM newim.im_friend_sync_accounts WHERE owner_id=$1
	), 0)`, actor).Scan(&head); err != nil {
		rollback(tx)
		return nil, 0, app.StorageUnavailable
	}
	return tx, head, nil
}

func scanChange(row pgx.Row) (app.Change, error) {
	var change app.Change
	var body []byte
	if err := row.Scan(&change.Revision, &change.Kind, &change.PeerID, &body); err != nil {
		return app.Change{}, app.StorageUnavailable
	}
	switch change.Kind {
	case app.ApplicationChanged:
		var application app.Application
		if err := json.Unmarshal(body, &application); err != nil {
			return app.Change{}, app.StorageUnavailable
		}
		change.Application = &application
	case app.FriendUpserted:
		var edge app.Edge
		if err := json.Unmarshal(body, &edge); err != nil {
			return app.Change{}, app.StorageUnavailable
		}
		change.Edge = &edge
	case app.FriendRemoved:
	default:
		return app.Change{}, app.StorageUnavailable
	}
	return change, nil
}

func pageLimit(limit int) int {
	if limit == 0 {
		return app.DefaultLimit
	}
	return limit
}

func validCursorID(value string) bool {
	return value == "" || user.ValidID(value)
}
