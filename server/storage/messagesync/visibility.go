package messagesync

import (
	"context"

	"github.com/jackc/pgx/v5"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/storage/conversationsync"
	"github.com/m-ice/NewIM/server/storage/membership"
	messagestore "github.com/m-ice/NewIM/server/storage/message"
	app "github.com/m-ice/NewIM/server/sync/message"
)

func (r *Repository) SupportsVisibility() bool {
	return r != nil && r.visibility
}

func (r *Repository) messageWindow(ctx context.Context, tx pgx.Tx, user, id string) (int64, int64, error) {
	advanced, err := membership.Advanced(ctx, tx)
	if err != nil {
		return 0, 0, fixed(err)
	}
	head, err := readHead(ctx, tx, user, id, advanced)
	if err != nil {
		return 0, 0, err
	}
	if err = validateHead(ctx, tx, id, head.latestSeq, head.latestID, head.pointerSeq); err != nil {
		return 0, 0, err
	}
	tail := head.latestSeq
	if head.departedThroughSeq != nil {
		if *head.departedThroughSeq > tail {
			return 0, 0, app.Fail(app.StorageUnavailable)
		}
		tail = *head.departedThroughSeq
	}
	floor := head.historyAfterSeq
	if r.accountSettings {
		personalFloor, err := conversationsync.PersonalHistoryFloor(ctx, tx, user, id, tail)
		if err != nil {
			return 0, 0, app.Fail(app.StorageUnavailable)
		}
		floor = max(floor, personalFloor)
	}
	return floor, tail, nil
}

func (r *Repository) ReadVisibility(ctx context.Context, user string, request app.VisibilityRequest) ([]protocol.VisibilityEvent, error) {
	if ctx == nil || !r.SupportsVisibility() {
		return nil, app.Fail(app.StorageUnavailable)
	}
	if !identifier(user) || !identifier(request.ConversationID) {
		return nil, app.Fail(app.Forbidden)
	}
	if request.AfterRevision < 0 || request.ThroughRevision < request.AfterRevision || request.Limit < 1 || request.Limit > app.MaxPageItems {
		return nil, app.Fail(app.InvalidCursor)
	}
	tx, err := r.begin(ctx, r.readTxOptions)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	if err = verifyReadTransaction(ctx, tx); err != nil {
		return nil, err
	}
	floor, tail, err := r.messageWindow(ctx, tx, user, request.ConversationID)
	if err != nil {
		return nil, err
	}
	var horizon int64
	err = tx.QueryRow(ctx, `SELECT GREATEST(
		COALESCE((SELECT visibility_revision FROM newim.im_message_visibility_events
			WHERE conversation_id=$1 AND audience_user_id IS NULL AND target_seq<=$3 ORDER BY visibility_revision DESC LIMIT 1),0),
		COALESCE((SELECT visibility_revision FROM newim.im_message_visibility_events
			WHERE conversation_id=$1 AND audience_user_id=$2 AND target_seq<=$3 ORDER BY visibility_revision DESC LIMIT 1),0)
	)`, request.ConversationID, user, tail).Scan(&horizon)
	if err != nil {
		return nil, fixed(err)
	}
	if request.ThroughRevision > horizon {
		return nil, app.Fail(app.InvalidCursor)
	}
	items, err := readVisibilityRows(ctx, tx, user, request, floor, tail)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fixed(err)
	}
	return items, nil
}

func readVisibilityRows(ctx context.Context, tx pgx.Tx, user string, request app.VisibilityRequest, floor, tail int64) ([]protocol.VisibilityEvent, error) {
	const selection = `SELECT visibility_revision,target_seq,action FROM newim.im_message_visibility_events
		WHERE conversation_id=$1 AND visibility_revision>$3 AND visibility_revision<=$4
		  AND target_seq>$5 AND target_seq<=$6 AND `
	const limit = " ORDER BY visibility_revision LIMIT $7"
	rows, err := tx.Query(ctx, `WITH selected AS (
		SELECT * FROM ((`+selection+`audience_user_id IS NULL`+limit+`) UNION ALL
			(`+selection+`audience_user_id=$2`+limit+`)) audience`+limit+`
	)
	SELECT e.visibility_revision,e.target_seq,e.action,m.server_msg_id
	FROM selected e JOIN newim.im_messages m ON m.conversation_id=$1 AND m.conversation_seq=e.target_seq
	ORDER BY e.visibility_revision`,
		request.ConversationID, user, request.AfterRevision, request.ThroughRevision, floor, tail, request.Limit+1)
	if err != nil {
		return nil, fixed(err)
	}
	defer rows.Close()
	result := make([]protocol.VisibilityEvent, 0, request.Limit+1)
	for rows.Next() {
		item := protocol.VisibilityEvent{ConversationID: request.ConversationID}
		var key string
		if err := rows.Scan(&item.Revision, &item.ConversationSeq, &key, &item.ServerMsgID); err != nil {
			return nil, fixed(err)
		}
		item.Action = messagestore.VisibilityAction(key)
		result = append(result, item)
	}
	return result, fixed(rows.Err())
}
