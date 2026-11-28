package message

import (
	"context"
	"errors"
	"math"

	"github.com/jackc/pgx/v5"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/auth/session"
	app "github.com/m-ice/NewIM/server/message"
	"github.com/m-ice/NewIM/server/storage/authsession"
	"github.com/m-ice/NewIM/server/storage/conversationsync"
)

func (r *Repository) SupportsVisibility() bool {
	return r != nil && r.visibility && r.projectConversations && r.projectReadState
}

// ChangeMessageVisibility commits the event and its account projections in the
// same transaction. The bearer remains locked until the result is committed.
func (r *Repository) ChangeMessageVisibility(ctx context.Context, bearer session.BearerSession, request protocol.VisibilityRequest) (protocol.VisibilityEvent, error) {
	if !r.SupportsVisibility() || ctx == nil {
		return protocol.VisibilityEvent{}, app.Fail(app.SendStorageUnavailable)
	}
	if !request.Valid() {
		return protocol.VisibilityEvent{}, app.Fail(app.SendInvalidInput)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return protocol.VisibilityEvent{}, err
	}
	defer rollback(tx)
	expiry, err := authsession.LockVerifiedBearer(ctx, tx, bearer)
	if err != nil {
		return protocol.VisibilityEvent{}, err
	}
	event, changeErr := r.changeVisibility(ctx, tx, bearer.UserID(), request)
	if err = authsession.CheckLockedBearerExpiry(ctx, tx, expiry); err != nil {
		return protocol.VisibilityEvent{}, err
	}
	if changeErr != nil {
		return protocol.VisibilityEvent{}, changeErr
	}
	if err = tx.Commit(ctx); err != nil {
		return protocol.VisibilityEvent{}, mapStorageError(err)
	}
	return event, nil
}

func (r *Repository) changeVisibility(ctx context.Context, tx pgx.Tx, actor string, request protocol.VisibilityRequest) (protocol.VisibilityEvent, error) {
	var event protocol.VisibilityEvent
	if err := conversationsync.LockSourceMembership(ctx, tx); err != nil {
		return event, privateStorageError(err)
	}
	if err := r.authorizer.Authorize(ctx, conversationTx{tx: tx}, actor, request.ConversationID); err != nil {
		return event, err
	}
	target, err := authorizedVisibilityTarget(ctx, tx, actor, request)
	if err != nil {
		return event, err
	}
	action := visibilityActionKey(request.Action)
	event = protocol.VisibilityEvent{
		ConversationID: request.ConversationID, ServerMsgID: request.ServerMsgID,
		ConversationSeq: target, Action: request.Action,
	}
	if found, err := matchVisibilityRequest(ctx, tx, actor, request.RequestID, action, &event); err != nil || found {
		return event, err
	}
	var hidden bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM newim.im_message_visibility_events
		WHERE conversation_id=$1 AND target_seq=$2 AND (audience_user_id IS NULL OR audience_user_id=$3))`,
		request.ConversationID, target, actor).Scan(&hidden)
	if err != nil {
		return event, mapStorageError(err)
	}
	if hidden {
		return event, app.Fail(app.SendIDConflict)
	}
	var revision int64
	err = tx.QueryRow(ctx, `SELECT COALESCE((
		SELECT visibility_revision FROM newim.im_message_visibility_events
		WHERE conversation_id=$1 ORDER BY visibility_revision DESC LIMIT 1
	),0)`, request.ConversationID).Scan(&revision)
	if err != nil {
		return event, mapStorageError(err)
	}
	if revision == math.MaxInt64 {
		return event, app.Fail(app.SendSequenceExhausted)
	}
	var audience *string
	if request.Action == protocol.DeleteForMe {
		audience = &actor
	}
	err = tx.QueryRow(ctx, `INSERT INTO newim.im_message_visibility_events
		(conversation_id,visibility_revision,target_seq,actor_user_id,request_id,action,audience_user_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING
		RETURNING visibility_revision`, request.ConversationID, revision+1, target, actor, request.RequestID, action, audience).Scan(&event.Revision)
	if errors.Is(err, pgx.ErrNoRows) {
		if found, err := matchVisibilityRequest(ctx, tx, actor, request.RequestID, action, &event); err != nil || found {
			return event, err
		}
		return event, app.Fail(app.SendIDConflict)
	}
	if err != nil {
		return event, mapStorageError(err)
	}
	if audience != nil {
		err = conversationsync.RecordSourceWatermark(ctx, tx, actor, request.ConversationID, r.accountSettings)
	} else {
		err = conversationsync.RecordSourceConversation(ctx, tx, request.ConversationID, nil, true, r.accountSettings)
	}
	return event, privateStorageError(err)
}

func authorizedVisibilityTarget(ctx context.Context, tx pgx.Tx, actor string, request protocol.VisibilityRequest) (int64, error) {
	var target int64
	var sender string
	var groupManager bool
	err := tx.QueryRow(ctx, `SELECT message.conversation_seq, message.sender_id,
		COALESCE(c.kind='group' AND m.group_role IN ('owner','admin'),false)
	FROM newim.im_messages message
	JOIN newim.im_conversations c USING (conversation_id)
	JOIN newim.im_conversation_members m ON m.conversation_id=c.conversation_id AND m.user_id=$3
	LEFT JOIN newim.im_conversation_account_settings s ON s.conversation_id=c.conversation_id AND s.user_id=$3
	LEFT JOIN newim.im_groups g ON g.group_id=c.conversation_id
	WHERE message.conversation_id=$1 AND message.server_msg_id=$2 AND m.departed_at IS NULL
	  AND s.deleted_through_seq IS NULL
	  AND message.conversation_seq>GREATEST(m.history_after_seq,COALESCE(s.clear_through_seq,0))
	  AND (c.kind='direct' OR (c.kind='group' AND g.group_id IS NOT NULL AND g.dissolved_at IS NULL))`,
		request.ConversationID, request.ServerMsgID, actor).Scan(&target, &sender, &groupManager)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, app.Fail(app.SendUnauthorized)
	}
	if err != nil {
		return 0, mapStorageError(err)
	}
	if request.Action == protocol.Recall && sender != actor || request.Action == protocol.AdminRemove && !groupManager {
		return 0, app.Fail(app.SendUnauthorized)
	}
	return target, nil
}

func matchVisibilityRequest(ctx context.Context, tx pgx.Tx, actor, requestID, action string, event *protocol.VisibilityEvent) (bool, error) {
	var conversation, previousAction string
	var target, revision int64
	err := tx.QueryRow(ctx, `SELECT conversation_id,target_seq,action,visibility_revision
		FROM newim.im_message_visibility_events WHERE actor_user_id=$1 AND request_id=$2`,
		actor, requestID).Scan(&conversation, &target, &previousAction, &revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, mapStorageError(err)
	}
	if conversation != event.ConversationID || target != event.ConversationSeq || previousAction != action {
		return false, app.Fail(app.SendIDConflict)
	}
	event.Revision = revision
	return true, nil
}

func visibilityActionKey(action protocol.VisibilityAction) string {
	switch action {
	case protocol.DeleteForMe:
		return "delete_for_me"
	case protocol.AdminRemove:
		return "admin_remove"
	default:
		return "recall"
	}
}
