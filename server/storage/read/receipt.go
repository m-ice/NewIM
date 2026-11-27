package read

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/m-ice/NewIM/server/auth/session"
	app "github.com/m-ice/NewIM/server/read"
)

func (r *Repository) GetReceipt(ctx context.Context, bearer session.BearerSession, conversationID, serverMsgID string) (app.Receipt, error) {
	if !app.ValidConversationID(conversationID) || !app.ValidMessageID(serverMsgID) {
		return app.Receipt{}, app.Fail(app.InvalidInput)
	}
	return withVerifiedBearer(ctx, r, bearer, func(ctx context.Context, tx pgx.Tx) (app.Receipt, error) {
		return readReceipt(ctx, tx, bearer.UserID(), conversationID, serverMsgID)
	})
}

type receiptMember struct {
	userID       string
	historyAfter int64
}

func readReceipt(ctx context.Context, tx pgx.Tx, senderID, conversationID, serverMsgID string) (app.Receipt, error) {
	var latest int64
	var low, high string
	err := tx.QueryRow(ctx, `SELECT c.last_seq,c.direct_user_low,c.direct_user_high
		FROM newim.im_conversations AS c
		WHERE c.conversation_id=$1 AND c.kind='direct'
		  AND c.direct_user_low<c.direct_user_high AND $2 IN (c.direct_user_low,c.direct_user_high)
		  AND EXISTS (SELECT 1 FROM newim.im_conversation_members AS m
			WHERE m.conversation_id=c.conversation_id AND m.user_id=$2 AND m.departed_at IS NULL)
		FOR SHARE OF c`, conversationID, senderID).Scan(&latest, &low, &high)
	if err != nil {
		return app.Receipt{}, visibilityError(err)
	}
	members, err := lockDirectMembers(ctx, tx, conversationID)
	if err != nil {
		return app.Receipt{}, err
	}
	if len(members) != 2 || members[0].userID != low || members[1].userID != high {
		return app.Receipt{}, app.Fail(app.NotFound)
	}
	sender, peer := members[0], members[1]
	if sender.userID != senderID {
		sender, peer = peer, sender
	}
	var messageSeq int64
	err = tx.QueryRow(ctx, `SELECT conversation_seq FROM newim.im_messages
		WHERE conversation_id=$1 AND server_msg_id=$2 AND sender_id=$3`, conversationID, serverMsgID, senderID).
		Scan(&messageSeq)
	if err != nil {
		return app.Receipt{}, visibilityError(err)
	}
	if messageSeq < 0 || messageSeq > latest || sender.historyAfter < 0 || sender.historyAfter > latest {
		return app.Receipt{}, app.Fail(app.StorageUnavailable)
	}
	if messageSeq <= sender.historyAfter {
		return app.Receipt{}, app.Fail(app.NotFound)
	}
	if peer.historyAfter < 0 || peer.historyAfter > latest {
		return app.Receipt{}, app.Fail(app.StorageUnavailable)
	}

	var peerRead int64
	err = tx.QueryRow(ctx, `SELECT read_seq FROM newim.im_read_states
		WHERE conversation_id=$1 AND user_id=$2`, conversationID, peer.userID).Scan(&peerRead)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return app.Receipt{}, app.Fail(app.StorageUnavailable)
	}
	if peerRead < 0 || peerRead > latest {
		return app.Receipt{}, app.Fail(app.StorageUnavailable)
	}
	return app.Receipt{
		ServerMsgID: serverMsgID,
		Read:        messageSeq > peer.historyAfter && messageSeq <= peerRead,
	}, nil
}

func lockDirectMembers(ctx context.Context, tx pgx.Tx, conversationID string) ([]receiptMember, error) {
	// Recheck the active membership after acquiring the conversation lock.
	rows, err := tx.Query(ctx, `SELECT user_id,history_after_seq FROM newim.im_conversation_members
		WHERE conversation_id=$1 AND departed_at IS NULL
		ORDER BY user_id LIMIT 3 FOR SHARE`, conversationID)
	if err != nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	members, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (receiptMember, error) {
		var member receiptMember
		err := row.Scan(&member.userID, &member.historyAfter)
		return member, err
	})
	if err != nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	return members, nil
}
