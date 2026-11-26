package message

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	app "github.com/m-ice/NewIM/server/message"
)

type userPair struct {
	low, high string
}

func newUserPair(first, second string) (userPair, bool) {
	if first == second || !identifier(first) || !identifier(second) {
		return userPair{}, false
	}
	if first > second {
		first, second = second, first
	}
	return userPair{low: first, high: second}, true
}

func (p userPair) peer(userID string) string {
	switch userID {
	case p.low:
		return p.high
	case p.high:
		return p.low
	default:
		return ""
	}
}

func (p userPair) lock(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `SELECT pg_catalog.pg_advisory_xact_lock(
		pg_catalog.hashtextextended('newim.private-message:' || $1::text || ':' || $2::text, 0))`, p.low, p.high)
	return privateStorageError(err)
}

func (p userPair) verifyUsers(ctx context.Context, tx pgx.Tx) error {
	var count int
	err := tx.QueryRow(ctx, `SELECT count(*) FROM (
		SELECT user_id FROM newim.im_users WHERE user_id IN ($1, $2) ORDER BY user_id FOR KEY SHARE
	) AS locked_users`, p.low, p.high).Scan(&count)
	if err != nil {
		return privateStorageError(err)
	}
	if count != 2 {
		return app.Fail(app.SendInvalidInput)
	}
	return nil
}

func (p userPair) verifyMembers(ctx context.Context, tx pgx.Tx, conversationID string) error {
	rows, err := tx.Query(ctx, `SELECT user_id FROM newim.im_conversation_members
		WHERE conversation_id = $1 ORDER BY user_id LIMIT 3 FOR SHARE`, conversationID)
	if err != nil {
		return privateStorageError(err)
	}
	members, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return privateStorageError(err)
	}
	if len(members) != 2 || members[0] != p.low || members[1] != p.high {
		return app.Fail(app.SendStorageUnavailable)
	}
	return nil
}

func verifyConversationClassification(ctx context.Context, pool *pgxpool.Pool) error {
	var invalid bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM newim.im_conversations AS c
		WHERE c.kind IS NULL OR (c.kind = 'direct' AND ARRAY(
			SELECT m.user_id::text FROM newim.im_conversation_members AS m
			WHERE m.conversation_id = c.conversation_id ORDER BY m.user_id LIMIT 3
		) IS DISTINCT FROM ARRAY[c.direct_user_low::text, c.direct_user_high::text])
	)`).Scan(&invalid)
	if err != nil || invalid {
		return app.Fail(app.SendStorageUnavailable)
	}
	return nil
}

func directPair(ctx context.Context, tx pgx.Tx, conversationID string) (userPair, error) {
	var kind, low, high *string
	err := tx.QueryRow(ctx, `SELECT kind, direct_user_low, direct_user_high
		FROM newim.im_conversations WHERE conversation_id = $1`, conversationID).Scan(&kind, &low, &high)
	if err != nil {
		return userPair{}, privateStorageError(err)
	}
	if kind != nil && *kind == "group" && low == nil && high == nil {
		return userPair{}, nil
	}
	if kind != nil && *kind == "direct" && low != nil && high != nil {
		pair, ok := newUserPair(*low, *high)
		if ok && pair.low == *low {
			return pair, nil
		}
	}
	return userPair{}, app.Fail(app.SendStorageUnavailable)
}

func checkPrivateSend(ctx context.Context, tx pgx.Tx, senderID string, request protocol.Send) (app.PersistedMessage, bool, error) {
	pair, err := directPair(ctx, tx, request.ConversationID)
	if err != nil || pair == (userPair{}) {
		return app.PersistedMessage{}, false, err
	}
	peerID := pair.peer(senderID)
	if peerID == "" {
		return app.PersistedMessage{}, false, app.Fail(app.SendStorageUnavailable)
	}
	if err = pair.lock(ctx, tx); err != nil {
		return app.PersistedMessage{}, false, err
	}
	receipt, found, err := loadReceipt(ctx, tx, senderID, request)
	if found || err != nil {
		return receipt, found, err
	}
	if err = pair.verifyMembers(ctx, tx, request.ConversationID); err != nil {
		return app.PersistedMessage{}, false, err
	}
	return app.PersistedMessage{}, false, checkRecipientBlock(ctx, tx, senderID, peerID)
}

func checkRecipientBlock(ctx context.Context, tx pgx.Tx, senderID, recipientID string) error {
	var blocked bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM newim.im_blocks WHERE user_id = $1 AND blocked_user_id = $2
	)`, recipientID, senderID).Scan(&blocked)
	if err != nil {
		return privateStorageError(err)
	}
	if blocked {
		return app.Fail(app.BlockedByRecipient)
	}
	return nil
}

// CreateDirect returns a verified existing private conversation or creates one
// with both members in a single transaction.
func (r *Repository) CreateDirect(ctx context.Context, ownerID, peerID string) (string, error) {
	pair, ok := newUserPair(ownerID, peerID)
	if !ok || ctx == nil {
		return "", app.Fail(app.SendInvalidInput)
	}
	if r == nil || !r.privatePolicy {
		return "", app.Fail(app.SendStorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", privateStorageError(err)
	}
	defer rollback(tx)
	if err = pair.lock(ctx, tx); err != nil {
		return "", err
	}
	if err = pair.verifyUsers(ctx, tx); err != nil {
		return "", err
	}

	var conversationID string
	err = tx.QueryRow(ctx, `SELECT conversation_id FROM newim.im_conversations
		WHERE kind = 'direct' AND direct_user_low = $1 AND direct_user_high = $2`, pair.low, pair.high).Scan(&conversationID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		conversationID, err = createDirectConversation(ctx, tx, pair, ownerID, peerID)
	case err != nil:
		return "", privateStorageError(err)
	default:
		// The existing send path locks the conversation row before the pair.
		// Returning an existing private conversation only locks the member rows.
		err = pair.verifyMembers(ctx, tx, conversationID)
	}
	if err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", app.Fail(app.SendStorageUnavailable)
	}
	return conversationID, nil
}

func createDirectConversation(ctx context.Context, tx pgx.Tx, pair userPair, ownerID, peerID string) (string, error) {
	if err := checkRecipientBlock(ctx, tx, ownerID, peerID); err != nil {
		return "", err
	}
	conversationID, err := app.NewCSPRNGIDGenerator().NewID()
	if err != nil {
		return "", err
	}
	_, err = tx.Exec(ctx, `INSERT INTO newim.im_conversations (conversation_id, kind, direct_user_low, direct_user_high)
		VALUES ($1, 'direct', $2, $3)`, conversationID, pair.low, pair.high)
	if err != nil {
		return "", privateStorageError(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO newim.im_conversation_members (conversation_id, user_id)
		VALUES ($1, $2), ($1, $3)`, conversationID, pair.low, pair.high)
	if err != nil {
		return "", privateStorageError(err)
	}
	return conversationID, nil
}

// SetBlocked changes one direction of the relationship for a trusted account.
func (r *Repository) SetBlocked(ctx context.Context, ownerID, peerID string, blocked bool) error {
	pair, ok := newUserPair(ownerID, peerID)
	if !ok || ctx == nil {
		return app.Fail(app.SendInvalidInput)
	}
	if r == nil || !r.privatePolicy {
		return app.Fail(app.SendStorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return privateStorageError(err)
	}
	defer rollback(tx)
	if err = pair.lock(ctx, tx); err != nil {
		return err
	}
	if err = pair.verifyUsers(ctx, tx); err != nil {
		return err
	}
	if blocked {
		_, err = tx.Exec(ctx, `INSERT INTO newim.im_blocks (user_id, blocked_user_id)
			VALUES ($1, $2) ON CONFLICT (user_id, blocked_user_id) DO NOTHING`, ownerID, peerID)
	} else {
		_, err = tx.Exec(ctx, `DELETE FROM newim.im_blocks WHERE user_id = $1 AND blocked_user_id = $2`, ownerID, peerID)
	}
	if err != nil {
		return privateStorageError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return app.Fail(app.SendStorageUnavailable)
	}
	return nil
}

func privateStorageError(err error) error {
	mapped := mapStorageError(err)
	if mapped == nil || app.ErrorCode(mapped) == app.SendLockUnavailable {
		return mapped
	}
	return app.Fail(app.SendStorageUnavailable)
}
