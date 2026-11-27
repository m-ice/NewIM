package message

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	app "github.com/m-ice/NewIM/server/message"
	"github.com/m-ice/NewIM/server/storage/conversationsync"
	"github.com/m-ice/NewIM/server/storage/postgrespair"
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
	return privateStorageError(postgrespair.Lock(ctx, tx, p.low, p.high))
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

func (r *Repository) checkSendPolicy(ctx context.Context, tx pgx.Tx, senderID string, request protocol.Send, advanced bool) (app.PersistedMessage, bool, error) {
	if advanced {
		managed, muted, err := groupSendPermission(ctx, tx, senderID, request.ConversationID)
		if err != nil {
			return app.PersistedMessage{}, false, err
		}
		if managed {
			if r.directOnly {
				return app.PersistedMessage{}, false, app.Fail(app.SendUnauthorized)
			}
			receipt, found, err := loadReceipt(ctx, tx, senderID, request)
			if found || err != nil {
				return receipt, found, err
			}
			if muted {
				return app.PersistedMessage{}, false, app.Fail(app.SendUnauthorized)
			}
			return app.PersistedMessage{}, false, nil
		}
	}

	strictDirect := r.directOnly || r.managedGroups
	if !strictDirect {
		receipt, found, err := loadReceipt(ctx, tx, senderID, request)
		if found || err != nil {
			return receipt, found, err
		}
	}
	if r.privatePolicy {
		return r.checkPrivateSend(ctx, tx, senderID, request, strictDirect)
	}
	return app.PersistedMessage{}, false, nil
}

// groupSendPermission runs with the conversation and sender member rows locked.
// Mute restricts new sends, independently of eligibility to replay a receipt.
func groupSendPermission(ctx context.Context, tx pgx.Tx, senderID, conversationID string) (bool, bool, error) {
	var role *string
	var managed, classified, departed, dissolved, groupMuted, memberMuted bool
	err := tx.QueryRow(ctx, `SELECT g.group_id IS NOT NULL, COALESCE(c.kind = 'group', false),
		m.group_role, m.departed_at IS NOT NULL, g.dissolved_at IS NOT NULL,
		COALESCE(g.muted, false), COALESCE(m.mute_until > clock_timestamp(), false)
	FROM newim.im_conversation_members m
	JOIN newim.im_conversations c ON c.conversation_id = m.conversation_id
	LEFT JOIN newim.im_groups g ON g.group_id = c.conversation_id
	WHERE m.conversation_id = $1 AND m.user_id = $2`, conversationID, senderID).
		Scan(&managed, &classified, &role, &departed, &dissolved, &groupMuted, &memberMuted)
	if err != nil {
		return false, false, privateStorageError(err)
	}
	if !managed && role == nil && !departed {
		return false, false, nil
	}
	if !managed || !classified || role == nil || departed || dissolved {
		return false, false, app.Fail(app.SendUnauthorized)
	}
	switch *role {
	case "owner":
		return true, false, nil
	case "admin":
		return true, memberMuted, nil
	case "member":
		return true, groupMuted || memberMuted, nil
	default:
		return false, false, app.Fail(app.SendUnauthorized)
	}
}

func (r *Repository) checkPrivateSend(ctx context.Context, tx pgx.Tx, senderID string, request protocol.Send, strictDirect bool) (app.PersistedMessage, bool, error) {
	pair, err := directPair(ctx, tx, request.ConversationID)
	if err != nil {
		return app.PersistedMessage{}, false, err
	}
	if pair == (userPair{}) {
		if strictDirect {
			return app.PersistedMessage{}, false, app.Fail(app.SendUnauthorized)
		}
		return app.PersistedMessage{}, false, nil
	}
	peerID := pair.peer(senderID)
	if peerID == "" {
		return app.PersistedMessage{}, false, app.Fail(app.SendStorageUnavailable)
	}
	if err = pair.lock(ctx, tx); err != nil {
		return app.PersistedMessage{}, false, err
	}
	if !strictDirect {
		receipt, found, err := loadReceipt(ctx, tx, senderID, request)
		if found || err != nil {
			return receipt, found, err
		}
	}
	if err = pair.verifyMembers(ctx, tx, request.ConversationID); err != nil {
		return app.PersistedMessage{}, false, err
	}
	if strictDirect {
		// Verify the stored direct pair before replaying even an existing receipt.
		receipt, found, err := loadReceipt(ctx, tx, senderID, request)
		if found || err != nil {
			return receipt, found, err
		}
	}
	if err = checkRecipientBlock(ctx, tx, senderID, peerID); err != nil || !r.strangerPolicy {
		return app.PersistedMessage{}, false, err
	}
	return app.PersistedMessage{}, false, checkStrangerSend(ctx, tx, request.ConversationID, senderID, peerID)
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
	if r.projectConversations {
		if err = conversationsync.LockSourceMembership(ctx, tx); err != nil {
			return "", privateStorageError(err)
		}
	}
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
		if err == nil && r.projectConversations {
			err = privateStorageError(conversationsync.RecordSourceConversation(ctx, tx, conversationID, nil))
		}
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
