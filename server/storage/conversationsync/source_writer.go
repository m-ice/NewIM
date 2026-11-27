package conversationsync

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

// CheckSourceSchema checks the source projection columns through the caller's pool.
func CheckSourceSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if ctx == nil || pool == nil {
		return app.Fail(app.StorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	_, err := pool.Exec(ctx, `SELECT a.source_ready, a.source_after_key, m.history_after_seq, m.departed_at
		FROM newim.im_conversation_sync_accounts a
		LEFT JOIN newim.im_conversation_members m ON false LIMIT 0`)
	return fixed(err)
}

// LockSourceMembership is called before taking any conversation lock in a source transaction.
func LockSourceMembership(ctx context.Context, tx pgx.Tx) error {
	if ctx == nil || tx == nil {
		return app.Fail(app.StorageUnavailable)
	}
	_, err := tx.Exec(ctx, "LOCK TABLE newim.im_conversation_members IN ROW EXCLUSIVE MODE")
	return fixed(err)
}

// InitializeSourceAccount adds an empty, ready projection for a user inserted in this transaction.
func InitializeSourceAccount(ctx context.Context, tx pgx.Tx, userID string) error {
	if ctx == nil || tx == nil || !identifier(userID) {
		return app.Fail(app.StorageUnavailable)
	}
	_, err := tx.Exec(ctx, "INSERT INTO newim.im_conversation_sync_accounts(user_id, source_ready) VALUES ($1, true)", userID)
	return fixed(err)
}

const sourceConversationTargets = `
		SELECT user_id, conversation_id FROM newim.im_conversation_members
		WHERE conversation_id=$1 AND departed_at IS NULL
		UNION
		SELECT removed.user_id::newim.identifier, $1::newim.identifier
		FROM unnest($2::text[]) AS removed(user_id)`

const sourceAccountPageTargets = `
		SELECT DISTINCT $1::newim.identifier AS user_id, page.conversation_id::newim.identifier AS conversation_id
		FROM unnest($2::text[]) AS page(conversation_id)`

// RecordSourceConversation runs after the source mutation with the conversation
// locked. A failed write must abort the source transaction.
func RecordSourceConversation(ctx context.Context, tx pgx.Tx, conversationID string, removed []string) error {
	if ctx == nil || tx == nil || !identifier(conversationID) {
		return app.Fail(app.StorageUnavailable)
	}
	for _, userID := range removed {
		if !identifier(userID) {
			return app.Fail(app.StorageUnavailable)
		}
	}

	// The conflict clause locks existing rows without issuing updates. Inserting
	// new rows and locking existing rows in one sorted pass gives both the same order.
	_, err := tx.Exec(ctx, `INSERT INTO newim.im_conversation_sync_accounts AS account(user_id)
		SELECT audience.user_id FROM (`+sourceConversationTargets+`) audience
		WHERE EXISTS (
			SELECT 1 FROM newim.im_conversation_sync_accounts a WHERE a.user_id=audience.user_id
		) OR EXISTS (
			SELECT 1 FROM newim.im_conversation_members m
			WHERE m.conversation_id=audience.conversation_id AND m.user_id=audience.user_id AND m.departed_at IS NULL
		)
		ORDER BY audience.user_id
		ON CONFLICT (user_id) DO UPDATE SET source_ready=account.source_ready WHERE false`, conversationID, removed)
	if err != nil {
		return fixed(err)
	}
	return appendSourceProjection(ctx, tx, sourceConversationTargets, conversationID, removed)
}

// appendSourceProjection receives fixed package SQL yielding unique identifiers
// for (user_id, conversation_id). Its caller holds the conversation and account locks.
func appendSourceProjection(ctx context.Context, tx pgx.Tx, targetSQL string, args ...any) error {
	var valid bool
	err := tx.QueryRow(ctx, `WITH audience AS (`+targetSQL+`),
		head AS MATERIALIZED (
			SELECT a.user_id, a.conversation_id, m.user_id IS NOT NULL AS active,
				CASE WHEN c.conversation_id IS NULL THEN false
					WHEN m.user_id IS NULL THEN true
					WHEN c.last_seq=0 THEN m.history_after_seq=0 AND c.latest_server_msg_id IS NULL
					ELSE m.history_after_seq<=c.last_seq AND msg.conversation_seq IS NOT DISTINCT FROM c.last_seq
				END AS valid,
				CASE WHEN m.user_id IS NOT NULL THEN
					CASE WHEN c.last_seq>m.history_after_seq THEN c.last_seq ELSE 0 END
				END AS latest_seq,
				CASE WHEN c.last_seq>m.history_after_seq THEN c.latest_server_msg_id END AS latest_server_msg_id
			FROM audience a
			LEFT JOIN newim.im_conversations c ON c.conversation_id=a.conversation_id
			LEFT JOIN newim.im_conversation_members m ON m.conversation_id=a.conversation_id
				AND m.user_id=a.user_id AND m.departed_at IS NULL
			LEFT JOIN newim.im_messages msg ON msg.conversation_id=c.conversation_id
				AND msg.server_msg_id=c.latest_server_msg_id
		),
		healthy AS (
			SELECT NOT EXISTS (SELECT 1 FROM head WHERE NOT valid) AS valid
		),
		pending AS (
			SELECT h.user_id, h.conversation_id, h.latest_seq, h.latest_server_msg_id,
				CASE WHEN h.active THEN 'upsert' ELSE 'remove' END AS kind
			FROM head h
			LEFT JOIN LATERAL (
				SELECT kind, latest_seq, latest_server_msg_id FROM newim.im_conversation_sync_changes
				WHERE user_id=h.user_id AND conversation_id=h.conversation_id
				ORDER BY change_seq DESC LIMIT 1
			) previous ON true
			WHERE (SELECT valid FROM healthy)
				AND ((h.active AND (previous.kind, previous.latest_seq, previous.latest_server_msg_id)
					IS DISTINCT FROM ('upsert', h.latest_seq, h.latest_server_msg_id))
					OR (NOT h.active AND previous.kind='upsert'))
		),
		allocated AS MATERIALIZED (
			SELECT p.*, a.last_change_seq + row_number() OVER (
				PARTITION BY p.user_id ORDER BY p.conversation_id
			) AS change_seq
			FROM pending p
			JOIN newim.im_conversation_sync_accounts a USING (user_id)
		),
		advanced AS (
			UPDATE newim.im_conversation_sync_accounts account
			SET last_change_seq=next.change_seq
			FROM (SELECT user_id, max(change_seq) AS change_seq FROM allocated GROUP BY user_id) next
			WHERE account.user_id=next.user_id
			RETURNING account.user_id
		),
		keyed AS (
			INSERT INTO newim.im_conversation_sync_keys(user_id, conversation_id, first_change_seq)
			SELECT user_id, conversation_id, change_seq FROM allocated
			ON CONFLICT (user_id, conversation_id) DO NOTHING
			RETURNING user_id, conversation_id
		),
		written AS (
			INSERT INTO newim.im_conversation_sync_changes
				(user_id, change_seq, conversation_id, kind, latest_seq, latest_server_msg_id)
			SELECT a.user_id, a.change_seq, a.conversation_id, a.kind, a.latest_seq, a.latest_server_msg_id
			FROM allocated a
			JOIN advanced USING (user_id)
			LEFT JOIN keyed k USING (user_id, conversation_id)
			LEFT JOIN newim.im_conversation_sync_keys existing USING (user_id, conversation_id)
			WHERE k.user_id IS NOT NULL OR existing.user_id IS NOT NULL
			RETURNING 1
		)
		SELECT valid AND (SELECT count(*) FROM written)=(SELECT count(*) FROM pending) FROM healthy`, args...).Scan(&valid)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "22003" {
			return app.Fail(app.SequenceExhausted)
		}
		return fixed(err)
	}
	if !valid {
		return app.Fail(app.StorageUnavailable)
	}
	return nil
}
