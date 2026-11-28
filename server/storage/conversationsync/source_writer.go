package conversationsync

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

// CheckSourceSchema checks source projection columns through the caller's pool.
func CheckSourceSchema(ctx context.Context, pool *pgxpool.Pool, includeReadState bool) error {
	if includeReadState {
		return CheckReadSourceSchema(ctx, pool)
	}
	return checkSourceColumns(ctx, pool, `SELECT a.source_ready, a.source_after_key, m.history_after_seq, m.departed_at
		FROM newim.im_conversation_sync_accounts a
		LEFT JOIN newim.im_conversation_members m ON false LIMIT 0`)
}

// CheckReadSourceSchema checks the base projection and read-watermark columns.
func CheckReadSourceSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return checkSourceColumns(ctx, pool, `SELECT a.source_ready, a.source_after_key, m.history_after_seq, m.departed_at,
			a.read_state_epoch, p.last_read_seq, r.read_seq
		FROM newim.im_conversation_sync_accounts a
		LEFT JOIN newim.im_conversation_members m ON false
		LEFT JOIN newim.im_conversation_sync_changes p ON false
		LEFT JOIN newim.im_read_states r ON false LIMIT 0`)
}

func checkSourceColumns(ctx context.Context, pool *pgxpool.Pool, query string) error {
	if ctx == nil || pool == nil {
		return app.Fail(app.StorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	_, err := pool.Exec(ctx, query)
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

// InitializeReadSourceAccount marks a new account ready for read sync in its first epoch.
func InitializeReadSourceAccount(ctx context.Context, tx pgx.Tx, userID string) error {
	if ctx == nil || tx == nil || !identifier(userID) {
		return app.Fail(app.StorageUnavailable)
	}
	_, err := tx.Exec(ctx, `INSERT INTO newim.im_conversation_sync_accounts
		(user_id,epoch,source_ready,read_state_epoch) VALUES ($1,1,true,1)`, userID)
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

const sourceWatermarkTarget = `
		SELECT user_id, conversation_id FROM newim.im_conversation_members
		WHERE user_id=$1 AND conversation_id=$2 AND departed_at IS NULL`

// RecordSourceConversation runs after the source mutation with the conversation
// locked. A failed write must abort the source transaction.
func RecordSourceConversation(ctx context.Context, tx pgx.Tx, conversationID string, removed []string, includeReadState, accountSettings bool) error {
	if ctx == nil || tx == nil || !identifier(conversationID) {
		return app.Fail(app.StorageUnavailable)
	}
	for _, userID := range removed {
		if !identifier(userID) {
			return app.Fail(app.StorageUnavailable)
		}
	}

	return recordSourceProjection(ctx, tx, includeReadState, accountSettings, sourceConversationTargets, conversationID, removed)
}

// RecordSourceSend projects a newly committed message while holding the conversation lock.
func RecordSourceSend(ctx context.Context, tx pgx.Tx, conversationID string, includeReadState, accountSettings bool) error {
	if ctx == nil || tx == nil || !identifier(conversationID) {
		return app.Fail(app.StorageUnavailable)
	}
	return recordSourceProjectionWithSend(ctx, tx, includeReadState, accountSettings, true, sourceConversationTargets, conversationID, []string(nil))
}

// RecordSourceWatermark records an advanced watermark for the active account only.
// The caller holds the conversation, membership and updated watermark row locks.
func RecordSourceWatermark(ctx context.Context, tx pgx.Tx, userID, conversationID string, accountSettings bool) error {
	if ctx == nil || tx == nil || !identifier(userID) || !identifier(conversationID) {
		return app.Fail(app.StorageUnavailable)
	}
	return recordSourceProjection(ctx, tx, true, accountSettings, sourceWatermarkTarget, userID, conversationID)
}

func recordSourceProjection(ctx context.Context, tx pgx.Tx, includeReadState, accountSettings bool, targetSQL string, args ...any) error {
	return recordSourceProjectionWithSend(ctx, tx, includeReadState, accountSettings, false, targetSQL, args...)
}

func recordSourceProjectionWithSend(ctx context.Context, tx pgx.Tx, includeReadState, accountSettings, freshSend bool, targetSQL string, args ...any) error {
	// The conflict clause locks existing rows without issuing updates. Inserting
	// new rows and locking existing rows in one sorted pass gives both the same order.
	_, err := tx.Exec(ctx, `INSERT INTO newim.im_conversation_sync_accounts AS account(user_id)
		SELECT audience.user_id FROM (`+targetSQL+`) audience
		WHERE EXISTS (
			SELECT 1 FROM newim.im_conversation_sync_accounts a WHERE a.user_id=audience.user_id
		) OR EXISTS (
			SELECT 1 FROM newim.im_conversation_members m
			WHERE m.conversation_id=audience.conversation_id AND m.user_id=audience.user_id AND m.departed_at IS NULL
		)
		ORDER BY audience.user_id
		ON CONFLICT (user_id) DO UPDATE SET source_ready=account.source_ready WHERE false`, args...)
	if err != nil {
		return fixed(err)
	}
	return appendSourceProjectionWithSend(ctx, tx, includeReadState, accountSettings, freshSend, targetSQL, args...)
}

// appendSourceProjection receives fixed package SQL yielding unique identifiers
// for (user_id, conversation_id). Its caller holds the conversation and account locks.
func appendSourceProjection(ctx context.Context, tx pgx.Tx, includeReadState, accountSettings bool, targetSQL string, args ...any) error {
	return appendSourceProjectionWithSend(ctx, tx, includeReadState, accountSettings, false, targetSQL, args...)
}

func appendSourceProjectionWithSend(ctx context.Context, tx pgx.Tx, includeReadState, accountSettings, freshSend bool, targetSQL string, args ...any) error {
	var visibility bool
	if includeReadState && accountSettings {
		var err error
		visibility, err = VisibilityAvailable(ctx, tx)
		if err != nil {
			return err
		}
	}
	var valid bool
	err := tx.QueryRow(ctx, projectionQuery(includeReadState, accountSettings, visibility, freshSend, targetSQL), args...).Scan(&valid)
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

// The lateral limit keeps the globally unique message lookup independent of conversation size.
const sourceSummaryPoint = "SELECT conversation_id,conversation_seq FROM newim.im_messages WHERE server_msg_id="

func sourceProjectionQuery(includeReadState, accountSettings bool, targetSQL string) string {
	return projectionQuery(includeReadState, accountSettings, false, false, targetSQL)
}

func projectionQuery(includeReadState, accountSettings, visibility, freshSend bool, targetSQL string) string {
	// Keep the 014 path independent of the read-state columns and values.
	var readCheck, readValue, readJoin string
	active := "m.user_id IS NOT NULL"
	var readColumn, headReadColumn, previousReadColumn, allocatedReadColumn string
	floor := "m.history_after_seq"
	var settingsCheck, settingsValues, settingsJoin, hidden, pendingHidden string
	var settingsColumns, headSettings, previousSettings, allocatedSettings string
	if accountSettings {
		active += " AND s.deleted_through_seq IS NULL"
		floor = "GREATEST(m.history_after_seq, COALESCE(s.clear_through_seq, 0))"
		settingsJoin = ` LEFT JOIN newim.im_conversation_account_settings s
			ON s.conversation_id=a.conversation_id AND s.user_id=a.user_id`
		settingsCheck = " AND (s.clear_through_seq IS NULL OR s.clear_through_seq<=c.last_seq)"
		hidden = ", s.deleted_through_seq IS NOT NULL AND m.user_id IS NOT NULL AS personally_hidden"
		pendingHidden = " OR (h.personally_hidden AND previous.kind IS NULL)"
		settingsValues = `,
				CASE WHEN ` + active + ` THEN s.version END AS settings_version,
				CASE WHEN ` + active + ` THEN s.pinned END AS settings_pinned,
				CASE WHEN ` + active + ` THEN s.muted END AS settings_muted,
				CASE WHEN ` + active + ` THEN s.draft END AS settings_draft,
				CASE WHEN ` + active + ` THEN s.clear_through_seq END AS settings_clear_through_seq`
		settingsColumns = ",settings_version,settings_pinned,settings_muted,settings_draft,settings_clear_through_seq"
		headSettings = ",h.settings_version,h.settings_pinned,h.settings_muted,h.settings_draft,h.settings_clear_through_seq"
		previousSettings = ",previous.settings_version,previous.settings_pinned,previous.settings_muted,previous.settings_draft,previous.settings_clear_through_seq"
		allocatedSettings = ",a.settings_version,a.settings_pinned,a.settings_muted,a.settings_draft,a.settings_clear_through_seq"
	}
	if includeReadState {
		readCheck = " AND (r.read_seq IS NULL OR r.read_seq BETWEEN 0 AND c.last_seq)"
		readValue = `, CASE WHEN ` + active + `
					THEN GREATEST(m.history_after_seq, COALESCE(r.read_seq, 0))
				END AS last_read_seq`
		readJoin = `
			LEFT JOIN newim.im_read_states r ON r.conversation_id=m.conversation_id AND r.user_id=m.user_id`
		readColumn = ", last_read_seq"
		headReadColumn = ", h.last_read_seq"
		previousReadColumn = ", previous.last_read_seq"
		allocatedReadColumn = ", a.last_read_seq"
	}
	var headVisibility, summaryVisibility, columnsVisibility, previousVisibility, pendingVisibility, allocatedVisibility string
	messagePoint := sourceSummaryPoint
	if visibility {
		headVisibility = visibilityHead(active, floor)
		summaryVisibility = visibilitySummary(freshSend)
		columnsVisibility, previousVisibility = visibilityColumns, visibilityPreviousColumns
		pendingVisibility, allocatedVisibility = visibilityHeadColumns, visibilityAllocatedColumns
		messagePoint = "SELECT conversation_id,conversation_seq,sender_id FROM newim.im_messages WHERE server_msg_id="
	}
	return `WITH audience AS (` + targetSQL + `),
		head AS MATERIALIZED (
			SELECT a.user_id, a.conversation_id, ` + active + ` AS active` + hidden + `,
				(CASE WHEN c.conversation_id IS NULL THEN false
					WHEN m.user_id IS NULL THEN true
					WHEN c.last_seq=0 THEN m.history_after_seq=0 AND c.latest_server_msg_id IS NULL
					ELSE m.history_after_seq<=c.last_seq AND msg.conversation_seq IS NOT DISTINCT FROM c.last_seq
				END` + readCheck + settingsCheck + `) AS valid,
				CASE WHEN ` + active + ` THEN
					CASE WHEN c.last_seq>` + floor + ` THEN c.last_seq ELSE 0 END
				END AS latest_seq,
				CASE WHEN (` + active + `) AND c.last_seq>` + floor + ` THEN c.latest_server_msg_id END AS latest_server_msg_id` + readValue + settingsValues + headVisibility + `
			FROM audience a
			LEFT JOIN newim.im_conversations c ON c.conversation_id=a.conversation_id
			LEFT JOIN newim.im_conversation_members m ON m.conversation_id=a.conversation_id
				AND m.user_id=a.user_id AND m.departed_at IS NULL
			LEFT JOIN LATERAL (
				` + messagePoint + `c.latest_server_msg_id LIMIT 1
			) msg ON msg.conversation_id=c.conversation_id` + readJoin + settingsJoin + `
		),
		healthy AS (
			SELECT NOT EXISTS (SELECT 1 FROM head WHERE NOT valid) AS valid
		),
		pending AS (
			SELECT h.user_id, h.conversation_id, h.latest_seq, h.latest_server_msg_id` + headReadColumn + headSettings + pendingVisibility + `,
				CASE WHEN h.active THEN 'upsert' ELSE 'remove' END AS kind
			FROM head h
			LEFT JOIN LATERAL (
				SELECT kind, latest_seq, latest_server_msg_id` + readColumn + settingsColumns + columnsVisibility + ` FROM newim.im_conversation_sync_changes
				WHERE user_id=h.user_id AND conversation_id=h.conversation_id
				ORDER BY change_seq DESC LIMIT 1
			) previous ON true` + summaryVisibility + `
			WHERE (SELECT valid FROM healthy)
				AND ((h.active AND (previous.kind, previous.latest_seq, previous.latest_server_msg_id` + previousReadColumn + previousSettings + previousVisibility + `)
					IS DISTINCT FROM ('upsert', h.latest_seq, h.latest_server_msg_id` + headReadColumn + headSettings + pendingVisibility + `))
					OR (NOT h.active AND previous.kind='upsert')` + pendingHidden + `)
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
				(user_id, change_seq, conversation_id, kind, latest_seq, latest_server_msg_id` + readColumn + settingsColumns + columnsVisibility + `)
			SELECT a.user_id, a.change_seq, a.conversation_id, a.kind, a.latest_seq, a.latest_server_msg_id` + allocatedReadColumn + allocatedSettings + allocatedVisibility + `
			FROM allocated a
			JOIN advanced USING (user_id)
			LEFT JOIN keyed k USING (user_id, conversation_id)
			LEFT JOIN newim.im_conversation_sync_keys existing USING (user_id, conversation_id)
			WHERE k.user_id IS NOT NULL OR existing.user_id IS NOT NULL
			RETURNING 1
		)
		SELECT valid AND (SELECT count(*) FROM written)=(SELECT count(*) FROM pending) FROM healthy`
}
