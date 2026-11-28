package conversationsync

import (
	"context"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

type visibilitySchemaQuery interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// VisibilityAvailable detects the additive schema while accepting older snapshots.
func VisibilityAvailable(ctx context.Context, db visibilitySchemaQuery) (bool, error) {
	var events bool
	var columns int
	err := db.QueryRow(ctx, `SELECT to_regclass('newim.im_message_visibility_events') IS NOT NULL,
		(SELECT count(*) FROM pg_catalog.pg_attribute
		 WHERE attrelid=to_regclass('newim.im_conversation_sync_changes')
		   AND attnum>0 AND NOT attisdropped
		   AND attname IN ('unread_count','visibility_revision'))`).Scan(&events, &columns)
	if err != nil {
		return false, fixed(err)
	}
	if events != (columns == 2) || !events && columns != 0 {
		return false, app.Fail(app.StorageUnavailable)
	}
	return events, nil
}

const visibilityColumns = ",unread_count,visibility_revision"
const visibilityHeadColumns = ",summary.unread_count,h.visibility_revision"
const visibilityPreviousColumns = ",previous.unread_count,previous.visibility_revision"
const visibilityAllocatedColumns = ",a.unread_count,a.visibility_revision"

func visibilityHead(active, floor string) string {
	return `, GREATEST(` + floor + `,COALESCE(r.read_seq,0)) AS unread_after,
		msg.sender_id AS latest_sender, CASE WHEN ` + active + ` THEN GREATEST(
			COALESCE((SELECT e.visibility_revision FROM newim.im_message_visibility_events e
				WHERE e.conversation_id=a.conversation_id AND e.audience_user_id IS NULL
				ORDER BY e.visibility_revision DESC LIMIT 1),0),
			COALESCE((SELECT e.visibility_revision FROM newim.im_message_visibility_events e
				WHERE e.conversation_id=a.conversation_id AND e.audience_user_id=a.user_id
				ORDER BY e.visibility_revision DESC LIMIT 1),0)
		) END AS visibility_revision`
}

func visibilitySummary(freshSend bool) string {
	// These prior values represent the same effective boundary and event snapshot.
	// A send may increment the count with only the newly committed tail row.
	const sameWindow = `previous.kind='upsert' AND previous.unread_count IS NOT NULL
		AND previous.visibility_revision=h.visibility_revision
		AND GREATEST(previous.last_read_seq,COALESCE(previous.settings_clear_through_seq,0))=h.unread_after`
	var increment string
	if freshSend {
		increment = ` WHEN ` + sameWindow + ` AND previous.latest_seq=h.latest_seq-1
			THEN previous.unread_count + (h.latest_sender<>h.user_id)::integer`
	}
	return ` LEFT JOIN LATERAL (
		SELECT CASE
			WHEN NOT h.active THEN NULL
			WHEN h.latest_seq<=h.unread_after THEN 0
			WHEN ` + sameWindow + ` AND previous.latest_seq=h.latest_seq THEN previous.unread_count` + increment + `
			ELSE (SELECT count(*) FROM newim.im_messages unread
				WHERE unread.conversation_id=h.conversation_id AND unread.conversation_seq>h.unread_after
				  AND unread.conversation_seq<=h.latest_seq AND unread.sender_id<>h.user_id
				  AND NOT EXISTS (SELECT 1 FROM newim.im_message_visibility_events e
					WHERE e.conversation_id=unread.conversation_id AND e.target_seq=unread.conversation_seq
					  AND e.audience_user_id IS NULL)
				  AND NOT EXISTS (SELECT 1 FROM newim.im_message_visibility_events e
					WHERE e.conversation_id=unread.conversation_id AND e.target_seq=unread.conversation_seq
					  AND e.audience_user_id=h.user_id))
		END AS unread_count OFFSET 0
	) summary ON true`
}
