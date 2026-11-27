package conversationsync

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/storage/authsession"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

// AccountSettingsAvailable distinguishes an older schema from an incomplete upgrade.
func AccountSettingsAvailable(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	if ctx == nil || pool == nil {
		return false, app.Fail(app.StorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	var settingsTable bool
	var snapshotColumns int
	err := pool.QueryRow(ctx, `SELECT to_regclass('newim.im_conversation_account_settings') IS NOT NULL,
		(SELECT count(*) FROM pg_catalog.pg_attribute
		 WHERE attrelid=to_regclass('newim.im_conversation_sync_changes')
		   AND attnum>0 AND NOT attisdropped
		   AND attname IN ('settings_version','settings_pinned','settings_muted','settings_draft','settings_clear_through_seq'))`).
		Scan(&settingsTable, &snapshotColumns)
	if err != nil {
		return false, fixed(err)
	}
	if !settingsTable && snapshotColumns == 0 {
		return false, nil
	}
	if !settingsTable || snapshotColumns != 5 {
		return false, app.Fail(app.StorageUnavailable)
	}
	err = checkSourceColumns(ctx, pool, `SELECT s.conversation_id,s.user_id,s.version,s.pinned,s.muted,s.draft,
		s.clear_through_seq,s.deleted_through_seq FROM newim.im_conversation_account_settings s LIMIT 0`)
	return err == nil, err
}

// PersonalHistoryFloor reads the persisted restriction in the caller's authorized snapshot.
func PersonalHistoryFloor(ctx context.Context, tx pgx.Tx, userID, id string, head int64) (int64, error) {
	var floor int64
	err := tx.QueryRow(ctx, `SELECT clear_through_seq FROM newim.im_conversation_account_settings
		WHERE conversation_id=$1 AND user_id=$2`, id, userID).Scan(&floor)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil || floor < 0 || floor > head {
		return 0, app.Fail(app.StorageUnavailable)
	}
	return floor, nil
}

// RestoreAccountSettings is called only for a fresh send with the conversation locked.
func RestoreAccountSettings(ctx context.Context, tx pgx.Tx, id string, seq int64) error {
	_, err := tx.Exec(ctx, `UPDATE newim.im_conversation_account_settings AS s
		SET deleted_through_seq=NULL, version=version+1
		FROM newim.im_conversation_members AS m
		WHERE s.conversation_id=$1 AND s.deleted_through_seq<$2
		  AND m.conversation_id=s.conversation_id AND m.user_id=s.user_id AND m.departed_at IS NULL`, id, seq)
	return fixed(err)
}

func (r *Repository) AccountSettings(ctx context.Context, bearer session.BearerSession, id string) (app.AccountSettings, error) {
	return r.accessAccountSettings(ctx, bearer, id, nil)
}

func (r *Repository) MutateAccountSettings(ctx context.Context, bearer session.BearerSession, id string, mutation app.AccountSettingsMutation) (app.AccountSettings, error) {
	if !mutation.Valid() {
		return app.AccountSettings{}, app.Fail(app.AccountSettingsInvalid)
	}
	return r.accessAccountSettings(ctx, bearer, id, &mutation)
}

func (r *Repository) accessAccountSettings(ctx context.Context, bearer session.BearerSession, id string, mutation *app.AccountSettingsMutation) (app.AccountSettings, error) {
	var zero app.AccountSettings
	if ctx == nil || r == nil || !r.accountSettings || !r.exposeAccountSettings {
		return zero, app.Fail(app.StorageUnavailable)
	}
	if !identifier(id) {
		return zero, app.Fail(app.AccountSettingsInvalid)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return zero, err
	}
	defer rollback(tx)
	expiry, err := authsession.LockVerifiedBearer(ctx, tx, bearer)
	if err != nil {
		return zero, err
	}
	settings, opErr := applyAccountSettings(ctx, tx, bearer.UserID(), id, mutation)
	if err = authsession.CheckLockedBearerExpiry(ctx, tx, expiry); err != nil {
		return zero, err
	}
	if opErr != nil {
		return zero, opErr
	}
	if err = tx.Commit(ctx); err != nil {
		return zero, fixed(err)
	}
	return settings, nil
}

func applyAccountSettings(ctx context.Context, tx pgx.Tx, userID, id string, mutation *app.AccountSettingsMutation) (app.AccountSettings, error) {
	var current app.AccountSettings
	lock := " FOR SHARE OF c"
	if mutation != nil {
		if err := LockSourceMembership(ctx, tx); err != nil {
			return current, err
		}
		lock = " FOR UPDATE OF c"
	}
	var head int64
	err := tx.QueryRow(ctx, `SELECT c.last_seq FROM newim.im_conversations c
		WHERE c.conversation_id=$1 AND EXISTS (
			SELECT 1 FROM newim.im_conversation_members m WHERE m.conversation_id=$1 AND m.user_id=$2 AND m.departed_at IS NULL
		)`+lock, id, userID).Scan(&head)
	if err != nil {
		return current, accountVisibility(err)
	}
	var floor int64
	err = tx.QueryRow(ctx, `SELECT history_after_seq FROM newim.im_conversation_members
		WHERE conversation_id=$1 AND user_id=$2 AND departed_at IS NULL FOR SHARE`, id, userID).Scan(&floor)
	if err != nil {
		return current, accountVisibility(err)
	}
	if floor < 0 || floor > head {
		return current, app.Fail(app.StorageUnavailable)
	}
	query := `SELECT version,pinned,muted,draft,clear_through_seq,deleted_through_seq
		FROM newim.im_conversation_account_settings WHERE conversation_id=$1 AND user_id=$2`
	if mutation != nil {
		query += " FOR UPDATE"
	}
	var version, clear int64
	var deleted *int64
	var draft []byte
	err = tx.QueryRow(ctx, query, id, userID).
		Scan(&version, &current.Pinned, &current.Muted, &draft, &clear, &deleted)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return current, fixed(err)
	}
	current.Draft = string(draft)
	if version < 0 || clear < 0 || clear > head || deleted != nil && (*deleted < 0 || *deleted > clear) ||
		!app.ValidAccountDraft(current.Draft) {
		return app.AccountSettings{}, app.Fail(app.StorageUnavailable)
	}
	current.Version, current.ClearThroughSeq, current.Deleted = uint64(version), uint64(clear), deleted != nil
	if mutation == nil {
		return current, nil
	}
	next, err := applySettingsMutation(current, uint64(head), *mutation)
	if err != nil || next == current {
		return next, err
	}
	if next.Deleted {
		deleted = &head
	}
	_, err = tx.Exec(ctx, `INSERT INTO newim.im_conversation_account_settings
		(conversation_id,user_id,version,pinned,muted,draft,clear_through_seq,deleted_through_seq)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (conversation_id,user_id) DO UPDATE SET
			version=EXCLUDED.version,pinned=EXCLUDED.pinned,muted=EXCLUDED.muted,draft=EXCLUDED.draft,
			clear_through_seq=EXCLUDED.clear_through_seq,deleted_through_seq=EXCLUDED.deleted_through_seq`,
		id, userID, int64(next.Version), next.Pinned, next.Muted, []byte(next.Draft), int64(next.ClearThroughSeq), deleted)
	if err != nil {
		return app.AccountSettings{}, fixed(err)
	}
	if err = recordSourceProjection(ctx, tx, true, true, sourceWatermarkTarget, userID, id); err != nil {
		return app.AccountSettings{}, err
	}
	return next, nil
}

func applySettingsMutation(current app.AccountSettings, head uint64, mutation app.AccountSettingsMutation) (app.AccountSettings, error) {
	if current.Version != mutation.ExpectedVersion {
		return app.AccountSettings{}, app.Fail(app.AccountStateConflict)
	}
	if current.Deleted {
		if mutation.Action == app.DeleteAccountConversation {
			return current, nil
		}
		return app.AccountSettings{}, app.Fail(app.ConversationDeleted)
	}
	next := current
	switch mutation.Action {
	case app.ChangeAccountSettings:
		if mutation.Pinned != nil {
			next.Pinned = *mutation.Pinned
		}
		if mutation.Muted != nil {
			next.Muted = *mutation.Muted
		}
		if mutation.Draft != nil {
			next.Draft = *mutation.Draft
		}
	case app.ClearAccountHistory:
		next.ClearThroughSeq = head
	case app.DeleteAccountConversation:
		next = app.AccountSettings{Version: current.Version, ClearThroughSeq: head, Deleted: true}
	}
	if next != current {
		if next.Version >= app.MaxSequence {
			return app.AccountSettings{}, app.Fail(app.SequenceExhausted)
		}
		next.Version++
	}
	return next, nil
}

func accountVisibility(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Fail(app.AccountSettingsNotFound)
	}
	return fixed(err)
}
