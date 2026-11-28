package conversationsync

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/m-ice/NewIM/server/storage/membership"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

type readScope struct {
	activeMembersOnly  bool
	requireSourceReady bool
	includeReadState   bool
	accountSettings    bool
	visibility         bool
}

// Read releases its consistent snapshot before returning any candidate.
// Read 在返回候选前释放一致性快照，不跨页保留事务。
func (r *Repository) Read(ctx context.Context, user string, request app.ReadRequest) (app.ReadResult, error) {
	var result app.ReadResult
	if ctx == nil {
		return result, app.Fail(app.StorageUnavailable)
	}
	if !identifier(user) {
		return result, app.Fail(app.Forbidden)
	}
	if request.Limit < 1 || request.Limit > app.MaxPageItems {
		return result, app.Fail(app.LimitExceeded)
	}
	if request.Kind != app.BootstrapRead && request.Kind != app.DeltaRead {
		return result, app.Fail(app.InvalidCursor)
	}
	if request.Epoch > app.MaxSequence || request.Fence > app.MaxSequence || request.AfterSeq > app.MaxSequence || request.AfterKey != "" && !identifier(request.AfterKey) || request.TerminalKey != "" && !identifier(request.TerminalKey) {
		return result, app.Fail(app.InvalidCursor)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer rollback(tx)
	activeMembersOnly, err := membership.Advanced(ctx, tx)
	if err != nil {
		return result, fixed(err)
	}
	if err = userExists(ctx, tx, user); err != nil {
		return result, err
	}
	scope := readScope{
		activeMembersOnly: activeMembersOnly, requireSourceReady: r.requireSourceReady, includeReadState: r.includeReadState, accountSettings: r.accountSettings, visibility: r.visibility,
	}
	query := "SELECT epoch,last_change_seq,min_valid_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1"
	if scope.requireSourceReady {
		if !scope.activeMembersOnly {
			return result, app.Fail(app.StorageUnavailable)
		}
		query += " AND source_ready"
	}
	if scope.includeReadState {
		query += " AND read_state_epoch=epoch"
	}
	var epoch, head, floor int64
	err = tx.QueryRow(ctx, query, user).Scan(&epoch, &head, &floor)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, app.Fail(app.NotReady)
	}
	if err != nil {
		return result, fixed(err)
	}
	result.Epoch, result.Head, result.Floor = uint64(epoch), uint64(head), uint64(floor)
	if !(request.Kind == app.BootstrapRead && request.Start) {
		if request.Epoch != result.Epoch {
			return app.ReadResult{}, app.Fail(app.CursorExpired)
		}
		position := request.Fence
		if request.Kind == app.DeltaRead {
			position = request.AfterSeq
		}
		if position < result.Floor {
			return app.ReadResult{}, app.Fail(app.CursorExpired)
		}
		if position > result.Head || request.Fence > result.Head || (!request.Start && request.AfterSeq > request.Fence) {
			return app.ReadResult{}, app.Fail(app.InvalidCursor)
		}
	}
	result.Fence = request.Fence
	if request.Start {
		result.Fence = result.Head
	}
	if request.Kind == app.BootstrapRead {
		result.TerminalKey = request.TerminalKey
		if request.Start {
			err = tx.QueryRow(ctx, "SELECT conversation_id FROM newim.im_conversation_sync_keys WHERE user_id=$1 ORDER BY conversation_id DESC LIMIT 1", user).Scan(&result.TerminalKey)
			if errors.Is(err, pgx.ErrNoRows) {
				err = nil
			}
			if err != nil {
				return app.ReadResult{}, fixed(err)
			}
		}
		if request.AfterKey > result.TerminalKey {
			return app.ReadResult{}, app.Fail(app.InvalidCursor)
		}
		err = readDirectory(ctx, tx, user, request.AfterKey, scope, &result)
	} else {
		err = readDelta(ctx, tx, user, request, scope, &result)
	}
	if err != nil {
		return app.ReadResult{}, err
	}
	if scope.accountSettings && !r.exposeAccountSettings {
		for _, candidate := range result.Candidates {
			if candidate.State != nil {
				candidate.State.Settings = nil
			}
			if candidate.Latest != nil {
				candidate.Latest.Settings = nil
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return app.ReadResult{}, fixed(err)
	}
	return result, nil
}

func readDirectory(ctx context.Context, tx pgx.Tx, user, after string, scope readScope, result *app.ReadResult) error {
	// LIMIT precedes the first revision filter; future entries still consume budget.
	// 先 LIMIT 再判断首个版本，未来目录键仍计入扫描预算。
	rows, err := tx.Query(ctx, "SELECT conversation_id,first_change_seq FROM newim.im_conversation_sync_keys WHERE user_id=$1 AND conversation_id>$2 AND conversation_id<=$3 ORDER BY conversation_id LIMIT 200", user, after, result.TerminalKey)
	if err != nil {
		return fixed(err)
	}
	type entry struct {
		key   string
		first int64
	}
	entries := make([]entry, 0, app.MaxDirectoryCandidates)
	for rows.Next() {
		var e entry
		if err = rows.Scan(&e.key, &e.first); err != nil {
			rows.Close()
			return fixed(err)
		}
		entries = append(entries, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return fixed(err)
	}
	result.DirectoryExhausted = len(entries) < app.MaxDirectoryCandidates || (len(entries) > 0 && entries[len(entries)-1].key >= result.TerminalKey)
	for _, e := range entries {
		candidate := app.Candidate{Key: e.key}
		if uint64(e.first) <= result.Fence {
			candidate.State, err = latestItem(ctx, tx, user, e.key, result.Fence, scope)
			if err != nil {
				return err
			}
			if candidate.State == nil {
				return app.Fail(app.StorageUnavailable)
			}
			if err = authorize(ctx, tx, user, scope, &candidate); err != nil {
				return err
			}
		}
		result.Candidates = append(result.Candidates, candidate)
	}
	return nil
}

func readDelta(ctx context.Context, tx pgx.Tx, user string, request app.ReadRequest, scope readScope, result *app.ReadResult) error {
	rows, err := tx.Query(ctx, "SELECT "+itemColumns(scope)+" FROM newim.im_conversation_sync_changes WHERE user_id=$1 AND change_seq>$2 AND change_seq<=$3 ORDER BY change_seq LIMIT $4", user, int64(request.AfterSeq), int64(result.Fence), request.Limit+1)
	if err != nil {
		return fixed(err)
	}
	for rows.Next() {
		item, err := scanItem(rows, scope)
		if err != nil {
			rows.Close()
			return err
		}
		result.Candidates = append(result.Candidates, app.Candidate{Key: item.ConversationID, State: item})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return fixed(err)
	}
	for i := range result.Candidates {
		if err = authorize(ctx, tx, user, scope, &result.Candidates[i]); err != nil {
			return err
		}
	}
	return nil
}

func authorize(ctx context.Context, tx pgx.Tx, user string, scope readScope, candidate *app.Candidate) error {
	if candidate.State.Kind != "upsert" {
		return nil
	}
	var err error
	if scope.requireSourceReady || scope.accountSettings {
		return authorizeSourceSummary(ctx, tx, user, candidate, scope)
	}
	candidate.Member, err = hasMember(ctx, tx, user, candidate.Key, scope.activeMembersOnly)
	if err != nil {
		return fixed(err)
	}
	if !candidate.Member {
		candidate.Latest, err = latest(ctx, tx, user, candidate.Key, app.MaxSequence)
	}
	return err
}

func authorizeSourceSummary(ctx context.Context, tx pgx.Tx, user string, candidate *app.Candidate, scope readScope) error {
	var floor int64
	var personalFloor, deleted *int64
	query := `SELECT m.history_after_seq FROM newim.im_conversation_members m
		WHERE m.user_id=$1 AND m.conversation_id=$2 AND m.departed_at IS NULL`
	fields := []any{&floor}
	if scope.accountSettings {
		query = `SELECT m.history_after_seq,s.clear_through_seq,s.deleted_through_seq
			FROM newim.im_conversation_members m
			LEFT JOIN newim.im_conversation_account_settings s USING (conversation_id,user_id)
			WHERE m.user_id=$1 AND m.conversation_id=$2 AND m.departed_at IS NULL`
		fields = append(fields, &personalFloor, &deleted)
	}
	err := tx.QueryRow(ctx, query, user, candidate.Key).Scan(fields...)
	if errors.Is(err, pgx.ErrNoRows) {
		candidate.Latest, err = latestItem(ctx, tx, user, candidate.Key, app.MaxSequence, scope)
		return err
	}
	if err != nil || floor < 0 || personalFloor != nil && *personalFloor < 0 {
		return app.Fail(app.StorageUnavailable)
	}
	if !scope.requireSourceReady && personalFloor == nil {
		candidate.Member = true
		return nil
	}
	history := uint64(0)
	if personalFloor != nil {
		history = uint64(*personalFloor)
	}
	if deleted == nil {
		visible, err := visibleSourceSummary(ctx, tx, candidate.State, uint64(floor), history)
		if err != nil || visible {
			candidate.Member = visible
			return err
		}
	}

	// Current history or a personal deletion can invalidate a frozen summary.
	candidate.HistoryRestricted = true
	replacement, err := latestItem(ctx, tx, user, candidate.Key, app.MaxSequence, scope)
	if err != nil || replacement == nil || replacement.Revision <= candidate.State.Revision {
		return err
	}
	if replacement.Kind == "remove" {
		candidate.Latest = replacement
		return nil
	}
	if deleted != nil {
		return nil
	}
	visible, err := visibleSourceSummary(ctx, tx, replacement, uint64(floor), history)
	if visible {
		candidate.Latest = replacement
	}
	return err
}

const visibleSummarySQL = `
	SELECT COALESCE((SELECT conversation_id=$1 AND conversation_seq=$3
		FROM (` + sourceSummaryPoint + `$2 LIMIT 1) AS summary), false)`

func visibleSourceSummary(ctx context.Context, tx pgx.Tx, item *app.Item, memberFloor, personalFloor uint64) (bool, error) {
	seq := *item.LatestConversationSeq
	if (seq == 0) != (item.LatestServerMsgID == nil) {
		return false, app.Fail(app.StorageUnavailable)
	}
	if item.LastReadSeq != nil && *item.LastReadSeq < memberFloor {
		return false, nil
	}
	var frozenFloor uint64
	if item.Settings != nil {
		frozenFloor = item.Settings.ClearThroughSeq
	}
	if frozenFloor < personalFloor {
		return false, nil
	}
	if seq == 0 {
		return true, nil
	}
	if seq <= max(memberFloor, personalFloor) {
		return false, nil
	}
	var matches bool
	err := tx.QueryRow(ctx, visibleSummarySQL, item.ConversationID, *item.LatestServerMsgID, int64(seq)).Scan(&matches)
	if err != nil || !matches {
		return false, app.Fail(app.StorageUnavailable)
	}
	return true, nil
}

func hasMember(ctx context.Context, tx pgx.Tx, user, conversation string, activeMembersOnly bool) (bool, error) {
	query := "SELECT EXISTS(SELECT 1 FROM newim.im_conversation_members WHERE user_id=$1 AND conversation_id=$2)"
	if activeMembersOnly {
		query = "SELECT EXISTS(SELECT 1 FROM newim.im_conversation_members WHERE user_id=$1 AND conversation_id=$2 AND departed_at IS NULL)"
	}
	var member bool
	err := tx.QueryRow(ctx, query, user, conversation).Scan(&member)
	return member, err
}

func itemColumns(scope readScope) string {
	columns := "conversation_id,change_seq,kind,latest_seq,latest_server_msg_id"
	if scope.includeReadState {
		columns += ",last_read_seq"
	}
	if scope.accountSettings {
		columns += ",settings_version,settings_pinned,settings_muted,settings_draft,settings_clear_through_seq"
	}
	if scope.visibility {
		columns += visibilityColumns
	}
	return columns
}

func latest(ctx context.Context, tx pgx.Tx, user, conversation string, fence uint64) (*app.Item, error) {
	return latestItem(ctx, tx, user, conversation, fence, readScope{})
}

func latestItem(ctx context.Context, tx pgx.Tx, user, conversation string, fence uint64, scope readScope) (*app.Item, error) {
	row := tx.QueryRow(ctx, "SELECT "+itemColumns(scope)+" FROM newim.im_conversation_sync_changes WHERE user_id=$1 AND conversation_id=$2 AND change_seq<=$3 ORDER BY change_seq DESC LIMIT 1", user, conversation, int64(fence))
	return scanItem(row, scope)
}

func scanItem(row interface{ Scan(...any) error }, scope readScope) (*app.Item, error) {
	item := &app.Item{}
	var revision int64
	var sequence, readSequence *int64
	fields := []any{&item.ConversationID, &revision, &item.Kind, &sequence, &item.LatestServerMsgID}
	if scope.includeReadState {
		fields = append(fields, &readSequence)
	}
	var settingsVersion, clearThrough *int64
	var pinned, muted *bool
	var draft []byte
	if scope.accountSettings {
		fields = append(fields, &settingsVersion, &pinned, &muted, &draft, &clearThrough)
	}
	var unreadCount, visibilityRevision *int64
	if scope.visibility {
		fields = append(fields, &unreadCount, &visibilityRevision)
	}
	err := row.Scan(fields...)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fixed(err)
	}
	if revision <= 0 || !identifier(item.ConversationID) {
		return nil, app.Fail(app.StorageUnavailable)
	}
	if settingsVersion != nil || pinned != nil || muted != nil || draft != nil || clearThrough != nil {
		if item.Kind != "upsert" || settingsVersion == nil || *settingsVersion <= 0 ||
			pinned == nil || muted == nil || draft == nil || clearThrough == nil || *clearThrough < 0 ||
			!app.ValidAccountDraft(string(draft)) {
			return nil, app.Fail(app.StorageUnavailable)
		}
		item.Settings = &app.AccountSettings{Version: uint64(*settingsVersion), Pinned: *pinned, Muted: *muted,
			Draft: string(draft), ClearThroughSeq: uint64(*clearThrough)}
	}
	if unreadCount != nil || visibilityRevision != nil {
		if item.Kind != "upsert" || unreadCount == nil || visibilityRevision == nil ||
			*unreadCount < 0 || *visibilityRevision < 0 {
			return nil, app.Fail(app.StorageUnavailable)
		}
		count, visibility := uint64(*unreadCount), uint64(*visibilityRevision)
		item.UnreadCount = &count
		item.VisibilityRevision = &visibility
	}
	item.Revision = uint64(revision)
	if item.Kind == "upsert" {
		if sequence == nil || *sequence < 0 {
			return nil, app.Fail(app.StorageUnavailable)
		}
		s := uint64(*sequence)
		item.LatestConversationSeq = &s
		if readSequence != nil {
			if *readSequence < 0 || (*sequence != 0 && *readSequence > *sequence) {
				return nil, app.Fail(app.StorageUnavailable)
			}
			value := uint64(*readSequence)
			item.LastReadSeq = &value
		}
	} else if item.Kind != "remove" || sequence != nil || item.LatestServerMsgID != nil || readSequence != nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	return item, nil
}
