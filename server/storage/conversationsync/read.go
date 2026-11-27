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
	scope := readScope{activeMembersOnly: activeMembersOnly, requireSourceReady: r.requireSourceReady}
	query := "SELECT epoch,last_change_seq,min_valid_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1"
	if scope.requireSourceReady {
		if !scope.activeMembersOnly {
			return result, app.Fail(app.StorageUnavailable)
		}
		query += " AND source_ready"
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
			candidate.State, err = latest(ctx, tx, user, e.key, result.Fence)
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
	rows, err := tx.Query(ctx, "SELECT conversation_id,change_seq,kind,latest_seq,latest_server_msg_id FROM newim.im_conversation_sync_changes WHERE user_id=$1 AND change_seq>$2 AND change_seq<=$3 ORDER BY change_seq LIMIT $4", user, int64(request.AfterSeq), int64(result.Fence), request.Limit+1)
	if err != nil {
		return fixed(err)
	}
	for rows.Next() {
		item, err := scanItem(rows)
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
	if scope.requireSourceReady {
		return authorizeSourceSummary(ctx, tx, user, candidate)
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

func authorizeSourceSummary(ctx context.Context, tx pgx.Tx, user string, candidate *app.Candidate) error {
	var floor int64
	err := tx.QueryRow(ctx, `SELECT history_after_seq FROM newim.im_conversation_members
		WHERE user_id=$1 AND conversation_id=$2 AND departed_at IS NULL`, user, candidate.Key).Scan(&floor)
	if errors.Is(err, pgx.ErrNoRows) {
		candidate.Latest, err = latest(ctx, tx, user, candidate.Key, app.MaxSequence)
		return err
	}
	if err != nil || floor < 0 {
		return app.Fail(app.StorageUnavailable)
	}
	visible, err := visibleSourceSummary(ctx, tx, candidate.State, uint64(floor))
	if err != nil || visible {
		candidate.Member = visible
		return err
	}

	// Record the authorization change without expiring an earlier page.
	candidate.HistoryRestricted = true
	replacement, err := latest(ctx, tx, user, candidate.Key, app.MaxSequence)
	if err != nil || replacement == nil || replacement.Kind != "upsert" || replacement.Revision <= candidate.State.Revision {
		return err
	}
	visible, err = visibleSourceSummary(ctx, tx, replacement, uint64(floor))
	if visible {
		candidate.Latest = replacement
	}
	return err
}

func visibleSourceSummary(ctx context.Context, tx pgx.Tx, item *app.Item, floor uint64) (bool, error) {
	seq := *item.LatestConversationSeq
	if (seq == 0) != (item.LatestServerMsgID == nil) {
		return false, app.Fail(app.StorageUnavailable)
	}
	if seq == 0 {
		return true, nil
	}
	if seq <= floor {
		return false, nil
	}
	var matches bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM newim.im_messages
		WHERE conversation_id=$1 AND server_msg_id=$2 AND conversation_seq=$3)`,
		item.ConversationID, *item.LatestServerMsgID, int64(seq)).Scan(&matches)
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

func latest(ctx context.Context, tx pgx.Tx, user, conversation string, fence uint64) (*app.Item, error) {
	row := tx.QueryRow(ctx, "SELECT conversation_id,change_seq,kind,latest_seq,latest_server_msg_id FROM newim.im_conversation_sync_changes WHERE user_id=$1 AND conversation_id=$2 AND change_seq<=$3 ORDER BY change_seq DESC LIMIT 1", user, conversation, int64(fence))
	return scanItem(row)
}

func scanItem(row interface{ Scan(...any) error }) (*app.Item, error) {
	item := &app.Item{}
	var revision int64
	var sequence *int64
	err := row.Scan(&item.ConversationID, &revision, &item.Kind, &sequence, &item.LatestServerMsgID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fixed(err)
	}
	if revision <= 0 || !identifier(item.ConversationID) {
		return nil, app.Fail(app.StorageUnavailable)
	}
	item.Revision = uint64(revision)
	if item.Kind == "upsert" {
		if sequence == nil || *sequence < 0 {
			return nil, app.Fail(app.StorageUnavailable)
		}
		s := uint64(*sequence)
		item.LatestConversationSeq = &s
	} else if item.Kind != "remove" || sequence != nil || item.LatestServerMsgID != nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	return item, nil
}
