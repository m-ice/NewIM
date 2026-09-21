package conversationsync

import (
	"context"
	"errors"
	"slices"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/sync/conversation"
)

// Batch binds declared sources/accounts to the caller's locked transaction.
// Batch 将声明的会话和账户绑定到调用方已加锁的事务，不可并发使用。
type Batch struct {
	tx            pgx.Tx
	conversations map[string]struct{}
	users         map[string]struct{}
}

// PrepareBatch must precede all source mutations and row locks in a fresh
// READ COMMITTED transaction. The caller must roll back any returned error.
// PrepareBatch 必须在新事务的所有源变更/行锁之前调用；失败后须整体回滚。
func (r *Repository) PrepareBatch(ctx context.Context, tx pgx.Tx, conversationIDs, userIDs []string) (*Batch, error) {
	conversations, err := declaredIDs(conversationIDs)
	if err != nil {
		return nil, err
	}
	users, err := declaredIDs(userIDs)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, app.Fail(app.StorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	if err = configure(ctx, tx); err != nil {
		return nil, err
	}
	var isolation, readOnly string
	err = tx.QueryRow(ctx, "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')").Scan(&isolation, &readOnly)
	if err != nil {
		return nil, fixed(err)
	}
	if isolation != "read committed" || readOnly != "off" {
		return nil, app.Fail(app.StorageUnavailable)
	}
	if _, err = tx.Exec(ctx, "LOCK TABLE newim.im_conversation_members IN ROW EXCLUSIVE MODE"); err != nil {
		return nil, fixed(err)
	}
	batch := &Batch{tx: tx, conversations: make(map[string]struct{}, len(conversations)), users: make(map[string]struct{}, len(users))}
	for _, id := range conversations {
		var found string
		err = tx.QueryRow(ctx, "SELECT conversation_id FROM newim.im_conversations WHERE conversation_id=$1 FOR UPDATE", id).Scan(&found)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, app.Fail(app.Forbidden)
		}
		if err != nil {
			return nil, fixed(err)
		}
		batch.conversations[id] = struct{}{}
	}
	for _, id := range users {
		if err = userExists(ctx, tx, id); err != nil {
			return nil, err
		}
		var found string
		err = tx.QueryRow(ctx, "SELECT user_id FROM newim.im_conversation_sync_accounts WHERE user_id=$1 FOR UPDATE", id).Scan(&found)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, app.Fail(app.NotReady)
		}
		if err != nil {
			return nil, fixed(err)
		}
		batch.users[id] = struct{}{}
	}
	return batch, nil
}

func declaredIDs(input []string) ([]string, error) {
	// Bound input before copying; repeated IDs do not provide extra work budget.
	// 复制之前限制输入；重复标识不额外扩大工作预算。
	if len(input) == 0 || len(input) > 100 {
		return nil, app.Fail(app.LimitExceeded)
	}
	ids := slices.Clone(input)
	for _, id := range ids {
		if !identifier(id) {
			return nil, app.Fail(app.LimitExceeded)
		}
	}
	slices.Sort(ids)
	return slices.Compact(ids), nil
}

// RecordUpsert projects the current authorized source summary without commit.
// RecordUpsert 投影当前已授权的源摘要，但不提交外层事务。
func (b *Batch) RecordUpsert(ctx context.Context, conversation, user string) (uint64, error) {
	return b.record(ctx, conversation, user, false)
}

// RecordRemoval records absent membership with prior projected visibility.
// RecordRemoval 只记录已有投影可见性对应的成员撤销事实。
func (b *Batch) RecordRemoval(ctx context.Context, conversation, user string) (uint64, error) {
	return b.record(ctx, conversation, user, true)
}

func (b *Batch) record(ctx context.Context, conversation, user string, remove bool) (uint64, error) {
	if b == nil || b.tx == nil {
		return 0, app.Fail(app.Forbidden)
	}
	if _, ok := b.conversations[conversation]; !ok {
		return 0, app.Fail(app.Forbidden)
	}
	if _, ok := b.users[user]; !ok {
		return 0, app.Fail(app.Forbidden)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	var member bool
	err := b.tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM newim.im_conversation_members WHERE user_id=$1 AND conversation_id=$2)", user, conversation).Scan(&member)
	if err != nil {
		return 0, fixed(err)
	}
	if member == remove {
		return 0, app.Fail(app.Forbidden)
	}
	previous, err := latest(ctx, b.tx, user, conversation, app.MaxSequence)
	if err != nil {
		return 0, err
	}
	if remove && previous == nil {
		return 0, app.Fail(app.Forbidden)
	}
	kind := "remove"
	var sequence *int64
	var message *string
	if !remove {
		kind = "upsert"
		var seq int64
		err = b.tx.QueryRow(ctx, "SELECT last_seq,latest_server_msg_id FROM newim.im_conversations WHERE conversation_id=$1", conversation).Scan(&seq, &message)
		if err != nil {
			return 0, fixed(err)
		}
		if seq < 0 {
			return 0, app.Fail(app.StorageUnavailable)
		}
		sequence = &seq
	}
	if previous != nil && previous.Kind == kind && (remove || (*previous.LatestConversationSeq == uint64(*sequence) && sameString(previous.LatestServerMsgID, message))) {
		return previous.Revision, nil
	}
	var head int64
	err = b.tx.QueryRow(ctx, "SELECT last_change_seq FROM newim.im_conversation_sync_accounts WHERE user_id=$1", user).Scan(&head)
	if err != nil {
		return 0, fixed(err)
	}
	if uint64(head) == app.MaxSequence {
		return 0, app.Fail(app.SequenceExhausted)
	}
	revision := head + 1
	_, err = b.tx.Exec(ctx, "INSERT INTO newim.im_conversation_sync_keys(user_id,conversation_id,first_change_seq) VALUES($1,$2,$3) ON CONFLICT(user_id,conversation_id) DO NOTHING", user, conversation, revision)
	if err != nil {
		return 0, fixed(err)
	}
	_, err = b.tx.Exec(ctx, "INSERT INTO newim.im_conversation_sync_changes(user_id,change_seq,conversation_id,kind,latest_seq,latest_server_msg_id) VALUES($1,$2,$3,$4,$5,$6)", user, revision, conversation, kind, sequence, message)
	if err != nil {
		return 0, fixed(err)
	}
	_, err = b.tx.Exec(ctx, "UPDATE newim.im_conversation_sync_accounts SET last_change_seq=$2 WHERE user_id=$1", user, revision)
	if err != nil {
		return 0, fixed(err)
	}
	return uint64(revision), nil
}

func sameString(a, b *string) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }
