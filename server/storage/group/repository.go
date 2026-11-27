// Package group stores managed groups and their account changes in PostgreSQL.
// group 在 PostgreSQL 中存储纳管群组和账号变更。
package group

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/m-ice/NewIM/server/group"
	"github.com/m-ice/NewIM/server/storage/conversationsync"
	"github.com/m-ice/NewIM/server/storage/membership"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
	"github.com/m-ice/NewIM/server/user"
)

const requestTimeout = 5 * time.Second

const memberColumns = "user_id,group_role,mute_until,history_after_seq,departed_through_seq"
const entryColumns = "group_id,actor_id,request_id,user_id,kind,state,message,responded_by"

// Config holds trusted database settings.
// Config 保存可信数据库配置。
type Config struct {
	DSN                  string
	AllowLocalSocket     bool
	ApplicationName      string
	Guard                *postgresidentity.Guard
	ProjectConversations bool
	ProjectReadState     bool
}

// Repository owns a bounded database pool.
// Repository 持有有界数据库连接池。
type Repository struct {
	pool                 *pgxpool.Pool
	projectConversations bool
	projectReadState     bool
	accountSettings      bool
}

var _ app.Store = (*Repository)(nil)

// Open verifies transport and checks that the managed-group schema is ready.
// Open 验证数据库传输配置并检查纳管群结构是否就绪。
func Open(ctx context.Context, config Config) (*Repository, error) {
	if ctx == nil || config.DSN == "" || (config.ProjectReadState && !config.ProjectConversations) {
		return nil, app.StorageUnavailable
	}
	if config.ApplicationName == "" {
		config.ApplicationName = "newim-group"
	}
	if !user.ValidID(config.ApplicationName) {
		return nil, app.InvalidInput
	}
	poolConfig, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, app.StorageUnavailable
	}
	connConfig := poolConfig.ConnConfig
	if strings.HasPrefix(connConfig.Host, "/") {
		if !config.AllowLocalSocket || len(connConfig.Fallbacks) != 0 {
			return nil, app.StorageUnavailable
		}
	} else {
		tlsConfig := connConfig.TLSConfig
		if tlsConfig == nil || tlsConfig.InsecureSkipVerify || tlsConfig.ServerName != connConfig.Host {
			return nil, app.StorageUnavailable
		}
		for _, fallback := range connConfig.Fallbacks {
			tlsConfig := fallback.TLSConfig
			if fallback.Host == "" || strings.HasPrefix(fallback.Host, "/") ||
				tlsConfig == nil || tlsConfig.InsecureSkipVerify || tlsConfig.ServerName != fallback.Host {
				return nil, app.StorageUnavailable
			}
		}
	}
	poolConfig.MaxConns = 8
	poolConfig.MinConns = 0
	poolConfig.MinIdleConns = 0
	connConfig.ConnectTimeout = 2 * time.Second
	connConfig.DialFunc = (&net.Dialer{Timeout: 2 * time.Second}).DialContext
	connConfig.MaxProtocolMessageBodyLen = 1 << 20
	connConfig.RuntimeParams["application_name"] = config.ApplicationName
	connConfig.RuntimeParams["statement_timeout"] = "3000"
	connConfig.RuntimeParams["lock_timeout"] = "1000"
	connConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	connConfig.RuntimeParams["transaction_timeout"] = "5000"
	if guard := config.Guard; guard != nil {
		poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			if err := guard.AfterConnect(ctx, conn); err != nil {
				return app.StorageUnavailable
			}
			return nil
		}
		poolConfig.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
			if err := guard.Verify(ctx, conn); err != nil {
				return false, app.StorageUnavailable
			}
			return true, nil
		}
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, app.StorageUnavailable
	}
	accountSettings, err := conversationsync.AccountSettingsAvailable(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, app.StorageUnavailable
	}
	repo := &Repository{
		pool: pool, projectConversations: config.ProjectConversations, projectReadState: config.ProjectReadState, accountSettings: accountSettings,
	}
	if err = repo.checkSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if config.ProjectConversations && conversationsync.CheckSourceSchema(ctx, pool, config.ProjectReadState) != nil {
		pool.Close()
		return nil, app.StorageUnavailable
	}
	return repo, nil
}

// Close releases the database pool.
// Close 释放数据库连接池。
func (r *Repository) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

func (r *Repository) begin(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	if r == nil || r.pool == nil {
		return nil, app.StorageUnavailable
	}
	tx, err := r.pool.BeginTx(ctx, options)
	if err != nil {
		return nil, app.StorageUnavailable
	}
	return tx, nil
}

func (r *Repository) checkSchema(ctx context.Context) error {
	tx, err := r.begin(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer rollback(tx)
	advanced, err := membership.Advanced(ctx, tx)
	if err != nil || !advanced {
		return app.StorageUnavailable
	}
	if _, err = tx.Exec(ctx, `SELECT g.group_id, a.request_id, s.latest_revision, c.revision
		FROM newim.im_groups AS g
		CROSS JOIN newim.im_group_requests AS a
		CROSS JOIN newim.im_group_sync_accounts AS s
		CROSS JOIN newim.im_group_sync_changes AS c LIMIT 0`); err != nil {
		return app.StorageUnavailable
	}
	return commit(ctx, tx)
}

func (r *Repository) beginWrite(ctx context.Context) (pgx.Tx, error) {
	tx, err := r.begin(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	// The membership intent lock precedes conversation and account rows.
	if _, err = tx.Exec(ctx, "LOCK TABLE newim.im_conversation_members IN ROW EXCLUSIVE MODE"); err != nil {
		rollback(tx)
		return nil, app.StorageUnavailable
	}
	return tx, nil
}

func (r *Repository) recordConversationSource(ctx context.Context, tx pgx.Tx, groupID string, removed []app.Membership) error {
	if !r.projectConversations {
		return nil
	}
	var removedIDs []string
	if len(removed) > 0 {
		removedIDs = make([]string, len(removed))
		for i, member := range removed {
			removedIDs[i] = member.UserID
		}
	}
	if err := conversationsync.RecordSourceConversation(ctx, tx, groupID, removedIDs, r.projectReadState, r.accountSettings); err != nil {
		return app.StorageUnavailable
	}
	return nil
}

type groupRow struct {
	group       app.Group
	dissolvedAt *time.Time
	head        int64
}

func loadGroup(ctx context.Context, tx pgx.Tx, groupID string) (groupRow, error) {
	var record groupRow
	metadata := &record.group
	profile := &metadata.Profile
	err := tx.QueryRow(ctx, `SELECT g.group_id, COALESCE(owner.user_id::text, ''),
		g.name,g.face_url,g.description,g.notification,g.ex,g.join_mode,g.member_limit,
		g.members_can_invite,g.history_enabled,g.muted,g.dissolved_at,c.last_seq
		FROM newim.im_groups g
		JOIN newim.im_conversations c ON c.conversation_id=g.group_id AND c.kind='group'
		LEFT JOIN LATERAL (
			SELECT user_id FROM newim.im_conversation_members
			WHERE conversation_id=g.group_id AND group_role='owner'
				AND (departed_at IS NULL OR g.dissolved_at IS NOT NULL)
			ORDER BY departed_at NULLS FIRST, user_id LIMIT 1
		) AS owner ON true
		WHERE g.group_id=$1`, groupID).Scan(&metadata.GroupID, &metadata.OwnerID,
		&profile.Name, &profile.FaceURL, &profile.Description, &profile.Notification, &profile.Ex,
		&metadata.JoinMode, &metadata.MemberLimit, &metadata.MembersCanInvite, &metadata.HistoryEnabled,
		&metadata.Muted, &record.dissolvedAt, &record.head)
	if err != nil {
		return groupRow{}, storageError(err)
	}
	if metadata.OwnerID == "" || record.head < 0 {
		return groupRow{}, app.StorageUnavailable
	}
	return record, nil
}

func lockGroup(ctx context.Context, tx pgx.Tx, groupID string) (groupRow, error) {
	var locked string
	if err := tx.QueryRow(ctx, `SELECT conversation_id FROM newim.im_conversations
		WHERE conversation_id=$1 AND kind='group' FOR UPDATE`, groupID).Scan(&locked); err != nil {
		return groupRow{}, storageError(err)
	}
	return loadGroup(ctx, tx, groupID)
}

func loadMember(ctx context.Context, tx pgx.Tx, groupID, userID string) (app.Membership, error) {
	member, err := scanMember(tx.QueryRow(ctx, "SELECT "+memberColumns+` FROM newim.im_conversation_members
		WHERE conversation_id=$1 AND user_id=$2 AND group_role IS NOT NULL`, groupID, userID))
	return member, storageError(err)
}

func scanMember(row pgx.Row) (app.Membership, error) {
	var member app.Membership
	err := row.Scan(&member.UserID, &member.Role, &member.MuteUntil, &member.HistoryAfterSeq, &member.DepartedThroughSeq)
	return member, err
}

func scanEntry(row pgx.Row) (app.Entry, error) {
	var entry app.Entry
	var responder *string
	err := row.Scan(&entry.GroupID, &entry.ActorID, &entry.RequestID, &entry.UserID,
		&entry.Kind, &entry.State, &entry.Message, &responder)
	if responder != nil {
		entry.RespondedBy = *responder
	}
	return entry, err
}

func manager(role app.Role) bool {
	return role == app.Owner || role == app.Admin
}

func activeMember(ctx context.Context, tx pgx.Tx, group groupRow, actor string) (app.Membership, error) {
	if group.dissolvedAt != nil {
		return app.Membership{}, app.NotFound
	}
	member, err := loadMember(ctx, tx, group.group.GroupID, actor)
	if err == app.NotFound || member.DepartedThroughSeq != nil {
		return app.Membership{}, app.Forbidden
	}
	return member, err
}

func requireUsers(ctx context.Context, tx pgx.Tx, ids []string) error {
	var count int
	err := tx.QueryRow(ctx, `SELECT count(*) FROM (
		SELECT user_id FROM newim.im_users WHERE user_id=ANY($1::text[]) ORDER BY user_id FOR KEY SHARE
	) AS accounts`, ids).Scan(&count)
	if err != nil {
		return app.StorageUnavailable
	}
	if count != len(ids) {
		return app.NotFound
	}
	return nil
}

func storageError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return app.NotFound
	}
	var known app.Code
	if errors.As(err, &known) {
		return known
	}
	return app.StorageUnavailable
}

func commit(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return app.StorageUnavailable
	}
	return nil
}

func committed[T any](ctx context.Context, tx pgx.Tx, result T) (T, error) {
	if err := commit(ctx, tx); err != nil {
		var zero T
		return zero, err
	}
	return result, nil
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
