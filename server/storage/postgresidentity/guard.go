// Package postgresidentity verifies that a PostgreSQL connection still points at
// the logical database incarnation observed by the process.
// postgresidentity 校验 PostgreSQL 连接是否仍指向进程首次成功观测到的逻辑数据库实例。
package postgresidentity

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const identityQuery = `SELECT
	current_database(),
	(SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database()),
	pg_catalog.pg_postmaster_start_time(),
	(pg_catalog.pg_control_system()).system_identifier::text,
	CASE
		WHEN pg_catalog.pg_is_in_recovery() THEN NULL
		ELSE (
			SELECT s.timeline_id
			FROM pg_catalog.pg_split_walfile_name(
				pg_catalog.pg_walfile_name(pg_catalog.pg_current_wal_insert_lsn())
			) AS s
		)
	END`

var (
	errIdentityMismatch      = errors.New("postgres identity mismatch")
	errIdentityRecovery      = errors.New("postgres identity recovery")
	errIdentityMalformed     = errors.New("postgres identity malformed")
	errIdentityConfiguration = errors.New("postgres identity configuration invalid")
	errIdentityTransient     = errors.New("postgres identity temporarily unavailable")
)

// Guard owns the process-wide baseline for one logical PostgreSQL database.
// Guard 持有单个逻辑 PostgreSQL 数据库的进程级基线。
type Guard struct {
	mu sync.Mutex

	baselineSet bool
	baseline    [sha256.Size]byte
	tripped     error
	onTrip      func(error)
}

// NewGuard creates a guard. onTrip publishes terminal state synchronously at
// most once under the guard mutex, before Verify/Tripped return. It must only
// perform bounded in-memory publication: no I/O, pool closure, waiting, or
// guard reentry. Shutdown remains in the server coordinator outside pgx hooks.
// NewGuard 创建 Guard；onTrip 在锁内至多同步发布一次终态，并先于 Verify/Tripped 返回。
// 回调只能执行有界内存发布，禁止 I/O、关闭池、等待或重入 Guard；关闭由 hook 外协调器负责。
func NewGuard(onTrip func(error)) *Guard {
	return &Guard{onTrip: onTrip}
}

// AfterConnect validates a new pooled connection before pgx adds it to the pool.
// AfterConnect 在 pgx 将新连接加入连接池前完成校验。
func (g *Guard) AfterConnect(ctx context.Context, conn *pgx.Conn) error {
	return g.Verify(ctx, conn)
}

// Verify validates the current connection before business SQL uses it.
// Verify 在业务 SQL 使用当前连接前完成校验。
func (g *Guard) Verify(ctx context.Context, conn *pgx.Conn) error {
	if g == nil {
		return nil
	}
	if ctx == nil || conn == nil {
		return g.trip(errIdentityMalformed)
	}
	return g.verify(ctx, conn)
}

// Tripped returns the permanent terminal error, or nil while recovery remains possible.
// Tripped 返回永久 terminal 错误；仍可恢复时返回 nil。
func (g *Guard) Tripped() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.tripped
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type identity struct {
	database         string
	oid              uint32
	postmasterStart  time.Time
	systemIdentifier string
	timelineID       uint32
}

type canonicalIdentity struct {
	Database         string `json:"database"`
	OID              uint32 `json:"oid"`
	PostmasterStart  string `json:"postmasterStart"`
	SystemIdentifier string `json:"systemIdentifier"`
	TimelineID       uint32 `json:"timelineId"`
}

func (g *Guard) verify(ctx context.Context, conn rowQuerier) error {
	if err := g.Tripped(); err != nil {
		return err
	}
	if ctx == nil || conn == nil {
		return g.trip(errIdentityMalformed)
	}

	var database string
	var oid uint32
	var postmasterStart time.Time
	var systemIdentifier string
	var timelineID *uint32
	if err := conn.QueryRow(ctx, identityQuery).Scan(
		&database,
		&oid,
		&postmasterStart,
		&systemIdentifier,
		&timelineID,
	); err != nil {
		if isTransient(err) {
			return errIdentityTransient
		}
		return g.trip(errIdentityConfiguration)
	}
	if timelineID == nil {
		return g.trip(errIdentityRecovery)
	}

	observed, err := newIdentity(database, oid, postmasterStart, systemIdentifier, *timelineID)
	if err != nil {
		return g.trip(errIdentityMalformed)
	}
	digest, err := digestIdentity(observed)
	if err != nil {
		return g.trip(errIdentityMalformed)
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.tripped != nil {
		return g.tripped
	}
	if !g.baselineSet {
		g.baseline = digest
		g.baselineSet = true
		return nil
	}
	if g.baseline != digest {
		return g.tripLocked(errIdentityMismatch)
	}
	return nil
}

func (g *Guard) trip(err error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.tripLocked(err)
}

func (g *Guard) tripLocked(err error) error {
	if g.tripped != nil {
		return g.tripped
	}
	g.tripped = err
	if g.onTrip != nil {
		g.onTrip(err)
	}
	return err
}

func newIdentity(database string, oid uint32, postmasterStart time.Time, systemIdentifier string, timelineID uint32) (identity, error) {
	if database == "" || !utf8.ValidString(database) || oid == 0 || postmasterStart.IsZero() || postmasterStart.Year() < 1 || postmasterStart.Year() > 9999 || timelineID == 0 {
		return identity{}, errIdentityMalformed
	}
	if !canonicalSystemIdentifier(systemIdentifier) {
		return identity{}, errIdentityMalformed
	}
	return identity{
		database:         database,
		oid:              oid,
		postmasterStart:  postmasterStart.UTC(),
		systemIdentifier: systemIdentifier,
		timelineID:       timelineID,
	}, nil
}

func canonicalSystemIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return err == nil && parsed != 0 && strconv.FormatUint(parsed, 10) == value
}

func digestIdentity(observed identity) ([sha256.Size]byte, error) {
	wire, err := canonicalIdentityBytes(observed)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(wire), nil
}

func canonicalIdentityBytes(observed identity) ([]byte, error) {
	wire, err := json.Marshal(canonicalIdentity{
		Database:         observed.database,
		OID:              observed.oid,
		PostmasterStart:  observed.postmasterStart.UTC().Format(time.RFC3339Nano),
		SystemIdentifier: observed.systemIdentifier,
		TimelineID:       observed.timelineID,
	})
	if err != nil {
		return nil, err
	}
	return wire, nil
}

func isTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr == nil {
		return false
	}
	if strings.HasPrefix(pgErr.Code, "08") {
		return true
	}
	switch pgErr.Code {
	case "57P01", "57P02", "57P03", "57014":
		return true
	default:
		return false
	}
}
