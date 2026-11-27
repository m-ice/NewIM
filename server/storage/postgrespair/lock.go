// Package postgrespair provides the shared transaction lock for one user pair.
// postgrespair 提供账号对共用的事务锁。
package postgrespair

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Lock serializes operations on a pair of validated account IDs in either order.
// Lock 按无向账号对串行执行事务，调用方须先校验账号标识。
func Lock(ctx context.Context, tx pgx.Tx, first, second string) error {
	if first > second {
		first, second = second, first
	}
	_, err := tx.Exec(ctx, `SELECT pg_catalog.pg_advisory_xact_lock(
		pg_catalog.hashtextextended('newim.private-message:' || $1::text || ':' || $2::text, 0))`, first, second)
	return err
}
