// Package membership detects the member row shape used by the current transaction.
package membership

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

var errInvalidSchema = errors.New("unsupported conversation member schema")

// Advanced returns whether the member table has the managed-group columns.
// Hold the table schema stable through the caller's transaction so an online
// migration cannot change the meaning of a legacy authorization query.
func Advanced(ctx context.Context, tx pgx.Tx) (bool, error) {
	if _, err := tx.Exec(ctx, `LOCK TABLE newim.im_conversation_members IN ACCESS SHARE MODE`); err != nil {
		return false, err
	}
	var totalColumns, baseColumns, managedColumns, validManagedColumns int
	var groupTable bool
	err := tx.QueryRow(ctx, `SELECT count(*),
		count(*) FILTER (WHERE attname IN ('conversation_id', 'user_id')
			AND atttypid = 'newim.identifier'::pg_catalog.regtype AND attnotnull),
		count(*) FILTER (WHERE attname IN ('group_role', 'mute_until', 'history_after_seq',
			'departed_at', 'departed_through_seq')),
		count(*) FILTER (WHERE
			(attname = 'group_role' AND atttypid = 'pg_catalog.text'::pg_catalog.regtype AND NOT attnotnull)
			OR (attname IN ('mute_until', 'departed_at')
				AND atttypid = 'pg_catalog.timestamptz'::pg_catalog.regtype AND NOT attnotnull)
			OR (attname = 'history_after_seq' AND atttypid = 'pg_catalog.int8'::pg_catalog.regtype AND attnotnull)
			OR (attname = 'departed_through_seq' AND atttypid = 'pg_catalog.int8'::pg_catalog.regtype AND NOT attnotnull)),
		pg_catalog.to_regclass('newim.im_groups') IS NOT NULL
	FROM pg_catalog.pg_attribute
	WHERE attrelid = 'newim.im_conversation_members'::pg_catalog.regclass AND attnum > 0 AND NOT attisdropped`).
		Scan(&totalColumns, &baseColumns, &managedColumns, &validManagedColumns, &groupTable)
	if err != nil {
		return false, err
	}
	switch {
	case totalColumns == 2 && baseColumns == 2 && !groupTable:
		return false, nil
	case baseColumns == 2 && managedColumns == 5 && validManagedColumns == 5 && groupTable:
		return true, nil
	default:
		return false, errInvalidSchema
	}
}
