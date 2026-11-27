package message

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/m-ice/NewIM/server/message"
)

// ActorTypesAvailable checks whether the database supports persisted actor types.
// ActorTypesAvailable 检查数据库是否支持持久账号分类。
func ActorTypesAvailable(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	if ctx == nil || pool == nil {
		return false, app.Fail(app.SendStorageUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	var installed bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('newim.im_user_actor_types') IS NOT NULL`).Scan(&installed); err != nil {
		return false, app.Fail(app.SendStorageUnavailable)
	}
	if installed {
		if _, err := pool.Exec(ctx, `SELECT user_id,actor_type FROM newim.im_user_actor_types LIMIT 0`); err != nil {
			return false, app.Fail(app.SendStorageUnavailable)
		}
	}
	return installed, nil
}
