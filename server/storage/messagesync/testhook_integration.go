//go:build integration

package messagesync

import (
	"context"

	"github.com/jackc/pgx/v5"
	app "github.com/m-ice/NewIM/server/sync/message"
)

// ReadWithTestHook runs one read with a hook after the read-only snapshot is
// established. It exists only in integration-tagged test builds.
// ReadWithTestHook 仅在集成测试构建中运行快照后的测试钩子。
func (r *Repository) ReadWithTestHook(ctx context.Context, user string, request app.ReadRequest, hook func() error) (app.ReadResult, error) {
	return r.read(ctx, user, request, hook)
}

// SetReadTxOptionsForTest changes only this repository instance and exists only
// in integration-tagged test builds.
// SetReadTxOptionsForTest 仅用于集成测试且只改变当前 repository 实例。
func (r *Repository) SetReadTxOptionsForTest(options pgx.TxOptions) {
	r.readTxOptions = options
}
