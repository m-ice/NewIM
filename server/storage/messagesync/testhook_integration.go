//go:build integration

package messagesync

import (
	"context"

	app "github.com/m-ice/NewIM/server/sync/message"
)

// ReadWithTestHook runs one read with a hook after the read-only snapshot is
// established. It exists only in integration-tagged test builds.
// ReadWithTestHook 仅在集成测试构建中运行快照后的测试钩子。
func (r *Repository) ReadWithTestHook(ctx context.Context, user string, request app.ReadRequest, hook func() error) (app.ReadResult, error) {
	return r.read(ctx, user, request, hook)
}
