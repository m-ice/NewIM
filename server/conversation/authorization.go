// Package conversation defines the trusted sender authorization seam.
// conversation 定义可信发送者授权边界；不实现设备、踢除、已读或静音策略。
package conversation

import "context"

// Principal is trusted server authority derived from an authenticated connection.
// Principal 是来自已认证连接的可信服务端权限身份。
type Principal struct{ UserID string }

// Valid reports whether the sender identifier uses the accepted ASCII grammar.
// Valid 判断发送者标识是否符合既定 ASCII 语法。
func (p Principal) Valid() bool { return validIdentifier(p.UserID) }

// Row is the minimal result shape needed by the authorizer.
// Row 是授权检查所需的最小结果形状。
type Row interface{ Scan(...any) error }

// Tx is the storage transaction seam used by authorization.
// Tx 是授权使用的存储事务边界。
type Tx interface {
	QueryRow(context.Context, string, ...any) Row
}

// Authorizer verifies membership before persistence in the caller's transaction.
// Authorizer 在调用方事务中先校验会话成员关系，再允许持久化。
type Authorizer interface {
	Authorize(context.Context, Tx, string, string) error
}

func validIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if !((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}
