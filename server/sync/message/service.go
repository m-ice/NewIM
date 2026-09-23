package message

import (
	"context"
	"errors"
	"time"

	"github.com/m-ice/NewIM/server/auth/session"
)

const defaultRequestTimeout = 5 * time.Second

// Service coordinates validation and bounded page assembly.
// Service 编排校验与有界页组装，不拥有数据库连接。
type Service struct {
	store          Store
	observer       Observer
	requestTimeout time.Duration
}

// NewService constructs a message sync service from trusted configuration.
// NewService 使用可信配置构造消息同步服务。
func NewService(store Store, config Config) (*Service, error) {
	if store == nil {
		return nil, Fail(StorageUnavailable)
	}
	timeout := config.RequestTimeout
	if timeout == 0 {
		timeout = defaultRequestTimeout
	}
	if timeout < time.Millisecond || timeout > defaultRequestTimeout {
		return nil, Fail(StorageUnavailable)
	}
	return &Service{store: store, observer: config.Observer, requestTimeout: timeout}, nil
}

// ReadAfterSeq validates the caller and returns a bounded message page.
// ReadAfterSeq 校验调用者并返回有界消息页；错误永远返回零页。
func (s *Service) ReadAfterSeq(ctx context.Context, identity session.ConnectionIdentity, request ReadRequest) (page Page, err error) {
	started := time.Now()
	defer func() { s.observe(started, err, page) }()
	if s == nil || s.store == nil {
		return Page{}, Fail(StorageUnavailable)
	}
	// These checks deliberately precede every store call.
	// 这些检查必须先于任何 store 调用。
	if !validIdentity(identity) || !identifier(request.ConversationID) {
		return Page{}, Fail(Forbidden)
	}
	if ctx == nil {
		return Page{}, Fail(StorageUnavailable)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	result, readErr := s.store.Read(attemptCtx, identity.UserID(), request)
	if readErr != nil {
		return Page{}, redact(readErr)
	}
	return s.assemble(request, result)
}

func (s *Service) assemble(request ReadRequest, result ReadResult) (Page, error) {
	if result.LatestSeq < 0 || result.LatestSeq > MaxSequence || result.Limit < 1 || result.Limit > MaxPageItems || len(result.Rows) > result.Limit+1 {
		return Page{}, Fail(StorageUnavailable)
	}
	if !request.HasAfterSeq {
		if request.AfterSeq != 0 {
			return Page{}, Fail(InvalidCursor)
		}
	} else {
		if request.AfterSeq < 0 || request.AfterSeq > result.LatestSeq {
			return Page{}, Fail(InvalidCursor)
		}
	}
	if err := validateRows(request, result); err != nil {
		return Page{}, err
	}
	rows := result.Rows
	hasNext := false
	if len(rows) > result.Limit {
		rows = rows[:result.Limit]
		hasNext = true
	}
	items := make([]Item, 0, len(rows))
	used := pageOverhead
	for _, row := range rows {
		cost := itemCost(row)
		if used+cost > MaxResponseBytes {
			if len(items) == 0 {
				return Page{}, Fail(LimitExceeded)
			}
			hasNext = true
			break
		}
		items = append(items, cloneItem(row))
		used += cost
	}
	if len(rows) > len(items) && len(items) > 0 {
		hasNext = true
	}
	page := Page{Items: items, LatestSeq: result.LatestSeq, HasNext: hasNext}
	if len(items) > 0 {
		next := items[len(items)-1].ConversationSeq
		page.NextAfterSeq = &next
	}
	return page, nil
}

func validateRows(request ReadRequest, result ReadResult) error {
	if len(result.Rows) == 0 {
		if !request.HasAfterSeq {
			if result.LatestSeq != 0 {
				return Fail(StorageUnavailable)
			}
			return nil
		}
		if request.AfterSeq == result.LatestSeq {
			return nil
		}
		return Fail(StorageUnavailable)
	}
	for i, row := range result.Rows {
		if row.ConversationID != request.ConversationID || !identifier(row.ServerMsgID) || !identifier(row.ClientMsgID) || !identifier(row.SenderID) || !identifier(row.ConversationID) {
			return Fail(StorageUnavailable)
		}
		if row.ConversationSeq <= request.AfterSeq || row.ConversationSeq > result.LatestSeq {
			return Fail(StorageUnavailable)
		}
		if row.ProtocolVersion == 0 || row.ProtocolVersion > 2147483647 || row.SchemaVersion == 0 || row.SchemaVersion > 2147483647 || !messageType(row.MessageType) || row.ServerTime < 0 || len(row.Payload) < 2 || len(row.Payload) > 65536 {
			return Fail(StorageUnavailable)
		}
		if i > 0 && row.ConversationSeq != result.Rows[i-1].ConversationSeq+1 {
			return Fail(StorageUnavailable)
		}
	}
	expected := int64(1)
	if request.HasAfterSeq && request.AfterSeq > 0 {
		if request.AfterSeq == MaxSequence {
			return Fail(StorageUnavailable)
		}
		expected = request.AfterSeq + 1
	}
	if result.Rows[0].ConversationSeq != expected {
		return Fail(StorageUnavailable)
	}
	return nil
}

func itemCost(item Item) int {
	return len(item.Payload) + len(item.ServerMsgID) + len(item.ClientMsgID) + len(item.SenderID) + len(item.ConversationID) + len(item.MessageType) + itemOverhead
}

func cloneItem(item Item) Item {
	item.Payload = append([]byte(nil), item.Payload...)
	return item
}

func (s *Service) observe(started time.Time, err error, page Page) {
	if s == nil || s.observer == nil {
		return
	}
	code := ErrorCode(err)
	bytes := 0
	for _, item := range page.Items {
		bytes += itemCost(item)
	}
	defer func() { _ = recover() }()
	s.observer.Observe(Observation{Operation: "read_after_seq", Code: code, Elapsed: time.Since(started), Items: len(page.Items), Bytes: bytes})
}

func redact(err error) error {
	if err == nil {
		return nil
	}
	var known *Error
	if errors.As(err, &known) && known != nil {
		return Fail(known.Code)
	}
	return Fail(StorageUnavailable)
}

func validIdentity(identity session.ConnectionIdentity) bool {
	if !identifier(identity.UserID()) || !identifier(identity.DeviceID()) || !identifier(identity.SessionID()) || !identifier(identity.ConnectionID()) {
		return false
	}
	token := identity.TokenID()
	if len(token) != 32 {
		return false
	}
	for i := 0; i < len(token); i++ {
		c := token[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func identifier(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func messageType(value string) bool {
	if len(value) == 0 || len(value) > 64 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for i := 1; i < len(value); i++ {
		c := value[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}
	return true
}
