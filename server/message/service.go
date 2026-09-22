package message

import (
	"context"
	"strconv"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/conversation"
)

const defaultRequestTimeout = 5 * time.Second

// Config is trusted service configuration, never request input.
// Config 仅接受可信服务配置，不接受客户端请求输入。
type Config struct {
	IDs            IDGenerator
	Clock          Clock
	Observer       Observer
	RequestTimeout time.Duration
}

// Service coordinates send validation, authorization and durable persistence.
// Service 编排发送校验、授权与持久化，不拥有网络或数据库连接。
type Service struct {
	store          Store
	ids            IDGenerator
	clock          Clock
	observer       Observer
	requestTimeout time.Duration
}

// NewService constructs a send service from trusted configuration.
// NewService 使用可信配置构造发送服务。
func NewService(store Store, config Config) (*Service, error) {
	if store == nil || config.IDs == nil || config.Clock == nil {
		return nil, Fail(SendInvalidInput)
	}
	timeout := config.RequestTimeout
	if timeout == 0 {
		timeout = defaultRequestTimeout
	}
	if timeout < time.Millisecond || timeout > defaultRequestTimeout {
		return nil, Fail(SendInvalidInput)
	}
	return &Service{store: store, ids: config.IDs, clock: config.Clock, observer: config.Observer, requestTimeout: timeout}, nil
}

// Send returns a SERVER_PERSISTED ACK only after the storage transaction commits.
// Send 仅在存储事务提交后返回 SERVER_PERSISTED ACK；提交不确定时绝不返回成功。
func (s *Service) Send(ctx context.Context, identity session.ConnectionIdentity, request protocol.Send) (frame protocol.ServerFrame, err error) {
	started := time.Now()
	defer func() { s.observe(started, err) }()
	if s == nil || s.store == nil || s.ids == nil || s.clock == nil || ctx == nil {
		return protocol.ServerFrame{}, Fail(SendInvalidInput)
	}
	if !validIdentity(identity) {
		return protocol.ServerFrame{}, Fail(SendInvalidInput)
	}
	if _, encodeErr := protocol.EncodeSend(request); encodeErr != nil {
		return protocol.ServerFrame{}, Fail(SendInvalidInput)
	}
	principal := conversation.Principal{UserID: identity.UserID()}
	generate := func() (Generated, error) {
		now := s.clock.Now()
		if now.IsZero() {
			return Generated{}, Fail(SendInvalidInput)
		}
		serverTime := now.UnixMilli()
		if serverTime < 0 {
			return Generated{}, Fail(SendInvalidInput)
		}
		serverMsgID, idErr := s.ids.NewID()
		if idErr != nil {
			return Generated{}, Fail(SendStorageUnavailable)
		}
		eventID, idErr := s.ids.NewID()
		if idErr != nil {
			return Generated{}, Fail(SendStorageUnavailable)
		}
		if !validIdentifier(serverMsgID) || !validIdentifier(eventID) {
			return Generated{}, Fail(SendInvalidInput)
		}
		return Generated{ServerMsgID: serverMsgID, EventID: eventID, ServerTime: serverTime}, nil
	}
	attemptCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	persisted, persistErr := s.store.Persist(attemptCtx, principal, request, generate)
	if persistErr != nil {
		return protocol.ServerFrame{}, redactedError(persistErr)
	}
	if persisted.ClientMsgID != request.ClientMsgID || persisted.ConversationID != request.ConversationID ||
		persisted.SenderID != identity.UserID() || persisted.ConversationSeq < 0 || persisted.ServerTime < 0 {
		return protocol.ServerFrame{}, Fail(SendStorageUnavailable)
	}
	ack := protocol.Ack{
		ClientMsgID:     persisted.ClientMsgID,
		ConversationID:  persisted.ConversationID,
		SenderID:        persisted.SenderID,
		ServerMsgID:     persisted.ServerMsgID,
		ConversationSeq: strconv.FormatInt(persisted.ConversationSeq, 10),
		ServerTime:      strconv.FormatInt(persisted.ServerTime, 10),
	}
	frame = protocol.ServerFrame{Ack: &ack}
	if _, encodeErr := protocol.EncodeServerFrame(frame); encodeErr != nil {
		return protocol.ServerFrame{}, Fail(SendStorageUnavailable)
	}
	return frame, nil
}

func validIdentity(identity session.ConnectionIdentity) bool {
	if !validIdentifier(identity.UserID()) || !validIdentifier(identity.DeviceID()) ||
		!validIdentifier(identity.SessionID()) || !validIdentifier(identity.ConnectionID()) {
		return false
	}
	tokenID := identity.TokenID()
	if len(tokenID) != 32 {
		return false
	}
	for i := 0; i < len(tokenID); i++ {
		ch := tokenID[i]
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return false
		}
	}
	return true
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

func redactedError(err error) error {
	if err == nil {
		return nil
	}
	code := ErrorCode(err)
	if code == "" {
		code = SendUnknown
	}
	return &Error{Code: code, Disposition: RetryDisposition(err)}
}

func (s *Service) observe(started time.Time, err error) {
	if s == nil || s.observer == nil {
		return
	}
	observation := Observation{Operation: "send", Code: ErrorCode(err), Elapsed: time.Since(started)}
	if err == nil {
		observation.Code = Code("SERVER_PERSISTED")
	}
	defer func() { _ = recover() }()
	s.observer.Observe(observation)
}
