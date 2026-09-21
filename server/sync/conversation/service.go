package conversation

import (
	"context"
	"encoding/json"
	"time"
)

const defaultRequestTimeout = 5 * time.Second

// Config is trusted service configuration, never request input.
// Config 仅接受可信服务配置，不接受客户端输入。
type Config struct {
	Keys           map[string][]byte
	ActiveKeyID    string
	Clock          func() time.Time
	Observer       Observer
	RequestTimeout time.Duration
}

// Service coordinates bounded reads and authenticated continuation tokens.
// Service 编排有界读取与认证续页游标，不拥有网络或数据库连接。
type Service struct {
	store          Store
	keys           map[string][]byte
	activeKeyID    string
	clock          func() time.Time
	observer       Observer
	requestTimeout time.Duration
}

// NewService constructs an application service from trusted configuration.
// NewService 使用可信配置构造 application service；不生成或持久化密钥。
func NewService(store Store, config Config) (*Service, error) {
	if store == nil || len(config.Keys) == 0 || !validKeyID(config.ActiveKeyID) {
		return nil, Fail(StorageUnavailable)
	}
	keys := make(map[string][]byte, len(config.Keys))
	for keyID, key := range config.Keys {
		if !validKeyID(keyID) || len(key) < 32 {
			return nil, Fail(StorageUnavailable)
		}
		keys[keyID] = append([]byte(nil), key...)
	}
	if _, ok := keys[config.ActiveKeyID]; !ok {
		return nil, Fail(StorageUnavailable)
	}
	timeout := config.RequestTimeout
	if timeout == 0 {
		timeout = defaultRequestTimeout
	}
	if timeout < 0 || timeout > defaultRequestTimeout {
		return nil, Fail(StorageUnavailable)
	}
	clock := config.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Service{
		store:          store,
		keys:           keys,
		activeKeyID:    config.ActiveKeyID,
		clock:          clock,
		observer:       config.Observer,
		requestTimeout: timeout,
	}, nil
}

// BeginBootstrap starts a bounded directory snapshot at committed head H.
// BeginBootstrap 从已提交 head H 开始有界目录快照；不物化账户全量。
func (s *Service) BeginBootstrap(ctx context.Context, principal Principal, limit int) (page Page, err error) {
	started := time.Now()
	var candidates int
	defer func() { s.observe("begin_bootstrap", started, err, candidates, page) }()
	if s == nil || s.store == nil {
		return Page{}, Fail(StorageUnavailable)
	}
	if !validIdentifier(principal.UserID) {
		return Page{}, Fail(Forbidden)
	}
	pageLimit, err := normalizeLimit(limit)
	if err != nil {
		return Page{}, err
	}
	now, err := s.currentUnix()
	if err != nil {
		return Page{}, err
	}
	roundExpires, err := addSeconds(now, uint64(bootstrapRoundLifetime/time.Second))
	if err != nil {
		return Page{}, err
	}
	result, err := s.read(ctx, principal.UserID, ReadRequest{Kind: BootstrapRead, Start: true, Limit: pageLimit})
	if err != nil {
		return Page{}, err
	}
	candidates = len(result.Candidates)
	if err = validateCommonReadResult(result); err != nil {
		return Page{}, err
	}
	if result.Fence != result.Head {
		return Page{}, Fail(StorageUnavailable)
	}
	if err = validateBootstrapCandidates(result, "", result.TerminalKey); err != nil {
		return Page{}, err
	}
	base := syncCursor{
		kind:        cursorKindBootstrapPage,
		keyID:       s.activeKeyID,
		account:     principal.UserID,
		epoch:       result.Epoch,
		issued:      now,
		expires:     roundExpires,
		limit:       pageLimit,
		fence:       result.Fence,
		terminalKey: result.TerminalKey,
	}
	entries, position, hasMore, err := assembleBootstrap(result, base)
	if err != nil {
		return Page{}, err
	}
	page, err = s.renderAssembledPage(base, entries, position, hasMore, now)
	if err != nil {
		return Page{}, err
	}
	return page, nil
}

// ContinueBootstrap resumes the frozen H fence from a bootstrap page cursor.
// ContinueBootstrap 使用已认证 bootstrap page cursor 恢复冻结的 H fence。
func (s *Service) ContinueBootstrap(ctx context.Context, principal Principal, cursor string) (page Page, err error) {
	started := time.Now()
	var candidates int
	defer func() { s.observe("continue_bootstrap", started, err, candidates, page) }()
	if s == nil || s.store == nil {
		return Page{}, Fail(StorageUnavailable)
	}
	if !validIdentifier(principal.UserID) {
		return Page{}, Fail(Forbidden)
	}
	now, err := s.currentUnix()
	if err != nil {
		return Page{}, err
	}
	token, err := decodeCursor(s.keys, cursor, cursorKindBootstrapPage, principal.UserID, now)
	if err != nil {
		return Page{}, err
	}
	result, err := s.read(ctx, principal.UserID, ReadRequest{
		Kind:        BootstrapRead,
		Start:       false,
		Epoch:       token.epoch,
		Fence:       token.fence,
		AfterKey:    token.afterKey,
		TerminalKey: token.terminalKey,
		Limit:       token.limit,
	})
	if err != nil {
		return Page{}, err
	}
	candidates = len(result.Candidates)
	if err = validateCommonReadResult(result); err != nil {
		return Page{}, err
	}
	if result.Epoch != token.epoch || result.Floor > token.fence {
		return Page{}, Fail(CursorExpired)
	}
	if result.Fence != token.fence || result.Head < token.fence || result.TerminalKey != token.terminalKey {
		return Page{}, Fail(StorageUnavailable)
	}
	if err = validateBootstrapCandidates(result, token.afterKey, token.terminalKey); err != nil {
		return Page{}, err
	}
	entries, position, hasMore, err := assembleBootstrap(result, token)
	if err != nil {
		return Page{}, err
	}
	page, err = s.renderAssembledPage(token, entries, position, hasMore, now)
	if err != nil {
		return Page{}, err
	}
	return page, nil
}

// BeginDelta starts a new frozen delta round from a checkpoint.
// BeginDelta 从 checkpoint 开始新的冻结增量轮次并捕获 H2。
func (s *Service) BeginDelta(ctx context.Context, principal Principal, checkpoint string, limit int) (page Page, err error) {
	started := time.Now()
	var candidates int
	defer func() { s.observe("begin_delta", started, err, candidates, page) }()
	if s == nil || s.store == nil {
		return Page{}, Fail(StorageUnavailable)
	}
	if !validIdentifier(principal.UserID) {
		return Page{}, Fail(Forbidden)
	}
	pageLimit, err := normalizeLimit(limit)
	if err != nil {
		return Page{}, err
	}
	now, err := s.currentUnix()
	if err != nil {
		return Page{}, err
	}
	token, err := decodeCursor(s.keys, checkpoint, cursorKindCheckpoint, principal.UserID, now)
	if err != nil {
		return Page{}, err
	}
	roundExpires, err := addSeconds(now, uint64(bootstrapRoundLifetime/time.Second))
	if err != nil {
		return Page{}, err
	}
	result, err := s.read(ctx, principal.UserID, ReadRequest{
		Kind:     DeltaRead,
		Start:    true,
		Epoch:    token.epoch,
		Fence:    token.fence,
		AfterSeq: token.fence,
		Limit:    pageLimit,
	})
	if err != nil {
		return Page{}, err
	}
	candidates = len(result.Candidates)
	if err = validateCommonReadResult(result); err != nil {
		return Page{}, err
	}
	if result.Epoch != token.epoch || result.Floor > token.fence || result.Head < token.fence {
		return Page{}, Fail(CursorExpired)
	}
	if result.Fence != result.Head || result.Fence < token.fence {
		return Page{}, Fail(StorageUnavailable)
	}
	if err = validateDeltaCandidates(result, token.fence, result.Fence, pageLimit); err != nil {
		return Page{}, err
	}
	base := syncCursor{
		kind:     cursorKindDeltaPage,
		keyID:    s.activeKeyID,
		account:  principal.UserID,
		epoch:    result.Epoch,
		issued:   now,
		expires:  roundExpires,
		limit:    pageLimit,
		fence:    result.Fence,
		afterSeq: token.fence,
	}
	entries, position, hasMore, err := assembleDelta(result, base)
	if err != nil {
		return Page{}, err
	}
	page, err = s.renderAssembledPage(base, entries, position, hasMore, now)
	if err != nil {
		return Page{}, err
	}
	return page, nil
}

// ContinueDelta resumes a frozen delta page without moving its H2 fence.
// ContinueDelta 在不移动 H2 fence 的前提下恢复 delta page。
func (s *Service) ContinueDelta(ctx context.Context, principal Principal, cursor string) (page Page, err error) {
	started := time.Now()
	var candidates int
	defer func() { s.observe("continue_delta", started, err, candidates, page) }()
	if s == nil || s.store == nil {
		return Page{}, Fail(StorageUnavailable)
	}
	if !validIdentifier(principal.UserID) {
		return Page{}, Fail(Forbidden)
	}
	now, err := s.currentUnix()
	if err != nil {
		return Page{}, err
	}
	token, err := decodeCursor(s.keys, cursor, cursorKindDeltaPage, principal.UserID, now)
	if err != nil {
		return Page{}, err
	}
	result, err := s.read(ctx, principal.UserID, ReadRequest{
		Kind:     DeltaRead,
		Start:    false,
		Epoch:    token.epoch,
		Fence:    token.fence,
		AfterSeq: token.afterSeq,
		Limit:    token.limit,
	})
	if err != nil {
		return Page{}, err
	}
	candidates = len(result.Candidates)
	if err = validateCommonReadResult(result); err != nil {
		return Page{}, err
	}
	if result.Epoch != token.epoch || result.Floor > token.afterSeq {
		return Page{}, Fail(CursorExpired)
	}
	if result.Fence != token.fence || result.Head < token.fence {
		return Page{}, Fail(StorageUnavailable)
	}
	if err = validateDeltaCandidates(result, token.afterSeq, token.fence, token.limit); err != nil {
		return Page{}, err
	}
	entries, position, hasMore, err := assembleDelta(result, token)
	if err != nil {
		return Page{}, err
	}
	page, err = s.renderAssembledPage(token, entries, position, hasMore, now)
	if err != nil {
		return Page{}, err
	}
	return page, nil
}

func (s *Service) read(ctx context.Context, user string, request ReadRequest) (ReadResult, error) {
	if ctx == nil || s == nil || s.store == nil {
		return ReadResult{}, Fail(StorageUnavailable)
	}
	readCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	result, err := s.store.Read(readCtx, user, request)
	if err != nil {
		return ReadResult{}, redactError(err)
	}
	return result, nil
}

func redactError(err error) error {
	if err == nil {
		return nil
	}
	code := ErrorCode(err)
	if code == "" {
		code = StorageUnavailable
	}
	return Fail(code)
}

func normalizeLimit(limit int) (int, error) {
	if limit == 0 {
		return MaxPageItems, nil
	}
	if limit < 1 || limit > MaxPageItems {
		return 0, Fail(LimitExceeded)
	}
	return limit, nil
}

func (s *Service) currentUnix() (uint64, error) {
	if s == nil || s.clock == nil {
		return 0, Fail(StorageUnavailable)
	}
	now := s.clock()
	if now.IsZero() {
		return 0, Fail(StorageUnavailable)
	}
	unix := now.Unix()
	if unix < 0 || uint64(unix) > MaxSequence {
		return 0, Fail(StorageUnavailable)
	}
	return uint64(unix), nil
}

func addSeconds(now, seconds uint64) (uint64, error) {
	if now > MaxSequence || seconds > MaxSequence || now > MaxSequence-seconds {
		return 0, Fail(StorageUnavailable)
	}
	return now + seconds, nil
}

func validateCommonReadResult(result ReadResult) error {
	if result.Epoch == 0 || !validSequence(result.Epoch) || !validSequence(result.Head) || !validSequence(result.Floor) || !validSequence(result.Fence) {
		return Fail(StorageUnavailable)
	}
	if result.Floor > result.Head {
		return Fail(StorageUnavailable)
	}
	if result.TerminalKey != "" && !validIdentifier(result.TerminalKey) {
		return Fail(StorageUnavailable)
	}
	return nil
}

func validateBootstrapCandidates(result ReadResult, afterKey, terminalKey string) error {
	if len(result.Candidates) > MaxDirectoryCandidates {
		return Fail(StorageUnavailable)
	}
	if afterKey != "" && !validIdentifier(afterKey) {
		return Fail(InvalidCursor)
	}
	if terminalKey != "" && !validIdentifier(terminalKey) {
		return Fail(InvalidCursor)
	}
	if terminalKey == "" {
		if len(result.Candidates) != 0 || !result.DirectoryExhausted || result.Fence != 0 {
			return Fail(StorageUnavailable)
		}
		return nil
	}
	previous := afterKey
	for _, candidate := range result.Candidates {
		if !validIdentifier(candidate.Key) || candidate.Key <= previous {
			return Fail(StorageUnavailable)
		}
		if terminalKey != "" && candidate.Key > terminalKey {
			return Fail(StorageUnavailable)
		}
		previous = candidate.Key
		if candidate.State != nil {
			if err := validateItem(*candidate.State); err != nil {
				return err
			}
			if candidate.State.ConversationID != candidate.Key || candidate.State.Revision > result.Fence {
				return Fail(StorageUnavailable)
			}
		} else if candidate.Member || candidate.Latest != nil {
			return Fail(StorageUnavailable)
		}
		if candidate.Latest != nil {
			if err := validateItem(*candidate.Latest); err != nil {
				return err
			}
			if candidate.Latest.ConversationID != candidate.Key {
				return Fail(StorageUnavailable)
			}
		}
	}
	if result.DirectoryExhausted {
		if terminalKey == "" {
			if len(result.Candidates) != 0 {
				return Fail(StorageUnavailable)
			}
		} else if previous != terminalKey {
			return Fail(StorageUnavailable)
		}
	} else if len(result.Candidates) == 0 {
		return Fail(StorageUnavailable)
	}
	return nil
}

func validateDeltaCandidates(result ReadResult, afterSeq, fence uint64, limit int) error {
	if len(result.Candidates) > limit+1 || result.TerminalKey != "" || fence < afterSeq {
		return Fail(StorageUnavailable)
	}
	previous := afterSeq
	for _, candidate := range result.Candidates {
		if candidate.State == nil || !validIdentifier(candidate.Key) {
			return Fail(StorageUnavailable)
		}
		if err := validateItem(*candidate.State); err != nil {
			return err
		}
		if candidate.State.ConversationID != candidate.Key || candidate.State.Revision <= previous || candidate.State.Revision > fence {
			return Fail(StorageUnavailable)
		}
		previous = candidate.State.Revision
		if candidate.Latest != nil {
			if err := validateItem(*candidate.Latest); err != nil {
				return err
			}
			if candidate.Latest.ConversationID != candidate.Key {
				return Fail(StorageUnavailable)
			}
		}
	}
	return nil
}

func validateItem(item Item) error {
	if !validIdentifier(item.ConversationID) || item.Revision == 0 || !validSequence(item.Revision) {
		return Fail(StorageUnavailable)
	}
	switch item.Kind {
	case "upsert":
		if item.LatestConversationSeq == nil || !validSequence(*item.LatestConversationSeq) || !validOptionalIdentifier(item.LatestServerMsgID) {
			return Fail(StorageUnavailable)
		}
	case "remove":
		if item.LatestConversationSeq != nil || item.LatestServerMsgID != nil {
			return Fail(StorageUnavailable)
		}
	default:
		return Fail(StorageUnavailable)
	}
	return nil
}

func assembleBootstrap(result ReadResult, base syncCursor) ([]itemEnvelope, cursorPosition, bool, error) {
	entries := make([]itemEnvelope, 0, base.limit)
	position := cursorPosition{key: base.afterKey}
	hasMore := !result.DirectoryExhausted
	for index, candidate := range result.Candidates {
		item, include, err := selectBootstrapCandidate(candidate, result.Fence)
		if err != nil {
			return nil, cursorPosition{}, false, err
		}
		if !include {
			position = cursorPosition{key: candidate.Key}
			continue
		}
		if len(entries) >= base.limit {
			hasMore = true
			break
		}
		entries = append(entries, itemEnvelope{
			item:   *item,
			before: position,
			after:  cursorPosition{key: candidate.Key},
		})
		position = cursorPosition{key: candidate.Key}
		if len(entries) >= base.limit {
			hasMore = index+1 < len(result.Candidates) || !result.DirectoryExhausted
			break
		}
	}
	if !result.DirectoryExhausted {
		hasMore = true
	}
	return entries, position, hasMore, nil
}

func assembleDelta(result ReadResult, base syncCursor) ([]itemEnvelope, cursorPosition, bool, error) {
	entries := make([]itemEnvelope, 0, base.limit)
	position := cursorPosition{seq: base.afterSeq}
	hasMore := len(result.Candidates) > base.limit
	for index, candidate := range result.Candidates {
		item, err := selectDeltaCandidate(candidate)
		if err != nil {
			return nil, cursorPosition{}, false, err
		}
		if len(entries) >= base.limit {
			hasMore = true
			break
		}
		entries = append(entries, itemEnvelope{
			item:   *item,
			before: position,
			after:  cursorPosition{seq: item.Revision},
		})
		position = cursorPosition{seq: item.Revision}
		if len(entries) >= base.limit {
			hasMore = index+1 < len(result.Candidates)
			break
		}
	}
	return entries, position, hasMore, nil
}

func selectBootstrapCandidate(candidate Candidate, fence uint64) (*Item, bool, error) {
	if candidate.State == nil {
		return nil, false, nil
	}
	switch candidate.State.Kind {
	case "remove":
		if candidate.Latest != nil {
			return nil, false, Fail(StorageUnavailable)
		}
		return nil, false, nil
	case "upsert":
		if candidate.Member {
			if candidate.Latest != nil {
				return nil, false, Fail(StorageUnavailable)
			}
			return candidate.State, true, nil
		}
		if candidate.Latest == nil {
			return nil, false, Fail(NotReady)
		}
		switch candidate.Latest.Kind {
		case "remove":
			if candidate.Latest.Revision > fence {
				return nil, false, Fail(CursorExpired)
			}
			return nil, false, Fail(StorageUnavailable)
		case "upsert":
			return nil, false, Fail(NotReady)
		default:
			return nil, false, Fail(StorageUnavailable)
		}
	default:
		return nil, false, Fail(StorageUnavailable)
	}
}

func selectDeltaCandidate(candidate Candidate) (*Item, error) {
	if candidate.State == nil {
		return nil, Fail(StorageUnavailable)
	}
	switch candidate.State.Kind {
	case "remove":
		if candidate.Latest != nil {
			return nil, Fail(StorageUnavailable)
		}
		return candidate.State, nil
	case "upsert":
		if candidate.Member {
			if candidate.Latest != nil {
				return nil, Fail(StorageUnavailable)
			}
			return candidate.State, nil
		}
		if candidate.Latest == nil {
			return nil, Fail(NotReady)
		}
		switch candidate.Latest.Kind {
		case "remove":
			if candidate.Latest.Revision > candidate.State.Revision {
				return nil, Fail(CursorExpired)
			}
			return nil, Fail(StorageUnavailable)
		case "upsert":
			return nil, Fail(NotReady)
		default:
			return nil, Fail(StorageUnavailable)
		}
	default:
		return nil, Fail(StorageUnavailable)
	}
}

func (s *Service) renderAssembledPage(base syncCursor, entries []itemEnvelope, finalPosition cursorPosition, hasMore bool, now uint64) (Page, error) {
	minimumEntries := 0
	if len(entries) > 0 {
		minimumEntries = 1
	}
	for keep := len(entries); keep >= minimumEntries; keep-- {
		position := finalPosition
		if keep < len(entries) {
			position = entries[keep].before
		}
		more := hasMore || keep < len(entries)
		if more && !cursorAdvanced(base, position) {
			continue
		}
		page, err := s.renderPage(base, entries[:keep], position, more, now)
		if err == nil {
			return page, nil
		}
		if ErrorCode(err) != LimitExceeded {
			return Page{}, err
		}
	}
	return Page{}, Fail(LimitExceeded)
}

func cursorAdvanced(base syncCursor, position cursorPosition) bool {
	if base.kind == cursorKindBootstrapPage {
		return position.key != "" && position.key > base.afterKey
	}
	if base.kind == cursorKindDeltaPage {
		return position.seq > base.afterSeq
	}
	return false
}

func (s *Service) renderPage(base syncCursor, entries []itemEnvelope, position cursorPosition, more bool, now uint64) (Page, error) {
	items := make([]Item, len(entries))
	for index := range entries {
		items[index] = entries[index].item
	}

	var token syncCursor
	if more {
		if !base.isPage() {
			return Page{}, Fail(StorageUnavailable)
		}
		token = base
		token.keyID = s.activeKeyID
		token.afterKey = position.key
		token.afterSeq = position.seq
	} else {
		expires, err := addSeconds(now, uint64(checkpointLifetime/time.Second))
		if err != nil {
			return Page{}, err
		}
		token = syncCursor{
			kind:    cursorKindCheckpoint,
			keyID:   s.activeKeyID,
			account: base.account,
			epoch:   base.epoch,
			issued:  now,
			expires: expires,
			limit:   base.limit,
			fence:   base.fence,
		}
	}
	next, err := encodeCursor(s.keys, token)
	if err != nil {
		return Page{}, err
	}
	page := Page{Items: items, NextCursor: next, HasMore: more}
	encoded, err := json.Marshal(page)
	if err != nil {
		return Page{}, Fail(StorageUnavailable)
	}
	if len(encoded) > MaxResponseBytes {
		return Page{}, Fail(LimitExceeded)
	}
	return page, nil
}

func (s *Service) observe(operation string, started time.Time, err error, candidates int, page Page) {
	if s == nil || s.observer == nil {
		return
	}
	bytes := 0
	if err == nil {
		if encoded, marshalErr := json.Marshal(page); marshalErr == nil {
			bytes = len(encoded)
		}
	}
	observation := Observation{
		Operation:  operation,
		Code:       ErrorCode(err),
		Elapsed:    time.Since(started),
		Candidates: candidates,
		Items:      len(page.Items),
		Bytes:      bytes,
	}
	defer func() { _ = recover() }()
	s.observer.Observe(observation)
}
