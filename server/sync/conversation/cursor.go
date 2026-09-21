package conversation

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"time"
)

const (
	cursorFormatVersion     byte = 1
	cursorKindBootstrapPage byte = 1
	cursorKindDeltaPage     byte = 2
	cursorKindCheckpoint    byte = 3

	cursorTagSize          = sha256.Size
	bootstrapRoundLifetime = 15 * time.Minute
	checkpointLifetime     = 24 * time.Hour
)

type syncCursor struct {
	kind        byte
	keyID       string
	account     string
	epoch       uint64
	issued      uint64
	expires     uint64
	limit       int
	fence       uint64
	afterSeq    uint64
	afterKey    string
	terminalKey string
}

type cursorPosition struct {
	key string
	seq uint64
}

type itemEnvelope struct {
	item   Item
	before cursorPosition
	after  cursorPosition
}

func (c syncCursor) isPage() bool {
	return c.kind == cursorKindBootstrapPage || c.kind == cursorKindDeltaPage
}

func validIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if !((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}

func validKeyID(value string) bool {
	if len(value) == 0 || len(value) > 32 {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if !((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}

func validOptionalIdentifier(value *string) bool {
	return value == nil || validIdentifier(*value)
}

func validSequence(value uint64) bool {
	return value <= MaxSequence
}

func cursorReadError() error {
	return Fail(InvalidCursor)
}

func encodeCursor(keys map[string][]byte, cursor syncCursor) (string, error) {
	if err := validateCursorForEncoding(cursor); err != nil {
		return "", err
	}
	key, ok := keys[cursor.keyID]
	if !ok || len(key) < 32 {
		return "", Fail(StorageUnavailable)
	}

	payload := make([]byte, 0, 160)
	payload = append(payload, cursorFormatVersion, cursor.kind)
	payload = append(payload, byte(len(cursor.keyID)))
	payload = append(payload, cursor.keyID...)
	payload = appendUint16(payload, uint16(len(cursor.account)))
	payload = append(payload, cursor.account...)
	payload = appendUint64(payload, cursor.epoch)
	payload = appendUint64(payload, cursor.issued)
	payload = appendUint64(payload, cursor.expires)
	payload = appendUint16(payload, uint16(cursor.limit))
	payload = appendUint64(payload, cursor.fence)

	switch cursor.kind {
	case cursorKindBootstrapPage:
		payload = appendUint16(payload, uint16(len(cursor.afterKey)))
		payload = append(payload, cursor.afterKey...)
		payload = appendUint16(payload, uint16(len(cursor.terminalKey)))
		payload = append(payload, cursor.terminalKey...)
	case cursorKindDeltaPage:
		payload = appendUint64(payload, cursor.afterSeq)
	case cursorKindCheckpoint:
		// Checkpoint has no position fields beyond the common fence.
	default:
		return "", cursorReadError()
	}

	tag := hmac.New(sha256.New, key)
	tag.Write(payload)
	raw := make([]byte, 0, len(payload)+cursorTagSize)
	raw = append(raw, payload...)
	raw = append(raw, tag.Sum(nil)...)
	encoded := base64.RawURLEncoding.EncodeToString(raw)
	if len(encoded) == 0 || len(encoded) > MaxCursorBytes {
		return "", Fail(StorageUnavailable)
	}
	return encoded, nil
}

func decodeCursor(keys map[string][]byte, encoded string, expectedKind byte, account string, now uint64) (syncCursor, error) {
	if len(encoded) == 0 || len(encoded) > MaxCursorBytes {
		return syncCursor{}, cursorReadError()
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(raw) != encoded {
		return syncCursor{}, cursorReadError()
	}
	if len(raw) <= cursorTagSize+3 {
		return syncCursor{}, cursorReadError()
	}
	payload := raw[:len(raw)-cursorTagSize]
	tag := raw[len(raw)-cursorTagSize:]
	if payload[0] != cursorFormatVersion || payload[1] != expectedKind {
		return syncCursor{}, cursorReadError()
	}
	keyIDLength := int(payload[2])
	if keyIDLength < 1 || keyIDLength > 32 || 3+keyIDLength > len(payload) {
		return syncCursor{}, cursorReadError()
	}
	keyID := string(payload[3 : 3+keyIDLength])
	if !validKeyID(keyID) {
		return syncCursor{}, cursorReadError()
	}
	key, ok := keys[keyID]
	if !ok || len(key) < 32 {
		return syncCursor{}, cursorReadError()
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	if !hmac.Equal(mac.Sum(nil), tag) {
		return syncCursor{}, cursorReadError()
	}

	reader := cursorReader{payload: payload, at: 3 + keyIDLength}
	accountValue, err := reader.identifier()
	if err != nil {
		return syncCursor{}, err
	}
	epoch, err := reader.sequence()
	if err != nil {
		return syncCursor{}, err
	}
	issued, err := reader.sequence()
	if err != nil {
		return syncCursor{}, err
	}
	expires, err := reader.sequence()
	if err != nil {
		return syncCursor{}, err
	}
	limitValue, err := reader.uint16()
	if err != nil {
		return syncCursor{}, err
	}
	fence, err := reader.sequence()
	if err != nil {
		return syncCursor{}, err
	}

	cursor := syncCursor{
		kind:    expectedKind,
		keyID:   keyID,
		account: accountValue,
		epoch:   epoch,
		issued:  issued,
		expires: expires,
		limit:   int(limitValue),
		fence:   fence,
	}
	switch expectedKind {
	case cursorKindBootstrapPage:
		cursor.afterKey, err = reader.identifierOrEmpty()
		if err != nil {
			return syncCursor{}, err
		}
		cursor.terminalKey, err = reader.identifierOrEmpty()
		if err != nil {
			return syncCursor{}, err
		}
	case cursorKindDeltaPage:
		cursor.afterSeq, err = reader.sequence()
		if err != nil {
			return syncCursor{}, err
		}
	case cursorKindCheckpoint:
		// No additional fields.
	default:
		return syncCursor{}, cursorReadError()
	}
	if err = reader.done(); err != nil {
		return syncCursor{}, err
	}
	if err = validateDecodedCursor(cursor, account, now); err != nil {
		return syncCursor{}, err
	}
	return cursor, nil
}

func validateCursorForEncoding(cursor syncCursor) error {
	if cursor.kind != cursorKindBootstrapPage && cursor.kind != cursorKindDeltaPage && cursor.kind != cursorKindCheckpoint {
		return cursorReadError()
	}
	if !validKeyID(cursor.keyID) || !validIdentifier(cursor.account) {
		return cursorReadError()
	}
	if cursor.epoch == 0 || !validSequence(cursor.epoch) || !validSequence(cursor.issued) || !validSequence(cursor.expires) || cursor.expires <= cursor.issued {
		return cursorReadError()
	}
	if cursor.limit < 1 || cursor.limit > MaxPageItems || !validSequence(cursor.fence) {
		return cursorReadError()
	}
	switch cursor.kind {
	case cursorKindBootstrapPage:
		if cursor.afterSeq != 0 {
			return cursorReadError()
		}
		if cursor.afterKey != "" && !validIdentifier(cursor.afterKey) {
			return cursorReadError()
		}
		if cursor.terminalKey != "" && !validIdentifier(cursor.terminalKey) {
			return cursorReadError()
		}
		if cursor.terminalKey == "" {
			if cursor.afterKey != "" {
				return cursorReadError()
			}
		} else if cursor.afterKey == "" || cursor.afterKey > cursor.terminalKey {
			return cursorReadError()
		}
	case cursorKindDeltaPage:
		if cursor.afterKey != "" || cursor.terminalKey != "" {
			return cursorReadError()
		}
		if !validSequence(cursor.afterSeq) || cursor.afterSeq > cursor.fence {
			return cursorReadError()
		}
	case cursorKindCheckpoint:
		if cursor.afterSeq != 0 || cursor.afterKey != "" || cursor.terminalKey != "" {
			return cursorReadError()
		}
	}
	maxLifetime := uint64(checkpointLifetime / time.Second)
	if cursor.kind != cursorKindCheckpoint {
		maxLifetime = uint64(bootstrapRoundLifetime / time.Second)
	}
	if cursor.expires-cursor.issued > maxLifetime {
		return cursorReadError()
	}
	return nil
}

func validateDecodedCursor(cursor syncCursor, account string, now uint64) error {
	if cursor.account != account {
		return cursorReadError()
	}
	if cursor.epoch == 0 || !validSequence(cursor.epoch) || !validSequence(cursor.issued) || !validSequence(cursor.expires) || cursor.expires <= cursor.issued {
		return cursorReadError()
	}
	if cursor.limit < 1 || cursor.limit > MaxPageItems || !validSequence(cursor.fence) {
		return cursorReadError()
	}
	maxLifetime := uint64(checkpointLifetime / time.Second)
	if cursor.kind != cursorKindCheckpoint {
		maxLifetime = uint64(bootstrapRoundLifetime / time.Second)
	}
	if cursor.expires-cursor.issued > maxLifetime {
		return cursorReadError()
	}
	switch cursor.kind {
	case cursorKindBootstrapPage:
		if cursor.afterSeq != 0 {
			return cursorReadError()
		}
		if cursor.terminalKey == "" {
			if cursor.afterKey != "" {
				return cursorReadError()
			}
		} else if cursor.afterKey == "" || cursor.afterKey > cursor.terminalKey {
			return cursorReadError()
		}
	case cursorKindDeltaPage:
		if cursor.afterKey != "" || cursor.terminalKey != "" {
			return cursorReadError()
		}
		if !validSequence(cursor.afterSeq) || cursor.afterSeq > cursor.fence {
			return cursorReadError()
		}
	case cursorKindCheckpoint:
		if cursor.afterSeq != 0 || cursor.afterKey != "" || cursor.terminalKey != "" {
			return cursorReadError()
		}
	default:
		return cursorReadError()
	}
	if now > MaxSequence {
		return Fail(StorageUnavailable)
	}
	if now < cursor.issued {
		return cursorReadError()
	}
	if now >= cursor.expires {
		return Fail(CursorExpired)
	}
	return nil
}

type cursorReader struct {
	payload []byte
	at      int
}

func (r *cursorReader) uint16() (uint16, error) {
	if r.at < 0 || r.at+2 > len(r.payload) {
		return 0, cursorReadError()
	}
	value := binary.BigEndian.Uint16(r.payload[r.at : r.at+2])
	r.at += 2
	return value, nil
}

func (r *cursorReader) sequence() (uint64, error) {
	if r.at < 0 || r.at+8 > len(r.payload) {
		return 0, cursorReadError()
	}
	value := binary.BigEndian.Uint64(r.payload[r.at : r.at+8])
	r.at += 8
	if !validSequence(value) {
		return 0, cursorReadError()
	}
	return value, nil
}

func (r *cursorReader) identifier() (string, error) {
	length, err := r.uint16()
	if err != nil {
		return "", err
	}
	if length == 0 || int(length) > 128 || r.at+int(length) > len(r.payload) {
		return "", cursorReadError()
	}
	value := string(r.payload[r.at : r.at+int(length)])
	r.at += int(length)
	if !validIdentifier(value) {
		return "", cursorReadError()
	}
	return value, nil
}

func (r *cursorReader) identifierOrEmpty() (string, error) {
	length, err := r.uint16()
	if err != nil {
		return "", err
	}
	if int(length) > 128 || r.at+int(length) > len(r.payload) {
		return "", cursorReadError()
	}
	value := string(r.payload[r.at : r.at+int(length)])
	r.at += int(length)
	if value != "" && !validIdentifier(value) {
		return "", cursorReadError()
	}
	return value, nil
}

func (r *cursorReader) done() error {
	if r.at != len(r.payload) {
		return cursorReadError()
	}
	return nil
}

func appendUint16(dst []byte, value uint16) []byte {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	return append(dst, encoded[:]...)
}

func appendUint64(dst []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(dst, encoded[:]...)
}
