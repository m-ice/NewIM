package conversationhttp

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/m-ice/NewIM/server/credentialfile"
	"github.com/m-ice/NewIM/server/user"
)

const (
	maxKeyringBytes = 4096
	maxCursorKeys   = 8
	cursorKeyBytes  = 32
)

var errInvalidKeyring = errors.New("invalid conversation sync keyring")

// LoadKeyring reads the configured active key and its verification keyring.
func LoadKeyring(path string) (string, map[string][]byte, error) {
	raw, err := credentialfile.Read(path, 1, maxKeyringBytes)
	if err != nil {
		return "", nil, errInvalidKeyring
	}
	defer clear(raw)
	if !utf8.Valid(raw) {
		return "", nil, errInvalidKeyring
	}
	keys := make(map[string][]byte)
	active, ok := decodeKeyring(json.NewDecoder(bytes.NewReader(raw)), keys)
	if !ok {
		for _, key := range keys {
			clear(key)
		}
		return "", nil, errInvalidKeyring
	}
	return active, keys, nil
}

func decodeKeyring(decoder *json.Decoder, keys map[string][]byte) (string, bool) {
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return "", false
	}
	var active string
	var keysSeen bool
	for decoder.More() {
		field, err := decoder.Token()
		if err != nil {
			return "", false
		}
		switch field {
		case "activeKeyId":
			if active != "" {
				return "", false
			}
			value, err := decoder.Token()
			id, ok := value.(string)
			if err != nil || !ok || !validKeyringID(id) {
				return "", false
			}
			active = id
		case "keys":
			if keysSeen || !decodeKeys(decoder, keys) {
				return "", false
			}
			keysSeen = true
		default:
			return "", false
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || keys[active] == nil {
		return "", false
	}
	_, err = decoder.Token()
	return active, err == io.EOF
}

func decodeKeys(decoder *json.Decoder, keys map[string][]byte) bool {
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return false
	}
	for decoder.More() {
		if len(keys) == maxCursorKeys {
			return false
		}
		field, err := decoder.Token()
		id, ok := field.(string)
		if err != nil || !ok || !validKeyringID(id) || keys[id] != nil {
			return false
		}
		value, err := decoder.Token()
		encoded, ok := value.(string)
		if err != nil || !ok || len(encoded) != 2*cursorKeyBytes || strings.ContainsAny(encoded, "ABCDEF") {
			return false
		}
		key, err := hex.DecodeString(encoded)
		if err != nil {
			clear(key)
			return false
		}
		keys[id] = key
	}
	end, err := decoder.Token()
	return err == nil && end == json.Delim('}') && len(keys) > 0
}

func validKeyringID(id string) bool {
	return len(id) <= 32 && user.ValidID(id)
}
