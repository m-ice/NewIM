package userhttp

import (
	"bytes"
	"encoding/json"
	"io"
	"unicode/utf8"

	"github.com/m-ice/NewIM/server/user"
)

const BodyLimit = 16 << 10

func DecodeProvision(raw []byte) (string, user.Patch, bool) {
	fields, ok := readObject(raw)
	if !ok {
		return "", user.Patch{}, false
	}
	id, ok := readString(fields["userId"])
	if !ok || !user.ValidID(id) {
		return "", user.Patch{}, false
	}
	delete(fields, "userId")
	patch, ok := readPatch(fields)
	return id, patch, ok
}

func decodeUpdate(raw []byte) (user.Patch, bool) {
	fields, ok := readObject(raw)
	if !ok {
		return user.Patch{}, false
	}
	patch, ok := readPatch(fields)
	return patch, ok && !patch.Empty()
}

func decodeLookup(raw []byte) ([]string, bool) {
	fields, ok := readObject(raw)
	if !ok || len(fields) != 1 {
		return nil, false
	}
	var ids []string
	if json.Unmarshal(fields["userIds"], &ids) != nil || len(ids) == 0 || len(ids) > user.MaxLookup {
		return nil, false
	}
	return ids, true
}

func readPatch(fields map[string]json.RawMessage) (user.Patch, bool) {
	var patch user.Patch
	for name, raw := range fields {
		value, ok := readString(raw)
		if !ok {
			return user.Patch{}, false
		}
		switch name {
		case "nickname":
			patch.Nickname = &value
		case "faceUrl":
			patch.FaceURL = &value
		case "ex":
			patch.Ex = &value
		default:
			return user.Patch{}, false
		}
	}
	return patch, patch.Valid()
}

func readString(raw []byte) (string, bool) {
	var value string
	if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}

func readObject(raw []byte) (map[string]json.RawMessage, bool) {
	if len(raw) == 0 || len(raw) > BodyLimit || !utf8.Valid(raw) {
		return nil, false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return nil, false
	}
	fields := make(map[string]json.RawMessage, 4)
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || len(fields) == 4 {
			return nil, false
		}
		if _, exists := fields[key]; exists {
			return nil, false
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return nil, false
		}
		fields[key] = value
	}
	if _, err = decoder.Token(); err != nil {
		return nil, false
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, false
	}
	return fields, true
}
