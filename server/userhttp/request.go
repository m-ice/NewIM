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
	id, patch, _, ok := DecodeProvisionBody(raw, false)
	return id, patch, ok
}

// DecodeProvisionBody accepts actorType only for the enabled trusted endpoint.
func DecodeProvisionBody(raw []byte, allowActor bool) (string, user.Patch, user.ActorType, bool) {
	maxFields := 4
	if allowActor {
		maxFields++
	}
	fields, ok := readObject(raw, maxFields)
	if !ok {
		return "", user.Patch{}, "", false
	}
	id, ok := readString(fields["userId"])
	if !ok || !user.ValidID(id) {
		return "", user.Patch{}, "", false
	}
	delete(fields, "userId")
	var actor user.ActorType
	if rawActor, exists := fields["actorType"]; exists {
		value, ok := readString(rawActor)
		actor = user.ActorType(value)
		if !allowActor || !ok || !actor.Valid() {
			return "", user.Patch{}, "", false
		}
		delete(fields, "actorType")
	}
	patch, ok := readPatch(fields)
	return id, patch, actor, ok
}

func decodeUpdate(raw []byte) (user.Patch, bool) {
	fields, ok := readObject(raw, 4)
	if !ok {
		return user.Patch{}, false
	}
	patch, ok := readPatch(fields)
	return patch, ok && !patch.Empty()
}

func decodeLookup(raw []byte) ([]string, bool) {
	fields, ok := readObject(raw, 4)
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

func readObject(raw []byte, maxFields int) (map[string]json.RawMessage, bool) {
	if len(raw) == 0 || len(raw) > BodyLimit || !utf8.Valid(raw) {
		return nil, false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return nil, false
	}
	fields := make(map[string]json.RawMessage, maxFields)
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || len(fields) == maxFields {
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
