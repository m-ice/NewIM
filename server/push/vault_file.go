package push

import (
	"encoding/base64"
	jsonv2 "encoding/json/v2"

	"github.com/m-ice/NewIM/server/credentialfile"
)

func LoadVault(path string) (*Vault, error) {
	data, err := credentialfile.Read(path, 1, 4096)
	defer clear(data)
	if err != nil {
		return nil, ErrConfig
	}
	var config struct {
		Active string            `json:"active"`
		Keys   map[string]string `json:"keys"`
	}
	if jsonv2.Unmarshal(data, &config, jsonv2.RejectUnknownMembers(true)) != nil || len(config.Keys) > 8 {
		return nil, ErrConfig
	}
	keys := make(map[string][]byte, len(config.Keys))
	defer func() {
		for _, key := range keys {
			clear(key)
		}
	}()
	for id, encoded := range config.Keys {
		key, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil {
			clear(key)
			return nil, ErrConfig
		}
		keys[id] = key
	}
	return NewVault(config.Active, keys)
}
