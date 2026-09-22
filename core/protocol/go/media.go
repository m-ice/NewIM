package protocol

import (
	"encoding/json"
	"strconv"
)

const (
	// MaxMediaPayloadBytes bounds the complete encoded media metadata object.
	// MaxMediaPayloadBytes 限制完整媒体元数据对象的编码大小。
	MaxMediaPayloadBytes = 4096
	// MaxMediaSize is the largest declared or actual media object in bytes.
	// MaxMediaSize 是声明或实际媒体对象的最大字节数。
	MaxMediaSize int64 = 104857600
)

// MediaMetadata is the closed media v1 payload shape.
// MediaMetadata 是封闭的 media v1 负载形状。
type MediaMetadata struct {
	MediaKey    string `json:"mediaKey"`
	Kind        string `json:"kind"`
	ContentType string `json:"contentType"`
	Size        string `json:"size"`
	SHA256      string `json:"sha256"`
}

// SupportedMediaKind reports whether kind belongs to the frozen v1 enum.
// SupportedMediaKind 判断 kind 是否属于冻结的 v1 枚举。
func SupportedMediaKind(kind string) bool {
	return kind == "image" || kind == "video" || kind == "audio" || kind == "file"
}

// ValidMediaKey reports whether a server-generated media identifier is valid.
// ValidMediaKey 判断服务端生成的媒体标识是否有效。
func ValidMediaKey(value string) bool { return identifier(value, 128, false) }

// ValidMediaContentType applies the exact bounded media v1 MIME grammar.
// ValidMediaContentType 应用精确且有界的 media v1 MIME 语法。
func ValidMediaContentType(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	slash := -1
	for i := 0; i < len(value); i++ {
		if value[i] == '/' {
			if slash >= 0 {
				return false
			}
			slash = i
		}
	}
	if slash < 1 || slash > 63 || len(value)-slash-1 < 1 || len(value)-slash-1 > 64 {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		first := i == 0 || i == slash+1
		if first {
			if ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' {
				continue
			}
			return false
		}
		if ch == '/' {
			continue
		}
		if ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '!' || ch == '#' || ch == '$' || ch == '&' || ch == '^' || ch == '_' || ch == '.' || ch == '+' || ch == '-' {
			continue
		}
		return false
	}
	return true
}

// ValidMediaSize reports whether size is the exact decimal string and range.
// ValidMediaSize 判断 size 是否为精确十进制字符串且位于允许范围内。
func ValidMediaSize(value string) bool {
	if len(value) == 0 || len(value) > 9 || value[0] < '1' || value[0] > '9' {
		return false
	}
	for i := 1; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	n, err := strconv.ParseInt(value, 10, 64)
	return err == nil && n >= 1 && n <= MaxMediaSize
}

// ValidMediaSHA256 reports whether value is 64 lowercase hexadecimal bytes.
// ValidMediaSHA256 判断 value 是否为 64 个小写十六进制字符。
func ValidMediaSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for i := 0; i < len(value); i++ {
		if !((value[i] >= '0' && value[i] <= '9') || (value[i] >= 'a' && value[i] <= 'f')) {
			return false
		}
	}
	return true
}

// ValidMediaMetadata validates all media v1 payload fields.
// ValidMediaMetadata 校验全部 media v1 负载字段。
func ValidMediaMetadata(metadata MediaMetadata) bool {
	return ValidMediaKey(metadata.MediaKey) && SupportedMediaKind(metadata.Kind) &&
		ValidMediaContentType(metadata.ContentType) && ValidMediaSize(metadata.Size) && ValidMediaSHA256(metadata.SHA256)
}

// DecodeMediaPayload strictly decodes one closed media v1 metadata object.
// DecodeMediaPayload 严格解码一个封闭的 media v1 元数据对象。
func DecodeMediaPayload(raw []byte) (MediaMetadata, error) {
	var metadata MediaMetadata
	if err := strictJSONLimits(raw, MaxMediaPayloadBytes, MaxDepth); err != nil {
		return metadata, err
	}
	if err := validateMediaPayload(raw); err != nil {
		return metadata, err
	}
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return MediaMetadata{}, InvalidMessage
	}
	return metadata, nil
}

// EncodeMediaPayload validates and emits the canonical closed object.
// EncodeMediaPayload 校验并输出规范的封闭对象。
func EncodeMediaPayload(metadata MediaMetadata) ([]byte, error) {
	if !ValidMediaMetadata(metadata) {
		return nil, InvalidMessage
	}
	raw, err := json.Marshal(metadata)
	if err != nil || len(raw) > MaxMediaPayloadBytes {
		if err != nil {
			return nil, InvalidJSON
		}
		return nil, TooLarge
	}
	if err = validateMediaPayload(raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func validateMediaPayload(raw []byte) error {
	if len(raw) > MaxMediaPayloadBytes {
		return TooLarge
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return InvalidMessage
	}
	if len(fields) != 5 {
		return InvalidMessage
	}
	metadata, err := decodeMediaFields(fields)
	if err != nil || !ValidMediaMetadata(metadata) {
		return InvalidMessage
	}
	return nil
}

func decodeMediaFields(fields map[string]json.RawMessage) (MediaMetadata, error) {
	var metadata MediaMetadata
	values := []struct {
		name string
		dest *string
	}{
		{"mediaKey", &metadata.MediaKey},
		{"kind", &metadata.Kind},
		{"contentType", &metadata.ContentType},
		{"size", &metadata.Size},
		{"sha256", &metadata.SHA256},
	}
	for _, value := range values {
		raw, ok := fields[value.name]
		if !ok || string(raw) == "null" || json.Unmarshal(raw, value.dest) != nil {
			return MediaMetadata{}, InvalidMessage
		}
	}
	return metadata, nil
}
