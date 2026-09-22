package media

import "strings"

// Media protocol v1 bounds shared by application and storage adapters.
// 应用与存储适配器共用的 media 协议 v1 边界。
const (
	MaxMediaPayloadBytes = 4096
	MaxMediaSize         = 104857600
	DefaultUploadTTL     = 120
	MaxUploadTTL         = 300
	DefaultDownloadTTL   = 60
	MaxDownloadTTL       = 300
)

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

func validTokenID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for i := 0; i < len(value); i++ {
		if !((value[i] >= '0' && value[i] <= '9') || (value[i] >= 'a' && value[i] <= 'f')) {
			return false
		}
	}
	return true
}

func validGrantID(value string) bool {
	if len(value) != 32 {
		return false
	}
	return validTokenID(value)
}

func validMediaKey(value string) bool { return validIdentifier(value) }

func validKind(value string) bool {
	return value == "image" || value == "video" || value == "audio" || value == "file"
}

func validContentType(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	slash := strings.IndexByte(value, '/')
	if slash < 1 || slash > 63 || strings.IndexByte(value[slash+1:], '/') >= 0 {
		return false
	}
	if len(value)-slash-1 < 1 || len(value)-slash-1 > 64 {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if i == 0 || i == slash+1 {
			if ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' {
				continue
			}
			return false
		}
		if ch == '/' {
			continue
		}
		if ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' ||
			ch == '!' || ch == '#' || ch == '$' || ch == '&' || ch == '^' ||
			ch == '_' || ch == '.' || ch == '+' || ch == '-' {
			continue
		}
		return false
	}
	return true
}

func validSHA256(value string) bool {
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
