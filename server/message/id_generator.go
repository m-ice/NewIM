package message

import (
	"crypto/rand"
	"encoding/hex"
	"io"
)

const generatedIDBytes = 16

type cspRNGIDGenerator struct {
	entropy io.Reader
}

// NewCSPRNGIDGenerator constructs the production server-side ID generator.
// NewCSPRNGIDGenerator 构造生产服务端 ID 生成器，输出 128-bit 小写十六进制 ID。
func NewCSPRNGIDGenerator() IDGenerator {
	return &cspRNGIDGenerator{entropy: rand.Reader}
}

// NewID returns one cryptographically random 32-character lowercase hex ID.
// NewID 返回一个密码学随机的 32 字符小写十六进制 ID。
func (g *cspRNGIDGenerator) NewID() (string, error) {
	if g == nil || g.entropy == nil {
		return "", Fail(SendStorageUnavailable)
	}
	var raw [generatedIDBytes]byte
	if _, err := io.ReadFull(g.entropy, raw[:]); err != nil {
		return "", Fail(SendStorageUnavailable)
	}
	return hex.EncodeToString(raw[:]), nil
}
