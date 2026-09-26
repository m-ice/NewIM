package message

import (
	"errors"
	"strings"
	"testing"
)

func TestCSPRNGIDGeneratorFormatAndUniqueness(t *testing.T) {
	generator := NewCSPRNGIDGenerator()
	if generator == nil {
		t.Fatal("nil generator")
	}
	seen := make(map[string]struct{}, 256)
	for i := 0; i < 256; i++ {
		id, err := generator.NewID()
		if err != nil {
			t.Fatalf("NewID() error=%v", err)
		}
		if len(id) != 32 {
			t.Fatalf("id=%q length=%d", id, len(id))
		}
		for _, ch := range id {
			if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
				t.Fatalf("id=%q contains non-lowercase-hex character", id)
			}
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate id=%q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestCSPRNGIDGeneratorReturnsStableError(t *testing.T) {
	generator := &cspRNGIDGenerator{entropy: failingEntropyReader{}}
	id, err := generator.NewID()
	if id != "" || ErrorCode(err) != SendStorageUnavailable || RetryDisposition(err) != "RetrySameIntent" {
		t.Fatalf("id=%q error=%v code=%s disposition=%s", id, err, ErrorCode(err), RetryDisposition(err))
	}
	if strings.Contains(err.Error(), "sensitive entropy failure") {
		t.Fatalf("entropy error reflected: %v", err)
	}

	var nilGenerator *cspRNGIDGenerator
	if _, err := nilGenerator.NewID(); ErrorCode(err) != SendStorageUnavailable {
		t.Fatalf("nil generator error=%v", err)
	}
}

type failingEntropyReader struct{}

func (failingEntropyReader) Read([]byte) (int, error) {
	return 0, errors.New("sensitive entropy failure")
}
