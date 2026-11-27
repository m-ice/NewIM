package credentialfile

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func writeCredential(t *testing.T, body []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadPreservesBytesAtBounds(t *testing.T) {
	for _, fixture := range []struct {
		body []byte
		mode os.FileMode
	}{
		{[]byte{0}, 0o400},
		{bytes.Repeat([]byte{'a'}, 4095), 0o600},
		{bytes.Repeat([]byte{'b'}, 4096), 0o600},
	} {
		path := writeCredential(t, fixture.body, fixture.mode)
		got, err := Read(path, 1, 4096)
		if err != nil || !bytes.Equal(got, fixture.body) {
			t.Fatalf("read size %d did not preserve exact bytes: %v", len(fixture.body), err)
		}
	}
}

func TestReadRejectsUnsafeFiles(t *testing.T) {
	for _, fixture := range []struct {
		name string
		size int
		mode os.FileMode
	}{
		{"empty", 0, 0o600},
		{"below minimum", 1, 0o600},
		{"oversize", 4097, 0o600},
		{"group readable", 2, 0o640},
		{"world readable", 2, 0o644},
		{"executable", 2, 0o700},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			path := writeCredential(t, make([]byte, fixture.size), fixture.mode)
			if got, err := Read(path, 2, 4096); got != nil || err != ErrInvalidFile {
				t.Fatal("unsafe credential file was accepted")
			}
		})
	}
	target := writeCredential(t, []byte{'a'}, 0o600)
	dir := t.TempDir()
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{link, dir, "relative", filepath.Join(dir, "missing")} {
		if got, err := Read(path, 1, 4096); got != nil || err != ErrInvalidFile {
			t.Fatal("unsafe credential path was accepted")
		}
	}
}

func TestReadRejectsFIFOWithoutWaiting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fifo")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := Read(path, 1, 4096)
		result <- err
	}()
	select {
	case err := <-result:
		if err != ErrInvalidFile {
			t.Fatal("FIFO was accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("credential reader waited for a FIFO writer")
	}
}
