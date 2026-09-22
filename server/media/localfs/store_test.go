package localfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	app "github.com/m-ice/NewIM/server/media"
)

func info(value []byte) app.ObjectInfo {
	return app.ObjectInfo{Size: int64(len(value)), SHA256: sha256.Sum256(value)}
}

func TestPutImmutableReplayConflictAndMode(t *testing.T) {
	root := filepath.Join(t.TempDir(), "objects")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	value := []byte("immutable media bytes")
	expected := info(value)
	actual, err := store.PutImmutable(context.Background(), "media_key", bytes.NewReader(value), expected)
	if err != nil || actual != expected {
		t.Fatalf("put = %+v, %v", actual, err)
	}
	if actual, err = store.PutImmutable(context.Background(), "media_key", bytes.NewReader(value), expected); err != nil || actual != expected {
		t.Fatalf("replay = %+v, %v", actual, err)
	}
	if _, err = store.PutImmutable(context.Background(), "media_key", bytes.NewReader([]byte("different")), info([]byte("different"))); app.ErrorCode(err) != app.MediaConflict {
		t.Fatalf("conflicting replay error = %v", err)
	}
	stat, err := os.Stat(filepath.Join(root, "media_key"))
	if err != nil || stat.Mode().Perm() != 0o600 {
		t.Fatalf("object mode = %v, %v", stat, err)
	}
}

func TestRejectsPathAndSymlinkComponents(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "objects")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.PutImmutable(context.Background(), "a/b", bytes.NewReader([]byte("x")), info([]byte("x"))); app.ErrorCode(err) != app.MediaInvalidInput {
		t.Fatalf("path escape error = %v", err)
	}
	outside := filepath.Join(parent, "outside")
	if err = os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PutImmutable(context.Background(), "linked", bytes.NewReader([]byte("outside")), info([]byte("outside"))); app.ErrorCode(err) != app.MediaConflict {
		t.Fatalf("symlink destination error = %v", err)
	}
	linkedRoot := filepath.Join(parent, "linked-root")
	if err = os.Symlink(root, linkedRoot); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(linkedRoot); err == nil {
		t.Fatal("symlink root accepted")
	}
}

func TestInjectedPublicationFailureLeavesNoObject(t *testing.T) {
	root := filepath.Join(t.TempDir(), "objects")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.publish = func(string, string) error { return errors.New("injected") }
	value := []byte("media")
	if _, err = store.PutImmutable(context.Background(), "media_key", bytes.NewReader(value), info(value)); app.ErrorCode(err) != app.MediaStorageUnavailable {
		t.Fatalf("publication error = %v", err)
	}
	if _, err = os.Stat(filepath.Join(root, "media_key")); !os.IsNotExist(err) {
		t.Fatalf("failed publication left target: %v", err)
	}
}

func TestConcurrentNoReplacePublication(t *testing.T) {
	root := filepath.Join(t.TempDir(), "objects")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	values := [][]byte{[]byte("first"), []byte("second")}
	var wg sync.WaitGroup
	errorsCh := make(chan error, len(values))
	for _, value := range values {
		value := value
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, putErr := store.PutImmutable(context.Background(), "race_key", bytes.NewReader(value), info(value))
			errorsCh <- putErr
		}()
	}
	wg.Wait()
	close(errorsCh)
	successes, conflicts := 0, 0
	for putErr := range errorsCh {
		switch app.ErrorCode(putErr) {
		case "":
			successes++
		case app.MediaConflict:
			conflicts++
		default:
			t.Fatalf("unexpected concurrent error = %v", putErr)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestRejectsNon0700Root(t *testing.T) {
	root := filepath.Join(t.TempDir(), "objects")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root); err == nil {
		t.Fatal("non-0700 root accepted")
	}
}
