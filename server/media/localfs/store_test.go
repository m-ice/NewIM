package localfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
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

func TestRejectsReplacedRoot(t *testing.T) {
	parent := t.TempDir()
	path := filepath.Join(parent, "objects")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(path, filepath.Join(parent, "original")); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()

	if err = checkOpenedRoot(root, before); app.ErrorCode(err) != app.MediaStorageUnavailable {
		t.Fatalf("replaced root error = %v", err)
	}
}

func TestExistingObjectReplaySyncsParentDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "objects")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	value := []byte("durable replay")
	expected := info(value)
	if _, err = store.PutImmutable(context.Background(), "media_key", bytes.NewReader(value), expected); err != nil {
		t.Fatal(err)
	}
	syncCalls := 0
	store.syncHook = func() error {
		syncCalls++
		return nil
	}
	if _, err = store.PutImmutable(context.Background(), "media_key", bytes.NewReader(value), expected); err != nil {
		t.Fatal(err)
	}
	if syncCalls != 1 {
		t.Fatalf("existing-object replay sync calls got %d want 1", syncCalls)
	}
	store.syncHook = func() error {
		syncCalls++
		return errors.New("injected sync failure")
	}
	if _, err = store.PutImmutable(context.Background(), "media_key", bytes.NewReader(value), expected); app.ErrorCode(err) != app.MediaStorageUnavailable {
		t.Fatalf("existing-object replay with sync failure error = %v", err)
	}
	if syncCalls != 2 {
		t.Fatalf("sync failure calls got %d want 2", syncCalls)
	}
}

func TestOpenVerifiedReturnsVerifiedDescriptorAtStart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "objects")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	value := []byte("immutable download")
	if _, err = store.PutImmutable(ctx, "media_key", bytes.NewReader(value), info(value)); err != nil {
		t.Fatal(err)
	}
	file, err := store.OpenVerified(ctx, "media_key", info(value))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	path := filepath.Join(root, "media_key")
	if err = os.Rename(path, filepath.Join(root, "archived_key")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, bytes.Repeat([]byte{'x'}, len(value)), 0o600); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(file)
	if err != nil || !bytes.Equal(body, value) {
		t.Fatalf("verified descriptor returned %q, %v", body, err)
	}
}

func TestOpenVerifiedRejectsInvalidObjects(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "objects")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	value := []byte("immutable download")
	expected := info(value)
	if _, err = store.PutImmutable(ctx, "media_key", bytes.NewReader(value), expected); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(parent, "outside")
	if err = os.WriteFile(outside, value, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink("media_key", filepath.Join(root, "inside_link")); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(outside, filepath.Join(root, "outside_link")); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(root, "directory"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		key      string
		expected app.ObjectInfo
		code     app.Code
	}{
		{"same root link", "inside_link", expected, app.MediaStorageUnavailable},
		{"outside link", "outside_link", expected, app.MediaStorageUnavailable},
		{"directory", "directory", expected, app.MediaStorageUnavailable},
		{"missing", "missing_file", expected, app.MediaStorageUnavailable},
		{"size mismatch", "media_key", info(value[:len(value)-1]), app.MediaStorageUnavailable},
		{"digest mismatch", "media_key", info(bytes.Repeat([]byte{'x'}, len(value))), app.MediaStorageUnavailable},
		{"invalid key", "../outside", expected, app.MediaInvalidInput},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := store.OpenVerified(ctx, test.key, test.expected)
			if file != nil {
				_ = file.Close()
				t.Fatal("returned a descriptor for an invalid object")
			}
			if code := app.ErrorCode(err); code != test.code {
				t.Fatalf("error code = %v, want %v", code, test.code)
			}
		})
	}
}

func TestReplayCancellationPreventsDurableSuccess(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	value := []byte("immutable media")
	expected := info(value)
	if _, err = store.PutImmutable(context.Background(), "media_key", bytes.NewReader(value), expected); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := io.MultiReader(bytes.NewReader(value), cancelReader{cancel: cancel})
	store.syncHook = func() error {
		t.Fatal("synced replay after cancellation")
		return nil
	}
	if _, err = store.PutImmutable(ctx, "media_key", reader, expected); app.ErrorCode(err) != app.MediaStorageUnavailable {
		t.Fatalf("canceled replay error = %v", err)
	}
}

type cancelReader struct{ cancel context.CancelFunc }

func (r cancelReader) Read([]byte) (int, error) {
	r.cancel()
	return 0, io.EOF
}
