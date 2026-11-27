package localfs

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	app "github.com/m-ice/NewIM/server/media"
)

func TestOpenRejectsDarwinACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "objects")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("/bin/chmod", "+a", "everyone allow search", path).Run(); err != nil {
		t.Fatal("set fixture ACL:", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatal("fixture ACL changed POSIX directory permissions")
	}
	store, err = Open(path)
	if store != nil {
		_ = store.Close()
		t.Fatal("opened object directory with an ACL")
	}
	if app.ErrorCode(err) != app.MediaStorageUnavailable {
		t.Fatalf("object directory ACL error = %v", err)
	}
}
