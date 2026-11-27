package credentialfile

import (
	"os"
	"os/exec"
	"testing"
)

func TestReadRejectsDarwinACL(t *testing.T) {
	path := writeCredential(t, []byte{'a'}, 0o600)
	if _, err := Read(path, 1, 4096); err != nil {
		t.Fatal("owner-only fixture rejected")
	}
	if err := exec.Command("/bin/chmod", "+a", "everyone allow read", path).Run(); err != nil {
		t.Fatal("set fixture ACL:", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode() != 0o600 {
		t.Fatal("fixture ACL changed POSIX permission bits")
	}
	if got, err := Read(path, 1, 4096); got != nil || err != ErrInvalidFile {
		t.Fatal("credential file with a granting ACL was accepted")
	}
}
