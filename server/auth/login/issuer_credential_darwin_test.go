package login

import (
	"os"
	"os/exec"
	"testing"
)

func TestLoadIssuerDigestRejectsDarwinACL(t *testing.T) {
	path := writeTestCredential(t, testIssuerSecret, 0o600)
	if _, err := loadIssuerDigest(path); err != nil {
		t.Fatal("plain owner-only fixture rejected:", err)
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
	if _, err := loadIssuerDigest(path); err != errInvalidIssuerConfig {
		t.Fatal("owner-only mode with a granting ACL was accepted")
	}
}
