package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/m-ice/NewIM/server/push"
)

func TestPushKitRuntimeConfig(t *testing.T) {
	t.Setenv("NEWIM_PUSH_APNS_KEY_FILE", "")
	t.Setenv("NEWIM_PUSH_FCM_CREDENTIALS_FILE", "")
	t.Setenv("NEWIM_PUSH_KIT_CREDENTIALS_FILE", "")
	if _, err := pushProviders(); !errors.Is(err, push.ErrConfig) {
		t.Fatal("an empty configuration enabled a push provider")
	}
	path := filepath.Join(t.TempDir(), "pushkit-credentials")
	t.Setenv("NEWIM_PUSH_KIT_CREDENTIALS_FILE", path)
	if _, err := pushProviders(); !errors.Is(err, push.ErrConfig) {
		t.Fatal("the runtime accepted a missing Push Kit credential file")
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]string{
		"project_id": "project123", "key_id": "key123", "sub_account": "test@example",
		"private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"token_uri":   "https://oauth-login.cloud.huawei.com/oauth2/v3/token",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	configured, err := pushProviders()
	if err != nil || len(configured) != 1 || configured[push.PushKit] == nil {
		t.Fatal("the runtime did not select the configured Push Kit provider")
	}
}
