package main

import (
	"maps"
	"path/filepath"
	"testing"
)

func TestConversationAccountSettingsConfig(t *testing.T) {
	root := t.TempDir()
	for _, test := range []struct {
		name, readSync, sync, settings string
		enabled, invalid               bool
	}{
		{name: "default off"},
		{name: "enabled", readSync: "1", sync: "1", settings: "1", enabled: true},
		{name: "requires read sync", sync: "1", settings: "1", invalid: true},
		{name: "requires public sync", readSync: "1", settings: "1", invalid: true},
		{name: "invalid flag", readSync: "1", sync: "1", settings: "true", invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := map[string]string{
				"NEWIM_GATEWAY_ADDR":                  "127.0.0.1:0",
				"NEWIM_GATEWAY_TLS_CERT_FILE":         filepath.Join(root, "cert.pem"),
				"NEWIM_GATEWAY_TLS_KEY_FILE":          filepath.Join(root, "key.pem"),
				"NEWIM_AUTH_DSN":                      "postgresql://test@localhost/newim",
				"NEWIM_AUTH_DEVICE_ADMISSION":         "1",
				"NEWIM_CONVERSATION_PROJECTION":       "1",
				"NEWIM_CONVERSATION_READ_SYNC":        test.readSync,
				"NEWIM_CONVERSATION_SYNC":             test.sync,
				"NEWIM_CONVERSATION_SYNC_KEYS_FILE":   filepath.Join(root, "sync.json"),
				"NEWIM_CONVERSATION_ACCOUNT_SETTINGS": test.settings,
			}
			got, err := readConfig(func(name string) string { return env[name] })
			if (err != nil) != test.invalid || got.accountSettings != test.enabled {
				t.Fatalf("account settings enabled=%t, error=%v", got.accountSettings, err)
			}
		})
	}
}

func TestConversationReadSyncConfig(t *testing.T) {
	root := t.TempDir()
	for _, test := range []struct {
		name, projection, readSync string
		enabled                    bool
		invalid                    bool
	}{
		{name: "legacy mode", projection: "1"},
		{name: "explicitly disabled", readSync: "0"},
		{name: "enabled", projection: "1", readSync: "1", enabled: true},
		{name: "requires source projection", readSync: "1", invalid: true},
		{name: "invalid toggle", projection: "1", readSync: "true", invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := map[string]string{
				"NEWIM_GATEWAY_ADDR":            "127.0.0.1:0",
				"NEWIM_GATEWAY_TLS_CERT_FILE":   filepath.Join(root, "cert.pem"),
				"NEWIM_GATEWAY_TLS_KEY_FILE":    filepath.Join(root, "key.pem"),
				"NEWIM_AUTH_DSN":                "postgresql://test@localhost/newim",
				"NEWIM_AUTH_DEVICE_ADMISSION":   "1",
				"NEWIM_CONVERSATION_PROJECTION": test.projection,
				"NEWIM_CONVERSATION_READ_SYNC":  test.readSync,
			}
			got, err := readConfig(func(name string) string { return env[name] })
			if (err != nil) != test.invalid || got.projectReadState != test.enabled {
				t.Fatalf("read state enabled=%t, error=%v", got.projectReadState, err)
			}
		})
	}
}

func TestStrangerMessageConfig(t *testing.T) {
	root := t.TempDir()
	for _, test := range []struct {
		name, routes, private, stranger, device string
		wantPolicy                              bool
		wantDevice                              int
		invalid                                 bool
	}{
		{name: "disabled"},
		{name: "inherits account quota", routes: "1", private: "1", wantDevice: 93},
		{name: "explicit quota", routes: "1", private: "1", stranger: "1", device: "8", wantPolicy: true, wantDevice: 8},
		{name: "requires message routes", private: "1", stranger: "1", invalid: true},
		{name: "requires private policy", routes: "1", stranger: "1", invalid: true},
		{name: "invalid toggle", routes: "1", private: "1", stranger: "true", invalid: true},
		{name: "invalid quota", routes: "1", private: "1", device: "0", invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := map[string]string{
				"NEWIM_GATEWAY_ADDR":                            "127.0.0.1:0",
				"NEWIM_GATEWAY_TLS_CERT_FILE":                   filepath.Join(root, "cert.pem"),
				"NEWIM_GATEWAY_TLS_KEY_FILE":                    filepath.Join(root, "key.pem"),
				"NEWIM_AUTH_DSN":                                "postgresql://test@localhost/newim",
				"NEWIM_AUTH_DEVICE_ADMISSION":                   "1",
				"NEWIM_TLS_PRIVATE_MESSAGES":                    test.routes,
				"NEWIM_PRIVATE_MESSAGE_POLICY":                  test.private,
				"NEWIM_STRANGER_MESSAGE_POLICY":                 test.stranger,
				"NEWIM_TLS_MESSAGE_ACCOUNT_ATTEMPTS_PER_MINUTE": "93",
				"NEWIM_TLS_MESSAGE_DEVICE_ATTEMPTS_PER_MINUTE":  test.device,
			}
			got, err := readConfig(func(name string) string { return env[name] })
			if test.invalid {
				if err == nil {
					t.Fatal("accepted invalid stranger message configuration")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.strangerPolicy != test.wantPolicy || got.messageDeviceLimit != test.wantDevice {
				t.Fatalf("got stranger policy=%t and device limit=%d", got.strangerPolicy, got.messageDeviceLimit)
			}
		})
	}
}

func TestMediaStorageConfig(t *testing.T) {
	root := t.TempDir()
	localRoot := filepath.Join(root, "objects")
	stagingRoot := filepath.Join(root, "staging")
	base := map[string]string{
		"NEWIM_GATEWAY_ADDR":           "127.0.0.1:0",
		"NEWIM_GATEWAY_TLS_CERT_FILE":  filepath.Join(root, "cert.pem"),
		"NEWIM_GATEWAY_TLS_KEY_FILE":   filepath.Join(root, "key.pem"),
		"NEWIM_AUTH_DSN":               "postgresql://test@localhost/newim",
		"NEWIM_AUTH_DEVICE_ADMISSION":  "1",
		"NEWIM_TLS_PRIVATE_MESSAGES":   "1",
		"NEWIM_PRIVATE_MESSAGE_POLICY": "1",
		"NEWIM_MEDIA_HTTP":             "1",
		"NEWIM_MEDIA_LOCAL_ROOT":       localRoot,
		"NEWIM_MEDIA_S3_REGION":        "us-east-1",
		"NEWIM_MEDIA_S3_BUCKET":        "newim-media",
		"NEWIM_MEDIA_S3_STAGING_ROOT":  stagingRoot,
	}
	for _, test := range []struct {
		name      string
		override  map[string]string
		wantLocal string
		wantSlots int
		bad       bool
	}{
		{name: "existing local settings", wantLocal: localRoot},
		{name: "explicit local settings", wantLocal: localRoot,
			override: map[string]string{"NEWIM_MEDIA_STORAGE": "localfs", "NEWIM_MEDIA_S3_MAX_TRANSFERS": "invalid"}},
		{name: "disabled", override: map[string]string{"NEWIM_MEDIA_HTTP": "0", "NEWIM_MEDIA_STORAGE": "invalid"}},
		{name: "unknown backend", bad: true, override: map[string]string{"NEWIM_MEDIA_STORAGE": "invalid"}},
		{name: "missing local root", bad: true, override: map[string]string{"NEWIM_MEDIA_LOCAL_ROOT": ""}},
		{name: "S3 default slots", wantSlots: 8,
			override: map[string]string{"NEWIM_MEDIA_STORAGE": "s3", "NEWIM_MEDIA_LOCAL_ROOT": ""}},
		{name: "S3 configured slots and CA", wantSlots: 32,
			override: map[string]string{"NEWIM_MEDIA_STORAGE": "s3", "NEWIM_MEDIA_S3_MAX_TRANSFERS": "32",
				"NEWIM_MEDIA_S3_CA_FILE": filepath.Join(root, "ca.pem")}},
		{name: "S3 slots over maximum", bad: true,
			override: map[string]string{"NEWIM_MEDIA_STORAGE": "s3", "NEWIM_MEDIA_S3_MAX_TRANSFERS": "33"}},
		{name: "S3 slots noncanonical", bad: true,
			override: map[string]string{"NEWIM_MEDIA_STORAGE": "s3", "NEWIM_MEDIA_S3_MAX_TRANSFERS": "01"}},
		{name: "S3 relative staging root", bad: true,
			override: map[string]string{"NEWIM_MEDIA_STORAGE": "s3", "NEWIM_MEDIA_S3_STAGING_ROOT": "staging"}},
		{name: "S3 relative CA", bad: true,
			override: map[string]string{"NEWIM_MEDIA_STORAGE": "s3", "NEWIM_MEDIA_S3_CA_FILE": "ca.pem"}},
		{name: "S3 missing bucket", bad: true,
			override: map[string]string{"NEWIM_MEDIA_STORAGE": "s3", "NEWIM_MEDIA_S3_BUCKET": ""}},
		{name: "S3 missing region", bad: true,
			override: map[string]string{"NEWIM_MEDIA_STORAGE": "s3", "NEWIM_MEDIA_S3_REGION": ""}},
		{name: "requires TLS message route", bad: true,
			override: map[string]string{"NEWIM_TLS_PRIVATE_MESSAGES": "0"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := maps.Clone(base)
			maps.Copy(env, test.override)
			got, err := readConfig(func(name string) string { return env[name] })
			if test.bad {
				if err == nil {
					t.Fatal("accepted invalid media configuration")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.mediaRoot != test.wantLocal {
				t.Fatal("unexpected local root")
			}
			if test.wantSlots == 0 {
				if got.mediaS3 != nil || (got.media == nil) != (test.wantLocal == "") {
					t.Fatal("unexpected enabled media backend")
				}
				return
			}
			if got.media == nil || got.mediaS3 == nil || got.mediaS3.MaxTransfers != test.wantSlots ||
				got.mediaS3.Region != base["NEWIM_MEDIA_S3_REGION"] || got.mediaS3.Bucket != base["NEWIM_MEDIA_S3_BUCKET"] ||
				got.mediaS3.StagingRoot != stagingRoot || got.mediaS3.CAFile != env["NEWIM_MEDIA_S3_CA_FILE"] {
				t.Fatal("unexpected S3 configuration")
			}
		})
	}
}
