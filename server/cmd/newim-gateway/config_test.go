package main

import (
	"maps"
	"path/filepath"
	"testing"
)

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
