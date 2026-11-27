package s3store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/m-ice/NewIM/server/media"
)

func TestOpenRejectsInvalidSettings(t *testing.T) {
	for _, endpoint := range []string{
		"http://objects.example", "https://user:secret@objects.example",
		"https://objects.example/a", "https://objects.example/%2f",
		"https://objects.example?", "https://objects.example/#",
		"https://objects.example:0", "https://objects.example:65536",
	} {
		t.Run(endpoint, func(t *testing.T) {
			cfg := Config{Region: "us-east-1", Bucket: "private-objects", StagingRoot: t.TempDir(), Endpoint: endpoint}
			store, err := Open(t.Context(), cfg)
			if store != nil || media.ErrorCode(err) != media.MediaInvalidInput || err.Error() != string(media.MediaInvalidInput) {
				t.Fatalf("Open returned %v", err)
			}
		})
	}
	valid := Config{Region: "us-east-1", Bucket: "private-objects", StagingRoot: "relative-path"}
	if store, err := Open(t.Context(), valid); store != nil || media.ErrorCode(err) != media.MediaInvalidInput {
		t.Fatalf("Open with relative staging root returned %v", err)
	}

	unsafe := filepath.Join(t.TempDir(), "unsafe")
	if err := os.Mkdir(unsafe, 0o755); err != nil {
		t.Fatal(err)
	}
	valid.StagingRoot = unsafe
	if store, err := Open(t.Context(), valid); store != nil || media.ErrorCode(err) != media.MediaStorageUnavailable || err.Error() != string(media.MediaStorageUnavailable) {
		t.Fatalf("Open with non-private staging root returned %v", err)
	}
}

func TestConditionalPutRetriesWithSameVerifiedBody(t *testing.T) {
	value := []byte("test media")
	expected := media.ObjectInfo{Size: int64(len(value)), SHA256: sha256.Sum256(value)}
	var attempts atomic.Int32
	service := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == http.MethodHead && req.URL.Path == "/private-objects":
			w.WriteHeader(http.StatusOK)
		case req.Method == http.MethodPut && req.URL.Path == "/private-objects/media_key":
			size := req.ContentLength
			digest := req.Header.Get("X-Amz-Checksum-Sha256")
			if size != expected.Size || digest != base64.StdEncoding.EncodeToString(expected.SHA256[:]) ||
				req.Header.Get("If-None-Match") != "*" {
				t.Errorf("missing exact length, checksum, or create precondition")
			}
			body, err := io.ReadAll(req.Body)
			if err != nil || !bytes.Equal(body, value) {
				t.Errorf("unexpected signed request body: %v", err)
			}
			if attempts.Add(1) == 1 {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, "<Error><Code>ConditionalRequestConflict</Code></Error>")
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected object operation %s %s", req.Method, req.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer service.Close()

	root := filepath.Join(t.TempDir(), "staging")
	caPath := filepath.Join(t.TempDir(), "service.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: service.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "fixture")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "fixture")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "http://unused.invalid")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(root, "absent"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(root, "absent"))
	store, err := Open(t.Context(), Config{
		Region: "us-east-1", Bucket: "private-objects", Endpoint: service.URL, CAFile: caPath, StagingRoot: root,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	got, err := store.PutImmutable(t.Context(), "media_key", bytes.NewReader(value), expected)
	if err != nil || got != expected || attempts.Load() != 2 {
		t.Fatalf("PutImmutable got %v, %v, attempts=%d", got, err, attempts.Load())
	}
	if entries, err := os.ReadDir(root); err != nil || len(entries) != 0 {
		t.Fatalf("staging entries %d, %v", len(entries), err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PutImmutable(context.Background(), "media_key", bytes.NewReader(value), expected); media.ErrorCode(err) != media.MediaStorageUnavailable {
		t.Fatalf("PutImmutable after Close returned %v", err)
	}
}
