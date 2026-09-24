//go:build integration

package main

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/buildinfo"
)

func TestWebhookRuntimeLifecycle(t *testing.T) {
	dsn := os.Getenv("NEWIM_WEBHOOK_DSN")
	if dsn == "" {
		dsn = "host=/var/run/postgresql user=newim_test dbname=newim_test sslmode=disable"
	}
	t.Setenv("NEWIM_WEBHOOK_DSN", dsn)
	t.Setenv("NEWIM_WEBHOOK_MASTER_KEY_B64", base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")))
	t.Setenv("NEWIM_WEBHOOK_ALLOW_LOCAL_SOCKET", "1")

	for attempt := 0; attempt < 2; attempt++ {
		apiAddr, opsAddr := twoFreeAddrs(t)
		var stderr bytes.Buffer
		done := make(chan int, 1)
		go func() {
			done <- run([]string{"--api-addr", apiAddr, "--ops-addr", opsAddr}, io.Discard, &stderr, func() (buildinfo.Info, error) {
				return buildinfo.Info{ServerVersion: "0.1.0-lifecycle-test"}, nil
			})
		}()
		stopped := false
		defer func() {
			if !stopped {
				_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
				}
			}
		}()

		if err := waitForStatus("http://"+apiAddr+"/api/v1/health", http.StatusOK); err != nil {
			t.Fatalf("api readiness failed on attempt %d: %v; stderr=%q", attempt, err, stderr.String())
		}
		if err := waitForStatus("http://"+opsAddr+"/ready", http.StatusOK); err != nil {
			t.Fatalf("ops readiness failed on attempt %d: %v; stderr=%q", attempt, err, stderr.String())
		}
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		select {
		case code := <-done:
			stopped = true
			if code != 0 {
				t.Fatalf("SIGTERM exit = %d on attempt %d; stderr=%q", code, attempt, stderr.String())
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("SIGTERM drain exceeded deadline on attempt %d; stderr=%q", attempt, stderr.String())
		}
	}
}
