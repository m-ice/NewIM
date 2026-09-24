package main

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/m-ice/NewIM/server/api"
)

func TestAuthRuntimeDisabledWithoutDSN(t *testing.T) {
	t.Setenv("NEWIM_AUTH_DSN", "")
	runtime, routes, err := newAuthRuntimeFromEnv(context.Background(), api.Config{APIAddr: "127.0.0.1:0"}, slog.Default())
	if err != nil || runtime != nil || len(routes) != 0 {
		t.Fatalf("disabled runtime=%v routes=%v err=%v", runtime, routes, err)
	}
}

func TestAuthRuntimeRejectsUnsafeStartupBeforeOpen(t *testing.T) {
	t.Setenv("NEWIM_AUTH_DSN", "postgres://example.invalid/newim")
	t.Setenv("NEWIM_AUTH_ALLOW_LOCAL_SOCKET", "0")
	for _, address := range []string{"0.0.0.0:8080", "localhost:8080", "[::]:8080"} {
		runtime, routes, err := newAuthRuntimeFromEnv(context.Background(), api.Config{APIAddr: address}, slog.Default())
		if err == nil || runtime != nil || len(routes) != 0 || errorCode(err) != string(api.CodeInvalidAuthConfig) {
			t.Fatalf("unsafe address %q runtime=%v routes=%v err=%v", address, runtime, routes, err)
		}
	}
}

func TestLoopbackAPIAddress(t *testing.T) {
	for address, want := range map[string]bool{
		"127.0.0.1:8080": true,
		"[::1]:8080":     true,
		"127.0.0.2:8080": true,
		"0.0.0.0:8080":   false,
		"localhost:8080": false,
		"[::]:8080":      false,
	} {
		if got := loopbackAPIAddress(address); got != want {
			t.Fatalf("loopbackAPIAddress(%q)=%t want %t", address, got, want)
		}
	}
}

func TestAuthRuntimeRejectsMalformedAndLocalSocketDSN(t *testing.T) {
	t.Setenv("NEWIM_AUTH_ALLOW_LOCAL_SOCKET", "0")
	t.Setenv("NEWIM_AUTH_DSN", "postgres://user:%zz@127.0.0.1/newim")
	runtime, routes, err := newAuthRuntimeFromEnv(context.Background(), api.Config{APIAddr: "127.0.0.1:8080"}, slog.Default())
	if err == nil || runtime != nil || len(routes) != 0 {
		t.Fatalf("malformed DSN runtime=%v routes=%v err=%v", runtime, routes, err)
	}
	t.Setenv("NEWIM_AUTH_DSN", "host=/definitely/missing user=test dbname=test sslmode=disable")
	runtime, routes, err = newAuthRuntimeFromEnv(context.Background(), api.Config{APIAddr: "127.0.0.1:8080"}, slog.Default())
	if err == nil || runtime != nil || len(routes) != 0 || strings.Contains(err.Error(), "definitely") {
		t.Fatalf("local-socket DSN runtime=%v routes=%v err=%v", runtime, routes, err)
	}
}
