package main

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/api"
)

func TestAuthRuntimeDisabledWithoutDSN(t *testing.T) {
	t.Setenv(messageHTTPEnv, "0")
	t.Setenv(authDSNEnv, "")
	runtime, routes, err := newAuthRuntimeFromEnv(context.Background(), api.Config{APIAddr: "127.0.0.1:0"}, slog.Default())
	if err != nil || runtime != nil || len(routes) != 0 {
		t.Fatalf("disabled runtime=%v routes=%v err=%v", runtime, routes, err)
	}
}

func TestMessageRuntimeMissingDSNFailsClosed(t *testing.T) {
	t.Setenv(messageHTTPEnv, "1")
	t.Setenv(authDSNEnv, "")
	t.Setenv(authAllowLocalSocketEnv, "1")
	runtime, routes, err := newAuthRuntimeFromEnv(context.Background(), api.Config{APIAddr: "127.0.0.1:0"}, slog.Default())
	if err == nil || runtime != nil || len(routes) != 0 || errorCode(err) != string(api.CodeInvalidMessageConfig) {
		t.Fatalf("runtime=%v routes=%v err=%v code=%q", runtime, routes, err, errorCode(err))
	}
}

func TestMessageRuntimeConfigValidation(t *testing.T) {
	for name, test := range map[string]struct {
		cfg        api.Config
		dsn        string
		allowLocal bool
		wantOK     bool
	}{
		"local socket loopback": {
			cfg:        api.Config{APIAddr: "127.0.0.1:8080"},
			dsn:        "host=/tmp user=test dbname=test sslmode=disable",
			allowLocal: true,
			wantOK:     true,
		},
		"ipv6 loopback": {
			cfg:        api.Config{APIAddr: "[::1]:8080"},
			dsn:        "postgres:///test?host=/tmp&sslmode=disable",
			allowLocal: true,
			wantOK:     true,
		},
		"missing dsn": {
			cfg:        api.Config{APIAddr: "127.0.0.1:8080"},
			allowLocal: true,
		},
		"tcp": {
			cfg:        api.Config{APIAddr: "127.0.0.1:8080"},
			dsn:        "postgres://test@127.0.0.1/test?sslmode=disable",
			allowLocal: true,
		},
		"dns": {
			cfg:        api.Config{APIAddr: "127.0.0.1:8080"},
			dsn:        "postgres://test@db.example.invalid/test",
			allowLocal: true,
		},
		"multi host": {
			cfg:        api.Config{APIAddr: "127.0.0.1:8080"},
			dsn:        "postgres://test@/test?host=/tmp,/var/run/postgresql&sslmode=disable",
			allowLocal: true,
		},
		"missing opt in": {
			cfg:        api.Config{APIAddr: "127.0.0.1:8080"},
			dsn:        "host=/tmp user=test dbname=test sslmode=disable",
			allowLocal: false,
		},
		"non loopback": {
			cfg:        api.Config{APIAddr: "0.0.0.0:8080"},
			dsn:        "host=/tmp user=test dbname=test sslmode=disable",
			allowLocal: true,
		},
		"dns listener": {
			cfg:        api.Config{APIAddr: "localhost:8080"},
			dsn:        "host=/tmp user=test dbname=test sslmode=disable",
			allowLocal: true,
		},
		"malformed dsn": {
			cfg:        api.Config{APIAddr: "127.0.0.1:8080"},
			dsn:        "postgres://user:%zz@127.0.0.1/newim",
			allowLocal: true,
		},
		"unix sql dsn with tcp fallback": {
			cfg:        api.Config{APIAddr: "127.0.0.1:8080"},
			dsn:        "host=/tmp,localhost user=test dbname=test sslmode=disable",
			allowLocal: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateMessageRuntimeConfig(test.cfg, test.dsn, test.allowLocal)
			if test.wantOK && err != nil {
				t.Fatalf("valid config rejected: %v", err)
			}
			if !test.wantOK && (err == nil || errorCode(err) != string(api.CodeInvalidMessageConfig)) {
				t.Fatalf("invalid config error=%v code=%q", err, errorCode(err))
			}
		})
	}
}

func TestAuthRuntimeRejectsUnsafeStartupBeforeOpen(t *testing.T) {
	t.Setenv(messageHTTPEnv, "0")
	t.Setenv(authDSNEnv, "postgres://example.invalid/newim")
	t.Setenv(authAllowLocalSocketEnv, "0")
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
	t.Setenv(messageHTTPEnv, "0")
	t.Setenv(authAllowLocalSocketEnv, "0")
	t.Setenv(authDSNEnv, "postgres://user:%zz@127.0.0.1/newim")
	runtime, routes, err := newAuthRuntimeFromEnv(context.Background(), api.Config{APIAddr: "127.0.0.1:8080"}, slog.Default())
	if err == nil || runtime != nil || len(routes) != 0 {
		t.Fatalf("malformed DSN runtime=%v routes=%v err=%v", runtime, routes, err)
	}
	t.Setenv(authDSNEnv, "host=/definitely/missing user=test dbname=test sslmode=disable")
	runtime, routes, err = newAuthRuntimeFromEnv(context.Background(), api.Config{APIAddr: "127.0.0.1:8080"}, slog.Default())
	if err == nil || runtime != nil || len(routes) != 0 || strings.Contains(err.Error(), "definitely") {
		t.Fatalf("local-socket DSN runtime=%v routes=%v err=%v", runtime, routes, err)
	}
}

func TestJoinRuntimeServerAndAuthDeadlineDoesNotCloseBeforeDrain(t *testing.T) {
	serverDone := make(chan error)
	authDone := make(chan error, 1)
	shutdown := make(chan struct{})
	var closeCalls atomic.Int32
	closeAuth := func() { closeCalls.Add(1) }
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(shutdown)
	}()
	result := joinRuntimeServerAndAuth(serverDone, nil, authDone, shutdown, func() {}, func() {}, closeAuth, 80*time.Millisecond)
	if !result.timedOut {
		t.Fatalf("expected shared deadline timeout, got %+v", result)
	}
	if got := closeCalls.Load(); got != 0 {
		t.Fatalf("closeAuth calls before server drain=%d want 0", got)
	}
}

func TestJoinRuntimeServerAndAuthClosesAfterServerDrain(t *testing.T) {
	serverDone := make(chan error)
	authDone := make(chan error, 1)
	shutdown := make(chan struct{})
	var closeCalls atomic.Int32
	closeAuth := func() {
		closeCalls.Add(1)
		authDone <- nil
	}
	go func() { close(shutdown) }()
	go func() {
		time.Sleep(20 * time.Millisecond)
		serverDone <- nil
	}()
	result := joinRuntimeServerAndAuth(serverDone, nil, authDone, shutdown, func() {}, func() {}, closeAuth, time.Second)
	if result.timedOut || result.serverErr != nil || result.authErr != nil {
		t.Fatalf("join result=%+v", result)
	}
	if got := closeCalls.Load(); got != 1 {
		t.Fatalf("closeAuth calls=%d want 1", got)
	}
}
