package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/buildinfo"
	app "github.com/m-ice/NewIM/server/webhook"
)

func TestWebhookRuntimeConfigFailsClosed(t *testing.T) {
	t.Setenv("NEWIM_WEBHOOK_DSN", "")
	runtime, err := newWebhookRuntimeFromEnv(context.Background(), slog.Default())
	if err != nil || runtime != nil {
		t.Fatalf("disabled runtime = %v, %v", runtime, err)
	}
	t.Setenv("NEWIM_WEBHOOK_DSN", "postgres://example.invalid/newim")
	t.Setenv("NEWIM_WEBHOOK_MASTER_KEY_B64", "")
	if _, err = newWebhookRuntimeFromEnv(context.Background(), slog.Default()); app.ErrorCode(err) != app.CodeInvalidConfig {
		t.Fatalf("incomplete runtime error = %v", err)
	}
}

func TestHelpAndInvalidArguments(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"-h"}} {
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr, func() (buildinfo.Info, error) {
			t.Fatal("help must not read build metadata")
			return buildinfo.Info{}, nil
		})
		if code != 0 || !strings.Contains(stdout.String(), "Usage:") || stderr.Len() != 0 {
			t.Fatalf("help contract changed: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
		}
	}

	for _, args := range [][]string{{"unknown"}, {"--api-addr", "bad"}, {"--api-addr", "127.0.0.1:1", "--ops-addr", "127.0.0.1:1"}} {
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr, func() (buildinfo.Info, error) {
			t.Fatal("invalid arguments must not read build metadata")
			return buildinfo.Info{}, nil
		})
		if code != 2 || stdout.Len() != 0 || stderr.Len() == 0 {
			t.Fatalf("invalid argument contract changed: args=%v code=%d stdout=%q stderr=%q", args, code, stdout.String(), stderr.String())
		}
	}
}

func TestBuildInfoFailureDoesNotStart(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(nil, &stdout, &stderr, func() (buildinfo.Info, error) {
		return buildinfo.Info{}, errors.New("private-build-error")
	})
	if code != 2 || strings.Contains(stderr.String(), "private-build-error") || !strings.Contains(stderr.String(), string(api.CodeBuildInfoUnavailable)) {
		t.Fatalf("build metadata failure contract changed: code=%d stderr=%q", code, stderr.String())
	}
}

func TestJoinRuntimeAndServerShutdownOrder(t *testing.T) {
	const grace = 100 * time.Millisecond

	t.Run("server first", func(t *testing.T) {
		serverDone := make(chan error, 1)
		workerDone := make(chan error, 1)
		workerCanceled := make(chan struct{})
		var workerCancelOnce sync.Once
		cancelServer := func() {}
		cancelWorker := func() { workerCancelOnce.Do(func() { close(workerCanceled) }) }
		resultDone := make(chan shutdownResult, 1)
		go func() {
			resultDone <- joinRuntimeAndServer(serverDone, workerDone, cancelServer, cancelWorker, grace)
		}()

		// A healthy pair must be allowed to run longer than the shutdown grace.
		select {
		case result := <-resultDone:
			t.Fatalf("join ended before either component exited: %+v", result)
		case <-time.After(20 * time.Millisecond):
		}
		serverDone <- nil
		select {
		case <-workerCanceled:
		case <-time.After(time.Second):
			t.Fatal("server exit did not cancel worker")
		}
		workerDone <- nil

		select {
		case result := <-resultDone:
			if result.timedOut || result.serverErr != nil || result.workerErr != nil {
				t.Fatalf("server-first join result = %+v", result)
			}
		case <-time.After(time.Second):
			t.Fatal("server-first join did not finish")
		}
	})

	t.Run("worker first", func(t *testing.T) {
		serverDone := make(chan error, 1)
		workerDone := make(chan error, 1)
		serverCanceled := make(chan struct{})
		var serverCancelOnce sync.Once
		cancelServer := func() { serverCancelOnce.Do(func() { close(serverCanceled) }) }
		cancelWorker := func() {}
		resultDone := make(chan shutdownResult, 1)
		go func() {
			resultDone <- joinRuntimeAndServer(serverDone, workerDone, cancelServer, cancelWorker, grace)
		}()

		workerDone <- errors.New("worker stopped")
		select {
		case <-serverCanceled:
		case <-time.After(time.Second):
			t.Fatal("worker exit did not cancel server")
		}
		serverDone <- nil

		select {
		case result := <-resultDone:
			if result.timedOut || result.serverErr != nil || result.workerErr == nil {
				t.Fatalf("worker-first join result = %+v", result)
			}
		case <-time.After(time.Second):
			t.Fatal("worker-first join did not finish")
		}
	})

	t.Run("deadline cancels both", func(t *testing.T) {
		serverDone := make(chan error, 1)
		workerDone := make(chan error)
		serverCanceled := make(chan struct{})
		workerCanceled := make(chan struct{})
		var serverCancelOnce, workerCancelOnce sync.Once
		cancelServer := func() { serverCancelOnce.Do(func() { close(serverCanceled) }) }
		cancelWorker := func() { workerCancelOnce.Do(func() { close(workerCanceled) }) }
		serverDone <- nil

		result := joinRuntimeAndServer(serverDone, workerDone, cancelServer, cancelWorker, 20*time.Millisecond)
		if !result.timedOut {
			t.Fatalf("join did not time out: %+v", result)
		}
		for name, canceled := range map[string]<-chan struct{}{"server": serverCanceled, "worker": workerCanceled} {
			select {
			case <-canceled:
			default:
				t.Fatalf("shutdown deadline did not cancel %s", name)
			}
		}
	})
}

func TestNewIMServerProcess(t *testing.T) {
	apiAddr, opsAddr := twoFreeAddrs(t)
	cmd, stdout, stderr := helperCommand(t, "--api-addr", apiAddr, "--ops-addr", opsAddr)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitForStatus("http://"+apiAddr+"/api/v1/health", http.StatusOK); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("health did not become ready: %v; stdout=%q stderr=%q", err, stdout.String(), stderr.String())
	}
	if err := waitForStatus("http://"+opsAddr+"/ready", http.StatusOK); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("readiness did not become ready: %v; stdout=%q stderr=%q", err, stdout.String(), stderr.String())
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("SIGTERM did not produce a clean exit: %v; stdout=%q stderr=%q", err, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "http_request") {
		t.Fatalf("normalized request logging missing: %q", stderr.String())
	}
}

func TestNewIMServerSecondBindFailure(t *testing.T) {
	apiAddr, _ := twoFreeAddrs(t)
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	cmd, stdout, stderr := helperCommand(t, "--api-addr", apiAddr, "--ops-addr", held.Addr().String())
	if err := cmd.Run(); err == nil {
		t.Fatalf("second bind failure unexpectedly started: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), string(api.CodeBindFailed)) {
		t.Fatalf("bind failure code missing: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func helperCommand(t *testing.T, args ...string) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(os.Environ(), "NIM_SERVER_HELPER=1", "NIM_SERVER_ARGS="+string(encoded))
	cmd.Stdout = &bytes.Buffer{}
	cmd.Stderr = &bytes.Buffer{}
	stdout := cmd.Stdout.(*bytes.Buffer)
	stderr := cmd.Stderr.(*bytes.Buffer)
	return cmd, stdout, stderr
}

// TestHelperProcess runs only in the subprocess started by helperCommand.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("NIM_SERVER_HELPER") != "1" {
		return
	}
	var args []string
	if err := json.Unmarshal([]byte(os.Getenv("NIM_SERVER_ARGS")), &args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	code := run(args, os.Stdout, os.Stderr, func() (buildinfo.Info, error) {
		return buildinfo.Info{ServerVersion: "0.1.0-test"}, nil
	})
	os.Exit(code)
}

func twoFreeAddrs(t *testing.T) (string, string) {
	t.Helper()
	for {
		first := freeAddr(t)
		second := freeAddr(t)
		if first != second {
			return first, second
		}
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func waitForStatus(url string, want int) error {
	deadline := time.Now().Add(5 * time.Second)
	client := &http.Client{Timeout: 250 * time.Millisecond}
	var last error
	for time.Now().Before(deadline) {
		response, err := client.Get(url)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == want {
				return nil
			}
			last = errors.New(response.Status)
		} else {
			last = err
		}
		time.Sleep(25 * time.Millisecond)
	}
	return last
}
