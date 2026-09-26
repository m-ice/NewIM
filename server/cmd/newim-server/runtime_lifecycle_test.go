package main

import (
	"sync"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/api"
)

func TestOrderedCloserOrderAndIdempotency(t *testing.T) {
	var mu sync.Mutex
	var order []string
	closer := newOrderedCloser(
		func() {
			mu.Lock()
			order = append(order, "message")
			mu.Unlock()
		},
		func() {
			mu.Lock()
			order = append(order, "auth")
			mu.Unlock()
		},
	)
	closer.Close()
	closer.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "message" || order[1] != "auth" {
		t.Fatalf("close order=%v want [message auth]", order)
	}
}

func TestJoinRuntimeServerAndFatalDrainsBeforeClose(t *testing.T) {
	serverDone := make(chan error, 1)
	fatal := make(chan struct{})
	shutdown := make(chan struct{})
	serverCanceled := make(chan struct{})
	workerCanceled := make(chan struct{})
	closeStarted := make(chan struct{})
	var serverOnce, workerOnce sync.Once

	resultDone := make(chan shutdownResult, 1)
	go func() {
		resultDone <- joinRuntimeServerAndFatal(
			serverDone,
			nil,
			shutdown,
			fatal,
			func() { serverOnce.Do(func() { close(serverCanceled) }) },
			func() { workerOnce.Do(func() { close(workerCanceled) }) },
			func() { close(closeStarted) },
			time.Second,
		)
	}()

	close(fatal)
	for name, canceled := range map[string]<-chan struct{}{"server": serverCanceled, "worker": workerCanceled} {
		select {
		case <-canceled:
		case <-time.After(time.Second):
			t.Fatalf("fatal guard did not cancel %s", name)
		}
	}
	select {
	case <-closeStarted:
		t.Fatal("runtime closed before HTTP server drained")
	default:
	}

	serverDone <- nil
	select {
	case result := <-resultDone:
		if result.timedOut || result.serverErr != nil {
			t.Fatalf("join result=%+v", result)
		}
		if result.terminalErr == nil || errorCode(result.terminalErr) != string(api.CodeInvalidMessageConfig) {
			t.Fatalf("terminal error=%v code=%q", result.terminalErr, errorCode(result.terminalErr))
		}
	case <-time.After(time.Second):
		t.Fatal("join did not finish after HTTP drain")
	}
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		t.Fatal("ordered runtime close did not start")
	}
}

func TestJoinRuntimeServerShutdownWaitsForDrain(t *testing.T) {
	serverDone := make(chan error, 1)
	shutdown := make(chan struct{})
	serverCanceled := make(chan struct{})
	closeStarted := make(chan struct{})
	resultDone := make(chan shutdownResult, 1)
	go func() {
		resultDone <- joinRuntimeServerAndFatal(
			serverDone,
			nil,
			shutdown,
			nil,
			func() { close(serverCanceled) },
			func() {},
			func() { close(closeStarted) },
			time.Second,
		)
	}()

	close(shutdown)
	select {
	case <-serverCanceled:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel server")
	}
	select {
	case <-closeStarted:
		t.Fatal("runtime closed before HTTP drain")
	default:
	}
	serverDone <- nil
	select {
	case result := <-resultDone:
		if result.timedOut || result.serverErr != nil || result.terminalErr != nil {
			t.Fatalf("join result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("join did not finish after HTTP drain")
	}
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		t.Fatal("runtime close did not run after HTTP drain")
	}
}

func TestJoinRuntimeServerAndFatalTimeoutClosesRuntime(t *testing.T) {
	serverDone := make(chan error)
	fatal := make(chan struct{})
	close(fatal)
	closeStarted := make(chan struct{})
	result := joinRuntimeServerAndFatal(
		serverDone,
		nil,
		nil,
		fatal,
		func() {},
		func() {},
		func() { close(closeStarted) },
		20*time.Millisecond,
	)
	if !result.timedOut {
		t.Fatalf("expected timeout, got %+v", result)
	}
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		t.Fatal("deadline did not start ordered runtime close")
	}
}

func TestJoinRuntimeServerAndFatalExitsAfterServerDone(t *testing.T) {
	serverDone := make(chan error, 1)
	fatal := make(chan struct{})
	resultDone := make(chan shutdownResult, 1)
	go func() {
		resultDone <- joinRuntimeServerAndFatal(
			serverDone,
			nil,
			nil,
			fatal,
			func() {},
			func() {},
			func() {},
			time.Second,
		)
	}()
	serverDone <- nil
	select {
	case result := <-resultDone:
		if result.timedOut || result.serverErr != nil || result.terminalErr != nil {
			t.Fatalf("join result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("join remained blocked on an open fatal channel after server completion")
	}
}
