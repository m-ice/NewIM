package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
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
	terminal := newTerminalState()
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
			terminal,
			func() { serverOnce.Do(func() { close(serverCanceled) }) },
			func() { workerOnce.Do(func() { close(workerCanceled) }) },
			func() { close(closeStarted) },
			time.Second,
		)
	}()

	terminal.Trip(errInvalidMessageConfig)
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
			newTerminalState(),
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
	terminal := newTerminalState()
	shutdown := make(chan struct{})
	close(shutdown)
	closeStarted := make(chan struct{})
	result := joinRuntimeServerAndFatal(
		serverDone,
		nil,
		shutdown,
		terminal,
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
	terminal := newTerminalState()
	resultDone := make(chan shutdownResult, 1)
	go func() {
		resultDone <- joinRuntimeServerAndFatal(
			serverDone,
			nil,
			nil,
			terminal,
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

func TestJoinRuntimeServerAndFatalFatalWinsShutdown(t *testing.T) {
	serverDone := make(chan error, 1)
	serverDone <- nil
	shutdown := make(chan struct{})
	close(shutdown)
	terminal := newTerminalState()
	terminal.Trip(errInvalidMessageConfig)

	result := joinRuntimeServerAndFatal(serverDone, nil, shutdown, terminal, func() {}, func() {}, func() {}, time.Second)
	if result.timedOut || result.terminalErr == nil || errorCode(result.terminalErr) != string(api.CodeInvalidMessageConfig) {
		t.Fatalf("join result=%+v", result)
	}
}

func TestJoinRuntimeServerAndFatalFatalWinsServerDone(t *testing.T) {
	serverDone := make(chan error, 1)
	serverDone <- nil
	terminal := newTerminalState()
	terminal.Trip(errInvalidMessageConfig)

	result := joinRuntimeServerAndFatal(serverDone, nil, nil, terminal, func() {}, func() {}, func() {}, time.Second)
	if result.timedOut || result.terminalErr == nil || errorCode(result.terminalErr) != string(api.CodeInvalidMessageConfig) {
		t.Fatalf("join result=%+v", result)
	}
}

func TestJoinRuntimeServerAndFatalFatalWinsTimeout(t *testing.T) {
	serverDone := make(chan error)
	terminal := newTerminalState()
	terminal.Trip(errInvalidMessageConfig)

	result := joinRuntimeServerAndFatal(serverDone, nil, nil, terminal, func() {}, func() {}, func() {}, 20*time.Millisecond)
	if result.timedOut || result.terminalErr == nil || errorCode(result.terminalErr) != string(api.CodeInvalidMessageConfig) {
		t.Fatalf("join result=%+v", result)
	}
}

// Hold publication to model a callback that has not yet been scheduled; guard
// completion must not become visible before the terminal error is published.
func TestGuardPublicationPrecedesCoordinatorCompletion(t *testing.T) {
	terminal := newTerminalState()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	guard := postgresidentity.NewGuard(func(error) {
		close(entered)
		<-release
		terminal.Trip(errInvalidMessageConfig)
	})
	verified := make(chan error, 1)
	go func() { verified <- guard.Verify(context.Background(), nil) }()
	<-entered
	select {
	case <-verified:
		t.Fatal("Verify completed before terminal publication")
	case <-time.After(25 * time.Millisecond):
	}
	unblock()
	if err := <-verified; err == nil {
		t.Fatal("guard did not fail closed")
	}
	if guard.Tripped() == nil {
		t.Fatal("guard lost terminal state")
	}
	serverDone := make(chan error, 1)
	serverDone <- nil
	result := joinRuntimeServerAndFatal(serverDone, nil, nil, terminal, func() {}, func() {}, func() {}, time.Second)
	if result.terminalErr != errInvalidMessageConfig {
		t.Fatalf("terminal result=%+v", result)
	}
}
