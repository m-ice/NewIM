package webhook

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type blockingLockConnection struct {
	execStarted   chan struct{}
	hijackCalled  chan struct{}
	releaseCalled chan struct{}
}

func (c *blockingLockConnection) Exec(ctx context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	close(c.execStarted)
	<-ctx.Done()
	return pgconn.CommandTag{}, ctx.Err()
}

func (*blockingLockConnection) QueryRow(context.Context, string, ...any) pgx.Row {
	return nil
}

func (c *blockingLockConnection) Hijack() *pgx.Conn {
	close(c.hijackCalled)
	return nil
}

func (c *blockingLockConnection) Release() {
	close(c.releaseCalled)
}

func TestWorkerLockReleaseIsBounded(t *testing.T) {
	connection := &blockingLockConnection{
		execStarted:   make(chan struct{}),
		hijackCalled:  make(chan struct{}),
		releaseCalled: make(chan struct{}),
	}
	lock := &WorkerLock{conn: connection, releaseTimeout: 20 * time.Millisecond}

	started := time.Now()
	lock.Release()
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("Release elapsed=%s; want bounded by configured timeout", elapsed)
	}
	select {
	case <-connection.execStarted:
	default:
		t.Fatal("Release did not attempt advisory unlock")
	}
	select {
	case <-connection.hijackCalled:
	default:
		t.Fatal("Release did not discard a connection blocked by timeout")
	}
	select {
	case <-connection.releaseCalled:
		t.Fatal("Release returned a timed-out connection to the pool")
	default:
	}
}

type blockingCheckConnection struct {
	checkStarted  chan struct{}
	releaseCheck  chan struct{}
	execStarted   chan struct{}
	releaseCalled chan struct{}
}

func (c *blockingCheckConnection) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	close(c.execStarted)
	return pgconn.CommandTag{}, nil
}

func (c *blockingCheckConnection) QueryRow(context.Context, string, ...any) pgx.Row {
	close(c.checkStarted)
	return blockingCheckRow{release: c.releaseCheck}
}

func (*blockingCheckConnection) Hijack() *pgx.Conn { return nil }

func (c *blockingCheckConnection) Release() { close(c.releaseCalled) }

type blockingCheckRow struct{ release <-chan struct{} }

func (r blockingCheckRow) Scan(...any) error {
	<-r.release
	return nil
}

func TestWorkerLockReleaseWaitsForInFlightCheck(t *testing.T) {
	connection := &blockingCheckConnection{
		checkStarted:  make(chan struct{}),
		releaseCheck:  make(chan struct{}),
		execStarted:   make(chan struct{}),
		releaseCalled: make(chan struct{}),
	}
	lock := &WorkerLock{conn: connection, releaseTimeout: time.Second}

	checkDone := make(chan error, 1)
	go func() { checkDone <- lock.Check(context.Background()) }()
	<-connection.checkStarted

	releaseDone := make(chan struct{})
	go func() {
		lock.Release()
		close(releaseDone)
	}()

	select {
	case <-connection.execStarted:
		t.Fatal("Release used the lock connection while Check was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(connection.releaseCheck)
	if err := <-checkDone; err == nil {
		t.Fatal("Check returned success without an observed advisory lock")
	}
	select {
	case <-releaseDone:
	case <-time.After(time.Second):
		t.Fatal("Release did not complete after Check finished")
	}
	select {
	case <-connection.execStarted:
	default:
		t.Fatal("Release did not attempt advisory unlock")
	}
	select {
	case <-connection.releaseCalled:
	default:
		t.Fatal("Release did not return the connection")
	}
}
