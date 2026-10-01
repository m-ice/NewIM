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
