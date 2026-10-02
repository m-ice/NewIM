package main

import (
	"context"
	"testing"
	"time"

	app "github.com/m-ice/NewIM/server/webhook"
)

type blockingWebhookWorker struct{}

func (*blockingWebhookWorker) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

type blockingWebhookLock struct {
	checkStarted chan struct{}
	releaseCheck chan struct{}
}

func (l *blockingWebhookLock) Check(context.Context) error {
	close(l.checkStarted)
	<-l.releaseCheck
	return app.Fail(app.CodeBacklogPaused)
}

func (*blockingWebhookLock) Release() {}

func TestWebhookRuntimeRunTreatsLockFailureDuringShutdownAsNormal(t *testing.T) {
	worker := &blockingWebhookWorker{}
	lock := &blockingWebhookLock{
		checkStarted: make(chan struct{}),
		releaseCheck: make(chan struct{}),
	}
	runtime := &webhookRuntime{worker: worker, lock: lock}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()

	select {
	case <-lock.checkStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("lock check did not run")
	}
	cancel()
	close(lock.releaseCheck)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown lock failure = %v; want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not stop after cancellation")
	}
}
