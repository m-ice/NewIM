package api

import (
	"testing"
	"time"
)

func TestObserverDispatcherBlockedObserverDoesNotBlockOrGrowUnbounded(t *testing.T) {
	blocked := make(chan struct{})
	release := make(chan struct{})
	drops := make(chan string, 1)
	dispatcher := NewObserverDispatcher(1, func(int) {
		close(blocked)
		<-release
	}, func(reason string) {
		select {
		case drops <- reason:
		default:
		}
	})
	if dispatcher == nil {
		t.Fatal("dispatcher is nil")
	}
	dispatcher.Enqueue(1)
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("observer did not start")
	}
	done := make(chan struct{})
	go func() {
		dispatcher.Enqueue(2)
		dispatcher.Enqueue(3)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("blocked observer backpressured enqueue")
	}
	select {
	case reason := <-drops:
		if reason != "queue_full" {
			t.Fatalf("drop reason=%q", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("queue overflow was not counted")
	}
	close(release)
	dispatcher.Close()
}

func TestObserverDispatcherPanicIsCounted(t *testing.T) {
	drops := make(chan string, 1)
	dispatcher := NewObserverDispatcher(1, func(int) { panic("observer failure") }, func(reason string) {
		select {
		case drops <- reason:
		default:
		}
	})
	dispatcher.Enqueue(1)
	select {
	case reason := <-drops:
		if reason != "writer_error" {
			t.Fatalf("drop reason=%q", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("observer panic was not counted")
	}
	dispatcher.Close()
}
