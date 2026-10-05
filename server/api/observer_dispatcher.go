package api

import "sync"

const defaultObserverQueueCapacity = 256

// ObserverDispatcher isolates best-effort observer callbacks from authoritative work.
// ObserverDispatcher 将最佳努力的 observer 回调与权威业务路径隔离，队列满或回调
// panic 时只记录固定 drop reason，不阻塞调用方。
type ObserverDispatcher[T any] struct {
	observer func(T)
	onDrop   func(string)
	queue    chan T
	close    sync.Once
}

// NewObserverDispatcher starts one bounded FIFO observer worker.
// NewObserverDispatcher 启动一个有界 FIFO observer worker；capacity 超过上限时收敛。
func NewObserverDispatcher[T any](capacity int, observer func(T), onDrop func(string)) *ObserverDispatcher[T] {
	if observer == nil {
		return nil
	}
	if capacity < 1 {
		capacity = 1
	}
	if capacity > 4096 {
		capacity = 4096
	}
	dispatcher := &ObserverDispatcher[T]{
		observer: observer,
		onDrop:   onDrop,
		queue:    make(chan T, capacity),
	}
	go dispatcher.run()
	return dispatcher
}

// Enqueue performs one non-blocking submission in FIFO order.
// Enqueue 按 FIFO 非阻塞提交一次观测；队列满时丢弃并计数。
func (d *ObserverDispatcher[T]) Enqueue(value T) {
	if d == nil {
		return
	}
	defer func() {
		if recover() != nil {
			d.drop("queue_full")
		}
	}()
	select {
	case d.queue <- value:
	default:
		d.drop("queue_full")
	}
}

// Close stops accepting observations without waiting for a blocked observer.
// Close 停止接收新观测但不会等待已经阻塞的 observer。
func (d *ObserverDispatcher[T]) Close() {
	if d == nil {
		return
	}
	d.close.Do(func() { close(d.queue) })
}

func (d *ObserverDispatcher[T]) run() {
	for value := range d.queue {
		func() {
			defer func() {
				if recover() != nil {
					d.drop("writer_error")
				}
			}()
			d.observer(value)
		}()
	}
}

func (d *ObserverDispatcher[T]) drop(reason string) {
	if d == nil || d.onDrop == nil {
		return
	}
	defer func() { _ = recover() }()
	d.onDrop(reason)
}
