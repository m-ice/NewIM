package main

import "sync"

// terminalState records the first terminal failure before publishing it to the
// coordinator, so completion/timeout races cannot erase a guard failure.
// terminalState 在发布首个 terminal 失败前先保存它，避免 completion/timeout 竞态丢失。
type terminalState struct {
	mu   sync.Mutex
	err  error
	done chan struct{}
}

func newTerminalState() *terminalState {
	return &terminalState{done: make(chan struct{})}
}

func (t *terminalState) Trip(err error) {
	if t == nil || err == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err != nil {
		return
	}
	t.err = err
	close(t.done)
}

func (t *terminalState) Err() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

func (t *terminalState) Done() <-chan struct{} {
	if t == nil {
		return nil
	}
	return t.done
}

// orderedCloser releases message resources before auth resources exactly once.
// orderedCloser 恰好一次地先释放 message 资源再释放 auth 资源。
type orderedCloser struct {
	once         sync.Once
	closeMessage func()
	closeAuth    func()
}

func newOrderedCloser(closeMessage, closeAuth func()) *orderedCloser {
	return &orderedCloser{closeMessage: closeMessage, closeAuth: closeAuth}
}

// Close is idempotent, including when one or both resources are absent.
// Close 幂等，且允许一个或两个资源缺失。
func (c *orderedCloser) Close() {
	if c == nil {
		return
	}
	c.once.Do(func() {
		if c.closeMessage != nil {
			c.closeMessage()
		}
		if c.closeAuth != nil {
			c.closeAuth()
		}
	})
}
