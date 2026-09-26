package main

import "sync"

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
