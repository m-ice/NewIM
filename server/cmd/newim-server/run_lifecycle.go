package main

import (
	"sync"
	"time"
)

// joinRuntimeServerAndFatal coordinates HTTP drain before ordered pool closure
// and turns a guard terminal signal into a bounded nonzero process result.
// joinRuntimeServerAndFatal 协调 HTTP 排空后按序关闭 pool，并将 guard terminal 信号
// 转换为有界非零进程结果。
func joinRuntimeServerAndFatal(serverDone, workerDone <-chan error, shutdown <-chan struct{}, terminal *terminalState, cancelServer, cancelWorker func(), closeRuntime func(), grace time.Duration) shutdownResult {
	var result shutdownResult
	var timer *time.Timer
	var deadline <-chan time.Time
	armDeadline := func() {
		if timer == nil {
			timer = time.NewTimer(grace)
			deadline = timer.C
		}
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	var closeOnce sync.Once
	closeDone := make(chan struct{})
	beginClose := func() {
		closeOnce.Do(func() {
			go func() {
				if closeRuntime != nil {
					closeRuntime()
				}
				close(closeDone)
			}()
		})
	}

	terminalDone := terminal.Done()
	syncTerminal := func() {
		if result.terminalErr != nil {
			return
		}
		if err := terminal.Err(); err != nil {
			result.terminalErr = err
			terminalDone = nil
			shutdown = nil
			cancelServer()
			cancelWorker()
			armDeadline()
		}
	}

	for serverDone != nil || workerDone != nil || shutdown != nil || closeDone != nil {
		syncTerminal()
		if result.terminalErr != nil && serverDone == nil {
			beginClose()
		}
		select {
		case <-terminalDone:
			syncTerminal()
		case <-shutdown:
			shutdown = nil
			syncTerminal()
			cancelServer()
			cancelWorker()
			armDeadline()
			if serverDone == nil {
				beginClose()
			}
		case err := <-serverDone:
			serverDone = nil
			shutdown = nil
			result.serverErr = err
			syncTerminal()
			cancelWorker()
			beginClose()
			armDeadline()
		case err := <-workerDone:
			workerDone = nil
			shutdown = nil
			result.workerErr = err
			syncTerminal()
			cancelServer()
			armDeadline()
			if serverDone == nil {
				beginClose()
			}
		case <-closeDone:
			closeDone = nil
		case <-deadline:
			syncTerminal()
			cancelServer()
			cancelWorker()
			if result.terminalErr == nil {
				result.timedOut = true
			}
			beginClose()
			return result
		}
	}
	syncTerminal()
	return result
}
