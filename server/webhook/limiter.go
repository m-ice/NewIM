package webhook

import (
	"sync"
	"time"
)

type destinationState struct {
	active int
	tokens float64
	last   time.Time
}

type destinationLimiter struct {
	mu     sync.Mutex
	max    int
	rate   float64
	burst  float64
	states map[string]*destinationState
}

func newDestinationLimiter(max int, rate, burst float64) *destinationLimiter {
	return &destinationLimiter{max: max, rate: rate, burst: burst, states: make(map[string]*destinationState)}
}

func (l *destinationLimiter) acquire(destination string, now time.Time) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	state := l.states[destination]
	if state == nil {
		state = &destinationState{tokens: l.burst, last: now}
		l.states[destination] = state
	}
	elapsed := now.Sub(state.last).Seconds()
	if elapsed > 0 {
		state.tokens += elapsed * l.rate
		if state.tokens > l.burst {
			state.tokens = l.burst
		}
		state.last = now
	}
	if state.active >= l.max || state.tokens < 1 {
		return false
	}
	state.active++
	state.tokens--
	return true
}

func (l *destinationLimiter) release(destination string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if state := l.states[destination]; state != nil && state.active > 0 {
		state.active--
	}
}
