package gateway

import (
	"net/netip"
	"sync"
	"time"
)

// ProfileBudget tracks TLS HTTP requests independently of WebSocket upgrades.
type ProfileBudget struct {
	mu        sync.Mutex
	window    time.Time
	addresses map[netip.Addr]int
	accounts  map[string]int
	ipLimit   int
	userLimit int
}

func NewProfileBudget(ipLimit, userLimit int) *ProfileBudget {
	return &ProfileBudget{
		addresses: make(map[netip.Addr]int), accounts: make(map[string]int),
		ipLimit: ipLimit, userLimit: userLimit,
	}
}

func (b *ProfileBudget) AllowAddress(addr netip.Addr) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advanceWindow()
	return countAttempt(b.addresses, addr, b.ipLimit)
}

func (b *ProfileBudget) AllowAccount(userID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advanceWindow()
	return countAttempt(b.accounts, userID, b.userLimit)
}

func (b *ProfileBudget) advanceWindow() {
	if newRateWindow(&b.window) {
		clear(b.addresses)
		clear(b.accounts)
	}
}
