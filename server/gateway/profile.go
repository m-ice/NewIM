package gateway

import (
	"net/netip"
	"sync"
	"time"
)

// ProfileBudget tracks TLS HTTP requests independently of WebSocket upgrades.
type ProfileBudget struct {
	mu          sync.Mutex
	window      time.Time
	addresses   map[netip.Addr]int
	accounts    map[string]int
	devices     map[deviceKey]int
	ipLimit     int
	userLimit   int
	deviceLimit int
}

// NewMessageBudget adds an authenticated device budget for message sends.
func NewMessageBudget(ipLimit, userLimit, deviceLimit int) *ProfileBudget {
	budget := NewProfileBudget(ipLimit, userLimit)
	// The account window already bounds every device when its cap is tighter.
	if deviceLimit < userLimit {
		budget.devices = make(map[deviceKey]int)
		budget.deviceLimit = deviceLimit
	}
	return budget
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

func (b *ProfileBudget) allowSend(userID, deviceID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advanceWindow()
	if b.devices == nil {
		return countAttempt(b.accounts, userID, b.userLimit)
	}
	key := deviceKey{user: userID, device: deviceID}
	if !canCountAttempt(b.accounts, userID, b.userLimit) || !canCountAttempt(b.devices, key, b.deviceLimit) {
		return false
	}
	b.accounts[userID]++
	b.devices[key]++
	return true
}

func (b *ProfileBudget) advanceWindow() {
	if newRateWindow(&b.window) {
		clear(b.addresses)
		clear(b.accounts)
		clear(b.devices)
	}
}
