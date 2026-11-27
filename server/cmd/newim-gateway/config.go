package main

import (
	"errors"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/m-ice/NewIM/server/gateway"
)

var errConfig = errors.New("invalid gateway configuration")

type config struct {
	addr             string
	certFile         string
	keyFile          string
	dsn              string
	allowLocalSocket bool
	drainTimeout     time.Duration
	gateway          gateway.Config
	profiles         *httpLimits
	friends          *httpLimits
	groups           *httpLimits
	privateMessages  *httpLimits
}

type httpLimits struct {
	ip, account int
}

func readConfig(getenv func(string) string) (config, error) {
	cfg := config{
		addr: getenv("NEWIM_GATEWAY_ADDR"), certFile: getenv("NEWIM_GATEWAY_TLS_CERT_FILE"),
		keyFile: getenv("NEWIM_GATEWAY_TLS_KEY_FILE"), dsn: strings.TrimSpace(getenv("NEWIM_AUTH_DSN")),
	}
	if _, _, err := net.SplitHostPort(cfg.addr); err != nil || cfg.dsn == "" ||
		!filepath.IsAbs(cfg.certFile) || !filepath.IsAbs(cfg.keyFile) || getenv("NEWIM_AUTH_DEVICE_ADMISSION") != "1" {
		return config{}, errConfig
	}
	switch getenv("NEWIM_AUTH_ALLOW_LOCAL_SOCKET") {
	case "", "0":
	case "1":
		cfg.allowLocalSocket = true
	default:
		return config{}, errConfig
	}

	var invalidLimit bool
	limit := func(name string, fallback, min, max int) int {
		raw := getenv(name)
		if raw == "" {
			return fallback
		}
		value, err := strconv.Atoi(raw)
		if err != nil || value < min || value > max || strconv.Itoa(value) != raw {
			invalidLimit = true
		}
		return value
	}
	cfg.gateway = gateway.Config{
		MaxConnections:   limit("NEWIM_GATEWAY_MAX_CONNECTIONS", 128, 1, 1024),
		IPAttempts:       limit("NEWIM_GATEWAY_IP_ATTEMPTS_PER_MINUTE", 64, 1, 4096),
		DeviceAttempts:   limit("NEWIM_GATEWAY_DEVICE_ATTEMPTS_PER_MINUTE", 12, 1, 4096),
		SubscribeTimeout: time.Duration(limit("NEWIM_GATEWAY_SUBSCRIBE_TIMEOUT_MS", 3000, 100, 15000)) * time.Millisecond,
		WriteTimeout:     time.Duration(limit("NEWIM_GATEWAY_WRITE_TIMEOUT_MS", 3000, 100, 5000)) * time.Millisecond,
	}
	cfg.drainTimeout = time.Duration(limit("NEWIM_GATEWAY_DRAIN_TIMEOUT_MS", 12000, 100, 15000)) * time.Millisecond
	budget := func(toggle, ip, account string) *httpLimits {
		switch getenv(toggle) {
		case "", "0":
			return nil
		case "1":
			return &httpLimits{ip: limit(ip, 120, 1, 4096), account: limit(account, 60, 1, 4096)}
		default:
			invalidLimit = true
			return nil
		}
	}
	cfg.profiles = budget("NEWIM_USER_PROFILE", "NEWIM_USER_PROFILE_IP_ATTEMPTS_PER_MINUTE", "NEWIM_USER_PROFILE_ACCOUNT_ATTEMPTS_PER_MINUTE")
	cfg.friends = budget("NEWIM_FRIEND_HTTP", "NEWIM_FRIEND_IP_ATTEMPTS_PER_MINUTE", "NEWIM_FRIEND_ACCOUNT_ATTEMPTS_PER_MINUTE")
	cfg.groups = budget("NEWIM_GROUP_HTTP", "NEWIM_GROUP_IP_ATTEMPTS_PER_MINUTE", "NEWIM_GROUP_ACCOUNT_ATTEMPTS_PER_MINUTE")
	cfg.privateMessages = budget("NEWIM_TLS_PRIVATE_MESSAGES", "NEWIM_TLS_MESSAGE_IP_ATTEMPTS_PER_MINUTE", "NEWIM_TLS_MESSAGE_ACCOUNT_ATTEMPTS_PER_MINUTE")
	if cfg.privateMessages != nil && getenv("NEWIM_PRIVATE_MESSAGE_POLICY") != "1" {
		return config{}, errConfig
	}
	if invalidLimit || (cfg.groups != nil && cfg.privateMessages == nil) {
		return config{}, errConfig
	}
	return cfg, nil
}
