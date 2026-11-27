package main

import (
	"errors"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/m-ice/NewIM/server/gateway"
	"github.com/m-ice/NewIM/server/media/s3store"
)

var errConfig = errors.New("invalid gateway configuration")

type config struct {
	addr                 string
	certFile             string
	keyFile              string
	dsn                  string
	allowLocalSocket     bool
	drainTimeout         time.Duration
	gateway              gateway.Config
	profiles             *httpLimits
	friends              *httpLimits
	groups               *httpLimits
	privateMessages      *httpLimits
	projectConversations bool
	conversationSync     *httpLimits
	readWatermark        *httpLimits
	syncKeysFile         string
	media                *httpLimits
	mediaRoot            string
	mediaS3              *s3store.Config
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
	cfg.conversationSync = budget("NEWIM_CONVERSATION_SYNC", "NEWIM_CONVERSATION_SYNC_IP_ATTEMPTS_PER_MINUTE", "NEWIM_CONVERSATION_SYNC_ACCOUNT_ATTEMPTS_PER_MINUTE")
	cfg.readWatermark = budget("NEWIM_READ_WATERMARK_HTTP", "NEWIM_READ_WATERMARK_IP_ATTEMPTS_PER_MINUTE", "NEWIM_READ_WATERMARK_ACCOUNT_ATTEMPTS_PER_MINUTE")
	cfg.media = budget("NEWIM_MEDIA_HTTP", "NEWIM_MEDIA_IP_ATTEMPTS_PER_MINUTE", "NEWIM_MEDIA_ACCOUNT_ATTEMPTS_PER_MINUTE")
	if cfg.privateMessages != nil && getenv("NEWIM_PRIVATE_MESSAGE_POLICY") != "1" {
		return config{}, errConfig
	}
	if invalidLimit || (cfg.groups != nil && cfg.privateMessages == nil) {
		return config{}, errConfig
	}
	if cfg.media != nil {
		if cfg.privateMessages == nil {
			return config{}, errConfig
		}
		switch getenv("NEWIM_MEDIA_STORAGE") {
		case "", "localfs":
			cfg.mediaRoot = getenv("NEWIM_MEDIA_LOCAL_ROOT")
			if !filepath.IsAbs(cfg.mediaRoot) {
				return config{}, errConfig
			}
		case "s3":
			cfg.mediaS3 = &s3store.Config{
				Region: getenv("NEWIM_MEDIA_S3_REGION"), Bucket: getenv("NEWIM_MEDIA_S3_BUCKET"),
				Endpoint: getenv("NEWIM_MEDIA_S3_ENDPOINT"), CAFile: getenv("NEWIM_MEDIA_S3_CA_FILE"),
				StagingRoot:  getenv("NEWIM_MEDIA_S3_STAGING_ROOT"),
				MaxTransfers: limit("NEWIM_MEDIA_S3_MAX_TRANSFERS", 8, 1, 32),
			}
			if invalidLimit || cfg.mediaS3.Region == "" || cfg.mediaS3.Bucket == "" ||
				!filepath.IsAbs(cfg.mediaS3.StagingRoot) ||
				(cfg.mediaS3.CAFile != "" && !filepath.IsAbs(cfg.mediaS3.CAFile)) {
				return config{}, errConfig
			}
		default:
			return config{}, errConfig
		}
	}
	switch getenv("NEWIM_CONVERSATION_PROJECTION") {
	case "", "0":
	case "1":
		cfg.projectConversations = true
	default:
		return config{}, errConfig
	}
	if cfg.conversationSync != nil {
		cfg.syncKeysFile = getenv("NEWIM_CONVERSATION_SYNC_KEYS_FILE")
		if !filepath.IsAbs(cfg.syncKeysFile) || (cfg.privateMessages != nil && !cfg.projectConversations) {
			return config{}, errConfig
		}
	}
	return cfg, nil
}
