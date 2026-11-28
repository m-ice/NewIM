package push

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/m-ice/NewIM/server/credentialfile"
)

type APNsConfig struct {
	TeamID  string
	KeyID   string
	Topic   string
	KeyFile string
	Sandbox bool
}

type APNsProvider struct {
	client     *http.Client
	url        string
	config     APNsConfig
	key        *ecdsa.PrivateKey
	now        func() time.Time
	mu         sync.Mutex
	bearer     string
	freshUntil time.Time
}

func NewAPNs(cfg APNsConfig) (*APNsProvider, error) {
	if !validID(cfg.TeamID) || !validID(cfg.KeyID) || !validTopic(cfg.Topic) {
		return nil, ErrConfig
	}
	encoded, err := credentialfile.Read(cfg.KeyFile, 1, 16384)
	defer clear(encoded)
	if err != nil {
		return nil, ErrConfig
	}
	der, err := privateKeyDER(encoded)
	defer clear(der)
	if err != nil {
		return nil, err
	}
	parsed, err := x509.ParsePKCS8PrivateKey(der)
	key, ok := parsed.(*ecdsa.PrivateKey)
	if err != nil || !ok || key.Curve != elliptic.P256() {
		return nil, ErrConfig
	}
	host := "https://api.push.apple.com"
	if cfg.Sandbox {
		host = "https://api.sandbox.push.apple.com"
	}
	return &APNsProvider{client: vendorClient(), url: host + "/3/device/", config: cfg, key: key, now: time.Now}, nil
}

func validTopic(topic string) bool {
	if len(topic) > 255 {
		return false
	}
	parts := strings.Split(topic, ".")
	if len(parts) < 2 {
		return false
	}
	for _, part := range parts {
		if !validID(part) {
			return false
		}
	}
	return true
}

func (p *APNsProvider) token(now time.Time) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bearer != "" && now.Before(p.freshUntil) {
		return p.bearer, nil
	}
	claims := map[string]any{"iss": p.config.TeamID, "iat": now.Unix()}
	header := map[string]string{"alg": "ES256", "kid": p.config.KeyID}
	signed, err := jwt(header, claims, func(hash [32]byte) ([]byte, error) {
		r, s, err := ecdsa.Sign(rand.Reader, p.key, hash[:])
		if err != nil {
			return nil, err
		}
		raw := make([]byte, 64)
		r.FillBytes(raw[:32])
		s.FillBytes(raw[32:])
		return raw, nil
	})
	if err != nil {
		return "", err
	}
	p.bearer, p.freshUntil = signed, now.Add(45*time.Minute)
	return p.bearer, nil
}

func (p *APNsProvider) Send(ctx context.Context, token []byte) Outcome {
	if !ValidToken(APNs, token) {
		return Outcome{Result: PermanentFailure}
	}
	now := p.now()
	bearer, err := p.token(now)
	if err != nil {
		return Outcome{Result: PermanentFailure}
	}
	body, err := json.Marshal(map[string]any{
		"aps": map[string]notification{"alert": genericNotification()}, "newim": "sync",
	})
	if err != nil {
		return Outcome{Result: PermanentFailure}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url+strings.ToLower(string(token)), bytes.NewReader(body))
	if err != nil {
		return Outcome{Result: PermanentFailure}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("apns-topic", p.config.Topic)
	req.Header.Set("apns-push-type", "alert")
	req.Header.Set("apns-priority", "10")
	req.Header.Set("apns-collapse-id", "newim-sync")
	req.Header.Set("apns-expiration", strconv.FormatInt(now.Add(notificationTTL).Unix(), 10))
	resp, err := p.client.Do(req)
	if err != nil {
		return Outcome{Result: TemporaryFailure}
	}
	payload, err := vendorResponse(resp)
	defer clear(payload)
	if err != nil {
		return Outcome{Result: TemporaryFailure}
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return Outcome{Result: Accepted}
	case http.StatusGone:
		var reason struct {
			Reason    string `json:"reason"`
			Timestamp int64  `json:"timestamp"`
		}
		if json.Unmarshal(payload, &reason) == nil && (reason.Reason == "Unregistered" || reason.Reason == "ExpiredToken") {
			invalidAt := time.Time{}
			if reason.Timestamp > 0 {
				invalidAt = time.UnixMilli(reason.Timestamp)
			}
			return Outcome{Result: InvalidToken, InvalidAt: invalidAt}
		}
	}
	fallback := time.Duration(0)
	if resp.StatusCode >= 500 {
		fallback = 15 * time.Minute
	}
	return vendorFailure(resp, now, fallback)
}
