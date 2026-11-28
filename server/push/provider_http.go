package push

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	vendorBodyLimit = 16 * 1024
	notificationTTL = 15 * time.Minute
)

func vendorClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
			ForceAttemptHTTP2:     true,
			TLSHandshakeTimeout:   3 * time.Second,
			ResponseHeaderTimeout: 6 * time.Second,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConns:          4,
			MaxIdleConnsPerHost:   2,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func vendorResponse(response *http.Response) ([]byte, error) {
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, vendorBodyLimit+1))
	if err != nil || len(body) > vendorBodyLimit {
		clear(body)
		return nil, errors.New("PUSH_VENDOR_RESPONSE_INVALID")
	}
	return body, nil
}

func retryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil && seconds > 0 {
		return time.Duration(min(seconds, int64(86400))) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(min(at.Sub(now), 24*time.Hour), 0)
	}
	return 0
}

func vendorFailure(response *http.Response, now time.Time, fallback time.Duration) Outcome {
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
		delay := retryAfter(response.Header.Get("Retry-After"), now)
		if delay == 0 {
			delay = fallback
		}
		return Outcome{Result: TemporaryFailure, RetryAfter: delay}
	}
	return Outcome{Result: PermanentFailure}
}

func jwt(header, claims any, sign func([32]byte) ([]byte, error)) (string, error) {
	h, headerErr := json.Marshal(header)
	c, claimsErr := json.Marshal(claims)
	if headerErr != nil || claimsErr != nil {
		return "", ErrConfig
	}
	encoding := base64.RawURLEncoding
	base := encoding.EncodeToString(h) + "." + encoding.EncodeToString(c)
	signature, err := sign(sha256.Sum256([]byte(base)))
	if err != nil {
		return "", ErrConfig
	}
	return base + "." + encoding.EncodeToString(signature), nil
}

func privateKeyDER(encoded []byte) ([]byte, error) {
	key, rest := pem.Decode(encoded)
	if key == nil || key.Type != "PRIVATE KEY" || strings.TrimSpace(string(rest)) != "" {
		return nil, ErrConfig
	}
	return key.Bytes, nil
}

type notification struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

func genericNotification() notification {
	return notification{Title: "新消息", Body: "打开应用查看"}
}
