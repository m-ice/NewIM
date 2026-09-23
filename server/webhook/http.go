package webhook

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

// IPResolver resolves a hostname before a protected outbound dial.
// IPResolver 在受保护出站拨号前解析 hostname。
type IPResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

// Policy controls the explicit non-production test seams for outbound requests.
// Policy 控制出站请求的显式非生产测试 seam；生产默认全部关闭。
type Policy struct {
	AllowHTTP     bool
	AllowLoopback bool
	AllowPrivate  bool
}

// SecureClient resolves, validates and pins outbound webhook connections.
// SecureClient 解析、校验并固定 Webhook 出站连接。
type SecureClient struct {
	resolver IPResolver
	policy   Policy
	timeout  time.Duration
	maxBody  int64
}

// NewSecureClient creates a production-safe client; test policies must be explicit.
// NewSecureClient 创建生产安全客户端；测试策略必须显式传入。
func NewSecureClient(resolver IPResolver, policy Policy, timeout time.Duration, maxBody int64) (*SecureClient, error) {
	if timeout <= 0 || timeout > time.Minute || maxBody <= 0 || maxBody > 1<<20 {
		return nil, Fail(CodeInvalidConfig)
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	return &SecureClient{resolver: resolver, policy: policy, timeout: timeout, maxBody: maxBody}, nil
}

// Do resolves all addresses once, validates them, pins the approved address and sends one POST.
// Do 一次解析全部地址、校验后固定到已批准地址，并发送一次 POST。
func (c *SecureClient) Do(ctx context.Context, rawURL string, headers map[string]string, body []byte) (Response, error) {
	if c == nil || c.resolver == nil || ctx == nil || len(body) > int(c.maxBody) {
		return Response{}, Fail(CodeInvalidConfig)
	}
	target, err := url.Parse(rawURL)
	if err != nil || target.IsAbs() == false || target.Host == "" || target.User != nil || target.Fragment != "" || target.Opaque != "" {
		return Response{}, Fail(CodeInvalidConfig)
	}
	host := target.Hostname()
	if host == "" || strings.ContainsAny(host, "\r\n\t ") {
		return Response{}, Fail(CodeInvalidConfig)
	}
	httpsRequired := !strings.EqualFold(target.Scheme, "https")
	if httpsRequired && !c.policy.AllowHTTP {
		return Response{}, Fail(CodeInvalidConfig)
	}
	if !strings.EqualFold(target.Scheme, "https") && !strings.EqualFold(target.Scheme, "http") {
		return Response{}, Fail(CodeInvalidConfig)
	}
	port := target.Port()
	if port == "" {
		if strings.EqualFold(target.Scheme, "https") {
			port = "443"
		} else {
			port = "80"
		}
	}
	addresses, err := c.resolver.LookupIPAddr(ctx, host)
	if err != nil || len(addresses) == 0 {
		return Response{}, Fail(CodeHTTPTemporary)
	}
	approved := make([]string, 0, len(addresses))
	for _, resolved := range addresses {
		if !c.allowed(resolved.IP) {
			continue
		}
		approved = append(approved, resolved.IP.String())
	}
	if len(approved) == 0 {
		return Response{}, Fail(CodeEndpointRevoked)
	}
	dialer := &net.Dialer{Timeout: c.timeout, KeepAlive: 30 * time.Second}
	dialContext := func(dialCtx context.Context, network, address string) (net.Conn, error) {
		_, dialPort, splitErr := net.SplitHostPort(address)
		if splitErr != nil {
			dialPort = port
		}
		var lastErr error
		for _, ip := range approved {
			conn, dialErr := dialer.DialContext(dialCtx, network, net.JoinHostPort(ip, dialPort))
			if dialErr == nil {
				return conn, nil
			}
			lastErr = dialErr
		}
		if lastErr == nil {
			lastErr = errors.New("no approved address")
		}
		return nil, lastErr
	}
	transport := &http.Transport{
		Proxy:             nil,
		DialContext:       dialContext,
		ForceAttemptHTTP2: true,
		TLSClientConfig:   &tls.Config{MinVersion: tls.VersionTLS12},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		Timeout:   c.timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		return Response{}, Fail(CodeInvalidConfig)
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		if strings.EqualFold(key, "Host") || strings.ContainsAny(key, "\r\n") || strings.ContainsAny(value, "\r\n") {
			return Response{}, Fail(CodeInvalidConfig)
		}
		request.Header.Set(key, value)
	}
	response, err := client.Do(request)
	if err != nil {
		return Response{}, classifyHTTPError(ctx, err)
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, c.maxBody+1)
	responseBody, readErr := io.ReadAll(limited)
	if readErr != nil {
		return Response{}, Fail(CodeHTTPTemporary)
	}
	if int64(len(responseBody)) > c.maxBody {
		return Response{}, Fail(CodeResponseTooLarge)
	}
	return Response{StatusCode: response.StatusCode, Body: responseBody}, nil
}

func classifyHTTPError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return Fail(CodeHTTPTemporary)
	}
	return Fail(CodeHTTPTemporary)
}

func (c *SecureClient) allowed(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	addr = addr.Unmap()
	if addr.IsLoopback() {
		return c.policy.AllowLoopback
	}
	if addr.IsPrivate() {
		return c.policy.AllowPrivate
	}
	if addr.IsUnspecified() || addr.IsMulticast() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || addr.IsInterfaceLocalMulticast() {
		return false
	}
	for _, prefix := range blockedPrefixes {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("2001:2::/48"),
	netip.MustParsePrefix("2001:10::/28"),
	netip.MustParsePrefix("2001:db8::/32"),
}
