package webhook

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSecureClientRejectsUnsafeTargets(t *testing.T) {
	client, err := NewSecureClient(nil, Policy{}, time.Second, 1024)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{
		"http://example.com/hook",
		"https://user:password@example.com/hook",
		"ftp://example.com/hook",
		"https://example.com/hook#fragment",
	} {
		if _, err = client.Do(context.Background(), target, nil, []byte(`{}`)); err == nil {
			t.Fatalf("unsafe target accepted: %s", target)
		}
	}
}

func TestSecureClientRealHTTPAndRedirectBoundary(t *testing.T) {
	var followed atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/followed", http.StatusFound)
			return
		}
		followed.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewSecureClient(nil, Policy{AllowHTTP: true, AllowLoopback: true}, time.Second, 1024)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(context.Background(), server.URL+"/redirect", nil, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusFound || followed.Load() != 0 {
		t.Fatalf("redirect status=%d followed=%d", response.StatusCode, followed.Load())
	}
}

func TestSecureClientResponseLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("0123456789abcdef"))
	}))
	defer server.Close()
	client, err := NewSecureClient(nil, Policy{AllowHTTP: true, AllowLoopback: true}, time.Second, 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Do(context.Background(), server.URL, nil, []byte(`{}`)); ErrorCode(err) != CodeResponseTooLarge {
		t.Fatalf("response limit error = %v", err)
	}
}

func TestRetryAfterParsing(t *testing.T) {
	if got := parseRetryAfter("2"); got != 2*time.Second {
		t.Fatalf("seconds retry-after = %s", got)
	}
	if got := parseRetryAfter("invalid"); got != 0 {
		t.Fatalf("invalid retry-after = %s", got)
	}
}

func TestSecureClientAddressPolicy(t *testing.T) {
	client, err := NewSecureClient(nil, Policy{}, time.Second, 1024)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		ip      string
		allowed bool
	}{
		{"127.0.0.1", false},
		{"::1", false},
		{"10.0.0.1", false},
		{"192.168.1.1", false},
		{"192.0.2.1", false},
		{"64:ff9b::7f00:1", false},
		{"64:ff9b:1::7f00:1", false},
		{"2002:7f00:1::1", false},
		{"2001::7f00:1", false},
		{"fec0::1", false},
		{"8.8.8.8", true},
		{"2001:4860:4860::8888", true},
	} {
		if got := client.allowed(net.ParseIP(test.ip)); got != test.allowed {
			t.Fatalf("allowed(%s)=%v want %v", test.ip, got, test.allowed)
		}
	}
}
