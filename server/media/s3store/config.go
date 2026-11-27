// Package s3store implements immutable media storage using signed S3 requests.
package s3store

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/m-ice/NewIM/server/media"
	"github.com/m-ice/NewIM/server/media/localfs"
)

const (
	defaultTransfers = 8
	maxTransfers     = 32
	startupTimeout   = 10 * time.Second
	maxCABytes       = 1 << 20
	maxErrorBytes    = 64 * 1024
)

type Config struct {
	Region       string
	Bucket       string
	Endpoint     string
	CAFile       string
	StagingRoot  string
	MaxTransfers int
}

// Open verifies the endpoint and staging directory before returning a store.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.MaxTransfers == 0 {
		cfg.MaxTransfers = defaultTransfers
	}
	endpoint, err := parseEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	if ctx == nil || !dnsLabel(cfg.Region) || !validBucket(cfg.Bucket) ||
		!filepath.IsAbs(cfg.StagingRoot) || cfg.MaxTransfers < 1 || cfg.MaxTransfers > maxTransfers {
		return nil, media.Fail(media.MediaInvalidInput)
	}
	if ctx.Err() != nil {
		return nil, unavailable()
	}
	ctx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()

	transport, err := objectTransport(cfg.CAFile, cfg.MaxTransfers)
	if err != nil {
		return nil, err
	}
	staging, err := localfs.OpenStaging(cfg.StagingRoot)
	if err != nil {
		return nil, unavailable()
	}
	store := &Store{staging: staging, transport: transport, bucket: cfg.Bucket, transfers: make(chan struct{}, cfg.MaxTransfers)}
	ready := false
	defer func() {
		if !ready {
			_ = store.Close()
		}
	}()

	// The credential chain keeps its own SDK transport (including EC2 metadata).
	// Only signed object requests use the endpoint-restricted client.
	loaded, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, unavailable()
	}
	client := &http.Client{
		Transport: objectEndpoint{transport: transport, endpoint: endpoint},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	// Construct S3 options explicitly so shared config and environment
	// endpoint overrides are never imported into the object client.
	options := s3.Options{
		Region:                     cfg.Region,
		Credentials:                loaded.Credentials,
		HTTPClient:                 client,
		Retryer:                    aws.NopRetryer{},
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
	}
	if endpoint != nil {
		options.BaseEndpoint = aws.String(endpoint.String())
		options.UsePathStyle = true
	}
	store.client = s3.New(options)
	if _, err = store.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(store.bucket)}); err != nil {
		return nil, unavailable()
	}
	ready = true
	return store, nil
}

func parseEndpoint(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, nil
	}
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Scheme != "https" || endpoint.User != nil ||
		!validHost(endpoint.Hostname()) || endpoint.RawQuery != "" || endpoint.ForceQuery ||
		strings.Contains(raw, "#") || strings.HasSuffix(endpoint.Host, ":") ||
		(endpoint.EscapedPath() != "" && endpoint.EscapedPath() != "/") {
		return nil, media.Fail(media.MediaInvalidInput)
	}
	if port := endpoint.Port(); port != "" {
		if number, err := strconv.ParseUint(port, 10, 16); err != nil || number == 0 {
			return nil, media.Fail(media.MediaInvalidInput)
		}
	}
	endpoint.Host = strings.ToLower(endpoint.Host)
	endpoint.Path = ""
	return endpoint, nil
}

func validHost(host string) bool {
	ip, err := netip.ParseAddr(host)
	return (err == nil && ip.Zone() == "") || dnsName(strings.ToLower(host))
}

func validBucket(bucket string) bool {
	_, ipErr := netip.ParseAddr(bucket)
	return len(bucket) >= 3 && len(bucket) <= 63 && ipErr != nil && dnsName(bucket)
}

func dnsName(name string) bool {
	if len(name) > 253 {
		return false
	}
	for label := range strings.SplitSeq(name, ".") {
		if !dnsLabel(label) {
			return false
		}
	}
	return true
}

func dnsLabel(label string) bool {
	if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
		return false
	}
	for i := range len(label) {
		ch := label[i]
		if !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-') {
			return false
		}
	}
	return true
}

type objectEndpoint struct {
	transport *http.Transport
	endpoint  *url.URL
}

func (t objectEndpoint) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "https" || req.URL.User != nil ||
		(t.endpoint != nil && !strings.EqualFold(req.URL.Host, t.endpoint.Host)) {
		return nil, errors.New("invalid media object endpoint")
	}
	response, err := t.transport.RoundTrip(req)
	if err == nil && response.StatusCode >= 300 {
		response.Body = struct {
			io.Reader
			io.Closer
		}{io.LimitReader(response.Body, maxErrorBytes), response.Body}
	}
	return response, err
}

func objectTransport(caPath string, limit int) (*http.Transport, error) {
	var roots *x509.CertPool
	if caPath != "" {
		pem, err := readCA(caPath)
		if err != nil {
			return nil, unavailable()
		}
		roots, err = x509.SystemCertPool()
		if err != nil || roots == nil {
			return nil, unavailable()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, unavailable()
		}
	}
	dialer := &net.Dialer{Timeout: startupTimeout, KeepAlive: 30 * time.Second}
	return &http.Transport{
		DialContext:            dialer.DialContext,
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           limit,
		MaxIdleConnsPerHost:    limit,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    startupTimeout,
		ResponseHeaderTimeout:  startupTimeout,
		MaxResponseHeaderBytes: 1 << 20,
		DisableCompression:     true,
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots},
	}, nil
}

func readCA(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, unavailable()
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, unavailable()
	}
	defer file.Close()
	pem, err := io.ReadAll(io.LimitReader(file, maxCABytes+1))
	if err != nil || len(pem) > maxCABytes {
		return nil, unavailable()
	}
	return pem, nil
}
