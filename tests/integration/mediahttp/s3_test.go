//go:build integration

package mediahttp_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

type s3Fixture struct {
	Bucket           string `json:"bucket"`
	Region           string `json:"region"`
	Endpoint         string `json:"endpoint"`
	MismatchEndpoint string `json:"mismatchEndpoint"`
	CAFile           string `json:"caFile"`
	AccessKey        string `json:"accessKey"`
	SecretKey        string `json:"secretKey"`
}

func readS3Fixture(t *testing.T) *s3Fixture {
	t.Helper()
	path := os.Getenv("NEWIM_TEST_S3_SETTINGS")
	if path == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	must(t, err)
	var settings s3Fixture
	if err = json.Unmarshal(raw, &settings); err != nil {
		t.Fatal("could not decode the private S3 fixture settings")
	}
	if settings.Bucket == "" || settings.Region == "" || settings.Endpoint == "" ||
		settings.MismatchEndpoint == "" || !filepath.IsAbs(settings.CAFile) ||
		settings.AccessKey == "" || settings.SecretKey == "" {
		t.Fatal("missing S3 fixture setting")
	}
	return &settings
}

func (settings *s3Fixture) client(t *testing.T, anonymous bool) *s3.Client {
	t.Helper()
	raw, err := os.ReadFile(settings.CAFile)
	must(t, err)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(raw) {
		t.Fatal("could not load the S3 fixture certificate")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	httpClient := &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var creds aws.CredentialsProvider = credentials.NewStaticCredentialsProvider(settings.AccessKey, settings.SecretKey, "")
	if anonymous {
		creds = aws.AnonymousCredentials{}
	}
	return s3.NewFromConfig(aws.Config{Region: settings.Region, Credentials: creds, HTTPClient: httpClient,
		Retryer: func() aws.Retryer { return aws.NopRetryer{} }}, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(settings.Endpoint)
		options.UsePathStyle = true
	})
}

func TestMediaS3ProviderReady(t *testing.T) {
	settings := readS3Fixture(t)
	if settings == nil {
		t.Fatal("S3 readiness gate requires the fixture settings")
	}
	client := settings.client(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Second)
	defer cancel()

	// SeaweedFS can serve an existing object's metadata before accepting new writes.
	bucket, key := aws.String(settings.Bucket), aws.String("fixture-readiness-"+rand.Text())
	data := []byte("synthetic S3 write readiness")
	digest := sha256.Sum256(data)
	awaitS3Fixture(t, ctx, "conditional PUT", func() error {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: bucket, Key: key, Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data))),
			IfNoneMatch: aws.String("*"), ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
			ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(digest[:])),
		})
		// A timed-out write can publish this fixture's otherwise unused key.
		if s3ProbeStatus(err) == http.StatusPreconditionFailed {
			return nil
		}
		return err
	})
	awaitS3Fixture(t, ctx, "verified GET", func() error {
		object, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key})
		if err != nil {
			return err
		}
		defer object.Body.Close()
		actual, err := io.ReadAll(io.LimitReader(object.Body, int64(len(data)+1)))
		if err != nil {
			return err
		}
		if !bytes.Equal(actual, data) {
			t.Fatal("S3 fixture readiness returned unexpected object bytes")
		}
		return nil
	})
	awaitS3Fixture(t, ctx, "probe DELETE", func() error {
		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: key})
		return err
	})
}

func awaitS3Fixture(t *testing.T, ctx context.Context, operation string, probe func() error) {
	t.Helper()
	tick := time.NewTicker(150 * time.Millisecond)
	defer tick.Stop()
	for {
		err := probe()
		if err == nil {
			return
		}
		status := s3ProbeStatus(err)
		var network net.Error
		var certificate *tls.CertificateVerificationError
		transient := status >= 500 || status == http.StatusTooManyRequests ||
			status == http.StatusConflict || status == http.StatusNotFound ||
			(errors.As(err, &network) && !errors.As(err, &certificate)) ||
			errors.Is(err, io.ErrUnexpectedEOF)
		if !transient {
			t.Fatalf("S3 fixture %s rejected: HTTP status=%d error=%T", operation, status, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("S3 fixture %s readiness deadline: HTTP status=%d error=%T", operation, status, err)
		case <-tick.C:
		}
	}
}

func s3ProbeStatus(err error) int {
	var response interface{ HTTPStatusCode() int }
	if errors.As(err, &response) {
		return response.HTTPStatusCode()
	}
	return 0
}

func TestMediaS3HTTPSecurity(t *testing.T) {
	if readS3Fixture(t) == nil {
		t.Fatal("S3 media security requires the fixture settings")
	}
	t.Run("strict-gateway-transport", testStrictTransport)
	t.Run("private-and-immutable", testPrivateObjects)
	t.Run("revoke-or-expire-during-upload", testTransferRevalidation)
	t.Run("verified-provider-and-endpoint", testS3Endpoint)
	t.Run("concurrent-replay-and-corrupted-object", testS3ImmutableObject)
	t.Run("staging-capacity", testS3Capacity)
}

func testS3Endpoint(t *testing.T) {
	f := openFixture(t)
	enabled := filepath.Join(t.TempDir(), "objects")
	for _, scenario := range []struct {
		name string
		env  map[string]string
	}{
		{"wrong-ca", map[string]string{"NEWIM_MEDIA_S3_CA_FILE": f.cert}},
		{"wrong-hostname", map[string]string{"NEWIM_MEDIA_S3_ENDPOINT": f.objects.MismatchEndpoint}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			log, err := f.gatewayCommand(ctx, enabled, scenario.env).CombinedOutput()
			if err == nil || ctx.Err() != nil || strings.TrimSpace(string(log)) != "newim-gateway media object storage unavailable" {
				t.Fatalf("gateway did not reject the invalid S3 TLS settings with its fixed startup error: %T", err)
			}
		})
	}

	ambient := []byte("[default]\nregion = " + f.objects.Region + "\nendpoint_url = " + f.objects.MismatchEndpoint + "\n")
	ambientFile := filepath.Join(t.TempDir(), "aws-config")
	must(t, os.WriteFile(ambientFile, ambient, 0o600))
	g := f.gateway(enabled, map[string]string{
		"AWS_CONFIG_FILE": ambientFile, "AWS_ENDPOINT_URL": f.objects.MismatchEndpoint,
		"AWS_ENDPOINT_URL_S3": f.objects.MismatchEndpoint,
	})
	owner := f.newAccount("media_s3_endpoint_owner")
	peer := f.newAccount("media_s3_endpoint_peer")
	id := f.direct(g, owner, "media_s3_endpoint_peer")
	data := []byte("synthetic explicit S3 endpoint")
	intent := intentForBytes(id, data)
	grant := f.grant(g, owner, intent)
	metadata := f.completed(g, owner, grant, intent, bytes.NewReader(data), int64(len(data)))
	f.requireDownload(g, peer, metadata, 0)
	client := f.objects.client(t, true)
	_, err := client.GetObject(f.ctx, &s3.GetObjectInput{Bucket: aws.String(f.objects.Bucket), Key: aws.String(metadata.MediaKey)})
	var denied interface{ HTTPStatusCode() int }
	if err == nil || !errors.As(err, &denied) || denied.HTTPStatusCode() != http.StatusForbidden {
		t.Fatalf("anonymous provider download was not denied: %T", err)
	}
	g.requireRedacted(t, f.objects.AccessKey, f.objects.SecretKey, f.objects.Endpoint, f.objects.CAFile, grant.UploadToken)
}

func testS3ImmutableObject(t *testing.T) {
	f := openFixture(t)
	root := filepath.Join(t.TempDir(), "objects")
	left, right := f.gateway(root, nil), f.gateway(root, nil)
	owner := f.newAccount("media_s3_concurrent_owner")
	peer := f.newAccount("media_s3_concurrent_peer")
	id := f.direct(left, owner, "media_s3_concurrent_peer")
	data := bytes.Repeat([]byte("synthetic concurrent S3 media"), 1024)
	intent := intentForBytes(id, data)
	grant := f.grant(left, owner, intent)
	resume1, first := f.pauseCompletion(left, owner, grant, data)
	defer resume1()
	resume2, second := f.pauseCompletion(right, owner, grant, data)
	defer resume2()
	resume1()
	resume2()
	a, b := <-first, <-second
	a.require(t, http.StatusOK)
	b.require(t, http.StatusOK)
	if !bytes.Equal(a.body, b.body) {
		t.Fatal("two gateways returned different metadata for the same uploaded bytes")
	}
	metadata, err := protocol.DecodeMediaPayload(a.body)
	must(t, err)
	f.requireDownload(right, peer, metadata, 0)

	// Change only the owned fixture object independently of the gateway writer.
	client := f.objects.client(t, false)
	key, bucket := aws.String(metadata.MediaKey), aws.String(f.objects.Bucket)
	damaged := bytes.Repeat([]byte{1}, len(data))
	for _, body := range [][]byte{damaged, damaged[:len(damaged)-1]} {
		_, err = client.PutObject(f.ctx, &s3.PutObjectInput{Bucket: bucket, Key: key,
			Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body)))})
		if err != nil {
			t.Fatalf("could not stage the deliberately mismatched fixture object: %T", err)
		}
		f.post(right, downloadPath, peer, map[string]string{"mediaKey": metadata.MediaKey}).
			requireError(t, http.StatusServiceUnavailable, "MEDIA_STORAGE_UNAVAILABLE")
		f.execute(f.uploadRequest(left, owner, grant, bytes.NewReader(data), int64(len(data)))).
			requireError(t, http.StatusConflict, "MEDIA_CONFLICT")
		stored, err := client.GetObject(f.ctx, &s3.GetObjectInput{Bucket: bucket, Key: key})
		if err != nil {
			t.Fatalf("fixture object disappeared during conditional retry: %T", err)
		}
		raw, err := io.ReadAll(io.LimitReader(stored.Body, protocol.MaxMediaSize+1))
		must(t, err)
		must(t, stored.Body.Close())
		if !bytes.Equal(raw, body) {
			t.Fatal("conditional retry overwrote an existing object")
		}
		f.requireAssetState(metadata.MediaKey, "ready")
	}
}

func testS3Capacity(t *testing.T) {
	f := openFixture(t)
	g := f.gateway(filepath.Join(t.TempDir(), "objects"), map[string]string{"NEWIM_MEDIA_S3_MAX_TRANSFERS": "1"})
	owner := f.newAccount("media_s3_capacity_owner")
	peer := f.newAccount("media_s3_capacity_peer")
	id := f.direct(g, owner, "media_s3_capacity_peer")
	data := bytes.Repeat([]byte("synthetic capacity fixture"), 4096)
	intent := intentForBytes(id, data)
	first, second := f.grant(g, owner, intent), f.grant(g, owner, intent)
	resume, result := f.pauseCompletion(g, owner, first, data)
	defer resume()
	f.execute(f.uploadRequest(g, owner, second, bytes.NewReader(data), int64(len(data)))).
		requireError(t, http.StatusServiceUnavailable, "MEDIA_STORAGE_UNAVAILABLE")
	f.requireAssetState(second.MediaKey, "pending")
	resume()
	(<-result).require(t, http.StatusOK)
	published := f.completed(g, owner, second, intent, bytes.NewReader(data), int64(len(data)))
	f.requireDownload(g, peer, published, 0)

	truncated := f.grant(g, owner, intent)
	req := f.uploadRequest(g, owner, truncated, bytes.NewReader(data[:len(data)/2]), int64(len(data)))
	if r := <-f.waitForBodyRead(req); r.err == nil && r.status == http.StatusOK {
		t.Fatal("disconnected upload committed despite its incomplete request body")
	}
	f.requireAssetState(truncated.MediaKey, "pending")
	client := f.objects.client(t, false)
	_, err := client.HeadObject(f.ctx, &s3.HeadObjectInput{Bucket: aws.String(f.objects.Bucket), Key: aws.String(truncated.MediaKey)})
	var missing interface{ HTTPStatusCode() int }
	if !errors.As(err, &missing) || missing.HTTPStatusCode() != http.StatusNotFound {
		t.Fatalf("incomplete request left a provider object or returned an unexpected status: %T", err)
	}
}
