package webhook_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
)

type fixtureCase struct {
	Name      string `json:"name"`
	Wire      string `json:"wire"`
	Secret    string `json:"secret"`
	Now       int64  `json:"now"`
	EventID   string `json:"eventId"`
	Delivery  string `json:"deliveryId"`
	Timestamp string `json:"timestamp"`
	Nonce     string `json:"nonce"`
	KeyID     string `json:"keyId"`
	Signature string `json:"signature"`
}

func loadFixture(t *testing.T) fixtureCase {
	t.Helper()
	raw, err := os.ReadFile("fixtures/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []fixtureCase
	if err = json.Unmarshal(raw, &cases); err != nil || len(cases) != 1 {
		t.Fatalf("fixture shape: %v, cases=%d", err, len(cases))
	}
	return cases[0]
}

func headers(f fixtureCase) protocol.WebhookHeaders {
	return protocol.WebhookHeaders{
		EventID:          f.EventID,
		DeliveryID:       f.Delivery,
		Timestamp:        f.Timestamp,
		Nonce:            f.Nonce,
		KeyID:            f.KeyID,
		SignatureVersion: protocol.WebhookSignatureVersion,
		Signature:        f.Signature,
	}
}

func headerMap(h protocol.WebhookHeaders) map[string][]string {
	return map[string][]string{
		protocol.WebhookHeaderNames.EventID:          {h.EventID},
		protocol.WebhookHeaderNames.DeliveryID:       {h.DeliveryID},
		protocol.WebhookHeaderNames.Timestamp:        {h.Timestamp},
		protocol.WebhookHeaderNames.Nonce:            {h.Nonce},
		protocol.WebhookHeaderNames.KeyID:            {h.KeyID},
		protocol.WebhookHeaderNames.SignatureVersion: {h.SignatureVersion},
		protocol.WebhookHeaderNames.Signature:        {h.Signature},
	}
}

func assertCode(t *testing.T, err error, want protocol.WebhookCode) {
	t.Helper()
	if got := protocol.WebhookErrorCode(err); got != want {
		t.Fatalf("webhook code got %q want %q (%v)", got, want, err)
	}
}

func liveHeaders(t *testing.T, f fixtureCase, body []byte, now time.Time) protocol.WebhookHeaders {
	t.Helper()
	h := headers(f)
	h.Timestamp = strconv.FormatInt(now.UnixMilli(), 10)
	h.Signature = ""
	signature, err := protocol.SignWebhook([]byte(f.Secret), h, body)
	if err != nil {
		t.Fatal(err)
	}
	h.Signature = signature
	return h
}

func TestWebhookEnvelope(t *testing.T) {
	f := loadFixture(t)
	body := []byte(f.Wire)
	envelope, err := protocol.DecodeWebhookEnvelope(body)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.EventID != f.EventID || envelope.EventType != "message.persisted" || envelope.OccurredAt != "1790189000000" || envelope.SchemaVersion != 1 {
		t.Fatalf("decoded envelope = %+v", envelope)
	}
	encoded, err := protocol.EncodeWebhookEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, body) {
		t.Fatalf("canonical encoding changed: %s", encoded)
	}

	var raw map[string]json.RawMessage
	if err = json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	raw["futureField"] = json.RawMessage(`true`)
	withFuture, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = protocol.DecodeWebhookEnvelope(withFuture); err != nil {
		t.Fatalf("unknown additive field rejected: %v", err)
	}
}

func TestWebhookSigningAndVerification(t *testing.T) {
	f := loadFixture(t)
	body := []byte(f.Wire)
	parsed, err := protocol.ParseWebhookHeaders(headerMap(headers(f)))
	if err != nil {
		t.Fatal(err)
	}
	signing, err := protocol.CanonicalWebhookSigningBytes(parsed, body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(signing), "\n") || strings.Count(string(signing), "\n") != 7 {
		t.Fatalf("canonical signing bytes are not newline terminated: %q", signing)
	}
	signature, err := protocol.SignWebhook([]byte(f.Secret), parsed, body)
	if err != nil {
		t.Fatal(err)
	}
	if signature != f.Signature {
		t.Fatalf("signature got %q want %q", signature, f.Signature)
	}
	now := time.Now()
	verification := liveHeaders(t, f, body, now)
	guard, err := protocol.NewMemoryWebhookReplayGuard(16)
	if err != nil {
		t.Fatal(err)
	}
	if err = protocol.VerifyWebhookSignature(context.Background(), []byte(f.Secret), verification, body, now, guard); err != nil {
		t.Fatal(err)
	}
}

func TestWebhookSecurity(t *testing.T) {
	f := loadFixture(t)
	body := []byte(f.Wire)
	now := time.Now()
	base := liveHeaders(t, f, body, now)

	t.Run("tampered_body", func(t *testing.T) {
		guard, _ := protocol.NewMemoryWebhookReplayGuard(16)
		err := protocol.VerifyWebhookSignature(context.Background(), []byte(f.Secret), base, []byte(strings.Replace(f.Wire, "alice", "alice2", 1)), now, guard)
		assertCode(t, err, protocol.WebhookInvalidSignature)
	})
	t.Run("wrong_secret", func(t *testing.T) {
		guard, _ := protocol.NewMemoryWebhookReplayGuard(16)
		err := protocol.VerifyWebhookSignature(context.Background(), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), base, body, now, guard)
		assertCode(t, err, protocol.WebhookInvalidSignature)
	})
	t.Run("timestamp_window", func(t *testing.T) {
		guard, _ := protocol.NewMemoryWebhookReplayGuard(16)
		err := protocol.VerifyWebhookSignature(context.Background(), []byte(f.Secret), base, body, now.Add(2*protocol.WebhookTimestampWindow), guard)
		assertCode(t, err, protocol.WebhookTimestampMismatch)
	})
	t.Run("replay", func(t *testing.T) {
		guard, _ := protocol.NewMemoryWebhookReplayGuard(16)
		if err := protocol.VerifyWebhookSignature(context.Background(), []byte(f.Secret), base, body, now, guard); err != nil {
			t.Fatal(err)
		}
		err := protocol.VerifyWebhookSignature(context.Background(), []byte(f.Secret), base, body, now, guard)
		assertCode(t, err, protocol.WebhookReplayDetected)
	})
	t.Run("concurrent_replay", func(t *testing.T) {
		guard, _ := protocol.NewMemoryWebhookReplayGuard(16)
		start := make(chan struct{})
		results := make(chan protocol.WebhookCode, 2)
		var wg sync.WaitGroup
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				err := protocol.VerifyWebhookSignature(context.Background(), []byte(f.Secret), base, body, now, guard)
				results <- protocol.WebhookErrorCode(err)
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		var accepted, replay int
		for code := range results {
			switch code {
			case "":
				accepted++
			case protocol.WebhookReplayDetected:
				replay++
			default:
				t.Fatalf("unexpected concurrent result %q", code)
			}
		}
		if accepted != 1 || replay != 1 {
			t.Fatalf("accepted=%d replay=%d", accepted, replay)
		}
	})
	t.Run("invalid_headers", func(t *testing.T) {
		bad := headerMap(base)
		bad[protocol.WebhookHeaderNames.DeliveryID] = []string{"bad\nvalue"}
		if _, err := protocol.ParseWebhookHeaders(bad); protocol.WebhookErrorCode(err) != protocol.WebhookInvalidHeaders {
			t.Fatalf("CRLF header accepted: %v", err)
		}
		bad = headerMap(base)
		bad[protocol.WebhookHeaderNames.Nonce] = []string{base.Nonce + "="}
		if _, err := protocol.ParseWebhookHeaders(bad); protocol.WebhookErrorCode(err) != protocol.WebhookInvalidHeaders {
			t.Fatalf("padded nonce accepted: %v", err)
		}
		bad = headerMap(base)
		bad[protocol.WebhookHeaderNames.KeyID] = []string{base.KeyID, base.KeyID}
		if _, err := protocol.ParseWebhookHeaders(bad); protocol.WebhookErrorCode(err) != protocol.WebhookInvalidHeaders {
			t.Fatalf("duplicate key id accepted: %v", err)
		}
	})
	t.Run("unknown_schema", func(t *testing.T) {
		wire := []byte(strings.Replace(f.Wire, `"schemaVersion":1`, `"schemaVersion":2`, 1))
		_, err := protocol.DecodeWebhookEnvelope(wire)
		assertCode(t, err, protocol.WebhookUnsupportedSchema)
	})
	t.Run("capacity_fail_closed", func(t *testing.T) {
		guard, _ := protocol.NewMemoryWebhookReplayGuard(1)
		if err := protocol.VerifyWebhookSignature(context.Background(), []byte(f.Secret), base, body, now, guard); err != nil {
			t.Fatal(err)
		}
		second := base
		second.Nonce = "EBESExQVFhcYGRobHB0eHw"
		signature, err := protocol.SignWebhook([]byte(f.Secret), second, body)
		if err != nil {
			t.Fatal(err)
		}
		second.Signature = signature
		err = protocol.VerifyWebhookSignature(context.Background(), []byte(f.Secret), second, body, now, guard)
		assertCode(t, err, protocol.WebhookCapacityExceeded)
	})
}
