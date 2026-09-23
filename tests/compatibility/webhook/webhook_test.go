package webhook_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	Expected  string `json:"expected"`
}

type fixtureSet struct {
	Positive   []fixtureCase `json:"positive"`
	Negative   []fixtureCase `json:"negative"`
	Decode     []fixtureCase `json:"decode"`
	RetiredKey fixtureCase   `json:"retired_key"`
}

func loadFixture(t *testing.T) fixtureSet {
	t.Helper()
	raw, err := os.ReadFile("fixtures/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures fixtureSet
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	if len(fixtures.Positive) < 4 || len(fixtures.Negative) < 4 || len(fixtures.Decode) < 4 || fixtures.RetiredKey.Name == "" {
		t.Fatalf("fixture coverage is incomplete: %+v", fixtures)
	}
	return fixtures
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
		protocol.WebhookHeaderEventID:          {h.EventID},
		protocol.WebhookHeaderDeliveryID:       {h.DeliveryID},
		protocol.WebhookHeaderTimestamp:        {h.Timestamp},
		protocol.WebhookHeaderNonce:            {h.Nonce},
		protocol.WebhookHeaderKeyID:            {h.KeyID},
		protocol.WebhookHeaderSignatureVersion: {h.SignatureVersion},
		protocol.WebhookHeaderSignature:        {h.Signature},
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

type resolver map[string]string

func (r resolver) ResolveWebhookSecret(_ context.Context, keyID string) ([]byte, error) {
	secret, ok := r[keyID]
	if !ok {
		return nil, protocol.WebhookUnknownKey
	}
	return []byte(secret), nil
}

type resolverFunc func(context.Context, string) ([]byte, error)

func (f resolverFunc) ResolveWebhookSecret(ctx context.Context, keyID string) ([]byte, error) {
	return f(ctx, keyID)
}

func TestWebhookEnvelope(t *testing.T) {
	fixtures := loadFixture(t)
	for _, c := range fixtures.Decode {
		t.Run(c.Name, func(t *testing.T) {
			envelope, err := protocol.DecodeWebhookEnvelope([]byte(c.Wire))
			if c.Expected == "ok" {
				if err != nil {
					t.Fatal(err)
				}
				encoded, err := protocol.EncodeWebhookEnvelope(envelope)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = protocol.DecodeWebhookEnvelope(encoded); err != nil {
					t.Fatalf("canonical encoding did not round trip: %v", err)
				}
				return
			}
			assertCode(t, err, protocol.WebhookCode(c.Expected))
		})
	}
	for _, c := range fixtures.Positive {
		t.Run(c.Name, func(t *testing.T) {
			envelope, err := protocol.DecodeWebhookEnvelope([]byte(c.Wire))
			if err != nil {
				t.Fatal(err)
			}
			if envelope.EventID == "" || envelope.Payload == nil {
				t.Fatalf("incomplete envelope: %+v", envelope)
			}
		})
	}
}

func TestWebhookSigningAndVerification(t *testing.T) {
	fixtures := loadFixture(t)
	for _, c := range fixtures.Positive {
		t.Run(c.Name, func(t *testing.T) {
			body := []byte(c.Wire)
			parsed, err := protocol.ParseWebhookHeaders(headerMap(headers(c)))
			if err != nil {
				t.Fatal(err)
			}
			signature, err := protocol.SignWebhook([]byte(c.Secret), parsed, body)
			if err != nil {
				t.Fatal(err)
			}
			if signature != c.Signature {
				t.Fatalf("signature got %q want %q", signature, c.Signature)
			}
			guard, err := protocol.NewMemoryWebhookReplayGuard(16)
			if err != nil {
				t.Fatal(err)
			}
			if err = protocol.VerifyWebhookSignature(context.Background(), []byte(c.Secret), parsed, body, time.UnixMilli(c.Now), guard); err != nil {
				t.Fatal(err)
			}
			resolver := resolver{c.KeyID: c.Secret}
			guard, _ = protocol.NewMemoryWebhookReplayGuard(16)
			if err = protocol.VerifyWebhookRequest(context.Background(), parsed, body, time.UnixMilli(c.Now), guard, resolver); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWebhookSecurity(t *testing.T) {
	fixtures := loadFixture(t)
	for _, c := range fixtures.Negative {
		t.Run(c.Name, func(t *testing.T) {
			guard, err := protocol.NewMemoryWebhookReplayGuard(16)
			if err != nil {
				t.Fatal(err)
			}
			err = protocol.VerifyWebhookSignature(context.Background(), []byte(c.Secret), headers(c), []byte(c.Wire), time.UnixMilli(c.Now), guard)
			assertCode(t, err, protocol.WebhookCode(c.Expected))
		})
	}
	t.Run("retired_key", func(t *testing.T) {
		c := fixtures.RetiredKey
		guard, _ := protocol.NewMemoryWebhookReplayGuard(16)
		err := protocol.VerifyWebhookRequest(context.Background(), headers(c), []byte(c.Wire), time.UnixMilli(c.Now), guard, resolver{"key_current": fixtures.Positive[0].Secret})
		assertCode(t, err, protocol.WebhookCode(c.Expected))
	})
	t.Run("replay", func(t *testing.T) {
		c := fixtures.Positive[0]
		guard, _ := protocol.NewMemoryWebhookReplayGuard(16)
		if err := protocol.VerifyWebhookSignature(context.Background(), []byte(c.Secret), headers(c), []byte(c.Wire), time.UnixMilli(c.Now), guard); err != nil {
			t.Fatal(err)
		}
		err := protocol.VerifyWebhookSignature(context.Background(), []byte(c.Secret), headers(c), []byte(c.Wire), time.UnixMilli(c.Now), guard)
		assertCode(t, err, protocol.WebhookReplayDetected)
	})
	t.Run("concurrent_replay", func(t *testing.T) {
		c := fixtures.Positive[0]
		guard, _ := protocol.NewMemoryWebhookReplayGuard(16)
		start := make(chan struct{})
		results := make(chan protocol.WebhookCode, 2)
		var wg sync.WaitGroup
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				err := protocol.VerifyWebhookSignature(context.Background(), []byte(c.Secret), headers(c), []byte(c.Wire), time.UnixMilli(c.Now), guard)
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
	t.Run("capacity_fail_closed", func(t *testing.T) {
		c := fixtures.Positive[0]
		guard, _ := protocol.NewMemoryWebhookReplayGuard(1)
		if err := protocol.VerifyWebhookSignature(context.Background(), []byte(c.Secret), headers(c), []byte(c.Wire), time.UnixMilli(c.Now), guard); err != nil {
			t.Fatal(err)
		}
		second := headers(c)
		second.Nonce = "EBESExQVFhcYGRobHB0eHw"
		signature, err := protocol.SignWebhook([]byte(c.Secret), second, []byte(c.Wire))
		if err != nil {
			t.Fatal(err)
		}
		second.Signature = signature
		err = protocol.VerifyWebhookSignature(context.Background(), []byte(c.Secret), second, []byte(c.Wire), time.UnixMilli(c.Now), guard)
		assertCode(t, err, protocol.WebhookCapacityExceeded)
	})
	t.Run("parser_rejects_noncanonical_signature", func(t *testing.T) {
		h := headers(fixtures.Positive[0])
		h.Signature = h.Signature[:len(h.Signature)-1] + "V"
		if _, err := protocol.ParseWebhookHeaders(headerMap(h)); protocol.WebhookErrorCode(err) != protocol.WebhookInvalidSignature {
			t.Fatalf("noncanonical signature accepted by parser: %v", err)
		}
	})
	t.Run("malformed_headers_skip_resolver", func(t *testing.T) {
		c := fixtures.Positive[0]
		h := headers(c)
		h.KeyID = "bad key"
		calls := 0
		r := resolverFunc(func(context.Context, string) ([]byte, error) {
			calls++
			return []byte(c.Secret), nil
		})
		guard, _ := protocol.NewMemoryWebhookReplayGuard(4)
		err := protocol.VerifyWebhookRequest(context.Background(), h, []byte(c.Wire), time.UnixMilli(c.Now), guard, r)
		assertCode(t, err, protocol.WebhookInvalidHeaders)
		if calls != 0 {
			t.Fatalf("resolver called %d times for malformed headers", calls)
		}
	})
	t.Run("malformed_body_skips_resolver", func(t *testing.T) {
		c := fixtures.Positive[0]
		calls := 0
		r := resolverFunc(func(context.Context, string) ([]byte, error) {
			calls++
			return []byte(c.Secret), nil
		})
		guard, _ := protocol.NewMemoryWebhookReplayGuard(4)
		err := protocol.VerifyWebhookRequest(context.Background(), headers(c), []byte("null"), time.UnixMilli(c.Now), guard, r)
		if protocol.WebhookErrorCode(err) != protocol.WebhookInvalidEnvelope {
			t.Fatalf("malformed body error = %v", err)
		}
		if calls != 0 {
			t.Fatalf("resolver called %d times for malformed body", calls)
		}
	})
	t.Run("transient_resolver_failure", func(t *testing.T) {
		c := fixtures.Positive[0]
		guard, _ := protocol.NewMemoryWebhookReplayGuard(4)
		r := resolverFunc(func(context.Context, string) ([]byte, error) {
			return nil, errors.New("temporary secret store failure")
		})
		err := protocol.VerifyWebhookRequest(context.Background(), headers(c), []byte(c.Wire), time.UnixMilli(c.Now), guard, r)
		assertCode(t, err, protocol.WebhookKeyUnavailable)
	})
	t.Run("error_code_preservation", func(t *testing.T) {
		if got := protocol.WebhookErrorCode(protocol.WebhookCapacityExceeded); got != protocol.WebhookCapacityExceeded {
			t.Fatalf("direct code got %q", got)
		}
		wrapped := fmt.Errorf("wrapped: %w", protocol.WebhookCapacityExceeded)
		if got := protocol.WebhookErrorCode(wrapped); got != protocol.WebhookCapacityExceeded {
			t.Fatalf("wrapped code got %q", got)
		}
	})
}

func TestWebhookStrictInputs(t *testing.T) {
	fixtures := loadFixture(t)
	base := fixtures.Positive[0]
	for _, name := range []string{
		protocol.WebhookHeaderEventID,
		protocol.WebhookHeaderDeliveryID,
		protocol.WebhookHeaderTimestamp,
		protocol.WebhookHeaderNonce,
		protocol.WebhookHeaderKeyID,
		protocol.WebhookHeaderSignatureVersion,
		protocol.WebhookHeaderSignature,
	} {
		t.Run("missing_"+name, func(t *testing.T) {
			values := headerMap(headers(base))
			delete(values, name)
			if _, err := protocol.ParseWebhookHeaders(values); protocol.WebhookErrorCode(err) != protocol.WebhookInvalidHeaders {
				t.Fatalf("missing header accepted: %v", err)
			}
		})
	}
	t.Run("duplicate_event_id", func(t *testing.T) {
		values := headerMap(headers(base))
		values[protocol.WebhookHeaderEventID] = []string{base.EventID, base.EventID}
		if _, err := protocol.ParseWebhookHeaders(values); protocol.WebhookErrorCode(err) != protocol.WebhookInvalidHeaders {
			t.Fatalf("duplicate event id accepted: %v", err)
		}
	})
	t.Run("malformed_json", func(t *testing.T) {
		cases := map[string][]byte{
			"duplicate_key":      []byte(strings.Replace(base.Wire, `"eventId":"`+base.EventID+`",`, `"eventId":"`+base.EventID+`","eventId":"dup",`, 1)),
			"invalid_utf8":       {0xff},
			"unpaired_surrogate": []byte(`{"eventId":"\ud800"}`),
			"trailing_token":     []byte(base.Wire + "null"),
			"payload_not_object": []byte(`{"eventId":"evt_01JABCDEF0123456789","eventType":"message.persisted","occurredAt":"1790189000000","schemaVersion":1,"payload":[]}`),
		}
		for name, wire := range cases {
			t.Run(name, func(t *testing.T) {
				_, err := protocol.DecodeWebhookEnvelope(wire)
				if protocol.WebhookErrorCode(err) == "" {
					t.Fatalf("malformed envelope accepted: %s", wire)
				}
			})
		}
	})
	t.Run("oversized_encoder_input", func(t *testing.T) {
		envelope := protocol.WebhookEnvelope{
			EventID:       base.EventID,
			EventType:     "message.persisted",
			OccurredAt:    "1790189000000",
			SchemaVersion: 1,
			Payload:       bytes.Repeat([]byte{'x'}, protocol.WebhookMaxEnvelopeBytes+1),
		}
		_, err := protocol.EncodeWebhookEnvelope(envelope)
		assertCode(t, err, protocol.WebhookEnvelopeTooLarge)
	})
}

func TestWebhookClockBoundaries(t *testing.T) {
	fixtures := loadFixture(t)
	base := fixtures.Positive[0]
	now := time.UnixMilli(base.Now)
	body := []byte(base.Wire)
	for _, delta := range []time.Duration{-protocol.WebhookTimestampWindow, protocol.WebhookTimestampWindow} {
		t.Run(delta.String(), func(t *testing.T) {
			h := liveHeaders(t, base, body, now.Add(delta))
			guard, _ := protocol.NewMemoryWebhookReplayGuard(4)
			if err := protocol.VerifyWebhookSignature(context.Background(), []byte(base.Secret), h, body, now, guard); err != nil {
				t.Fatalf("inclusive boundary rejected: %v", err)
			}
		})
	}
	for _, delta := range []time.Duration{-protocol.WebhookTimestampWindow - time.Millisecond, protocol.WebhookTimestampWindow + time.Millisecond} {
		t.Run("outside_"+delta.String(), func(t *testing.T) {
			h := liveHeaders(t, base, body, now.Add(delta))
			guard, _ := protocol.NewMemoryWebhookReplayGuard(4)
			err := protocol.VerifyWebhookSignature(context.Background(), []byte(base.Secret), h, body, now, guard)
			assertCode(t, err, protocol.WebhookTimestampMismatch)
		})
	}
}

func TestWebhookKeyRotationUse(t *testing.T) {
	fixtures := loadFixture(t)
	secretByKey := map[string]string{}
	for _, c := range fixtures.Positive {
		secretByKey[c.KeyID] = c.Secret
	}
	for _, c := range fixtures.Positive {
		if c.KeyID != "key_old" && c.KeyID != "key_new" {
			continue
		}
		t.Run(c.KeyID, func(t *testing.T) {
			guard, _ := protocol.NewMemoryWebhookReplayGuard(4)
			if err := protocol.VerifyWebhookRequest(context.Background(), headers(c), []byte(c.Wire), time.UnixMilli(c.Now), guard, resolver(secretByKey)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
