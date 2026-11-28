package push

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPushKitVendor(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	fixture := func(uri string) string {
		t.Helper()
		encoded, err := json.Marshal(map[string]string{
			"project_id": "project123", "key_id": "key123", "sub_account": "test@example",
			"private_key": string(pemFixture(t, key)), "token_uri": uri,
		})
		if err != nil {
			t.Fatal(err)
		}
		return credentialFixture(t, encoded)
	}
	if _, err := NewPushKit(fixture("https://example.invalid/oauth2")); err != ErrConfig {
		t.Fatal("Push Kit accepted a foreign credential audience")
	}
	p, err := NewPushKit(fixture(pushKitAudience))
	if err != nil {
		t.Fatal(err)
	}
	if p.sendURL != "https://push-api.cloud.huawei.com/v3/project123/messages:send" {
		t.Fatal("Push Kit did not bind the request path to the configured project")
	}

	now := time.Unix(1760000000, 0)
	p.now = func() time.Time { return now }
	const token = "synthetic-harmony-installation"
	const expiry = "80200005"
	details := func(reason, affected string) string {
		data, err := json.Marshal(map[string]any{
			"illegalTokens": map[string][]string{reason: {affected}}, "success": 0, "failure": 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	cases := []struct {
		name   string
		http   int
		retry  string
		body   string
		raw    string
		msg    string
		result Outcome
	}{
		{name: "accepted", http: 200, body: "80000000", result: Outcome{Result: Accepted}},
		{name: "expired JWT", http: 401, body: expiry, result: Outcome{Result: TemporaryFailure}},
		{name: "refresh after expiry", http: 200, body: "80000000", result: Outcome{Result: Accepted}},
		{name: "matched malformed token", http: 400, body: "80300007",
			msg: details("tokenFormatError", token), result: Outcome{Result: InvalidToken}},
		{name: "unmatched malformed token", http: 400, body: "80300007",
			msg: details("tokenFormatError", "another-token"), result: Outcome{Result: PermanentFailure}},
		{name: "authorization error keeps registration", http: 401, body: "80300007",
			msg: details("tokenFormatError", token), result: Outcome{Result: PermanentFailure}},
		{name: "missing permissions", http: 400, body: "80100000",
			msg: details("noPushTypeRight", token), result: Outcome{Result: PermanentFailure}},
		{name: "registration survives app configuration errors", http: 400, body: "80300007",
			msg: details("appinfoError", token), result: Outcome{Result: PermanentFailure}},
		{name: "rate limited", http: 429, retry: "23", result: Outcome{Result: TemporaryFailure, RetryAfter: 23 * time.Second}},
		{name: "vendor internal error", http: 200, body: "81000001", result: Outcome{Result: TemporaryFailure}},
		{name: "known success body on unexpected status", http: 400, body: "80000000", result: Outcome{Result: PermanentFailure}},
		{name: "success with unreadable body", http: 200, result: Outcome{Result: TemporaryFailure}},
		{name: "success without a result code", http: 200, raw: "{}", result: Outcome{Result: TemporaryFailure}},
	}
	wantPayload := map[string]any{
		"payload": map[string]any{"notification": map[string]any{
			"category": "IM", "title": "新消息", "body": "打开应用查看", "foregroundShow": false,
			"clickAction": map[string]any{"actionType": float64(0), "data": map[string]any{"newim": "sync"}},
		}},
		"target":      map[string]any{"token": []any{token}},
		"pushOptions": map[string]any{"ttl": notificationTTL.Seconds()},
	}
	var calls atomic.Int32
	var first, refreshed atomic.Value
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		index := int(calls.Add(1)) - 1
		if index >= len(cases) {
			t.Error("unexpected additional Push Kit request")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		test := cases[index]
		if r.TLS == nil || r.Method != http.MethodPost || r.URL.Path != "/v3/project123/messages:send" ||
			r.Header.Get("push-type") != "0" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("Push Kit request violated the verified TLS API contract")
		}
		raw := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		header, claims, sum, signature := decodeJWT(t, raw)
		if header["alg"] != "PS256" || header["kid"] != "key123" || header["typ"] != "JWT" ||
			claims["iss"] != "test@example" || claims["aud"] != pushKitAudience ||
			claims["iat"] != float64(now.Unix()) || claims["exp"] != float64(now.Add(time.Hour).Unix()) ||
			rsa.VerifyPSS(&key.PublicKey, crypto.SHA256, sum[:], signature,
				&rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash}) != nil {
			t.Error("Push Kit JWT claims or PS256 signature could not be verified")
		}
		switch index {
		case 0:
			first.Store(raw)
		case 1:
			if first.Load() != raw {
				t.Error("Push Kit did not reuse a fresh JWT")
			}
		case 2:
			if first.Load() == raw {
				t.Error("Push Kit did not replace an expired JWT")
			}
			refreshed.Store(raw)
		default:
			if refreshed.Load() != raw {
				t.Error("Push Kit changed its JWT after a non-credential failure")
			}
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil || !reflect.DeepEqual(body, wantPayload) {
			t.Error("Push Kit sent fields beyond the generic synchronization notification")
		}
		if test.retry != "" {
			w.Header().Set("Retry-After", test.retry)
		}
		w.WriteHeader(test.http)
		if test.body != "" {
			_ = json.NewEncoder(w).Encode(map[string]string{"code": test.body, "msg": test.msg})
		} else if test.raw != "" {
			_, _ = w.Write([]byte(test.raw))
		}
	}))
	defer server.Close()
	p.client, p.sendURL = server.Client(), server.URL+"/v3/project123/messages:send"

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := p.Send(context.Background(), []byte(token)); got != test.result {
				t.Fatalf("Push Kit outcome = %+v, want %+v", got, test.result)
			}
		})
	}
	if got := p.Send(context.Background(), []byte("contains whitespace")); got.Result != PermanentFailure ||
		int(calls.Load()) != len(cases) {
		t.Fatal("Push Kit attempted to send an invalid installation token")
	}
}
