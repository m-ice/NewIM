package push

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func pemFixture(t *testing.T, key any) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

func credentialFixture(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture-key")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func decodeJWT(t *testing.T, jwt string) (map[string]any, map[string]any, [32]byte, []byte) {
	t.Helper()
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		t.Error("provider request did not use JWT format")
		return nil, nil, [32]byte{}, nil
	}
	var header, claims map[string]any
	encoding := base64.RawURLEncoding
	for index, target := range []*map[string]any{&header, &claims} {
		raw, err := encoding.DecodeString(parts[index])
		if err != nil || json.Unmarshal(raw, target) != nil {
			t.Error("provider JWT has invalid fields")
		}
	}
	signature, err := encoding.DecodeString(parts[2])
	if err != nil {
		t.Error("provider JWT has an invalid signature encoding")
	}
	return header, claims, sha256.Sum256([]byte(strings.Join(parts[:2], "."))), signature
}

func TestVendorAPNs(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewAPNs(APNsConfig{TeamID: "TEAM", KeyID: "KEY", Topic: "im.example", KeyFile: credentialFixture(t, pemFixture(t, key))})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1750000000, 0)
	p.now = func() time.Time { return now }
	var count atomic.Int32
	var bearer atomic.Value
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || r.ProtoMajor != 2 || r.Method != "POST" || r.URL.Path != "/3/device/aabb" ||
			r.Header.Get("apns-topic") != "im.example" || r.Header.Get("apns-push-type") != "alert" ||
			r.Header.Get("apns-collapse-id") != "newim-sync" {
			t.Error("APNs request did not respect device and topic boundaries")
		}
		raw := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		header, claims, sum, signature := decodeJWT(t, raw)
		if header["alg"] != "ES256" || header["kid"] != "KEY" || claims["iss"] != "TEAM" ||
			claims["iat"] != float64(now.Unix()) || len(signature) != 64 ||
			!ecdsa.Verify(&key.PublicKey, sum[:], new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])) {
			t.Error("APNs JWT signature or claims are invalid")
		}
		var payload struct {
			Aps  struct{ Alert notification } `json:"aps"`
			Hint string                       `json:"newim"`
		}
		if json.NewDecoder(r.Body).Decode(&payload) != nil || payload.Hint != "sync" || payload.Aps.Alert != genericNotification() {
			t.Error("APNs did not send the generic sync hint")
		}
		n := count.Add(1)
		if n == 1 {
			bearer.Store(raw)
		} else if bearer.Load() != raw {
			t.Error("APNs generated a new token within the reuse period")
		}
		switch n {
		case 1:
			w.WriteHeader(200)
		case 2:
			w.WriteHeader(410)
			_, _ = w.Write([]byte(`{"reason":"Unregistered","timestamp":1749999999000}`))
		case 3:
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"reason":"BadDeviceToken"}`))
		case 4:
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(429)
		default:
			w.WriteHeader(503)
		}
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	p.client, p.url = server.Client(), server.URL+"/3/device/"
	for index, expected := range []Outcome{
		{Result: Accepted},
		{Result: InvalidToken, InvalidAt: now.Add(-time.Second)},
		{Result: PermanentFailure},
		{Result: TemporaryFailure, RetryAfter: time.Minute},
		{Result: TemporaryFailure, RetryAfter: 15 * time.Minute},
	} {
		if got := p.Send(t.Context(), []byte("AABB")); got != expected {
			t.Fatalf("APNs case %d: got %+v want %+v", index, got, expected)
		}
	}
}

func TestVendorFCM(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]string{
		"type": "service_account", "project_id": "fixture",
		"client_email": "sender@fixture.iam.gserviceaccount.com", "private_key_id": "key",
		"token_uri": googleOAuth, "private_key": string(pemFixture(t, key)),
	})
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewFCM(credentialFixture(t, data))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1750000000, 0)
	p.now = func() time.Time { return now }
	var issued, sends atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || r.Method != "POST" {
			t.Error("FCM fixture did not receive TLS POST")
		}
		switch r.URL.Path {
		case "/token":
			issued.Add(1)
			if err := r.ParseForm(); err != nil || r.Form.Get("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
				t.Error("FCM did not use the service account token grant")
			}
			header, claims, sum, signature := decodeJWT(t, r.Form.Get("assertion"))
			if header["alg"] != "RS256" || claims["aud"] != googleOAuth || claims["scope"] != fcmScope ||
				claims["iss"] != "sender@fixture.iam.gserviceaccount.com" ||
				rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, sum[:], signature) != nil {
				t.Error("FCM token exchange has an invalid signature or claims")
			}
			_, _ = w.Write([]byte(`{"token_type":"Bearer","access_token":"fixture_access","expires_in":3600}`))
		case "/v1/projects/fixture/messages:send":
			if r.Header.Get("Authorization") != "Bearer fixture_access" {
				t.Error("FCM send used incorrect credentials")
			}
			var payload struct {
				Message struct {
					Token        string
					Notification notification
					Data         map[string]string
				}
			}
			if json.NewDecoder(r.Body).Decode(&payload) != nil || payload.Message.Token != "fixture:device" ||
				payload.Message.Notification != genericNotification() || len(payload.Message.Data) != 1 || payload.Message.Data["newim"] != "sync" {
				t.Error("FCM payload differed from the generic sync hint")
			}
			switch sends.Add(1) {
			case 1:
				w.WriteHeader(200)
			case 2:
				w.WriteHeader(404)
				_, _ = w.Write([]byte(`{"error":{"status":"NOT_FOUND"}}`))
			case 3:
				w.WriteHeader(404)
				_, _ = w.Write([]byte(`{"error":{"details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"UNREGISTERED"}]}}`))
			case 4:
				w.WriteHeader(401)
			default:
				w.Header().Set("Retry-After", now.Add(2*time.Minute).UTC().Format(http.TimeFormat))
				w.WriteHeader(503)
			}
		default:
			t.Error("FCM requested an unexpected URI")
		}
	}))
	defer server.Close()
	p.client, p.tokenURL = server.Client(), server.URL+"/token"
	p.sendURL = server.URL + "/v1/projects/fixture/messages:send"
	for index, expected := range []Outcome{
		{Result: Accepted}, {Result: PermanentFailure}, {Result: InvalidToken},
		{Result: TemporaryFailure}, {Result: TemporaryFailure, RetryAfter: 2 * time.Minute},
	} {
		if got := p.Send(t.Context(), []byte("fixture:device")); got != expected {
			t.Fatalf("FCM case %d: got %+v want %+v", index, got, expected)
		}
	}
	if issued.Load() != 2 {
		t.Fatal("FCM did not reuse and refresh its OAuth token")
	}
}

func TestTokenVault(t *testing.T) {
	vault, err := NewVault("old", map[string][]byte{"old": bytes.Repeat([]byte{0x21}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	binding := TokenBinding{UserID: "user", DeviceID: "device", SessionID: "session", Channel: FCM, Version: 1}
	sealed, err := vault.Seal(binding, []byte("fixture:private"))
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := NewVault("new", map[string][]byte{"old": bytes.Repeat([]byte{0x21}, 32), "new": bytes.Repeat([]byte{0x31}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := rotated.Open(binding, sealed)
	defer clear(raw)
	if err != nil || string(raw) != "fixture:private" || bytes.Contains(sealed.Ciphertext, raw) {
		t.Fatal("rotation did not preserve bounded encrypted token access")
	}
	binding.Version++
	if _, err := rotated.Open(binding, sealed); err != ErrCiphertext {
		t.Fatal("a different registration version opened the ciphertext")
	}
}
