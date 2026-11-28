package push

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/m-ice/NewIM/server/credentialfile"
)

const (
	googleOAuth = "https://oauth2.googleapis.com/token"
	fcmScope    = "https://www.googleapis.com/auth/firebase.messaging"
	fcmError    = "type.googleapis.com/google.firebase.fcm.v1.FcmError"
)

type FCMProvider struct {
	client     *http.Client
	sendURL    string
	tokenURL   string
	email      string
	keyID      string
	key        *rsa.PrivateKey
	now        func() time.Time
	mu         sync.Mutex
	bearer     string
	freshUntil time.Time
}

func NewFCM(credentialsFile string) (*FCMProvider, error) {
	data, err := credentialfile.Read(credentialsFile, 1, 32768)
	defer clear(data)
	if err != nil {
		return nil, ErrConfig
	}
	var account struct {
		Type      string `json:"type"`
		ProjectID string `json:"project_id"`
		Email     string `json:"client_email"`
		KeyID     string `json:"private_key_id"`
		Key       string `json:"private_key"`
		TokenURI  string `json:"token_uri"`
	}
	if json.Unmarshal(data, &account) != nil || account.Type != "service_account" || !validID(account.ProjectID) ||
		!validID(account.KeyID) || !validServiceEmail(account.Email) || account.TokenURI != googleOAuth {
		return nil, ErrConfig
	}
	encoded := []byte(account.Key)
	defer clear(encoded)
	der, err := privateKeyDER(encoded)
	defer clear(der)
	if err != nil {
		return nil, err
	}
	parsed, err := x509.ParsePKCS8PrivateKey(der)
	key, ok := parsed.(*rsa.PrivateKey)
	if err != nil || !ok || key.N.BitLen() < 2048 || key.Validate() != nil {
		return nil, ErrConfig
	}
	return &FCMProvider{
		client: vendorClient(), tokenURL: googleOAuth, now: time.Now,
		sendURL: "https://fcm.googleapis.com/v1/projects/" + account.ProjectID + "/messages:send",
		email:   account.Email, keyID: account.KeyID, key: key,
	}, nil
}

func validServiceEmail(email string) bool {
	local, domain, ok := strings.Cut(email, "@")
	return ok && validTopic(domain) && validID(local)
}

func (p *FCMProvider) token(ctx context.Context, now time.Time) (string, Outcome) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bearer != "" && now.Before(p.freshUntil) {
		return p.bearer, Outcome{}
	}
	header := map[string]string{"typ": "JWT", "alg": "RS256", "kid": p.keyID}
	claims := map[string]any{
		"iss": p.email, "aud": googleOAuth, "scope": fcmScope,
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
	}
	assertion, err := jwt(header, claims, func(hash [32]byte) ([]byte, error) {
		return rsa.SignPKCS1v15(rand.Reader, p.key, crypto.SHA256, hash[:])
	})
	if err != nil {
		return "", Outcome{Result: PermanentFailure}
	}
	form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}, "assertion": {assertion}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", Outcome{Result: PermanentFailure}
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.client.Do(req)
	if err != nil {
		return "", Outcome{Result: TemporaryFailure}
	}
	payload, err := vendorResponse(resp)
	defer clear(payload)
	if err != nil {
		return "", Outcome{Result: TemporaryFailure}
	}
	if resp.StatusCode != http.StatusOK {
		return "", vendorFailure(resp, now, time.Minute)
	}
	var token struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if json.Unmarshal(payload, &token) != nil || !validBearer(token.AccessToken) ||
		!strings.EqualFold(token.TokenType, "bearer") || token.ExpiresIn <= 0 {
		return "", Outcome{Result: TemporaryFailure}
	}
	lifetime := time.Duration(min(token.ExpiresIn, int64(3600))) * time.Second
	p.bearer, p.freshUntil = token.AccessToken, now.Add(max(lifetime-30*time.Second, 0))
	return p.bearer, Outcome{}
}

func validBearer(token string) bool {
	if len(token) == 0 || len(token) > 4096 {
		return false
	}
	for i := range len(token) {
		if token[i] < 0x21 || token[i] >= 0x7f {
			return false
		}
	}
	return true
}

func (p *FCMProvider) Send(ctx context.Context, token []byte) Outcome {
	if !ValidToken(FCM, token) {
		return Outcome{Result: PermanentFailure}
	}
	now := p.now()
	bearer, failure := p.token(ctx, now)
	if failure.Result != 0 {
		return failure
	}
	body, err := json.Marshal(map[string]any{"message": map[string]any{
		"token": string(token), "notification": genericNotification(),
		"data": map[string]string{"newim": "sync"},
		"android": map[string]string{
			"collapse_key": "newim-sync",
			"ttl":          strconv.FormatInt(int64(notificationTTL/time.Second), 10) + "s",
		},
	}})
	if err != nil {
		return Outcome{Result: PermanentFailure}
	}
	defer clear(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.sendURL, bytes.NewReader(body))
	if err != nil {
		return Outcome{Result: PermanentFailure}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearer)
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
	case http.StatusNotFound:
		if unregisteredFCM(payload) {
			return Outcome{Result: InvalidToken}
		}
	case http.StatusUnauthorized:
		p.mu.Lock()
		if p.bearer == bearer {
			p.freshUntil = time.Time{}
		}
		p.mu.Unlock()
		return Outcome{Result: TemporaryFailure}
	}
	return vendorFailure(resp, now, time.Minute)
}

func unregisteredFCM(payload []byte) bool {
	var envelope struct {
		Error struct {
			Details []struct {
				Type string `json:"@type"`
				Code string `json:"errorCode"`
			} `json:"details"`
		} `json:"error"`
	}
	if json.Unmarshal(payload, &envelope) != nil {
		return false
	}
	for _, detail := range envelope.Error.Details {
		if detail.Type == fcmError && detail.Code == "UNREGISTERED" {
			return true
		}
	}
	return false
}
