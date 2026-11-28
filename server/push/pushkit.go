package push

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"net/http"
	"sync"
	"time"

	"github.com/m-ice/NewIM/server/credentialfile"
)

const (
	pushKitAudience = "https://oauth-login.cloud.huawei.com/oauth2/v3/token"
	pushKitEndpoint = "https://push-api.cloud.huawei.com/v3/"
)

type PushKitProvider struct {
	client  *http.Client
	sendURL string
	issuer  string
	keyID   string
	key     *rsa.PrivateKey
	now     func() time.Time

	mu       sync.Mutex
	bearer   string
	issuedAt time.Time
}

func NewPushKit(credentialsFile string) (*PushKitProvider, error) {
	data, err := credentialfile.Read(credentialsFile, 1, 32768)
	defer clear(data)
	if err != nil {
		return nil, ErrConfig
	}
	var account struct {
		ProjectID string `json:"project_id"`
		KeyID     string `json:"key_id"`
		Issuer    string `json:"sub_account"`
		Key       string `json:"private_key"`
		TokenURI  string `json:"token_uri"`
	}
	if jsonv2.Unmarshal(data, &account) != nil || !validID(account.ProjectID) ||
		!validID(account.KeyID) || len(account.Issuer) > 256 ||
		!ValidToken(PushKit, []byte(account.Issuer)) || account.TokenURI != pushKitAudience {
		return nil, ErrConfig
	}
	encoded := []byte(account.Key)
	defer clear(encoded)
	der, err := privateKeyDER(encoded)
	defer clear(der)
	if err != nil {
		return nil, ErrConfig
	}
	parsed, err := x509.ParsePKCS8PrivateKey(der)
	key, ok := parsed.(*rsa.PrivateKey)
	if err != nil || !ok || key.N.BitLen() < 2048 || key.Validate() != nil {
		return nil, ErrConfig
	}
	return &PushKitProvider{
		client: vendorClient(), now: time.Now,
		sendURL: pushKitEndpoint + account.ProjectID + "/messages:send",
		issuer:  account.Issuer, keyID: account.KeyID, key: key,
	}, nil
}

func (p *PushKitProvider) token(now time.Time) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bearer != "" && !now.Before(p.issuedAt) && now.Before(p.issuedAt.Add(45*time.Minute)) {
		return p.bearer, nil
	}
	header := map[string]string{"typ": "JWT", "alg": "PS256", "kid": p.keyID}
	claims := map[string]any{
		"aud": pushKitAudience, "iss": p.issuer, "iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
	}
	bearer, err := jwt(header, claims, func(sum [32]byte) ([]byte, error) {
		return rsa.SignPSS(rand.Reader, p.key, crypto.SHA256, sum[:], &rsa.PSSOptions{
			SaltLength: rsa.PSSSaltLengthEqualsHash,
		})
	})
	if err != nil {
		return "", err
	}
	p.bearer, p.issuedAt = bearer, now
	return bearer, nil
}

func (p *PushKitProvider) expire(bearer string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bearer == bearer {
		p.bearer = ""
	}
}

func (p *PushKitProvider) Send(ctx context.Context, token []byte) Outcome {
	if !ValidToken(PushKit, token) {
		return Outcome{Result: PermanentFailure}
	}
	now := p.now()
	bearer, err := p.token(now)
	if err != nil {
		return Outcome{Result: PermanentFailure}
	}

	alert := genericNotification()
	body, err := json.Marshal(map[string]any{
		"payload": map[string]any{"notification": map[string]any{
			"category": "IM", "title": alert.Title, "body": alert.Body, "foregroundShow": false,
			"clickAction": map[string]any{"actionType": 0, "data": map[string]string{"newim": "sync"}},
		}},
		"target":      map[string][]string{"token": {string(token)}},
		"pushOptions": map[string]int64{"ttl": int64(notificationTTL / time.Second)},
	})
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
	req.Header.Set("push-type", "0")

	resp, err := p.client.Do(req)
	if err != nil {
		return Outcome{Result: TemporaryFailure}
	}
	payload, err := vendorResponse(resp)
	defer clear(payload)
	if resp.StatusCode >= http.StatusInternalServerError || resp.StatusCode == http.StatusTooManyRequests {
		return vendorFailure(resp, now, 0)
	}
	if err != nil {
		return Outcome{Result: TemporaryFailure}
	}

	var result struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
	}
	if jsonv2.Unmarshal(payload, &result) != nil || result.Code == "" {
		if resp.StatusCode == http.StatusOK {
			return Outcome{Result: TemporaryFailure}
		}
		return Outcome{Result: PermanentFailure}
	}
	switch result.Code {
	case "80000000":
		if resp.StatusCode == http.StatusOK {
			return Outcome{Result: Accepted}
		}
	case "80200005":
		p.expire(bearer)
		return Outcome{Result: TemporaryFailure}
	case "81000001":
		return Outcome{Result: TemporaryFailure}
	case "80100000", "80300007":
		if (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusBadRequest) &&
			malformedPushKitToken(result.Msg, token) {
			return Outcome{Result: InvalidToken}
		}
	}
	return Outcome{Result: PermanentFailure}
}

func malformedPushKitToken(message string, token []byte) bool {
	var details struct {
		IllegalTokens map[string][]string `json:"illegalTokens"`
		Success       int                 `json:"success"`
		Failure       int                 `json:"failure"`
	}
	if jsonv2.Unmarshal([]byte(message), &details) != nil || details.Success != 0 ||
		details.Failure != 1 || len(details.IllegalTokens) != 1 {
		return false
	}
	invalid := details.IllegalTokens["tokenFormatError"]
	return len(invalid) == 1 && invalid[0] == string(token)
}
