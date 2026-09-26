//go:build integration

package messagehttp_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestMessageHTTPSecurity(t *testing.T) {
	f := openFixture(t)
	identity := f.seedIdentity("security", seedOptions{})
	conversationID := f.seedConversation("security", identity.UserID)
	server := startMessageServer(t, validMessageEnv())
	defer server.stop(t)
	server.waitHealthy(t)
	server.waitReady(t)

	const (
		clientMsgID = "security_client_sentinel"
		bodyText    = "security_body_sentinel"
	)
	body := textBody(t, clientMsgID, conversationID, bodyText)
	result := doRequest(t, http.MethodPost, messageURL(server), body, identity.RawToken, nil)
	if result.status != http.StatusOK {
		t.Fatalf("send status=%d body=%q", result.status, result.body)
	}

	var envelope map[string]json.RawMessage
	must(t, json.Unmarshal(result.body, &envelope))
	if len(envelope) != 3 || envelope["protocolVersion"] == nil || envelope["kind"] == nil || envelope["body"] == nil {
		t.Fatalf("success frame has unexpected fields: %v", envelope)
	}
	var frameBody map[string]json.RawMessage
	must(t, json.Unmarshal(envelope["body"], &frameBody))
	allowed := map[string]bool{
		"status": true, "clientMsgId": true, "conversationId": true, "senderId": true,
		"serverMsgId": true, "conversationSeq": true, "serverTime": true,
	}
	if len(frameBody) != len(allowed) {
		t.Fatalf("ACK fields=%v", frameBody)
	}
	for field := range frameBody {
		if !allowed[field] {
			t.Fatalf("ACK contains non-protocol field %q", field)
		}
	}

	metricsResponse, err := newHTTPClient().Get(metricsURL(server))
	must(t, err)
	metricsBody, readErr := io.ReadAll(metricsResponse.Body)
	_ = metricsResponse.Body.Close()
	must(t, readErr)
	if metricsResponse.StatusCode != http.StatusOK || !strings.Contains(string(metricsBody), `route="message_send"`) {
		t.Fatalf("metrics status=%d body=%q", metricsResponse.StatusCode, metricsBody)
	}

	for _, value := range []string{identity.RawToken, string(body), localDSN, "SELECT", "pgx"} {
		if strings.Contains(string(result.body), value) {
			t.Fatalf("response leaked %q: %q", value, result.body)
		}
	}
	for name, output := range map[string]string{"logs": server.logs(), "metrics": string(metricsBody)} {
		for _, value := range []string{identity.RawToken, string(body), clientMsgID, conversationID, identity.UserID, localDSN, "SELECT", "pgx"} {
			if strings.Contains(output, value) {
				t.Fatalf("%s leaked %q: %q", name, value, output)
			}
		}
	}

	malformed := []byte(`{"protocolVersion":1,"kind":"send","body":{"clientMsgId":"security_error_sentinel","conversationId":"` + conversationID + `","version":1,"type":"text","payload":{"text":"` + bodyText + `"}}`)
	result = doRequest(t, http.MethodPost, messageURL(server), malformed, identity.RawToken, nil)
	assertError(t, result, http.StatusBadRequest, "SEND_INVALID_INPUT")
	if strings.Contains(string(result.body), bodyText) || strings.Contains(string(result.body), identity.RawToken) {
		t.Fatalf("error response reflected sensitive input: %q", result.body)
	}
}
