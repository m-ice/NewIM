package webhookhttp_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/m-ice/NewIM/server/api"
	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	app "github.com/m-ice/NewIM/server/webhook"
	"github.com/m-ice/NewIM/server/webhookhttp"
)

const (
	routePrefix = "/api/v1/webhooks/"
	testToken   = "whm_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	testURL     = "https://webhook.example.test/events/fixture"
	testSecret  = "abababababababababababababababababababababababababababababababab"
	getBody     = `{"destinationId":"destination"}`
	createBody  = `{"requestId":"req","destinationId":"destination","url":"` + testURL + `","keyId":"key","secret":"` + testSecret + `"}`
	replayBody  = `{"requestId":"req","destinationId":"destination","deliveryId":"delivery"}`
)

type endpointList struct {
	after string
	limit int
}

type managementStub struct {
	calls []any
	err   error
}

func (s *managementStub) ChangeEndpoint(_ context.Context, m app.EndpointMutation) (app.EndpointInfo, error) {
	m.Secret = bytes.Clone(m.Secret)
	s.calls = append(s.calls, m)
	revision := int64(17)
	return app.EndpointInfo{DestinationID: m.DestinationID, Revision: &revision, Status: "active"}, s.err
}

func (s *managementStub) GetEndpoint(_ context.Context, id string) (app.EndpointInfo, error) {
	s.calls = append(s.calls, id)
	return app.EndpointInfo{DestinationID: id, Status: "active"}, s.err
}

func (s *managementStub) ListEndpoints(_ context.Context, after string, limit int) (app.EndpointPage, error) {
	s.calls = append(s.calls, endpointList{after, limit})
	return app.EndpointPage{Items: []app.EndpointInfo{}, NextAfterID: "next"}, s.err
}

func (s *managementStub) ListDeliveries(_ context.Context, q app.DeliveryQuery) (app.DeliveryPage, error) {
	s.calls = append(s.calls, q)
	return app.DeliveryPage{Items: []app.DeliveryInfo{{DeliveryID: "delivery", DestinationID: q.DestinationID}}}, s.err
}

func (s *managementStub) ReplayDelivery(_ context.Context, r app.ReplayRequest) (app.ReplayReceipt, error) {
	s.calls = append(s.calls, r)
	return app.ReplayReceipt{RequestID: r.RequestID, DestinationID: r.DestinationID, DeliveryID: r.DeliveryID, Revision: 17}, s.err
}

func writeCredential(t *testing.T, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test-management-token")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func newRoutes(t *testing.T, store app.ManagementStore) map[string]api.APIRoute {
	t.Helper()
	routes, err := webhookhttp.NewRoutes(store, writeCredential(t, testToken+"\n", 0o600))
	if err != nil {
		t.Fatal(err)
	}
	byPath := make(map[string]api.APIRoute, len(routes))
	for _, route := range routes {
		if _, exists := byPath[route.Path]; exists {
			t.Fatalf("duplicate route %q", route.Path)
		}
		byPath[route.Path] = route
	}
	return byPath
}

func request(path, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, routePrefix+path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+testToken)
	r.Header.Set("Content-Type", "application/json; charset=UTF-8")
	return r
}

func serve(t *testing.T, routes map[string]api.APIRoute, path string, r *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	route, ok := routes[routePrefix+path]
	if !ok || route.Handler == nil {
		t.Fatalf("missing route %q", path)
	}
	w := httptest.NewRecorder()
	route.Handler.ServeHTTP(w, r)
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("management response lacks no-store")
	}
	return w
}

func requireError(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	want := `{"error":{"code":"` + code + `"}}`
	if w.Code != status || strings.TrimSpace(w.Body.String()) != want {
		t.Fatalf("got status %d and body %q, want %d and %s", w.Code, w.Body.String(), status, want)
	}
}

func TestRoutesDispatch(t *testing.T) {
	store := &managementStub{}
	routes := newRoutes(t, store)
	secret := bytes.Repeat([]byte{0xab}, app.ManagementSecretSize)
	tests := []struct {
		path     string
		body     string
		status   int
		response string
		call     any
	}{
		{"endpoints/create", createBody, 201, `"revision":"17"`, app.EndpointMutation{
			Operation: app.CreateEndpoint, RequestID: "req", DestinationID: "destination", URL: testURL, KeyID: "key", Secret: secret,
		}},
		{"endpoints/get", getBody, 200, `"destinationId":"destination"`, "destination"},
		{"endpoints/list", `{}`, 200, `"nextAfterId":"next"`, endpointList{"", 50}},
		{"endpoints/update", `{"requestId":"req","destinationId":"destination","expectedRevision":"17","url":"` + testURL + `"}`, 200,
			`"revision":"17"`, app.EndpointMutation{Operation: app.UpdateEndpoint, RequestID: "req", DestinationID: "destination", ExpectedRevision: 17, URL: testURL}},
		{"endpoints/revoke", `{"requestId":"req","destinationId":"destination","expectedRevision":"17"}`, 200,
			`"revision":"17"`, app.EndpointMutation{Operation: app.RevokeEndpoint, RequestID: "req", DestinationID: "destination", ExpectedRevision: 17}},
		{"endpoints/rotate", `{"requestId":"req","destinationId":"destination","expectedRevision":"17","keyId":"key","secret":"` + testSecret + `"}`, 200,
			`"revision":"17"`, app.EndpointMutation{Operation: app.RotateEndpoint, RequestID: "req", DestinationID: "destination", ExpectedRevision: 17, KeyID: "key", Secret: secret}},
		{"deliveries/list", `{"destinationId":"destination","afterId":"previous","limit":100}`, 200, `"deliveryId":"delivery"`,
			app.DeliveryQuery{DestinationID: "destination", AfterID: "previous", Limit: 100}},
		{"deliveries/replay", replayBody, 200, `"revision":"17"`, app.ReplayRequest{RequestID: "req", DestinationID: "destination", DeliveryID: "delivery"}},
	}
	if len(routes) != len(tests) {
		t.Fatalf("got %d routes, want %d", len(routes), len(tests))
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			route := routes[routePrefix+tc.path]
			if !reflect.DeepEqual(route.AllowedMethods, []string{http.MethodPost}) || route.MaxBodyBytes != 8192 || !route.RejectQuery {
				t.Fatalf("incorrect route limits for %s", tc.path)
			}
			store.calls = nil
			w := serve(t, routes, tc.path, request(tc.path, tc.body))
			if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.response) {
				t.Fatalf("got status %d and body %q, want %d containing %s", w.Code, w.Body.String(), tc.status, tc.response)
			}
			if !reflect.DeepEqual(store.calls, []any{tc.call}) {
				t.Fatalf("incorrect dispatch for %s", tc.path)
			}
			for _, private := range []string{testURL, testSecret, testToken} {
				if strings.Contains(w.Body.String(), private) {
					t.Fatal("successful response exposed private input")
				}
			}
		})
	}
	store.calls = nil
	const path = "deliveries/list"
	w := serve(t, routes, path, request(path, `{"destinationId":"destination","deliveryId":"delivery"}`))
	if w.Code != http.StatusOK || !reflect.DeepEqual(store.calls, []any{app.DeliveryQuery{DestinationID: "destination", DeliveryID: "delivery", Limit: 50}}) {
		t.Fatal("exact delivery lookup or default limit was not delegated")
	}
}

func TestRoutesRejectInvalidJSON(t *testing.T) {
	store := &managementStub{}
	routes := newRoutes(t, store)
	for _, tc := range []struct{ name, path, body string }{
		{"duplicate", "endpoints/get", `{"destinationId":"one","destinationId":"two"}`},
		{"escaped duplicate", "endpoints/get", `{"destinationId":"one","destination\u0049d":"two"}`},
		{"unknown", "endpoints/get", `{"destinationId":"destination","actor":"operator"}`},
		{"case mismatch", "endpoints/get", `{"DestinationId":"destination"}`},
		{"trailing", "endpoints/get", getBody + ` {}`},
		{"invalid UTF-8", "endpoints/get", "{\"destinationId\":\"\xff\"}"},
		{"unpaired surrogate", "endpoints/get", `{"destinationId":"\uD800"}`},
		{"null object", "endpoints/list", `null`},
		{"array", "endpoints/list", `[]`},
		{"null limit", "endpoints/list", `{"limit":null}`},
		{"zero limit", "endpoints/list", `{"limit":0}`},
		{"large limit", "endpoints/list", `{"limit":101}`},
		{"missing destination", "deliveries/list", `{"deliveryId":"delivery"}`},
		{"exclusive cursors", "deliveries/list", `{"destinationId":"destination","deliveryId":"delivery","afterId":"previous"}`},
		{"numeric revision", "endpoints/revoke", `{"requestId":"req","destinationId":"destination","expectedRevision":1}`},
		{"noncanonical revision", "endpoints/revoke", `{"requestId":"req","destinationId":"destination","expectedRevision":"01"}`},
		{"zero revision", "endpoints/revoke", `{"requestId":"req","destinationId":"destination","expectedRevision":"0"}`},
		{"overflow revision", "endpoints/revoke", `{"requestId":"req","destinationId":"destination","expectedRevision":"9223372036854775808"}`},
		{"cross-operation field", "endpoints/revoke", `{"requestId":"req","destinationId":"destination","expectedRevision":"1","keyId":"key"}`},
		{"uppercase secret", "endpoints/create", strings.Replace(createBody, testSecret, strings.ToUpper(testSecret), 1)},
		{"short secret", "endpoints/create", strings.Replace(createBody, testSecret, testSecret[1:], 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := serve(t, routes, tc.path, request(tc.path, tc.body))
			requireError(t, w, http.StatusBadRequest, string(app.CodeManagementInput))
		})
	}
	if len(store.calls) != 0 {
		t.Fatal("invalid requests reached the management store")
	}
}

func TestRoutesBearerAndHTTPBoundaries(t *testing.T) {
	store := &managementStub{}
	routes := newRoutes(t, store)
	const path = "endpoints/get"
	for _, tc := range []struct {
		name   string
		values []string
	}{
		{"missing", nil},
		{"unrelated namespace", []string{"Bearer " + strings.TrimPrefix(testToken, "whm_")}},
		{"different management token", []string{"Bearer " + testToken[:len(testToken)-1] + "0"}},
		{"multiple", []string{"Bearer " + testToken, "Bearer " + testToken}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := request(path, getBody)
			r.Header["Authorization"] = tc.values
			r.Header.Set("X-Authenticated-User", "operator")
			w := serve(t, routes, path, r)
			requireError(t, w, http.StatusUnauthorized, string(bearerhttp.CodeInvalidToken))
			if !strings.HasPrefix(w.Header().Get("WWW-Authenticate"), "Bearer ") {
				t.Fatal("invalid token response lacks Bearer challenge")
			}
		})
	}
	r := request(path, getBody)
	r.Header.Set("Content-Type", "text/plain")
	requireError(t, serve(t, routes, path, r), http.StatusUnsupportedMediaType, api.HTTPUnsupportedMediaType)
	r = request(path, getBody+strings.Repeat(" ", 8193-len(getBody)))
	r.ContentLength = -1
	r.TransferEncoding = []string{"chunked"}
	requireError(t, serve(t, routes, path, r), http.StatusRequestEntityTooLarge, api.HTTPBodyTooLarge)
	r = request(path, getBody)
	r.Method = http.MethodGet
	w := serve(t, routes, path, r)
	requireError(t, w, http.StatusMethodNotAllowed, api.HTTPMethodNotAllowed)
	if w.Header().Get("Allow") != http.MethodPost {
		t.Fatal("method rejection did not advertise POST")
	}
	requireError(t, serve(t, routes, path, request(path+"?", getBody)), http.StatusBadRequest, api.HTTPInvalidQuery)
	requireError(t, serve(t, routes, path, request(path+"/", getBody)), http.StatusNotFound, api.HTTPRouteNotFound)
	if len(store.calls) != 0 {
		t.Fatal("invalid credentials and HTTP requests reached the management store")
	}
	w = serve(t, routes, path, request(path, getBody+strings.Repeat(" ", 8192-len(getBody))))
	if w.Code != http.StatusOK || len(store.calls) != 1 {
		t.Fatal("a valid request at the body limit did not reach the store")
	}
}

func TestManagementCredentialFile(t *testing.T) {
	store := &managementStub{}
	for _, tc := range []struct {
		name, contents string
		mode           os.FileMode
		valid          bool
	}{
		{"no newline", testToken, 0o400, true},
		{"LF", testToken + "\n", 0o600, true},
		{"CRLF", testToken + "\r\n", 0o600, false},
		{"extra newline", testToken + "\n\n", 0o600, false},
		{"uppercase suffix", "whm_" + strings.ToUpper(strings.TrimPrefix(testToken, "whm_")), 0o600, false},
		{"group read", testToken, 0o640, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeCredential(t, tc.contents, tc.mode)
			routes, err := webhookhttp.NewRoutes(store, path)
			if (err == nil) != tc.valid || !tc.valid && len(routes) != 0 {
				t.Fatal("credential loading did not match the file contract")
			}
			if err != nil && (strings.Contains(err.Error(), path) || strings.Contains(err.Error(), tc.contents)) {
				t.Fatal("startup error exposed credential path or contents")
			}
		})
	}
}

func TestRoutesMapManagementErrors(t *testing.T) {
	for _, tc := range []struct {
		code   app.Code
		status int
		err    error
	}{
		{app.CodeManagementInput, 400, app.Fail(app.CodeManagementInput)},
		{app.CodeManagementNotFound, 404, app.Fail(app.CodeManagementNotFound)},
		{app.CodeManagementConflict, 409, app.Fail(app.CodeManagementConflict)},
		{app.CodeManagementNotReplayable, 409, app.Fail(app.CodeManagementNotReplayable)},
		{app.CodeManagementCapacity, 503, app.Fail(app.CodeManagementCapacity)},
		{app.CodeStorageUnavailable, 503, errors.New("storage detail: " + testURL + testSecret)},
	} {
		t.Run(string(tc.code), func(t *testing.T) {
			store := &managementStub{err: tc.err}
			const path = "deliveries/replay"
			w := serve(t, newRoutes(t, store), path, request(path, replayBody))
			requireError(t, w, tc.status, string(tc.code))
			if len(store.calls) != 1 {
				t.Fatal("valid input did not reach the store exactly once")
			}
		})
	}
}
