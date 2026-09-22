//go:build integration

package auth_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	app "github.com/m-ice/NewIM/server/auth/session"
)

func TestAuthCheck(t *testing.T) {
	t.Run("opaque-token-storage-and-complete-identity", func(t *testing.T) {
		f := openFixture(t)
		binding := f.seedBinding("check_token")
		observer := &captureObserver{}
		service := f.service(observer, nil, nil)
		token := f.issue(service, binding, time.Hour)

		if len(token.RawToken()) != app.RawTokenLen || !strings.HasPrefix(token.RawToken(), app.TokenPrefix) {
			t.Fatalf("raw token length/prefix got %d/%q", len(token.RawToken()), token.RawToken()[:len(app.TokenPrefix)])
		}
		if len(token.TokenID()) != app.TokenIDHexLen {
			t.Fatalf("token id length got %d", len(token.TokenID()))
		}
		if got := f.tokenDigestHex(token.TokenID()); got != hashToken(token.RawToken()) {
			t.Fatal("stored digest does not match sha256(raw)")
		}
		if strings.Contains(f.scalarString("SELECT string_agg(token_id||encode(token_digest,'hex'),',') FROM newim.im_auth_tokens"), token.RawToken()) {
			t.Fatal("raw token appeared in database representation")
		}
		identity, err := f.authenticate(service, token, binding, "connection_1")
		must(t, err)
		if identity.UserID() != binding.UserID || identity.DeviceID() != binding.DeviceID ||
			identity.SessionID() != binding.SessionID || identity.ConnectionID() != "connection_1" ||
			identity.TokenID() != token.TokenID() {
			t.Fatalf("incomplete identity: %+v", identity)
		}
		issuedObservation, ok := observer.latest("issue")
		if !ok || issuedObservation.Code != app.AuthIssueOK {
			t.Fatalf("issue observation=%+v", issuedObservation)
		}
		authenticateObservation, ok := observer.latest("authenticate")
		if !ok || authenticateObservation.Code != app.AuthAuthenticateOK {
			t.Fatalf("authenticate observation=%+v", authenticateObservation)
		}
		if got := observer.count(); got != 2 {
			t.Fatalf("expected exactly one terminal event per operation, got %d", got)
		}
	})

	t.Run("strict-format-entropy-and-collision", func(t *testing.T) {
		f := openFixture(t)
		binding := f.seedBinding("check_collision")
		observer := &captureObserver{}
		pattern := []byte("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
		service := f.service(observer, &repeatingReader{pattern: pattern}, nil)
		first := f.issue(service, binding, time.Hour)

		for i, malformed := range []string{
			"", "n1_bad_bad", first.RawToken()[:len(first.RawToken())-1],
			"n1_" + "A" + first.TokenID()[1:] + "_" + first.RawToken()[len(app.TokenPrefix)+app.TokenIDHexLen+1:],
			first.RawToken() + "=",
		} {
			_, err := service.Authenticate(ctx, app.AuthenticateRequest{Token: malformed, Binding: binding, ConnectionID: "connection_1"})
			if got := app.ErrorCode(err); got != app.AuthTokenMalformed {
				t.Fatalf("malformed case %d len=%d tokenID=%s upper=%s got %s (%v)", i, len(malformed), first.TokenID(), strings.ToUpper(first.TokenID()), got, err)
			}
		}

		collisionService := f.service(&captureObserver{}, &repeatingReader{pattern: pattern}, nil)
		second, err := collisionService.Issue(ctx, app.IssueRequest{Binding: binding, TTL: time.Hour})
		if app.ErrorCode(err) != app.AuthTokenCollision {
			t.Fatalf("collision got err=%v firstID=%s secondID=%s", err, first.TokenID(), second.TokenID())
		}
		if got := f.tokenCountForSession(binding.SessionID); got != 1 {
			t.Fatalf("collision changed session token count to %d firstID=%s secondID=%s", got, first.TokenID(), second.TokenID())
		}

		entropyService := f.service(&captureObserver{}, errorReader{err: errors.New("entropy failure")}, nil)
		_, err = entropyService.Issue(ctx, app.IssueRequest{Binding: binding, TTL: time.Hour})
		mustCode(t, err, app.AuthEntropyUnavailable)
	})

	t.Run("authentication-error-precedence", func(t *testing.T) {
		f := openFixture(t)
		binding := f.seedBinding("check_precedence")
		observer := &captureObserver{}
		service := f.service(observer, nil, nil)

		_, err := service.Authenticate(nil, app.AuthenticateRequest{Token: "bad", Binding: binding, ConnectionID: "connection_1"})
		mustCode(t, err, app.AuthInvalidInput)

		active := f.issue(service, binding, time.Hour)
		_, err = service.Authenticate(ctx, app.AuthenticateRequest{Token: active.RawToken()[:len(active.RawToken())-1], Binding: binding, ConnectionID: "connection_1"})
		mustCode(t, err, app.AuthTokenMalformed)

		unknown := "n1_" + strings.Repeat("a", app.TokenIDHexLen) + "_" + strings.Repeat("A", app.TokenSecretLen)
		_, err = service.Authenticate(ctx, app.AuthenticateRequest{Token: unknown, Binding: binding, ConnectionID: "connection_1"})
		mustCode(t, err, app.AuthTokenUnknown)

		tampered := active.RawToken()
		for _, replacement := range []byte("AQgw") {
			if tampered[len(tampered)-1] != replacement {
				tampered = tampered[:len(tampered)-1] + string(replacement)
				break
			}
		}
		if tampered == active.RawToken() {
			t.Fatal("tamper did not change the final canonical base64url character")
		}
		_, err = service.Authenticate(ctx, app.AuthenticateRequest{Token: tampered, Binding: binding, ConnectionID: "connection_1"})
		mustCode(t, err, app.AuthTokenUnknown)

		wrong := app.SessionBinding{UserID: binding.UserID, DeviceID: binding.DeviceID, SessionID: "other_session"}
		_, err = service.Authenticate(ctx, app.AuthenticateRequest{Token: active.RawToken(), Binding: wrong, ConnectionID: "connection_1"})
		mustCode(t, err, app.AuthForbidden)

		revokedToken := f.issue(service, binding, time.Hour)
		if _, err = service.RevokeToken(ctx, binding.UserID, revokedToken.TokenID()); err != nil {
			t.Fatal(err)
		}
		if _, err = service.RevokeSession(ctx, binding); err != nil {
			t.Fatal(err)
		}
		f.now.Store(revokedToken.ExpiresAt().Unix() + 1)
		_, err = f.authenticate(service, revokedToken, binding, "connection_1")
		mustCode(t, err, app.AuthTokenRevoked)

		fresh := f.seedBinding("check_precedence_fresh")
		sessionRevoked := f.issue(service, fresh, time.Hour)
		if _, err = service.RevokeSession(ctx, fresh); err != nil {
			t.Fatal(err)
		}
		// Keep the token row active after the session revocation to exercise the
		// session-revocation branch independently from token-revocation precedence.
		f.sql("UPDATE newim.im_auth_tokens SET revoked_at=NULL WHERE token_id=$1", sessionRevoked.TokenID())
		_, err = f.authenticate(service, sessionRevoked, fresh, "connection_1")
		mustCode(t, err, app.AuthSessionRevoked)

		expiredBinding := f.seedBinding("check_expired")
		expired := f.issue(service, expiredBinding, time.Hour)
		f.now.Store(expired.ExpiresAt().Unix())
		_, err = f.authenticate(service, expired, expiredBinding, "connection_1")
		mustCode(t, err, app.AuthTokenExpired)
	})

	t.Run("revocation-ownership-and-idempotency", func(t *testing.T) {
		f := openFixture(t)
		alice := f.seedBinding("check_revoke_alice")
		bob := f.seedBinding("check_revoke_bob")
		observer := &captureObserver{}
		service := f.service(observer, nil, nil)
		token := f.issue(service, alice, time.Hour)

		if _, err := service.RevokeToken(ctx, bob.UserID, token.TokenID()); app.ErrorCode(err) != app.AuthForbidden {
			t.Fatalf("cross-account token revoke got %v", err)
		}
		if _, err := service.RevokeToken(ctx, alice.UserID, "0123456789abcdef0123456789abcdef"); app.ErrorCode(err) != app.AuthTokenUnknown {
			t.Fatalf("missing token revoke got %v", err)
		}
		outcome, err := service.RevokeToken(ctx, alice.UserID, token.TokenID())
		must(t, err)
		if outcome != app.RevokeOK {
			t.Fatalf("first revoke outcome=%s", outcome)
		}
		outcome, err = service.RevokeToken(ctx, alice.UserID, token.TokenID())
		must(t, err)
		if outcome != app.RevokeNoop {
			t.Fatalf("idempotent revoke outcome=%s", outcome)
		}
		_, err = f.authenticate(service, token, alice, "connection_1")
		mustCode(t, err, app.AuthTokenRevoked)

		sessionToken := f.issue(service, alice, time.Hour)
		crossSession := app.SessionBinding{UserID: bob.UserID, DeviceID: bob.DeviceID, SessionID: alice.SessionID}
		if _, err = service.RevokeSession(ctx, crossSession); app.ErrorCode(err) != app.AuthForbidden {
			t.Fatalf("cross-account session revoke got %v", err)
		}
		outcome, err = service.RevokeSession(ctx, alice)
		must(t, err)
		if outcome != app.RevokeOK {
			t.Fatalf("session revoke outcome=%s", outcome)
		}
		outcome, err = service.RevokeSession(ctx, alice)
		must(t, err)
		if outcome != app.RevokeNoop {
			t.Fatalf("idempotent session revoke outcome=%s", outcome)
		}
		if !f.sessionRevoked(alice) {
			t.Fatal("session was not durably revoked")
		}
		_, err = f.authenticate(service, sessionToken, alice, "connection_1")
		mustCode(t, err, app.AuthTokenRevoked)

		missing := app.SessionBinding{UserID: "missing_user", DeviceID: "missing_device", SessionID: "missing_session"}
		if _, err = service.RevokeSession(ctx, missing); app.ErrorCode(err) != app.AuthSessionNotFound {
			t.Fatalf("missing session revoke got %v", err)
		}
	})

	t.Run("issue-binding-before-revocation-and-redaction", func(t *testing.T) {
		f := openFixture(t)
		alice := f.seedBinding("check_issue_owner_alice")
		bob := f.seedBinding("check_issue_owner_bob")
		observer := &captureObserver{}
		service := f.service(observer, nil, nil)
		cross := app.SessionBinding{UserID: alice.UserID, DeviceID: bob.DeviceID, SessionID: bob.SessionID}
		_, err := service.Issue(ctx, app.IssueRequest{Binding: cross, TTL: time.Hour})
		mustCode(t, err, app.AuthForbidden)
		f.revokeSessionSQL(bob, time.Unix(f.now.Load(), 0).UTC())
		_, err = service.Issue(ctx, app.IssueRequest{Binding: cross, TTL: time.Hour})
		mustCode(t, err, app.AuthForbidden)
		if count := f.scalarString("SELECT count(*) FROM newim.im_auth_tokens WHERE session_id=$1", bob.SessionID); count != "0" {
			t.Fatalf("cross-account issue inserted %s rows", count)
		}

		token := f.issue(service, alice, time.Hour)
		rendered := fmt.Sprintf("%v %#v", token, token)
		if strings.Contains(rendered, token.RawToken()) || strings.Contains(rendered, hashToken(token.RawToken())) {
			t.Fatal("issued token formatting exposed raw or digest material")
		}
		observations := fmt.Sprintf("%+v", observer)
		if strings.Contains(observations, token.RawToken()) || strings.Contains(observations, hashToken(token.RawToken())) {
			t.Fatal("observations exposed token material")
		}

		if _, err := app.NewService(nil, app.Config{}); app.ErrorCode(err) != app.AuthClockRequired {
			t.Fatalf("clock validation got %v", err)
		}
		if _, err := app.NewService(nil, app.Config{Clock: app.ClockFunc(time.Now)}); app.ErrorCode(err) != app.AuthObserverRequired {
			t.Fatalf("observer validation got %v", err)
		}

		panicService := f.service(panicObserver{}, nil, nil)
		panicToken := f.issue(panicService, alice, time.Hour)
		if _, err := f.authenticate(panicService, panicToken, alice, "connection_1"); err != nil {
			t.Fatalf("observer panic changed security result: %v", err)
		}
		if count := observer.count(); count == 0 {
			t.Fatal("mandatory observer recorded no terminal event")
		}
	})
}
