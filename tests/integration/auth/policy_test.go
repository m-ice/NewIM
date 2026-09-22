//go:build integration

package auth_test

import (
	"context"
	"errors"
	"testing"

	app "github.com/m-ice/NewIM/server/auth/session"
)

type staticPolicy struct {
	decision app.LoginDecision
	err      error
}

func (p staticPolicy) Decide(context.Context, app.LoginRequest) (app.LoginDecision, error) {
	return p.decision, p.err
}

func TestAuthPolicy(t *testing.T) {
	t.Run("injected-policy-and-validated-targets", func(t *testing.T) {
		f := openFixture(t)
		alice := f.seedBinding("policy_alice")
		bob := f.seedBinding("policy_bob")
		observer := &captureObserver{}
		policy := staticPolicy{decision: app.LoginDecision{KickTargets: []app.KickTarget{{
			UserID: alice.UserID, DeviceID: alice.DeviceID, SessionID: alice.SessionID,
		}}}}
		service := f.service(observer, nil, policy)
		plan, err := service.PlanLogin(ctx, app.LoginRequest{Binding: alice})
		must(t, err)
		if plan.Binding != alice || len(plan.KickTargets) != 1 || plan.KickTargets[0].SessionID != alice.SessionID {
			t.Fatalf("validated plan mismatch: %+v", plan)
		}
		observation, ok := observer.latest("plan_login")
		if !ok || observation.Code != app.AuthPlanLoginOK {
			t.Fatalf("plan observation=%+v", observation)
		}

		missing := f.service(&captureObserver{}, nil, nil)
		if _, err = missing.PlanLogin(ctx, app.LoginRequest{Binding: alice}); app.ErrorCode(err) != app.AuthPolicyRequired {
			t.Fatalf("missing policy got %v", err)
		}

		invalid := f.service(&captureObserver{}, nil, staticPolicy{err: errors.New("policy failed")})
		if _, err = invalid.PlanLogin(ctx, app.LoginRequest{Binding: alice}); app.ErrorCode(err) != app.AuthPolicyInvalid {
			t.Fatalf("invalid policy got %v", err)
		}

		cross := f.service(&captureObserver{}, nil, staticPolicy{decision: app.LoginDecision{KickTargets: []app.KickTarget{{
			UserID: bob.UserID, DeviceID: bob.DeviceID, SessionID: bob.SessionID,
		}}}})
		if _, err = cross.PlanLogin(ctx, app.LoginRequest{Binding: alice}); app.ErrorCode(err) != app.AuthForbidden {
			t.Fatalf("cross-account policy target got %v", err)
		}

		missingTarget := f.service(&captureObserver{}, nil, staticPolicy{decision: app.LoginDecision{KickTargets: []app.KickTarget{{
			UserID: alice.UserID, DeviceID: "missing_device", SessionID: "missing_session",
		}}}})
		if _, err = missingTarget.PlanLogin(ctx, app.LoginRequest{Binding: alice}); app.ErrorCode(err) != app.AuthPolicyInvalid {
			t.Fatalf("missing policy target got %v", err)
		}

		otherDevice := f.service(&captureObserver{}, nil, staticPolicy{decision: app.LoginDecision{KickTargets: []app.KickTarget{{
			UserID: alice.UserID, DeviceID: bob.DeviceID, SessionID: bob.SessionID,
		}}}})
		if _, err = otherDevice.PlanLogin(ctx, app.LoginRequest{Binding: alice}); app.ErrorCode(err) != app.AuthForbidden {
			t.Fatalf("cross-device policy target got %v", err)
		}
	})

	t.Run("stable-identity-and-construction-errors", func(t *testing.T) {
		if _, err := app.NewConnectionIdentity("", "device", "session", "connection", "0123456789abcdef0123456789abcdef"); app.ErrorCode(err) != app.AuthInvalidInput {
			t.Fatalf("partial identity got %v", err)
		}
		identity, err := app.NewConnectionIdentity("user", "device", "session", "connection", "0123456789abcdef0123456789abcdef")
		must(t, err)
		if identity.UserID() != "user" || identity.TokenID() != "0123456789abcdef0123456789abcdef" {
			t.Fatalf("identity getters mismatch: %+v", identity)
		}
	})
}
