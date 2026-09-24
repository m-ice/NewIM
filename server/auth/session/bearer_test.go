package session

import (
	"context"
	"strings"
	"testing"
	"time"
)

type bearerPanicStore struct{ Store }

func (bearerPanicStore) Authenticate(context.Context, string, func(TokenSnapshot) error) error {
	panic("store panic sentinel")
}

type bearerCaptureObserver struct {
	observations []Observation
}

func (o *bearerCaptureObserver) Observe(observation Observation) {
	o.observations = append(o.observations, observation)
}

func (o *bearerCaptureObserver) latest(operation string) (Observation, bool) {
	for i := len(o.observations) - 1; i >= 0; i-- {
		if o.observations[i].Operation == operation {
			return o.observations[i], true
		}
	}
	return Observation{}, false
}

func TestAuthenticateBearerPanicIsObservedAsUnavailable(t *testing.T) {
	observer := &bearerCaptureObserver{}
	service, err := NewService(bearerPanicStore{}, Config{
		Clock:    ClockFunc(func() time.Time { return time.Unix(1_800_000_000, 0).UTC() }),
		Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := TokenPrefix + strings.Repeat("a", TokenIDHexLen) + "_" + strings.Repeat("A", TokenSecretLen)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected store panic")
			}
			observation, ok := observer.latest("authenticate_bearer")
			if !ok || observation.Code != AuthStorageUnavailable {
				t.Fatalf("panic observation got %+v, ok=%t", observation, ok)
			}
		}()
		_, _ = service.AuthenticateBearer(context.Background(), raw)
	}()
}
