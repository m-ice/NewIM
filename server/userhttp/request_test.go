package userhttp

import (
	"testing"

	"github.com/m-ice/NewIM/server/user"
)

func TestProvisionActorTypeIsOptIn(t *testing.T) {
	raw := []byte(`{"userId":"bot_1","nickname":"Example","faceUrl":"","ex":"","actorType":"BOT"}`)
	id, patch, actor, ok := DecodeProvisionBody(raw, true)
	if !ok || id != "bot_1" || actor != user.ActorBot || patch.Nickname == nil || *patch.Nickname != "Example" {
		t.Fatal("enabled actor provisioning rejected the complete request")
	}
	if _, _, ok := DecodeProvision(raw); ok {
		t.Fatal("legacy provisioning accepted an actor type")
	}
	if _, ok := decodeUpdate([]byte(`{"nickname":"Example","actorType":"BOT"}`)); ok {
		t.Fatal("profile update accepted an actor type")
	}
	id, _, actor, ok = DecodeProvisionBody([]byte(`{"userId":"bot_1"}`), true)
	if !ok || id != "bot_1" || actor != "" {
		t.Fatal("an omitted actor type must stay unspecified")
	}
}

func TestProvisionActorTypeRequestShape(t *testing.T) {
	for _, raw := range []string{
		`{"userId":"person","actorType":null}`,
		`{"userId":"person","actorType":1}`,
		`{"userId":"person","actorType":""}`,
		`{"userId":"person","actorType":"bot"}`,
		`{"userId":"person","actorType":"UNKNOWN"}`,
		`{"userId":"person","actorType":"BOT","actorType":"SYSTEM"}`,
		`{"userId":"person","actorType":"BOT","role":"admin"}`,
	} {
		if _, _, _, ok := DecodeProvisionBody([]byte(raw), true); ok {
			t.Errorf("accepted invalid request: %s", raw)
		}
	}
}
