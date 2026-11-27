//go:build integration

package conversationsynchttp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/m-ice/NewIM/server/group"
)

const (
	provisionPath     = "/api/v1/internal/users/provision"
	createGroupPath   = "/api/v1/groups/create"
	joinGroupPath     = "/api/v1/groups/join"
	memberGroupPath   = "/api/v1/groups/member/action"
	dissolveGroupPath = "/api/v1/groups/dissolve"
)

func provision(t *testing.T, f *fixture, server *process, user string) {
	t.Helper()
	reply := f.call(server, http.MethodPost, provisionPath, f.issuerKey, encode(t, map[string]string{"userId": user}), nil)
	requireStatus(t, reply, http.StatusOK)
	if reply.verified {
		t.Fatal("trusted provisioning must use the loopback app credential boundary")
	}
}

func createGroup(t *testing.T, f *fixture, gateway *process, token, id string, members []string, history bool) {
	t.Helper()
	wire := map[string]any{"groupId": id, "profile": map[string]string{"name": "Projection fixture"}, "joinMode": 2, "historyEnabled": history}
	if members != nil {
		wire["memberIds"] = members
	}
	got := groupResult[group.View](t, f, gateway, token, createGroupPath, "view", wire)
	if got.Group.GroupID != id || got.Membership.Role != group.Owner {
		t.Fatal("managed group source did not establish its owner")
	}
}

func join(t *testing.T, f *fixture, gateway *process, token, id, request string) {
	t.Helper()
	entry := groupResult[group.Entry](t, f, gateway, token, joinGroupPath, "request", group.JoinRequest{GroupID: id, RequestID: request, Kind: group.Join})
	if entry.State != group.Accepted {
		t.Fatal("group join source did not accept an open join")
	}
}

func leave(t *testing.T, f *fixture, gateway *process, token, id string) {
	t.Helper()
	member := groupResult[group.Membership](t, f, gateway, token, memberGroupPath, "membership", group.MemberRequest{GroupID: id, Action: group.Leave})
	if member.DepartedThroughSeq == nil {
		t.Fatal("group leave source did not store its ending sequence")
	}
}

func groupResult[T any](t *testing.T, f *fixture, gateway *process, token, path, field string, body any) T {
	t.Helper()
	reply := f.call(gateway, http.MethodPost, path, token, encode(t, body), nil)
	if reply.err != nil || reply.status != http.StatusOK || !reply.verified {
		t.Fatalf("group API %s status=%d TLS=%t err=%v", path, reply.status, reply.verified, reply.err)
	}
	var envelope map[string]json.RawMessage
	must(t, json.Unmarshal(reply.body, &envelope))
	if string(envelope["version"]) != "1" || len(envelope) != 2 || len(envelope[field]) == 0 || string(envelope[field]) == "null" {
		t.Fatal("group source operation did not return its versioned result")
	}
	var result T
	must(t, json.Unmarshal(envelope[field], &result))
	return result
}

func encode(t *testing.T, body any) []byte {
	t.Helper()
	raw, err := json.Marshal(body)
	must(t, err)
	return raw
}
