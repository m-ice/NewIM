package messagehttp

import "testing"

func TestDecodePeerRequiresOnePeerField(t *testing.T) {
	for _, invalid := range []string{
		`null`,
		`{}`,
		`{"peerUserId":null}`,
		`{"peerUserId":""}`,
		`{"peerUserId":123}`,
		`{"peerUserId":"one","peerUserId":"two"}`,
		`{"peerUserId":"one","actorUserId":"two"}`,
		`{"peerUserId":"one"}{}`,
	} {
		if _, ok := decodePeer([]byte(invalid)); ok {
			t.Errorf("accepted invalid peer request %s", invalid)
		}
	}
	if peer, ok := decodePeer([]byte(`{"peerUserId":"recipient_1"}`)); !ok || peer != "recipient_1" {
		t.Fatalf("valid peer request returned %q, %v", peer, ok)
	}
}
