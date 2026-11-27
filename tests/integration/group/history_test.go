//go:build integration

package group_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"path/filepath"
	"slices"
	"testing"
	"time"

	auth "github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/group"
	mediaapp "github.com/m-ice/NewIM/server/media"
	"github.com/m-ice/NewIM/server/media/localfs"
	mediastore "github.com/m-ice/NewIM/server/storage/media"
	messagestore "github.com/m-ice/NewIM/server/storage/messagesync"
	messageapp "github.com/m-ice/NewIM/server/sync/message"
)

func testHistoryAndMedia(t *testing.T) {
	f := openFixture(t)
	const id = "grp_history_window"
	const ownerID = "grp_history_owner"
	const earlyID = "grp_history_early"
	const lateID = "grp_history_late"
	owner := f.admit(ownerID, "mobile")
	early := f.admit(earlyID, "mobile")
	late := f.admit(lateID, "mobile")
	first, second := f.gateway("1", nil), f.gateway("1", nil)
	post[group.View](t, f, first, owner, createGroupPath, "view", map[string]any{
		"groupId": id, "profile": map[string]string{"name": "Window fixture"}, "memberIds": []string{earlyID},
		"historyEnabled": false, "joinMode": 2,
	})
	reader := openMessageReader(t, f)
	earlyIdentity := trustedIdentity(t, f, early)
	lateIdentity := trustedIdentity(t, f, late)
	ownerIdentity := trustedIdentity(t, f, owner)
	media, signer := openMedia(t, f)
	upload := createMediaAsset(t, f, media, earlyIdentity, id)
	if download, err := media.ResolvePrivateDownload(f.ctx, earlyIdentity, upload, 0); err != nil || download == "" || signer.calls != 1 {
		t.Fatal("current member could not obtain a valid media download grant")
	}

	send(t, f, first, owner, id, "before_join", "message before joining")
	f.sql("INSERT INTO newim.im_read_states(conversation_id,user_id,read_seq) VALUES($1,$2,1)", id, earlyID)
	join(t, f, first, late, id, "first_join", group.Join, lateID)
	got := getGroup(t, f, second, late, id)
	if got.Membership.HistoryAfterSeq != 1 {
		t.Fatal("disabled history did not capture the committed head at join")
	}
	expectWindow(t, f, reader, lateIdentity, id, 0, 1)
	const visibleText = "message inside both memberships"
	visible := send(t, f, first, owner, id, "within_window", visibleText)
	stream := subscribe(t, f, second, late, id, 0)
	requireDelivered(t, f, stream, visible, visibleText)
	must(t, stream.CloseNow())

	earlyCursor := changes(t, f, second, early, 0, 100).LatestRevision
	lateCursor := changes(t, f, second, late, 0, 100).LatestRevision
	leave := changeMember(t, f, second, early, id, "", group.Leave, 0)
	remove := changeMember(t, f, first, owner, id, lateID, group.Remove, 0)
	if leave.DepartedThroughSeq == nil || *leave.DepartedThroughSeq != 2 ||
		remove.DepartedThroughSeq == nil || *remove.DepartedThroughSeq != 2 || remove.HistoryAfterSeq != 1 {
		t.Fatal("leaving and removing did not preserve the allowed committed sequence range")
	}
	expectRemovedChange(t, f, second, early, id, earlyCursor)
	expectRemovedChange(t, f, first, late, id, lateCursor)
	send(t, f, first, owner, id, "after_departure", "message after both departures")
	expectWindow(t, f, reader, earlyIdentity, id, 0, 2, 1, 2)
	expectWindow(t, f, reader, lateIdentity, id, 0, 2, 2)
	expectWindow(t, f, reader, lateIdentity, id, 2, 2)
	if _, err := reader.ReadAfterSeq(f.ctx, lateIdentity, messageapp.ReadRequest{ConversationID: id, HasAfterSeq: true, AfterSeq: 3}); messageapp.ErrorCode(err) != messageapp.InvalidCursor {
		t.Fatalf("departed member cursor above authorized head was not denied: %v", err)
	}
	past := subscribe(t, f, second, late, id, 0)
	requireDelivered(t, f, past, visible, visibleText)
	ctx, cancel := context.WithTimeout(f.ctx, 800*time.Millisecond)
	defer cancel()
	if _, _, err := past.Read(ctx); ctx.Err() != context.DeadlineExceeded || err == nil {
		t.Fatalf("departed WSS stream must wait at the authorized head, read error=%v", err)
	}

	f.call(second, http.MethodPost, sendPath, late, textBody(t, "departed_client", id, "must not commit"), nil).requireError(t, 403, "SEND_UNAUTHORIZED")
	f.call(second, http.MethodPost, getGroupPath, late, encode(t, map[string]string{"groupId": id}), nil).requireError(t, 403, "GROUP_FORBIDDEN")
	if download, err := media.ResolvePrivateDownload(f.ctx, earlyIdentity, upload, 0); mediaapp.ErrorCode(err) != mediaapp.MediaUnauthorized ||
		download != "" || signer.calls != 1 {
		t.Fatal("departed member obtained a media link or called the signer")
	}
	if _, err := media.BeginUpload(f.ctx, earlyIdentity, uploadRequest(id, []byte("new fixture"))); mediaapp.ErrorCode(err) != mediaapp.MediaUnauthorized {
		t.Fatalf("departed member obtained a new media upload grant: %v", err)
	}
	requireReferences(t, f, id, earlyID, upload)
	requireMessageRows(t, f, id, 3)

	join(t, f, second, late, id, "second_join", group.Join, "")
	reentered := getGroup(t, f, first, late, id).Membership
	if reentered.HistoryAfterSeq != 3 || reentered.DepartedThroughSeq != nil {
		t.Fatal("rejoining did not clear the previous ending bound and advance the history start")
	}
	expectWindow(t, f, reader, lateIdentity, id, 0, 3)
	send(t, f, first, owner, id, "after_reentry", "message after re-entry")
	expectWindow(t, f, reader, lateIdentity, id, 0, 4, 4)
	changeMember(t, f, second, late, id, "", group.Leave, 0)
	enabled := true
	post[group.View](t, f, first, owner, updateGroupPath, "view", group.UpdateRequest{GroupID: id, HistoryEnabled: &enabled})
	send(t, f, first, owner, id, "after_enabling_history", "history after enabling the policy")
	join(t, f, second, late, id, "history_enabled_rejoin", group.Join, "")
	if got := getGroup(t, f, first, late, id).Membership; got.HistoryAfterSeq != 3 || got.DepartedThroughSeq != nil {
		t.Fatal("rejoining with enabled history lowered the existing start or retained the prior end")
	}
	expectWindow(t, f, reader, lateIdentity, id, 0, 5, 4, 5)

	ownerCursor := changes(t, f, first, owner, 0, 100).LatestRevision
	lateCursor = changes(t, f, second, late, 0, 100).LatestRevision
	body := map[string]string{"groupId": id}
	if !post[bool](t, f, first, owner, dissolveGroupPath, "dissolved", body) ||
		!post[bool](t, f, second, owner, dissolveGroupPath, "dissolved", body) {
		t.Fatal("dissolve or its retry did not return the terminal state")
	}
	expectRemovedChange(t, f, first, owner, id, ownerCursor)
	expectRemovedChange(t, f, second, late, id, lateCursor)
	expectWindow(t, f, reader, ownerIdentity, id, 0, 5, 1, 2, 3, 4, 5)
	expectWindow(t, f, reader, earlyIdentity, id, 0, 2, 1, 2)
	expectWindow(t, f, reader, lateIdentity, id, 0, 5, 4, 5)
	f.call(first, http.MethodPost, sendPath, owner, textBody(t, "after_dissolve", id, "must not commit"), nil).
		requireError(t, 403, "SEND_UNAUTHORIZED")
	requireMessageRows(t, f, id, 5)
	requireReferences(t, f, id, earlyID, upload)
	testZeroDeparture(t, f, first, reader, owner, early, earlyID, earlyIdentity)
}

func testZeroDeparture(t *testing.T, f *fixture, gateway *process, reader *messageapp.Service, owner, member, memberID string, identity auth.ConnectionIdentity) {
	t.Helper()
	const id = "grp_history_departed_at_zero"
	post[group.View](t, f, gateway, owner, createGroupPath, "view", map[string]any{
		"groupId": id, "profile": map[string]string{"name": "Zero sequence fixture"}, "memberIds": []string{memberID},
	})
	left := changeMember(t, f, gateway, member, id, memberID, group.Leave, 0)
	if left.DepartedThroughSeq == nil || *left.DepartedThroughSeq != 0 {
		t.Fatal("empty group departure must retain a present end sequence of zero")
	}
	send(t, f, gateway, owner, id, "after_empty_departure", "only current members may read")
	expectWindow(t, f, reader, identity, id, 0, 0)
}

func expectRemovedChange(t *testing.T, f *fixture, gateway *process, raw, id string, after int64) {
	t.Helper()
	page := changes(t, f, gateway, raw, after, 100)
	if !slices.ContainsFunc(page.Items, func(change group.Change) bool { return change.GroupID == id && change.Kind == group.GroupRemoved }) {
		t.Fatal("departing member did not receive its own removal event")
	}
}

func trustedIdentity(t *testing.T, f *fixture, token string) auth.ConnectionIdentity {
	t.Helper()
	bearer, err := f.auth.AuthenticateBearer(f.ctx, token)
	must(t, err)
	id, err := auth.NewConnectionIdentity(bearer.UserID(), bearer.DeviceID(), bearer.SessionID(), bearer.TokenID(), bearer.TokenID())
	must(t, err)
	return id
}

func openMessageReader(t *testing.T, f *fixture) *messageapp.Service {
	t.Helper()
	store, err := messagestore.Open(f.ctx, messagestore.Config{DSN: testDSN, AllowLocalSocket: true, MaxConnections: 2, ApplicationName: "newim-group-delta-fixture"})
	must(t, err)
	t.Cleanup(store.Close)
	reader, err := messageapp.NewService(store, messageapp.Config{})
	must(t, err)
	return reader
}

func expectWindow(t *testing.T, f *fixture, reader *messageapp.Service, identity auth.ConnectionIdentity, id string, after, head int64, want ...int64) {
	t.Helper()
	page, err := reader.ReadAfterSeq(f.ctx, identity, messageapp.ReadRequest{ConversationID: id, HasAfterSeq: true, AfterSeq: after})
	must(t, err)
	if page.LatestSeq != head || page.HasNext || len(page.Items) != len(want) {
		t.Fatalf("message history window head=%d count=%d more=%t want head=%d count=%d", page.LatestSeq, len(page.Items), page.HasNext, head, len(want))
	}
	for i, seq := range want {
		if page.Items[i].ConversationSeq != seq || page.Items[i].ConversationID != id {
			t.Fatal("message history exposed a sequence outside the requested authorized window")
		}
	}
}

type fixtureSigner struct{ calls int }

func (s *fixtureSigner) Sign(context.Context, string, time.Time) (string, error) {
	s.calls++
	return "https://private.example.test/fixture", nil
}

func openMedia(t *testing.T, f *fixture) (*mediaapp.Service, *fixtureSigner) {
	t.Helper()
	store, err := mediastore.Open(f.ctx, mediastore.Config{DSN: testDSN, AllowLocalSocket: true, MaxConnections: 2})
	must(t, err)
	t.Cleanup(store.Close)
	objects, err := localfs.Open(filepath.Join(t.TempDir(), "objects"))
	must(t, err)
	t.Cleanup(func() { _ = objects.Close() })
	signer := &fixtureSigner{}
	service, err := mediaapp.NewService(store, mediaapp.Config{Objects: objects, Signer: signer, Clock: mediaapp.ClockFunc(time.Now)})
	must(t, err)
	return service, signer
}

func uploadRequest(id string, body []byte) mediaapp.BeginUploadRequest {
	return mediaapp.BeginUploadRequest{ConversationID: id, Kind: "image", ContentType: "image/jpeg", Size: int64(len(body)),
		SHA256: fmt.Sprintf("%x", sha256.Sum256(body)), Expiry: time.Minute}
}

func createMediaAsset(t *testing.T, f *fixture, service *mediaapp.Service, who auth.ConnectionIdentity, id string) string {
	t.Helper()
	body := []byte("owned media fixture")
	grant, err := service.BeginUpload(f.ctx, who, uploadRequest(id, body))
	must(t, err)
	asset, err := service.CompleteUpload(f.ctx, who, grant.RawToken, bytes.NewReader(body))
	must(t, err)
	return asset.MediaKey
}

func requireReferences(t *testing.T, f *fixture, id, user, asset string) {
	t.Helper()
	var kept bool
	must(t, f.db.QueryRow(f.ctx, `SELECT EXISTS (
		SELECT 1 FROM newim.im_read_states r
		JOIN newim.im_media_assets a ON a.conversation_id=r.conversation_id AND a.owner_user_id=r.user_id
		WHERE r.conversation_id=$1 AND r.user_id=$2 AND r.read_seq=1 AND a.media_key=$3 AND a.state='ready'
	)`, id, user, asset).Scan(&kept))
	if !kept {
		t.Fatal("existing read state and ready media reference were not retained")
	}
}
