//go:build integration

package mediahttp_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/group"
)

type grantRequest struct {
	ConversationID string `json:"conversationId"`
	Kind           string `json:"kind"`
	ContentType    string `json:"contentType"`
	Size           string `json:"size"`
	SHA256         string `json:"sha256"`
}

type uploadGrant struct {
	UploadToken string    `json:"uploadToken"`
	MediaKey    string    `json:"mediaKey"`
	ExpiresAt   time.Time `json:"expiresAt"`
}

func TestMediaHTTPCheck(t *testing.T) {
	t.Run("direct-and-group-transfer", testConversationTransfer)
	t.Run("streaming-size-and-deadline", testStreamingSize)
}

func testConversationTransfer(t *testing.T) {
	f := openFixture(t)
	root := filepath.Join(t.TempDir(), "objects")
	left, right := f.gateway(root, nil), f.gateway(root, nil)
	owner := f.newAccount("media_check_owner")
	peer := f.newAccount("media_check_peer")
	direct := f.direct(left, owner, "media_check_peer")
	const groupID = "media_check_group"
	f.post(left, "/api/v1/groups/create", owner, map[string]any{
		"groupId": groupID, "profile": map[string]string{"name": "Media transfer fixture"},
		"memberIds": []string{"media_check_peer"}, "joinMode": group.OpenJoining,
	}).require(t, http.StatusOK)
	for _, id := range []string{direct, groupID} {
		content := []byte("synthetic private media \x00\xff for " + id)
		req := intentForBytes(id, content)
		grant := f.grant(left, owner, req)
		metadata := f.completed(right, owner, grant, req, bytes.NewReader(content), int64(len(content)))
		replay := f.completed(left, owner, grant, req, bytes.NewReader(content), int64(len(content)))
		if metadata != replay {
			t.Fatal("shared object root retry changed committed metadata")
		}
		f.requireDownload(left, peer, metadata, 0)
		raw, err := protocol.EncodeMediaPayload(metadata)
		must(t, err)
		socket := f.subscribe(right, peer, id)
		a := ack(t, f.message(left, owner, id, "media_"+metadata.MediaKey, "media", raw))
		online := f.socketFrame(socket)
		page := f.delta(left, peer, id, "0")
		if len(page) != 1 || !bytes.Equal(page[0], online) {
			t.Fatal("HTTP delta did not return the same media frame as WSS")
		}
		requireMediaFrame(t, online, a.ServerMsgID, metadata)
		must(t, socket.CloseNow())
		left.requireRedacted(t, owner, peer, grant.UploadToken, metadata.SHA256, root, string(content))
		right.requireRedacted(t, owner, peer, grant.UploadToken, metadata.SHA256, root, string(content))
	}
}

func intentForBytes(conversation string, content []byte) grantRequest {
	hash := sha256.Sum256(content)
	return grantRequest{ConversationID: conversation, Kind: "file", ContentType: "text/html",
		Size: strconv.Itoa(len(content)), SHA256: hex.EncodeToString(hash[:])}
}

func (f *fixture) grant(g *gateway, token string, request grantRequest) uploadGrant {
	f.t.Helper()
	var result uploadGrant
	decode(f.t, f.post(g, grantPath, token, request), &result)
	if !protocol.ValidMediaKey(result.MediaKey) || result.UploadToken == "" || !result.ExpiresAt.After(time.Now()) {
		f.t.Fatal("media grant omitted the key, credential or expiry")
	}
	return result
}

func (f *fixture) uploadRequest(g *gateway, token string, grant uploadGrant, content io.Reader, length int64) *http.Request {
	f.t.Helper()
	req := f.request(g, completePath, token, content, objectType)
	req.Header.Set(uploadHeader, grant.UploadToken)
	req.ContentLength = length
	return req
}

func (f *fixture) completed(g *gateway, token string, grant uploadGrant, want grantRequest, content io.Reader, length int64) protocol.MediaMetadata {
	f.t.Helper()
	reply := f.execute(f.uploadRequest(g, token, grant, content, length))
	reply.require(f.t, http.StatusOK)
	got, err := protocol.DecodeMediaPayload(reply.body)
	must(f.t, err)
	if got.MediaKey != grant.MediaKey || got.Kind != want.Kind || got.ContentType != want.ContentType ||
		got.Size != want.Size || got.SHA256 != want.SHA256 {
		f.t.Fatal("upload completion did not return the exact published metadata")
	}
	return got
}

func (f *fixture) requireDownload(g *gateway, token string, want protocol.MediaMetadata, pause time.Duration) {
	f.t.Helper()
	body := encode(f.t, map[string]string{"mediaKey": want.MediaKey})
	res, err := f.client.Do(f.request(g, downloadPath, token, bytes.NewReader(body), jsonType))
	must(f.t, err)
	defer res.Body.Close()
	reply{status: res.StatusCode, header: res.Header}.require(f.t, http.StatusOK)
	if res.TLS == nil || len(res.TLS.VerifiedChains) == 0 {
		f.t.Fatal("object bytes did not use verified TLS")
	}
	if res.Header.Get("Content-Type") != objectType || res.Header.Get("Content-Disposition") != "attachment" ||
		strconv.FormatInt(res.ContentLength, 10) != want.Size {
		f.t.Fatal("download omitted attachment headers or announced the wrong size")
	}
	hash := sha256.New()
	if pause > 0 {
		_, err = io.CopyN(hash, res.Body, 1)
		must(f.t, err)
		time.Sleep(pause)
	}
	n, err := io.Copy(hash, res.Body)
	must(f.t, err)
	if pause > 0 {
		n++
	}
	if strconv.FormatInt(n, 10) != want.Size || hex.EncodeToString(hash.Sum(nil)) != want.SHA256 {
		f.t.Fatal("download bytes did not match committed size and SHA-256")
	}
}

func (f *fixture) subscribe(g *gateway, token, id string) *websocket.Conn {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	s, response, err := websocket.Dial(ctx, "wss://"+strings.TrimPrefix(g.url, "https://")+"/api/v1/gateway",
		&websocket.DialOptions{HTTPClient: f.client, HTTPHeader: http.Header{"Authorization": {"Bearer " + token}},
			Subprotocols: []string{"newim.v1"}})
	must(f.t, err)
	f.t.Cleanup(func() { _ = s.CloseNow() })
	if s.Subprotocol() != "newim.v1" || response.TLS == nil || len(response.TLS.VerifiedChains) == 0 {
		f.t.Fatal("gateway did not negotiate newim.v1 over verified TLS")
	}
	s.SetReadLimit(protocol.MaxFrameBytes)
	must(f.t, s.Write(ctx, websocket.MessageText, encode(f.t, map[string]string{"conversationId": id, "afterSeq": "0"})))
	return s
}

func (f *fixture) socketFrame(s *websocket.Conn) []byte {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 8*time.Second)
	defer cancel()
	kind, raw, err := s.Read(ctx)
	must(f.t, err)
	if kind != websocket.MessageText {
		f.t.Fatal("native media delivery did not use a text frame")
	}
	return raw
}

func (f *fixture) delta(g *gateway, token, id, after string) []json.RawMessage {
	f.t.Helper()
	var page struct {
		HistoryAfterSeq string            `json:"historyAfterSeq"`
		LatestSeq       string            `json:"latestSeq"`
		Items           []json.RawMessage `json:"items"`
		HasNext         bool              `json:"hasNext"`
		NextAfterSeq    *string           `json:"nextAfterSeq"`
	}
	decode(f.t, f.post(g, deltaPath, token, map[string]string{"conversationId": id, "afterSeq": after}), &page)
	if page.HistoryAfterSeq != "0" || page.LatestSeq != "1" || page.HasNext || page.Items == nil {
		f.t.Fatal("unexpected delta page for a conversation containing one committed media message")
	}
	if (len(page.Items) == 1 && (page.NextAfterSeq == nil || *page.NextAfterSeq != "1")) ||
		(len(page.Items) == 0 && page.NextAfterSeq != nil) {
		f.t.Fatal("delta page exposed an inconsistent resume sequence")
	}
	return page.Items
}

func requireMediaFrame(t *testing.T, raw []byte, msgID string, want protocol.MediaMetadata) {
	t.Helper()
	frame, err := protocol.DecodeServerFrame(raw)
	must(t, err)
	if frame.Message == nil || frame.Message.Type != "media" || frame.Message.Version != 1 ||
		frame.Message.ServerMsgID != msgID || frame.Message.ConversationSeq != "1" {
		t.Fatal("persisted media frame did not match its acknowledgement")
	}
	got, err := protocol.DecodeMediaPayload(frame.Message.Payload)
	must(t, err)
	if got != want {
		t.Fatal("persisted media metadata changed during delivery")
	}
}

type zeroes struct{}

func (zeroes) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

type pauseOnce struct{ delay time.Duration }

func (p *pauseOnce) Read([]byte) (int, error) {
	delay := p.delay
	p.delay = 0
	time.Sleep(delay)
	return 0, io.EOF
}

func testStreamingSize(t *testing.T) {
	f := openFixture(t)
	g := f.gateway(filepath.Join(t.TempDir(), "objects"), nil)
	owner := f.newAccount("media_stream_owner")
	peer := f.newAccount("media_stream_peer")
	id := f.direct(g, owner, "media_stream_peer")
	hash := sha256.New()
	size := protocol.MaxMediaSize
	_, err := io.CopyN(hash, zeroes{}, size)
	must(t, err)
	intent := grantRequest{ConversationID: id, Kind: "file", ContentType: objectType,
		Size: strconv.FormatInt(size, 10), SHA256: hex.EncodeToString(hash.Sum(nil))}
	grant := f.grant(g, owner, intent)
	// Send an initial part, then cross the normal 5-second request read deadline.
	const initial = 64 * 1024
	stream := io.MultiReader(io.LimitReader(zeroes{}, initial), &pauseOnce{delay: 6 * time.Second},
		io.LimitReader(zeroes{}, size-initial))
	metadata := f.completed(g, owner, grant, intent, stream, size)
	// The client leaves most of the 100 MiB unread across the normal write deadline.
	f.requireDownload(g, peer, metadata, 11*time.Second)

	over := f.grant(g, owner, intent)
	known := f.uploadRequest(g, owner, over, io.LimitReader(zeroes{}, size+1), size+1)
	known.Header.Set("Expect", "100-continue")
	f.execute(known).requireError(t, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
	chunked := f.uploadRequest(g, owner, over, io.LimitReader(zeroes{}, size+1), -1)
	f.execute(chunked).requireError(t, http.StatusRequestEntityTooLarge, "HTTP_BODY_TOO_LARGE")
	f.post(g, downloadPath, owner, map[string]string{"mediaKey": over.MediaKey}).requireError(t, http.StatusNotFound, "MEDIA_NOT_FOUND")
}
