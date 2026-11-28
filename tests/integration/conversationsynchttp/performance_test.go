//go:build integration

package conversationsynchttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/buildinfo"
)

const (
	perfDevices  = 16
	perfBatch    = 4
	perfSends    = 48
	perfReaders  = 4
	perfPage     = 8
	perfTextSize = 64
	perfRoom     = "performance_baseline_room"
)

type performanceSamples struct {
	WindowNS    int64   `json:"window_ns"`
	LatenciesNS []int64 `json:"latencies_ns"`
}

func (s *performanceSamples) record(start time.Time) {
	s.LatenciesNS = append(s.LatenciesNS, time.Since(start).Nanoseconds())
}

type performanceResult struct {
	Devices                  int                           `json:"devices"`
	ConnectionBatch          int                           `json:"connection_batch"`
	MessageSends             int                           `json:"message_sends"`
	TextBytes                int                           `json:"text_bytes"`
	HistoryReaders           int                           `json:"history_readers"`
	PageLimit                int                           `json:"page_limit"`
	ActiveSubscriptions      int                           `json:"active_subscriptions"`
	ReconnectedSubscriptions int                           `json:"reconnected_subscriptions"`
	ReadMessages             int                           `json:"read_messages"`
	CommittedMessageWrites   int                           `json:"committed_message_writes"`
	CommittedOutboxWrites    int                           `json:"committed_outbox_writes"`
	ReplacementStartNS       int64                         `json:"replacement_start_ns"`
	GoVersion                string                        `json:"go_version"`
	RuntimePlatform          string                        `json:"runtime_platform"`
	RuntimeCPUs              int                           `json:"runtime_cpus"`
	GoMaxProcs               int                           `json:"go_max_procs"`
	ServerVersion            string                        `json:"server_version"`
	PostgresVersion          string                        `json:"postgres_version"`
	PostgresMaxConnections   int                           `json:"postgres_max_connections"`
	PostgresSyncCommit       string                        `json:"postgres_synchronous_commit"`
	PostgresFSync            string                        `json:"postgres_fsync"`
	PostgresFullPageWrites   string                        `json:"postgres_full_page_writes"`
	Samples                  map[string]performanceSamples `json:"samples"`
}

func TestPerformanceBaseline(t *testing.T) {
	f := openFixture(t)
	f.requireMigration(22)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	f.ctx = ctx
	keys := writeKeys(t, "baseline", "baseline")
	gatewaySettings := map[string]string{
		"NEWIM_CONVERSATION_READ_SYNC": "1", "NEWIM_CONVERSATION_ACCOUNT_SETTINGS": "1",
		"NEWIM_GATEWAY_METRICS_ADDR": "127.0.0.1:0",
	}
	server := f.server("1", nil)
	gateway := f.syncGateway(keys, gatewaySettings)
	out := performanceResult{
		Devices: perfDevices, ConnectionBatch: perfBatch, MessageSends: perfSends, TextBytes: perfTextSize,
		HistoryReaders: perfReaders, PageLimit: perfPage, Samples: make(map[string]performanceSamples),
		GoVersion: runtime.Version(), RuntimePlatform: runtime.GOOS + "/" + runtime.GOARCH,
		RuntimeCPUs: runtime.NumCPU(), GoMaxProcs: runtime.GOMAXPROCS(0), ServerVersion: buildinfo.ServerVersion,
	}
	must(t, f.db.QueryRow(ctx, `SELECT current_setting('server_version'),current_setting('max_connections')::int,
		current_setting('synchronous_commit'),current_setting('fsync'),current_setting('full_page_writes')`).
		Scan(&out.PostgresVersion, &out.PostgresMaxConnections, &out.PostgresSyncCommit, &out.PostgresFSync, &out.PostgresFullPageWrites))

	users, tokens := make([]string, perfDevices), make([]string, perfDevices)
	for i := range users {
		users[i] = fmt.Sprintf("baseline_user_%02d", i)
		provision(t, f, server, users[i])
	}
	login := performanceSamples{}
	window := time.Now()
	for i, user := range users {
		start := time.Now()
		tokens[i] = admitConnectionDevice(t, f, server, user, "phone", "phone").Token
		login.record(start)
	}
	login.WindowNS = time.Since(window).Nanoseconds()
	out.Samples["login"] = login

	createGroup(t, f, gateway, tokens[0], perfRoom, users[1:], true)
	text := strings.Repeat("a", perfTextSize)
	seed := send(t, f, gateway, tokens[0], perfRoom, "baseline_seed", text)
	streams, connections := openPerformanceSockets(t, f, gateway, tokens, "0", seed)
	out.Samples["connect"] = connections
	out.ActiveSubscriptions = requireActiveSubscriptions(t, f, gateway, perfDevices)

	sends := performanceSamples{}
	acks := make([]protocol.Ack, 0, perfSends+2)
	acks = append(acks, seed)
	window = time.Now()
	for i := range perfSends {
		start := time.Now()
		ack := send(t, f, gateway, tokens[0], perfRoom, fmt.Sprintf("baseline_message_%03d", i), text)
		sends.record(start)
		acks = append(acks, ack)
	}
	sends.WindowNS = time.Since(window).Nanoseconds()
	out.Samples["send"] = sends
	must(t, f.db.QueryRow(ctx, `SELECT count(*), count(o.server_msg_id)
		FROM newim.im_messages m LEFT JOIN newim.im_outbox_events o USING (server_msg_id)
		WHERE m.conversation_id=$1 AND m.conversation_seq>1`, perfRoom).
		Scan(&out.CommittedMessageWrites, &out.CommittedOutboxWrites))
	requireCommittedHistory(t, f, perfRoom, acks...)

	for _, stream := range streams {
		for _, ack := range acks[1:] {
			requireSocketMessage(t, f, stream, ack)
		}
	}
	out.Samples["sync_page"], out.ReadMessages = readPerformancePages(t, f, gateway, tokens[:perfReaders], acks)

	window = time.Now()
	must(t, gateway.cmd.Process.Kill())
	select {
	case <-gateway.done:
		if gateway.waitErr == nil {
			t.Fatal("gateway was not interrupted")
		}
	case <-ctx.Done():
		t.Fatal("gateway did not exit within fixture deadline")
	}
	replacement := f.syncGateway(keys, gatewaySettings)
	_ = requireActiveSubscriptions(t, f, replacement, 0)
	out.ReplacementStartNS = time.Since(window).Nanoseconds()
	for _, stream := range streams {
		_ = stream.CloseNow()
	}
	tail := send(t, f, replacement, tokens[0], perfRoom, "baseline_after_restart", text)
	_, reconnect := openPerformanceSockets(t, f, replacement, tokens, acks[len(acks)-1].ConversationSeq, tail)
	out.Samples["reconnect"] = reconnect
	out.ReconnectedSubscriptions = requireActiveSubscriptions(t, f, replacement, perfDevices)
	requireCommittedHistory(t, f, perfRoom, append(acks, tail)...)

	raw, err := json.Marshal(out)
	must(t, err)
	fmt.Printf("NEWIM_PERF_RAW %s\n", raw)
}

func openPerformanceSockets(t *testing.T, f *fixture, gateway *process, tokens []string, after string, first protocol.Ack) ([]*websocket.Conn, performanceSamples) {
	t.Helper()
	payload := encode(t, map[string]string{"conversationId": perfRoom, "afterSeq": after})
	streams := make([]*websocket.Conn, len(tokens))
	samples := performanceSamples{LatenciesNS: make([]int64, len(tokens))}
	failures := make([]error, len(tokens))
	window := time.Now()
	for start := 0; start < len(tokens); start += perfBatch {
		var cohort sync.WaitGroup
		for i := start; i < min(start+perfBatch, len(tokens)); i++ {
			cohort.Go(func() {
				begin := time.Now()
				ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
				defer cancel()
				stream, err := dialHistorySocket(ctx, f.client, gateway.url, tokens[i], payload)
				if err == nil {
					streams[i] = stream
					err = checkPerformanceSocket(ctx, stream, first)
				}
				failures[i] = err
				samples.LatenciesNS[i] = time.Since(begin).Nanoseconds()
			})
		}
		cohort.Wait()
	}
	samples.WindowNS = time.Since(window).Nanoseconds()
	for _, stream := range streams {
		if stream != nil {
			t.Cleanup(func() { _ = stream.CloseNow() })
		}
	}
	for i, err := range failures {
		if err != nil {
			t.Fatalf("socket sample %d failed: %v", i+1, err)
		}
	}
	return streams, samples
}

func checkPerformanceSocket(ctx context.Context, stream *websocket.Conn, want protocol.Ack) error {
	kind, raw, err := stream.Read(ctx)
	if err != nil {
		return err
	}
	frame, err := protocol.DecodeServerFrame(raw)
	if err != nil {
		return err
	}
	if kind != websocket.MessageText || frame.Message == nil || frame.Message.ServerMsgID != want.ServerMsgID ||
		frame.Message.ConversationSeq != want.ConversationSeq || frame.Message.ConversationID != perfRoom {
		return fmt.Errorf("socket sample did not begin at the expected sequence %s", want.ConversationSeq)
	}
	return nil
}

func requireActiveSubscriptions(t *testing.T, f *fixture, gateway *process, expected int) int {
	t.Helper()
	metrics := readGatewayMetrics(t, f, gateway)
	for line := range bytes.SplitSeq(metrics, []byte("\n")) {
		if value, ok := bytes.CutPrefix(line, []byte("newim_gateway_active_subscriptions ")); ok {
			actual, err := strconv.Atoi(string(value))
			must(t, err)
			if actual != expected {
				t.Fatalf("active subscriptions=%d want %d", actual, expected)
			}
			return actual
		}
	}
	t.Fatal("active subscriptions sample is missing")
	return 0
}

func readPerformancePages(t *testing.T, f *fixture, gateway *process, tokens []string, acks []protocol.Ack) (performanceSamples, int) {
	t.Helper()
	samples := performanceSamples{}
	var read int
	window := time.Now()
	for _, token := range tokens {
		for next := 0; next < len(acks); {
			start := time.Now()
			body := encode(t, map[string]any{"conversationId": perfRoom, "afterSeq": strconv.Itoa(next), "limit": perfPage})
			reply := f.call(gateway, http.MethodPost, "/api/v1/sync/messages", token, body, nil)
			requireStatus(t, reply, http.StatusOK)
			if !reply.verified {
				t.Fatal("page measurement requires verified TLS")
			}
			var page struct {
				Latest string            `json:"latestSeq"`
				More   bool              `json:"hasNext"`
				Next   *string           `json:"nextAfterSeq"`
				Items  []json.RawMessage `json:"items"`
			}
			must(t, json.Unmarshal(reply.body, &page))
			end := min(next+perfPage, len(acks))
			if len(page.Items) != end-next || page.Latest != strconv.Itoa(len(acks)) || page.More != (end < len(acks)) ||
				page.Next == nil || *page.Next != strconv.Itoa(end) {
				t.Fatalf("message page did not advance to sequence %d", end)
			}
			for i, raw := range page.Items {
				frame, err := protocol.DecodeServerFrame(raw)
				must(t, err)
				want := acks[next+i]
				if frame.Message == nil || frame.Message.ServerMsgID != want.ServerMsgID ||
					frame.Message.ConversationSeq != want.ConversationSeq || frame.Message.ConversationID != perfRoom {
					t.Fatalf("page sample omitted committed sequence %s", want.ConversationSeq)
				}
			}
			samples.record(start)
			read += len(page.Items)
			next = end
		}
	}
	samples.WindowNS = time.Since(window).Nanoseconds()
	return samples, read
}
