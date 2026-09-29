//go:build integration

package messagehttp_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	auth "github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/conversation"
	message "github.com/m-ice/NewIM/server/message"
	authstore "github.com/m-ice/NewIM/server/storage/authsession"
	messagestore "github.com/m-ice/NewIM/server/storage/message"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
)

func identitySupplements(t *testing.T, migration string) {
	t.Run("same-postmaster-promotion-checkpoint-lag", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		primary := newIdentityCluster(t, "inplace-primary", migration)
		f := openFixtureDSN(t, primary.dsn(), identityName)
		identity := f.seedIdentity("inplace", seedOptions{})
		conversationID := f.seedConversation("inplace", identity.UserID)
		standby := newIdentityStandby(t, primary)
		before := standby.scalar("SELECT pg_postmaster_start_time()::text || '/' || (pg_control_system()).system_identifier::text")
		checkpointBefore := standby.scalar("SELECT (pg_control_checkpoint()).timeline_id")
		conn, err := pgx.Connect(ctx, standby.dsn())
		must(t, err)
		defer conn.Close(context.Background())
		guard := postgresidentity.NewGuard(nil)
		if err = guard.Verify(ctx, conn); err == nil || guard.Tripped() == nil {
			t.Fatal("real standby guard did not become terminal")
		}
		assertIdentityStartupRejected(t, standby, clusterMessageEnv(standby))

		// Only this owned cluster's idle checkpointer is paused. PostgreSQL promotion
		// accepts queries before its requested checkpoint; preserve that real window.
		pid, err := strconv.Atoi(standby.scalar("SELECT pid FROM pg_stat_activity WHERE backend_type='checkpointer' AND wait_event='CheckpointerMain'"))
		must(t, err)
		process, err := os.FindProcess(pid)
		must(t, err)
		must(t, process.Signal(syscall.SIGSTOP))
		resumed := false
		resume := func() {
			if !resumed {
				must(t, process.Signal(syscall.SIGCONT))
				resumed = true
			}
		}
		defer resume()
		standby.promote()
		after := standby.scalar("SELECT pg_postmaster_start_time()::text || '/' || (pg_control_system()).system_identifier::text")
		current := standby.scalar("SELECT (pg_split_walfile_name(pg_walfile_name(pg_current_wal_insert_lsn()))).timeline_id")
		checkpoint := standby.scalar("SELECT (pg_control_checkpoint()).timeline_id")
		if before != after || standby.scalar("SELECT pg_is_in_recovery()") != "f" || checkpoint != checkpointBefore || current == checkpoint {
			t.Fatalf("promotion did not isolate real checkpoint lag: samePostmaster=%v current=%s checkpoint=%s before=%s", before == after, current, checkpoint, checkpointBefore)
		}
		t.Logf("same postmaster promotion: current WAL timeline=%s checkpoint timeline=%s", current, checkpoint)
		if guard.Verify(ctx, conn) == nil {
			t.Fatal("previously terminal standby guard recovered without restart")
		}
		fresh := postgresidentity.NewGuard(nil)
		must(t, fresh.Verify(ctx, conn))
		// A process starts against the actual lag interval, persists, and survives
		// checkpoint catch-up. A checkpoint-based guard would spuriously trip later.
		server := startMessageServer(t, clusterMessageEnv(standby))
		server.waitHealthy(t)
		assertSendSucceeds(t, server, identity, conversationID, "inplace_lag")
		resume()
		standby.scalar("CHECKPOINT")
		if got := standby.scalar("SELECT (pg_control_checkpoint()).timeline_id"); got != current {
			t.Fatalf("checkpoint did not catch up: %s", got)
		}
		must(t, fresh.Verify(ctx, conn))
		assertSendSucceeds(t, server, identity, conversationID, "inplace_caught_up")
		server.stop(t)
		assertIdentityPoolsEmpty(t, standby)
	})

	t.Run("restricted-role-function-permissions", func(t *testing.T) {
		c := newIdentityCluster(t, "permissions", migration)
		c.scalar("CREATE ROLE identity_reader LOGIN; GRANT USAGE ON SCHEMA newim TO identity_reader; GRANT SELECT ON ALL TABLES IN SCHEMA newim TO identity_reader")
		functions := []string{"pg_control_system()", "pg_postmaster_start_time()", "pg_is_in_recovery()", "pg_current_wal_insert_lsn()", "pg_walfile_name(pg_lsn)", "pg_split_walfile_name(text)"}
		for _, fn := range functions {
			c.scalar("GRANT EXECUTE ON FUNCTION pg_catalog." + fn + " TO identity_reader")
		}
		env := clusterMessageEnv(c)
		env["NEWIM_AUTH_DSN"] = strings.Replace(c.dsn(), "user=newim_test", "user=identity_reader", 1)
		positive := startMessageServer(t, env)
		positive.waitHealthy(t)
		positive.stop(t)
		assertIdentityPoolsEmpty(t, c)
		for _, fn := range functions {
			t.Run(fn, func(t *testing.T) {
				// ACL changes are confined to this disposable cluster. PUBLIC defaults
				// must also be revoked to create a real denial for the restricted role.
				c.scalar("REVOKE EXECUTE ON FUNCTION pg_catalog." + fn + " FROM PUBLIC, identity_reader")
				defer c.scalar("GRANT EXECUTE ON FUNCTION pg_catalog." + fn + " TO identity_reader")
				if got := c.scalar("SELECT has_function_privilege('identity_reader','pg_catalog." + fn + "','EXECUTE')"); got != "f" {
					t.Fatal("fixture did not revoke EXECUTE")
				}
				assertIdentityStartupRejected(t, c, env)
			})
		}
	})

	t.Run("shared-guard-divergent-databases-old-pools", func(t *testing.T) {
		c := newIdentityCluster(t, "divergent", migration)
		c.run("createdb", "-h", c.sock, "-p", strconv.Itoa(c.port), "-U", "newim_test", "identity_other")
		otherDSN := strings.Replace(c.dsn(), "dbname="+identityName, "dbname=identity_other", 1)
		if got := c.scalar("SELECT count(DISTINCT oid) FROM pg_database WHERE datname IN ('" + identityName + "','identity_other')"); got != "2" {
			t.Fatal("fixture requires distinct real database OIDs")
		}
		f := openFixtureDSN(t, c.dsn(), identityName)
		identity := f.seedIdentity("old_pool", seedOptions{})
		conversationID := f.seedConversation("old_pool", identity.UserID)
		for _, first := range []string{"auth", "message"} {
			t.Run(first, func(t *testing.T) {
				guard := postgresidentity.NewGuard(nil)
				var closeFirst func()
				var exercise func() error
				appName := "identity_old_" + first
				if first == "auth" {
					repo, err := authstore.Open(ctx, authstore.Config{DSN: c.dsn(), AllowLocalSocket: true, MaxConnections: 1, ApplicationName: appName, Guard: guard})
					must(t, err)
					closeFirst = repo.Close
					exercise = func() error { _, err := repo.LookupSession(ctx, identity.SessionID); return err }
				} else {
					repo, err := messagestore.Open(ctx, messagestore.Config{DSN: c.dsn(), AllowLocalSocket: true, MaxConnections: 1, ApplicationName: appName, Guard: guard})
					must(t, err)
					closeFirst = repo.Close
					request, err := protocol.DecodeSend(textBody(t, "old_pool_send", conversationID, "persisted once"))
					must(t, err)
					exercise = func() error {
						_, err := repo.Persist(ctx, conversation.Principal{UserID: identity.UserID}, request, func() error { return nil }, func() (message.Generated, error) {
							return message.Generated{ServerMsgID: "old_pool_server", EventID: "old_pool_event", ServerTime: 1}, nil
						})
						return err
					}
				}
				defer closeFirst()
				must(t, exercise())
				pid := c.scalar("SELECT pid FROM pg_stat_activity WHERE application_name='" + appName + "'")
				if pid == "" {
					t.Fatal("no warmed pool backend")
				}
				before := c.scalar("SELECT pg_postmaster_start_time()::text")
				if first == "auth" {
					r, err := messagestore.Open(ctx, messagestore.Config{DSN: otherDSN, AllowLocalSocket: true, ApplicationName: "identity_failed", Guard: guard})
					if r != nil {
						r.Close()
					}
					if err == nil {
						t.Fatal("mismatched message database accepted")
					}
				} else {
					r, err := authstore.Open(ctx, authstore.Config{DSN: otherDSN, AllowLocalSocket: true, ApplicationName: "identity_failed", Guard: guard})
					if r != nil {
						r.Close()
					}
					if err == nil {
						t.Fatal("mismatched auth database accepted")
					}
				}
				if guard.Tripped() == nil || c.scalar("SELECT pg_postmaster_start_time()::text") != before {
					t.Fatal("mismatch did not isolate database identity")
				}
				if got := c.scalar("SELECT count(*) FROM pg_stat_activity WHERE pid=" + pid + " AND application_name='" + appName + "'"); got != "1" {
					t.Fatal("old backend did not survive divergence")
				}
				beforeRows := c.scalar("SELECT (SELECT count(*) FROM newim.im_messages)::text || '/' || (SELECT count(*) FROM newim.im_outbox_events)::text || '/' || (SELECT coalesce(sum(last_seq),0) FROM newim.im_conversations)::text")
				err := exercise()
				if first == "auth" && auth.ErrorCode(err) != auth.AuthStorageUnavailable {
					t.Fatalf("terminal auth acquire: %v", err)
				}
				if first == "message" && message.ErrorCode(err) != message.SendStorageUnavailable {
					t.Fatalf("terminal message transaction: %v", err)
				}
				if got := c.scalar("SELECT (SELECT count(*) FROM newim.im_messages)::text || '/' || (SELECT count(*) FROM newim.im_outbox_events)::text || '/' || (SELECT coalesce(sum(last_seq),0) FROM newim.im_conversations)::text"); got != beforeRows {
					t.Fatal("terminal repository changed durable rows")
				}
				closed := make(chan struct{})
				go func() { closeFirst(); close(closed) }()
				select {
				case <-closed:
				case <-time.After(5 * time.Second):
					t.Fatal("repository close timed out")
				}
				if got := c.scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('" + appName + "','identity_failed')"); got != "0" {
					t.Fatalf("pool leaked: %s", got)
				}
			})
		}
	})

	t.Run("same-postmaster-database-incarnation", func(t *testing.T) {
		c := newIdentityCluster(t, "incarnation", migration)
		f := openFixtureDSN(t, c.dsn(), identityName)
		identity := f.seedIdentity("incarnation", seedOptions{})
		conversationID := f.seedConversation("incarnation", identity.UserID)
		server := startMessageServer(t, clusterMessageEnv(c))
		server.waitHealthy(t)
		assertSendSucceeds(t, server, identity, conversationID, "incarnation_before")
		oid := c.scalar("SELECT oid FROM pg_database WHERE datname=current_database()")
		postmaster := c.scalar("SELECT pg_postmaster_start_time()::text")
		must(t, f.db.Close(context.Background()))
		c.run("psql", "-h", c.sock, "-p", strconv.Itoa(c.port), "-U", "newim_test", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", "DROP DATABASE "+identityName+" WITH (FORCE)")
		c.createDatabaseAndMigrate(migration)
		if c.scalar("SELECT oid FROM pg_database WHERE datname=current_database()") == oid || c.scalar("SELECT pg_postmaster_start_time()::text") != postmaster {
			t.Fatal("database incarnation fixture not isolated")
		}
		assertGuardTerminalAfterReplacement(t, server, identity, conversationID, "incarnation_rejected")
		fresh := openFixtureDSN(t, c.dsn(), identityName)
		newIdentity := fresh.seedIdentity("incarnation_new", seedOptions{})
		newConversation := fresh.seedConversation("incarnation_new", newIdentity.UserID)
		restarted := startMessageServer(t, clusterMessageEnv(c))
		restarted.waitHealthy(t)
		assertSendSucceeds(t, restarted, newIdentity, newConversation, "incarnation_restarted")
		restarted.stop(t)
		assertIdentityPoolsEmpty(t, c)
	})
}

func assertIdentityStartupRejected(t *testing.T, c *identityCluster, env map[string]string) {
	t.Helper()
	apiAddr, opsAddr := twoFreeAddrs(t)
	result := runServerExpectExit(t, []string{"--api-addr", apiAddr, "--ops-addr", opsAddr}, env, 8*time.Second)
	if result.code == 0 || strings.TrimSpace(result.stderr) != "SERVER_INVALID_MESSAGE_CONFIG" {
		t.Fatalf("identity startup exit=%d stderr=%q", result.code, result.stderr)
	}
	assertPortReusable(t, apiAddr)
	assertPortReusable(t, opsAddr)
	assertIdentityPoolsEmpty(t, c)
}
func assertIdentityPoolsEmpty(t *testing.T, c *identityCluster) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('newim-auth-http','newim-message-http')") == "0" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("identity startup/shutdown left pools in %s", c.name)
}
