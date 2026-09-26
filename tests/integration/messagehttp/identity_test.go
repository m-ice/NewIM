//go:build integration

package messagehttp_test

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	postgresBin  = "/usr/lib/postgresql/18/bin"
	postgresUID  = 999
	postgresGID  = 999
	identityName = "newim_test_identity"
)

type identityCluster struct {
	t       *testing.T
	name    string
	dir     string
	sock    string
	log     string
	port    int
	stopped bool
}

func TestMessageHTTPIdentity(t *testing.T) {
	migrationPath := strings.TrimSpace(os.Getenv("NEWIM_MESSAGE_HTTP_MIGRATION_SQL"))
	if migrationPath == "" {
		t.Fatal("NEWIM_MESSAGE_HTTP_MIGRATION_SQL is required")
	}
	migrationBytes, err := os.ReadFile(migrationPath)
	must(t, err)
	migration := strings.TrimSpace(string(migrationBytes))
	if migration == "" {
		t.Fatal("migration SQL is empty")
	}

	t.Run("standby-startup-rejected", func(t *testing.T) {
		primary := newIdentityCluster(t, "standby-primary", migration)
		standby := newIdentityStandby(t, primary)
		apiAddr, opsAddr := twoFreeAddrs(t)
		result := runServerExpectExit(t, []string{"--api-addr", apiAddr, "--ops-addr", opsAddr}, clusterMessageEnv(standby), 8*time.Second)
		if result.code == 0 || !strings.Contains(result.stderr, "SERVER_INVALID_MESSAGE_CONFIG") {
			t.Fatalf("standby startup exit=%d stdout=%q stderr=%q", result.code, result.stdout, result.stderr)
		}
		assertPortReusable(t, apiAddr)
		assertPortReusable(t, opsAddr)
	})

	t.Run("same-cluster-restart-rejected", func(t *testing.T) {
		cluster := newIdentityCluster(t, "restart", migration)
		f := openFixtureDSN(t, cluster.dsn(), identityName)
		identity := f.seedIdentity("identity_restart", seedOptions{})
		conversationID := f.seedConversation("identity_restart", identity.UserID)
		server := startMessageServer(t, clusterMessageEnv(cluster))
		server.waitHealthy(t)
		assertSendSucceeds(t, server, identity, conversationID, "identity_restart_client")

		cluster.stop()
		cluster.start()
		assertGuardTerminalAfterReplacement(t, server, identity, conversationID, "identity_restart_after")
	})

	t.Run("promoted-clone-replacement-rejected", func(t *testing.T) {
		primary := newIdentityCluster(t, "promotion-primary", migration)
		f := openFixtureDSN(t, primary.dsn(), identityName)
		identity := f.seedIdentity("identity_promotion", seedOptions{})
		conversationID := f.seedConversation("identity_promotion", identity.UserID)
		standby := newIdentityStandby(t, primary)
		server := startMessageServer(t, clusterMessageEnv(primary))
		server.waitHealthy(t)
		assertSendSucceeds(t, server, identity, conversationID, "identity_promotion_client")

		standby.promote()
		if standby.scalar("SELECT pg_is_in_recovery()") != "f" {
			t.Fatal("promoted clone still reports recovery")
		}
		timeline := standby.scalar("SELECT (pg_split_walfile_name(pg_walfile_name(pg_current_wal_insert_lsn()))).timeline_id")
		if timeline == "1" || timeline == "" {
			t.Fatalf("promoted clone timeline=%q", timeline)
		}

		primary.stop()
		standby.stop()
		standby.sock = primary.sock
		standby.port = primary.port
		standby.start()
		assertGuardTerminalAfterReplacement(t, server, identity, conversationID, "identity_promotion_after")
	})

	t.Run("restore-cross-cluster-rejected", func(t *testing.T) {
		primary := newIdentityCluster(t, "restore-primary", migration)
		f := openFixtureDSN(t, primary.dsn(), identityName)
		identity := f.seedIdentity("identity_restore", seedOptions{})
		conversationID := f.seedConversation("identity_restore", identity.UserID)
		server := startMessageServer(t, clusterMessageEnv(primary))
		server.waitHealthy(t)
		assertSendSucceeds(t, server, identity, conversationID, "identity_restore_client")

		restored := newBareIdentityCluster(t, "restore-target")
		restored.run("createdb", "-h", restored.sock, "-p", strconv.Itoa(restored.port), "-U", "newim_test", identityName)
		dumpPath := filepath.Join(filepath.Dir(restored.dir), "restore.dump")
		primary.run("pg_dump", "-h", primary.sock, "-p", strconv.Itoa(primary.port), "-U", "newim_test",
			"-d", identityName, "-Fc", "-f", dumpPath)
		restored.run("pg_restore", "-h", restored.sock, "-p", strconv.Itoa(restored.port), "-U", "newim_test",
			"-d", identityName, "--exit-on-error", "--no-owner", "--no-privileges", dumpPath)
		primary.stop()
		restored.stop()
		restored.sock = primary.sock
		restored.port = primary.port
		restored.start()
		assertGuardTerminalAfterReplacement(t, server, identity, conversationID, "identity_restore_after")
	})
}

func assertSendSucceeds(t *testing.T, server *runningServer, identity seedIdentity, conversationID, clientMsgID string) {
	t.Helper()
	result := doRequest(t, http.MethodPost, messageURL(server), textBody(t, clientMsgID, conversationID, "identity baseline"), identity.RawToken, nil)
	if result.status != http.StatusOK {
		t.Fatalf("baseline send status=%d body=%q", result.status, result.body)
	}
	_ = decodeACK(t, result.body)
}

func assertGuardTerminalAfterReplacement(t *testing.T, server *runningServer, identity seedIdentity, conversationID, clientMsgID string) {
	t.Helper()
	body := textBody(t, clientMsgID, conversationID, "must be rejected after replacement")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if server.pollExit() {
			break
		}
		result := doRequestErr(http.MethodPost, messageURL(server), body, identity.RawToken, nil)
		if result.err == nil {
			if result.status == http.StatusOK {
				t.Fatalf("replacement produced success ACK: %q", result.body)
			}
			if result.status != http.StatusServiceUnavailable {
				t.Fatalf("replacement status=%d body=%q", result.status, result.body)
			}
		}
		if server.pollExit() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !server.stopped {
		err := server.waitExit(t, 10*time.Second)
		t.Fatalf("replacement guard did not terminate process within bound: %v logs=%q", err, server.logs())
	}
	if server.failed == nil {
		t.Fatalf("replacement guard exited zero; logs=%q", server.logs())
	}
	if !strings.Contains(server.logs(), "SERVER_INVALID_MESSAGE_CONFIG") {
		t.Fatalf("replacement terminal code missing: %q", server.logs())
	}
}

func clusterMessageEnv(c *identityCluster) map[string]string {
	return map[string]string{
		"NEWIM_MESSAGE_HTTP":            "1",
		"NEWIM_AUTH_DSN":                c.dsn(),
		"NEWIM_AUTH_ALLOW_LOCAL_SOCKET": "1",
	}
}

func newIdentityCluster(t *testing.T, name, migration string) *identityCluster {
	t.Helper()
	c := newBareIdentityCluster(t, name)
	c.createDatabaseAndMigrate(migration)
	return c
}

func newBareIdentityCluster(t *testing.T, name string) *identityCluster {
	t.Helper()
	base := filepath.Join("/tmp", fmt.Sprintf("newim-message-http-%s-%d", name, time.Now().UnixNano()))
	must(t, os.MkdirAll(filepath.Join(base, "socket"), 0o700))
	must(t, os.Chown(base, postgresUID, postgresGID))
	c := &identityCluster{
		t:    t,
		name: name,
		dir:  filepath.Join(base, "data"),
		sock: filepath.Join(base, "socket"),
		log:  filepath.Join(base, "postgres.log"),
		port: freeAddrPort(t),
	}
	must(t, os.Chown(c.sock, postgresUID, postgresGID))
	c.run("initdb", "-D", c.dir, "-A", "trust", "-U", "newim_test", "--no-locale", "-E", "UTF8")
	c.start()
	t.Cleanup(c.stop)
	return c
}

func newIdentityStandby(t *testing.T, primary *identityCluster) *identityCluster {
	t.Helper()
	base := filepath.Join("/tmp", fmt.Sprintf("newim-message-http-standby-%d", time.Now().UnixNano()))
	must(t, os.MkdirAll(filepath.Join(base, "socket"), 0o700))
	must(t, os.Chown(base, postgresUID, postgresGID))
	standby := &identityCluster{
		t:    t,
		name: "standby",
		dir:  filepath.Join(base, "data"),
		sock: filepath.Join(base, "socket"),
		log:  filepath.Join(base, "postgres.log"),
		port: freeAddrPort(t),
	}
	must(t, os.Chown(standby.sock, postgresUID, postgresGID))
	primary.stop()
	must(t, exec.Command("/bin/cp", "-a", primary.dir, standby.dir).Run())
	must(t, os.Chown(standby.dir, postgresUID, postgresGID))
	must(t, os.WriteFile(filepath.Join(standby.dir, "standby.signal"), nil, 0o600))
	must(t, os.Chown(filepath.Join(standby.dir, "standby.signal"), postgresUID, postgresGID))
	standby.start()
	standby.waitForRecovery()
	primary.start()
	t.Cleanup(standby.stop)
	return standby
}

func (c *identityCluster) dsn() string {
	return fmt.Sprintf("host=%s port=%d user=newim_test dbname=%s sslmode=disable application_name=nim_message_http_identity", c.sock, c.port, identityName)
}

func (c *identityCluster) createDatabaseAndMigrate(migration string) {
	c.t.Helper()
	c.run("createdb", "-h", c.sock, "-p", strconv.Itoa(c.port), "-U", "newim_test", identityName)
	migrationPath := filepath.Join(filepath.Dir(c.dir), "migration.sql")
	must(c.t, os.WriteFile(migrationPath, []byte(migration+"\n"), 0o600))
	must(c.t, os.Chown(migrationPath, postgresUID, postgresGID))
	c.run("psql", "-h", c.sock, "-p", strconv.Itoa(c.port), "-U", "newim_test", "-d", identityName,
		"-v", "ON_ERROR_STOP=1", "-f", migrationPath)
}

func (c *identityCluster) start() {
	c.t.Helper()
	c.run("pg_ctl", "-D", c.dir, "-o",
		fmt.Sprintf("-c listen_addresses=127.0.0.1 -p %d -c unix_socket_directories=%s", c.port, c.sock),
		"-l", c.log, "-w", "-t", "20", "start")
	c.stopped = false
}

func (c *identityCluster) stop() {
	if c == nil || c.stopped {
		return
	}
	command := c.command("pg_ctl", "-D", c.dir, "-m", "immediate", "-w", "-t", "10", "stop")
	_ = command.Run()
	c.stopped = true
}

func (c *identityCluster) promote() {
	c.t.Helper()
	c.run("pg_ctl", "-D", c.dir, "-w", "-t", "20", "promote")
}

func (c *identityCluster) waitForRecovery() {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c.scalar("SELECT pg_is_in_recovery()") == "t" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	c.t.Fatalf("standby did not reach recovery state")
}

func (c *identityCluster) scalar(query string) string {
	c.t.Helper()
	output := c.run("psql", "-h", c.sock, "-p", strconv.Itoa(c.port), "-U", "newim_test", "-d", identityName, "-Atc", query)
	return strings.TrimSpace(string(output))
}

func (c *identityCluster) run(name string, args ...string) []byte {
	c.t.Helper()
	command := c.command(name, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		c.t.Fatalf("%s %s failed: %v output=%q", c.name, name, err, output)
	}
	return output
}

func (c *identityCluster) command(name string, args ...string) *exec.Cmd {
	argv := []string{"--reuid=" + strconv.Itoa(postgresUID), "--regid=" + strconv.Itoa(postgresGID), "--init-groups", filepath.Join(postgresBin, name)}
	argv = append(argv, args...)
	command := exec.Command("/usr/bin/setpriv", argv...)
	command.Env = os.Environ()
	return command
}

func freeAddrPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	must(t, listener.Close())
	return port
}
