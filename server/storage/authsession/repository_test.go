package authsession

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/m-ice/NewIM/server/auth/session"
	"github.com/m-ice/NewIM/server/storage/postgresidentity"
)

func TestOpenRedactsDSN(t *testing.T) {
	const sentinel = "dsn-sentinel-should-not-leak"
	_, err := Open(context.Background(), Config{DSN: "postgres://user:" + sentinel + "@%zz/db?sslmode=require"})
	if app.ErrorCode(err) != app.AuthStorageUnavailable {
		t.Fatalf("invalid DSN got %v", err)
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatal("DSN sentinel leaked through adapter error")
	}
}

func TestOpenRejectsInvalidApplicationName(t *testing.T) {
	_, err := Open(context.Background(), Config{DSN: "host=/tmp/missing user=test dbname=test sslmode=disable", AllowLocalSocket: true, ApplicationName: "bad name"})
	if app.ErrorCode(err) != app.AuthInvalidInput {
		t.Fatalf("invalid application name got %v", err)
	}
}

func TestInstallGuardNilPreservesHooks(t *testing.T) {
	poolConfig, err := pgxpool.ParseConfig("host=/tmp/newim-missing user=test dbname=test sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	installGuard(poolConfig, nil)
	if poolConfig.AfterConnect != nil || poolConfig.PrepareConn != nil {
		t.Fatal("nil guard installed hooks")
	}
}

func TestInstallGuardWiresHooksAndMapsFailure(t *testing.T) {
	poolConfig, err := pgxpool.ParseConfig("host=/tmp/newim-missing user=test dbname=test sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	guard := postgresidentity.NewGuard(nil)
	installGuard(poolConfig, guard)
	if poolConfig.AfterConnect == nil || poolConfig.PrepareConn == nil {
		t.Fatal("guard hooks not installed")
	}
	if err := poolConfig.AfterConnect(context.Background(), nil); app.ErrorCode(err) != app.AuthStorageUnavailable {
		t.Fatalf("after-connect result err=%v", err)
	}
	ok, err := poolConfig.PrepareConn(context.Background(), nil)
	if ok || app.ErrorCode(err) != app.AuthStorageUnavailable {
		t.Fatalf("prepare result ok=%v err=%v", ok, err)
	}
	if guard.Tripped() == nil {
		t.Fatal("guard failure did not trip terminal state")
	}
}
