package authsession

import (
	"context"
	"strings"
	"testing"

	app "github.com/m-ice/NewIM/server/auth/session"
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
