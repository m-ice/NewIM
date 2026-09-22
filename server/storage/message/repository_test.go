package message

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	protocol "github.com/m-ice/NewIM/core/protocol/go"
	app "github.com/m-ice/NewIM/server/message"
)

func TestMapStorageError(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		code        app.Code
		disposition protocol.RetryDisposition
	}{
		{"deadline", context.DeadlineExceeded, app.SendStorageUnavailable, protocol.RetrySameIntent},
		{"canceled", context.Canceled, app.SendStorageUnavailable, protocol.RetrySameIntent},
		{"io", io.EOF, app.SendStorageUnavailable, protocol.RetrySameIntent},
		{"net", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("reset")}, app.SendStorageUnavailable, protocol.RetrySameIntent},
		{"connect", &pgconn.ConnectError{}, app.SendStorageUnavailable, protocol.RetrySameIntent},
		{"missing", &pgconn.PgError{Code: "NI001"}, app.SendConversationMissing, protocol.StopAutomaticRetry},
		{"exhausted", &pgconn.PgError{Code: "NI002"}, app.SendSequenceExhausted, protocol.StopAutomaticRetry},
		{"isolation", &pgconn.PgError{Code: "NI003"}, app.SendInvalidInput, protocol.StopAutomaticRetry},
		{"lock", &pgconn.PgError{Code: "55P03"}, app.SendLockUnavailable, protocol.RetrySameIntent},
		{"unknown", errors.New("unclassified"), app.SendUnknown, protocol.StopAutomaticRetry},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			mapped := mapStorageError(test.err)
			if got := app.ErrorCode(mapped); got != test.code {
				t.Fatalf("code got %s want %s", got, test.code)
			}
			if got := app.RetryDisposition(mapped); got != test.disposition {
				t.Fatalf("disposition got %s want %s", got, test.disposition)
			}
		})
	}
}
