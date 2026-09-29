package postgresidentity

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type queryRowFunc func(context.Context, string, ...any) pgx.Row

func (f queryRowFunc) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return f(ctx, sql, args...)
}

type scanRowFunc func(...any) error

func (f scanRowFunc) Scan(dest ...any) error {
	return f(dest...)
}

func identityQueryer(value identity) rowQuerier {
	return queryRowFunc(func(context.Context, string, ...any) pgx.Row {
		return scanRowFunc(func(dest ...any) error {
			*dest[0].(*string) = value.database
			*dest[1].(*uint32) = value.oid
			*dest[2].(*time.Time) = value.postmasterStart
			*dest[3].(*string) = value.systemIdentifier
			timelineID := value.timelineID
			*dest[4].(**uint32) = &timelineID
			return nil
		})
	})
}

func recoveryQueryer(value identity) rowQuerier {
	return queryRowFunc(func(context.Context, string, ...any) pgx.Row {
		return scanRowFunc(func(dest ...any) error {
			*dest[0].(*string) = value.database
			*dest[1].(*uint32) = value.oid
			*dest[2].(*time.Time) = value.postmasterStart
			*dest[3].(*string) = value.systemIdentifier
			*dest[4].(**uint32) = nil
			return nil
		})
	})
}

func errorQueryer(err error) rowQuerier {
	return queryRowFunc(func(context.Context, string, ...any) pgx.Row {
		return scanRowFunc(func(...any) error { return err })
	})
}

func baselineIdentity() identity {
	return identity{
		database:         "newim",
		oid:              16384,
		postmasterStart:  time.Date(2026, 9, 25, 1, 2, 3, 123456789, time.UTC),
		systemIdentifier: "18446744073709551615",
		timelineID:       2,
	}
}

func TestCanonicalIdentityDigestAndValidation(t *testing.T) {
	observed, err := newIdentity(
		"newim",
		16384,
		time.Date(2026, 9, 25, 1, 2, 3, 123456789, time.FixedZone("UTC+8", 8*60*60)),
		"18446744073709551615",
		7,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"database":"newim","oid":16384,"postmasterStart":"2026-09-24T17:02:03.123456789Z","systemIdentifier":"18446744073709551615","timelineId":7}`
	wire, err := canonicalIdentityBytes(observed)
	if err != nil {
		t.Fatal(err)
	}
	if string(wire) != want {
		t.Fatalf("canonical identity got %s want %s", wire, want)
	}
	digest, err := digestIdentity(observed)
	if err != nil {
		t.Fatal(err)
	}
	if digest != sha256.Sum256([]byte(want)) {
		t.Fatalf("digest got %x want sha256(%s)", digest, want)
	}

	cases := []struct {
		name             string
		database         string
		oid              uint32
		postmasterStart  time.Time
		systemIdentifier string
		timelineID       uint32
	}{
		{"empty database", "", 1, time.Now(), "1", 1},
		{"invalid database utf8", string([]byte{0xff}), 1, time.Now(), "1", 1},
		{"zero oid", "newim", 0, time.Now(), "1", 1},
		{"zero postmaster", "newim", 1, time.Time{}, "1", 1},
		{"empty system id", "newim", 1, time.Now(), "", 1},
		{"signed system id", "newim", 1, time.Now(), "-1", 1},
		{"leading zero system id", "newim", 1, time.Now(), "01", 1},
		{"overflow system id", "newim", 1, time.Now(), "18446744073709551616", 1},
		{"zero timeline", "newim", 1, time.Now(), "1", 0},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := newIdentity(test.database, test.oid, test.postmasterStart, test.systemIdentifier, test.timelineID); err == nil {
				t.Fatal("malformed identity accepted")
			}
		})
	}
}

func TestIdentityQueryUsesRecoveryIsolatedInsertionTimeline(t *testing.T) {
	for _, required := range []string{
		"current_database()",
		"pg_database",
		"pg_postmaster_start_time()",
		"pg_control_system()",
		"pg_is_in_recovery()",
		"pg_current_wal_insert_lsn()",
		"pg_walfile_name(",
		"pg_split_walfile_name(",
	} {
		if !strings.Contains(identityQuery, required) {
			t.Fatalf("identity query missing %q", required)
		}
	}
	if strings.Contains(identityQuery, "pg_control_checkpoint") {
		t.Fatal("identity query uses lagging checkpoint timeline")
	}
}

func TestGuardBaselineAndMismatch(t *testing.T) {
	guard := NewGuard(nil)
	ctx := context.Background()
	baseline := baselineIdentity()
	if err := guard.verify(ctx, identityQueryer(baseline)); err != nil {
		t.Fatalf("first baseline: %v", err)
	}
	if guard.Tripped() != nil {
		t.Fatalf("first baseline tripped: %v", guard.Tripped())
	}
	if err := guard.verify(ctx, identityQueryer(baseline)); err != nil {
		t.Fatalf("matching identity: %v", err)
	}

	changed := baseline
	changed.timelineID++
	if err := guard.verify(ctx, identityQueryer(changed)); !errors.Is(err, errIdentityMismatch) {
		t.Fatalf("mismatch error got %v", err)
	}
	if !errors.Is(guard.Tripped(), errIdentityMismatch) {
		t.Fatalf("terminal error got %v", guard.Tripped())
	}
	if err := guard.verify(ctx, identityQueryer(changed)); !errors.Is(err, errIdentityMismatch) {
		t.Fatalf("post-trip error got %v", err)
	}
}

func TestGuardTransientErrorsRemainRecoverable(t *testing.T) {
	transient := []struct {
		name string
		err  error
	}{
		{"canceled", context.Canceled},
		{"deadline", context.DeadlineExceeded},
		{"unexpected eof", io.ErrUnexpectedEOF},
		{"class 08", &pgconn.PgError{Code: "08006"}},
		{"57p01", &pgconn.PgError{Code: "57P01"}},
		{"57p02", &pgconn.PgError{Code: "57P02"}},
		{"57p03", &pgconn.PgError{Code: "57P03"}},
		{"57014", &pgconn.PgError{Code: "57014"}},
		{"connect", &pgconn.ConnectError{}},
		{"net", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("reset")}},
	}
	for _, test := range transient {
		t.Run(test.name, func(t *testing.T) {
			guard := NewGuard(nil)
			ctx := context.Background()
			if err := guard.verify(ctx, errorQueryer(test.err)); !errors.Is(err, errIdentityTransient) {
				t.Fatalf("transient error got %v", err)
			}
			if guard.Tripped() != nil {
				t.Fatalf("transient error tripped guard: %v", guard.Tripped())
			}
			if err := guard.verify(ctx, identityQueryer(baselineIdentity())); err != nil {
				t.Fatalf("recovery baseline: %v", err)
			}
			if err := guard.verify(ctx, errorQueryer(test.err)); !errors.Is(err, errIdentityTransient) {
				t.Fatalf("post-baseline transient error got %v", err)
			}
			if guard.Tripped() != nil {
				t.Fatalf("post-baseline transient error tripped guard: %v", guard.Tripped())
			}
			if err := guard.verify(ctx, identityQueryer(baselineIdentity())); err != nil {
				t.Fatalf("post-transient baseline changed: %v", err)
			}
			changed := baselineIdentity()
			changed.oid++
			if err := guard.verify(ctx, identityQueryer(changed)); !errors.Is(err, errIdentityMismatch) {
				t.Fatalf("post-recovery mismatch got %v", err)
			}
		})
	}
}

func TestGuardDeterministicErrorsTrip(t *testing.T) {
	baseline := baselineIdentity()
	cases := []struct {
		name string
		row  rowQuerier
		want error
	}{
		{"recovery", recoveryQueryer(baseline), errIdentityRecovery},
		{"permission", errorQueryer(&pgconn.PgError{Code: "42501"}), errIdentityConfiguration},
		{"missing function", errorQueryer(&pgconn.PgError{Code: "42883"}), errIdentityConfiguration},
		{"scan malformed", errorQueryer(errors.New("scan failed")), errIdentityConfiguration},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			guard := NewGuard(nil)
			if err := guard.verify(context.Background(), test.row); !errors.Is(err, test.want) {
				t.Fatalf("deterministic error got %v want %v", err, test.want)
			}
			if !errors.Is(guard.Tripped(), test.want) {
				t.Fatalf("terminal error got %v want %v", guard.Tripped(), test.want)
			}
		})
	}

	guard := NewGuard(nil)
	malformed := baseline
	malformed.systemIdentifier = "01"
	if err := guard.verify(context.Background(), identityQueryer(malformed)); !errors.Is(err, errIdentityMalformed) {
		t.Fatalf("malformed identity got %v", err)
	}
	if !errors.Is(guard.Tripped(), errIdentityMalformed) {
		t.Fatalf("malformed terminal got %v", guard.Tripped())
	}
}

func TestGuardPublishesTripSynchronouslyOnce(t *testing.T) {
	var calls atomic.Int32
	published := make(chan struct{})
	guard := NewGuard(func(error) {
		calls.Add(1)
		close(published)
	})
	var wait sync.WaitGroup
	for attempt := 0; attempt < 32; attempt++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := guard.Verify(context.Background(), nil); err == nil {
				t.Error("malformed connection accepted")
			}
			select {
			case <-published:
			default:
				t.Error("Verify returned before terminal publication")
			}
		}()
	}
	wait.Wait()
	if calls.Load() != 1 {
		t.Fatalf("callback calls=%d", calls.Load())
	}
}

func TestGuardConcurrentFirstBaselineIsAtomic(t *testing.T) {
	guard := NewGuard(nil)
	ctx := context.Background()
	observed := baselineIdentity()
	var wait sync.WaitGroup
	errs := make(chan error, 32)
	for attempt := 0; attempt < cap(errs); attempt++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errs <- guard.verify(ctx, identityQueryer(observed))
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent baseline: %v", err)
		}
	}
}
