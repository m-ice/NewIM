package push

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

const MaxAttempts = 8

type Delivery struct {
	EventID string
	TokenBinding
	LeaseID  string
	Attempts int
}

type ClaimStatus uint8

const (
	NoDelivery ClaimStatus = iota
	LeaseAcquired
	LeaseExhausted
)

type Result uint8

const (
	Accepted Result = iota + 1
	TemporaryFailure
	PermanentFailure
	InvalidToken
	Cancelled
)

type Outcome struct {
	Result     Result
	RetryAfter time.Duration
	InvalidAt  time.Time
}

type Provider interface {
	Send(context.Context, []byte) Outcome
}

type Queue interface {
	Enable(context.Context) error
	Fanout(context.Context) (int64, error)
	Claim(context.Context) (Delivery, ClaimStatus, error)
	Prepare(context.Context, Delivery) ([]byte, bool, error)
	// Finish returns the committed decision, or zero if the lease no longer applies.
	Finish(context.Context, Delivery, Outcome) (Result, error)
	PrunePresence(context.Context) error
}

// Counts records completed dispatch decisions without installation or message identifiers.
type Counts struct {
	Accepted, Retry, Failed, Invalid, Cancelled uint64
}

type Worker struct {
	queue     Queue
	providers map[Channel]Provider
	outcomes  [Cancelled + 1]atomic.Uint64
}

func (w *Worker) Counts() Counts {
	return Counts{
		Accepted: w.outcomes[Accepted].Load(), Retry: w.outcomes[TemporaryFailure].Load(),
		Failed: w.outcomes[PermanentFailure].Load(), Invalid: w.outcomes[InvalidToken].Load(),
		Cancelled: w.outcomes[Cancelled].Load(),
	}
}

func NewWorker(queue Queue, providers map[Channel]Provider) (*Worker, error) {
	if queue == nil || len(providers) == 0 || len(providers) > 2 {
		return nil, ErrConfig
	}
	worker := &Worker{queue: queue, providers: make(map[Channel]Provider, len(providers))}
	for channel, provider := range providers {
		if !channel.Valid() || provider == nil {
			return nil, ErrConfig
		}
		worker.providers[channel] = provider
	}
	return worker, nil
}

// Run returns on shutdown. Claimed work can be reclaimed after the lease expires.
func (w *Worker) Run(ctx context.Context, onStorageFailure func(), report func(Counts)) error {
	if err := w.queue.Enable(ctx); err != nil {
		return err
	}
	var maintenance time.Time
	var reported Counts
	for ctx.Err() == nil {
		var cycleErr error
		if !time.Now().Before(maintenance) {
			cycleErr = w.queue.PrunePresence(ctx)
			maintenance = time.Now().Add(time.Minute)
			if current := w.Counts(); report != nil && current != reported {
				report(current)
				reported = current
			}
		}
		progress, err := w.Cycle(ctx)
		if err != nil || cycleErr != nil {
			if onStorageFailure != nil && ctx.Err() == nil {
				onStorageFailure()
			}
		} else if progress {
			continue
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return nil
}

// Cycle independently schedules one committed event and processes one due device notification.
func (w *Worker) Cycle(ctx context.Context) (bool, error) {
	scheduled, scheduleErr := w.queue.Fanout(ctx)
	job, status, err := w.queue.Claim(ctx)
	if err != nil {
		return scheduled > 0, errors.Join(scheduleErr, err)
	}
	switch status {
	case LeaseExhausted:
		w.outcomes[PermanentFailure].Add(1)
		return true, scheduleErr
	case LeaseAcquired:
	default:
		return scheduled > 0, scheduleErr
	}
	outcome, err := w.send(ctx, job)
	if err != nil {
		return false, errors.Join(scheduleErr, err)
	}
	if outcome.Result == TemporaryFailure {
		outcome.RetryAfter = backoff(job.Attempts, outcome.RetryAfter)
	}
	finished, err := w.queue.Finish(ctx, job, outcome)
	if err == nil && finished >= Accepted && finished <= Cancelled {
		w.outcomes[finished].Add(1)
		return true, scheduleErr
	}
	return scheduled > 0, errors.Join(scheduleErr, err)
}

func (w *Worker) send(ctx context.Context, job Delivery) (Outcome, error) {
	token, eligible, err := w.queue.Prepare(ctx, job)
	defer clear(token)
	if errors.Is(err, ErrCiphertext) {
		return Outcome{Result: PermanentFailure}, nil
	}
	if err != nil {
		return Outcome{}, err
	}
	if !eligible {
		return Outcome{Result: Cancelled}, nil
	}
	provider := w.providers[job.Channel]
	if provider == nil {
		return Outcome{Result: PermanentFailure}, nil
	}
	bounded, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	outcome := provider.Send(bounded, token)
	if outcome.Result < Accepted || outcome.Result > InvalidToken {
		outcome = Outcome{Result: PermanentFailure}
	}
	return outcome, nil
}

func backoff(attempt int, provider time.Duration) time.Duration {
	window := 2 * time.Second * time.Duration(1<<min(max(attempt-1, 0), MaxAttempts-1))
	delay := window/2 + rand.N(window/2)
	return max(delay, min(provider, 24*time.Hour))
}
