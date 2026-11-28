package push

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

const MaxAttempts = 8

type Delivery struct {
	EventID string
	TokenBinding
	LeaseID  string
	Attempts int
}

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
}

type Provider interface {
	Send(context.Context, []byte) Outcome
}

type Queue interface {
	Enable(context.Context) error
	Fanout(context.Context) (int64, error)
	Claim(context.Context) (Delivery, bool, error)
	Prepare(context.Context, Delivery) ([]byte, bool, error)
	Finish(context.Context, Delivery, Outcome) error
	PrunePresence(context.Context) error
}

type Worker struct {
	queue     Queue
	providers map[Channel]Provider
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
func (w *Worker) Run(ctx context.Context, onStorageFailure func()) error {
	if err := w.queue.Enable(ctx); err != nil {
		return err
	}
	var pruneAfter time.Time
	for ctx.Err() == nil {
		var cycleErr error
		if !time.Now().Before(pruneAfter) {
			cycleErr = w.queue.PrunePresence(ctx)
			pruneAfter = time.Now().Add(time.Minute)
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
	job, found, err := w.queue.Claim(ctx)
	if err != nil || !found {
		return scheduled > 0, errors.Join(scheduleErr, err)
	}
	outcome, err := w.send(ctx, job)
	if err != nil {
		return false, errors.Join(scheduleErr, err)
	}
	if outcome.Result == TemporaryFailure {
		outcome.RetryAfter = backoff(job.Attempts, outcome.RetryAfter)
	}
	err = w.queue.Finish(ctx, job, outcome)
	return err == nil, errors.Join(scheduleErr, err)
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
