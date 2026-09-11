package ops

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/rs/zerolog"
)

// Executor performs an op against the server. Implemented by the app layer,
// which owns the IMAP connection pool.
type Executor interface {
	Execute(ctx context.Context, op *Op) error

	// Compensate is called when an op is abandoned — the server refused it, or
	// it exhausted its retries — and will therefore never be applied.
	//
	// This is what keeps the local store honest. The local half of a mutation
	// was applied optimistically the moment the user acted; if the server half
	// can never follow, that optimism has to be walked back, or the two sides
	// disagree permanently and the next sync surfaces the message in two
	// places at once.
	//
	// Errors are logged, not retried: compensation is the last resort.
	Compensate(ctx context.Context, op *Op, cause error)
}

// ErrUnrecoverable marks a failure that retrying cannot fix — a deleted
// folder, an account that no longer exists, a payload referencing messages
// that are gone. The drainer abandons the op rather than retrying forever.
var ErrUnrecoverable = errors.New("unrecoverable op failure")

const (
	// idleInterval is how often the drainer looks for work when the queue is
	// empty. Enqueue also nudges it, so this is a backstop for ops whose
	// not_before is still in the future and for retry backoff.
	idleInterval = 2 * time.Second

	// maxAttempts before an op is abandoned. With the backoff below that is
	// roughly ten minutes of trying.
	maxAttempts = 8

	// baseBackoff is the first retry delay; it doubles per attempt up to
	// maxBackoff.
	baseBackoff = 2 * time.Second
	maxBackoff  = 2 * time.Minute
)

// Drainer executes queued ops one at a time, oldest first.
//
// Single-threaded on purpose. Ops against one mailbox must apply in the order
// the user performed them — a move followed by a flag change on the same
// message is not commutative — and the IMAP connection pool is happier with
// one mutation in flight than with several racing for the same mailbox
// selection.
type Drainer struct {
	store   *Store
	exec    Executor
	log     zerolog.Logger
	wake    chan struct{}
	stopped chan struct{}
	cancel  context.CancelFunc
}

// NewDrainer creates a drainer. Call Start to run it.
func NewDrainer(store *Store, exec Executor, log zerolog.Logger) *Drainer {
	return &Drainer{
		store:   store,
		exec:    exec,
		log:     log,
		wake:    make(chan struct{}, 1),
		stopped: make(chan struct{}),
	}
}

// Start runs the drain loop until ctx is cancelled.
//
// Ops left in running state by a previous process are released back to pending
// first — a running row means the app died mid-execution.
func (d *Drainer) Start(ctx context.Context) {
	ctx, d.cancel = context.WithCancel(ctx)

	if n, err := d.store.ReleaseRunning(); err != nil {
		d.log.Warn().Err(err).Msg("Failed to release stranded ops")
	} else if n > 0 {
		d.log.Info().Int("count", n).Msg("Released ops stranded by a previous run")
	}

	go func() {
		defer close(d.stopped)
		ticker := time.NewTicker(idleInterval)
		defer ticker.Stop()

		for {
			d.drainReady(ctx)

			select {
			case <-ctx.Done():
				return
			case <-d.wake:
			case <-ticker.C:
			}
		}
	}()
}

// Wake asks the drainer to look for work now rather than at the next tick.
// Non-blocking: a wake already queued is enough.
func (d *Drainer) Wake() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// drainReady executes every op that is ready right now, then returns.
func (d *Drainer) drainReady(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		op, err := d.store.ClaimNext(time.Now())
		if err != nil {
			d.log.Warn().Err(err).Msg("Failed to claim next op")
			return
		}
		if op == nil {
			return
		}

		d.run(ctx, op)
	}
}

// Flush executes everything currently queued, ignoring defer windows, and
// returns when the queue is empty or ctx expires.
//
// Not used on the shutdown path — quitting must not wait on the network, and
// the queue is durable, so unfinished work runs at next launch. Kept for
// callers that need the queue drained synchronously (and for tests to assert
// deferred ops are reachable).
func (d *Drainer) Flush(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Far-future claim time so deferred ops are included — on shutdown
		// there is no later opportunity to run them.
		op, err := d.store.ClaimNext(time.Now().Add(365 * 24 * time.Hour))
		if err != nil {
			return err
		}
		if op == nil {
			return nil
		}

		d.run(ctx, op)
	}
}

// Stop halts the drain loop and waits for any in-flight op to finish, giving
// up when ctx expires.
//
// Shutdown calls this so the process is not torn down underneath an operation
// that is mid-flight against the server. Anything unfinished stays queued and
// runs at next launch.
func (d *Drainer) Stop(ctx context.Context) {
	if d.cancel != nil {
		d.cancel()
	}
	select {
	case <-d.stopped:
	case <-ctx.Done():
		d.log.Warn().Msg("Drain loop did not stop in time; flushing anyway")
	}
}

// Stopped returns a channel closed when the drain loop has exited.
func (d *Drainer) Stopped() <-chan struct{} { return d.stopped }

func (d *Drainer) run(ctx context.Context, op *Op) {
	err := d.exec.Execute(ctx, op)
	if err == nil {
		if cErr := d.store.Complete(op.ID); cErr != nil {
			d.log.Warn().Err(cErr).Str("op", op.ID).Msg("Failed to clear completed op")
		}
		d.log.Debug().Str("op", op.Describe()).Msg("Op completed")
		return
	}

	// Context cancellation is shutdown, not failure. Put the op back untouched
	// so the next run picks it up without burning a retry attempt.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if _, rErr := d.store.ReleaseRunning(); rErr != nil {
			d.log.Warn().Err(rErr).Str("op", op.ID).Msg("Failed to requeue op after cancellation")
		}
		return
	}

	if errors.Is(err, ErrUnrecoverable) || op.Attempt+1 >= maxAttempts {
		d.log.Error().Err(err).Str("op", op.Describe()).Msg("Abandoning op")
		if aErr := d.store.Abandon(op.ID); aErr != nil {
			d.log.Warn().Err(aErr).Str("op", op.ID).Msg("Failed to abandon op")
		}
		// Roll the local half back before anyone can observe the divergence.
		d.exec.Compensate(ctx, op, err)
		return
	}

	retryAt := time.Now().Add(backoffFor(op.Attempt))
	d.log.Warn().Err(err).Str("op", op.Describe()).
		Time("retryAt", retryAt).Msg("Op failed, will retry")
	if fErr := d.store.Fail(op.ID, err, retryAt); fErr != nil {
		d.log.Warn().Err(fErr).Str("op", op.ID).Msg("Failed to record op failure")
	}
}

// backoffFor returns the delay before retrying an op that has already failed
// attempt times: exponential from baseBackoff, capped at maxBackoff.
func backoffFor(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := time.Duration(math.Pow(2, float64(attempt))) * baseBackoff
	if d > maxBackoff || d <= 0 {
		return maxBackoff
	}
	return d
}
