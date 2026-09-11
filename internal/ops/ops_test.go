package ops

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hkdb/aerion/internal/database"
	"github.com/rs/zerolog"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()

	path := filepath.Join(t.TempDir(), "test.db")
	db, err := database.Open(path)
	if err != nil {
		t.Fatalf("database.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Migrate(); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO accounts (id, name, email, imap_host, smtp_host, username)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		"acct-1", "Test", "t@example.com", "imap.example.com", "smtp.example.com", "t@example.com",
	); err != nil {
		t.Fatalf("seed account: %v", err)
	}
	return NewStore(db)
}

func movePayload(ids ...string) Payload {
	refs := make([]MessageRef, 0, len(ids))
	for i, id := range ids {
		refs = append(refs, MessageRef{ID: id, UID: uint32(100 + i), MessageID: "<" + id + "@example.com>"})
	}
	return Payload{Messages: refs, SourceFolderID: "inbox", DestFolderID: "trash"}
}

// recordingExecutor captures what the drainer asked it to run, and can be told
// to fail a given number of times first.
type recordingExecutor struct {
	mu          sync.Mutex
	ran         []string
	failFor     int
	failWith    error
	compensated []string
}

func (r *recordingExecutor) Execute(_ context.Context, op *Op) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.failFor > 0 {
		r.failFor--
		return r.failWith
	}
	r.ran = append(r.ran, op.ID)
	return nil
}

// Compensate records the abandonment so tests can assert the local half gets
// rolled back rather than left diverged.
func (r *recordingExecutor) Compensate(_ context.Context, op *Op, _ error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.compensated = append(r.compensated, op.ID)
}

func (r *recordingExecutor) compensations() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.compensated...)
}

func (r *recordingExecutor) runs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ran...)
}

// waitForEmptyQueue polls until every op has been executed AND cleared.
// Polling rather than signalling from the executor: the drainer removes a
// completed op after Execute returns, so a signal raised inside Execute would
// fire while the queue still holds the row.
func waitForEmptyQueue(t *testing.T, s *Store) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n, err := s.PendingCount()
		if err != nil {
			t.Fatalf("PendingCount: %v", err)
		}
		if n == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	n, _ := s.PendingCount()
	t.Fatalf("queue still holds %d op(s) after 5s", n)
}

// TestCancelPendingRemovesOp is the behavior undo depends on: while an op is
// still pending, cancelling it takes it out of the queue entirely so nothing
// ever reaches the server.
func TestCancelPendingRemovesOp(t *testing.T) {
	s := newTestStore(t)

	id, err := s.Enqueue("acct-1", TypeMove, movePayload("m1"), time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	cancelled, err := s.Cancel(id)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !cancelled {
		t.Fatal("expected a pending op to cancel")
	}

	op, err := s.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if op != nil {
		t.Errorf("op still queued after cancel: %+v", op)
	}
}

// TestCancelRunningOpFails is the other half of the contract. Once the drainer
// has claimed an op the server may already know about it, so cancelling must
// report false and let the caller reverse instead.
func TestCancelRunningOpFails(t *testing.T) {
	s := newTestStore(t)

	id, err := s.Enqueue("acct-1", TypeMove, movePayload("m1"), time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	claimed, err := s.ClaimNext(time.Now())
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if claimed == nil || claimed.ID != id {
		t.Fatalf("ClaimNext returned %+v, want the enqueued op", claimed)
	}

	cancelled, err := s.Cancel(id)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if cancelled {
		t.Error("cancelled an op that was already running")
	}
}

// TestDeferWindowWithholdsOp: an op is not claimable until its defer window
// has elapsed. That window is the whole reason undo can be a local cancel.
func TestDeferWindowWithholdsOp(t *testing.T) {
	s := newTestStore(t)

	if _, err := s.Enqueue("acct-1", TypeMove, movePayload("m1"), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	op, err := s.ClaimNext(time.Now())
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if op != nil {
		t.Errorf("claimed a deferred op early: %+v", op)
	}

	// Past the window it becomes available.
	op, err = s.ClaimNext(time.Now().Add(2 * time.Hour))
	if err != nil {
		t.Fatalf("ClaimNext (later): %v", err)
	}
	if op == nil {
		t.Error("expected the op to be claimable once its window elapsed")
	}
}

// TestClaimOrderIsFIFO: ops must reach the server in the order the user
// performed them — a move followed by a flag change on the same message is not
// commutative.
func TestClaimOrderIsFIFO(t *testing.T) {
	s := newTestStore(t)

	past := time.Now().Add(-time.Minute)
	var ids []string
	for i := 0; i < 3; i++ {
		id, err := s.Enqueue("acct-1", TypeFlag, movePayload(fmt.Sprintf("m%d", i)), past)
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		ids = append(ids, id)
	}

	for i, want := range ids {
		got, err := s.ClaimNext(time.Now())
		if err != nil {
			t.Fatalf("ClaimNext: %v", err)
		}
		if got == nil {
			t.Fatalf("claim %d returned nothing", i)
		}
		if got.ID != want {
			t.Errorf("claim %d = %s, want %s", i, got.ID, want)
		}
		if err := s.Complete(got.ID); err != nil {
			t.Fatalf("Complete: %v", err)
		}
	}
}

// TestPayloadRoundTrip confirms the on-disk payload survives encode/decode
// with the UID intact — losing it would leave the drainer unable to address
// the message on the server.
func TestPayloadRoundTrip(t *testing.T) {
	s := newTestStore(t)

	want := Payload{
		Messages:       []MessageRef{{ID: "m1", UID: 4242, MessageID: "<a@b.com>"}},
		SourceFolderID: "inbox",
		DestFolderID:   "trash",
		FlagType:       "starred",
		FlagValue:      true,
	}
	id, err := s.Enqueue("acct-1", TypeFlag, want, time.Now())
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := s.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("op not found")
	}
	if len(got.Payload.Messages) != 1 || got.Payload.Messages[0].UID != 4242 {
		t.Errorf("messages = %+v, want UID 4242 preserved", got.Payload.Messages)
	}
	if got.Payload.FlagType != "starred" || !got.Payload.FlagValue {
		t.Errorf("flag = %q/%v, want starred/true", got.Payload.FlagType, got.Payload.FlagValue)
	}
	if got.Payload.DestFolderID != "trash" {
		t.Errorf("destFolderId = %q, want trash", got.Payload.DestFolderID)
	}
}

// TestReleaseRunningRequeuesStrandedOps: a running row means the process died
// mid-execution. Startup must put those back rather than leave them stranded,
// or a delete would be lost and the message would come back on next sync.
func TestReleaseRunningRequeuesStrandedOps(t *testing.T) {
	s := newTestStore(t)

	if _, err := s.Enqueue("acct-1", TypeDelete, movePayload("m1"), time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := s.ClaimNext(time.Now()); err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}

	// Simulates the next process start.
	n, err := s.ReleaseRunning()
	if err != nil {
		t.Fatalf("ReleaseRunning: %v", err)
	}
	if n != 1 {
		t.Errorf("released %d ops, want 1", n)
	}

	op, err := s.ClaimNext(time.Now())
	if err != nil {
		t.Fatalf("ClaimNext after release: %v", err)
	}
	if op == nil {
		t.Error("stranded op was not requeued")
	}
}

// TestFailSchedulesRetry: a failed op stays queued and comes back after its
// backoff rather than being dropped.
func TestFailSchedulesRetry(t *testing.T) {
	s := newTestStore(t)

	id, err := s.Enqueue("acct-1", TypeMove, movePayload("m1"), time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := s.ClaimNext(time.Now()); err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}

	retryAt := time.Now().Add(30 * time.Second)
	if err := s.Fail(id, errors.New("server said no"), retryAt); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	// Not claimable before the backoff elapses.
	if op, err := s.ClaimNext(time.Now()); err != nil {
		t.Fatalf("ClaimNext: %v", err)
	} else if op != nil {
		t.Error("claimed a failed op before its backoff elapsed")
	}

	op, err := s.ClaimNext(retryAt.Add(time.Second))
	if err != nil {
		t.Fatalf("ClaimNext after backoff: %v", err)
	}
	if op == nil {
		t.Fatal("failed op did not come back for retry")
	}
	if op.Attempt != 1 {
		t.Errorf("attempt = %d, want 1", op.Attempt)
	}
	if op.LastError != "server said no" {
		t.Errorf("lastError = %q, want the recorded cause", op.LastError)
	}
}

// TestDrainerRunsQueuedOps exercises the loop end to end.
func TestDrainerRunsQueuedOps(t *testing.T) {
	s := newTestStore(t)
	exec := &recordingExecutor{}
	d := NewDrainer(s, exec, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Start(ctx)

	for i := 0; i < 2; i++ {
		if _, err := s.Enqueue("acct-1", TypeMove, movePayload(fmt.Sprintf("m%d", i)), time.Now()); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	d.Wake()

	// Completed ops are executed and then cleared from the queue.
	waitForEmptyQueue(t, s)
	if runs := exec.runs(); len(runs) != 2 {
		t.Errorf("drainer ran %d ops, want 2", len(runs))
	}
}

// TestDrainerAbandonsUnrecoverable: an op that cannot ever succeed is dropped
// instead of retried forever.
func TestDrainerAbandonsUnrecoverable(t *testing.T) {
	s := newTestStore(t)
	exec := &recordingExecutor{failFor: 1, failWith: fmt.Errorf("%w: folder gone", ErrUnrecoverable)}
	d := NewDrainer(s, exec, zerolog.Nop())

	if _, err := s.Enqueue("acct-1", TypeMove, movePayload("m1"), time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Start(ctx)
	d.Wake()

	// Abandoned means gone from the queue without ever succeeding.
	waitForEmptyQueue(t, s)
	if runs := exec.runs(); len(runs) != 0 {
		t.Errorf("executor reported %d successful runs, want 0", len(runs))
	}
}

// TestFlushRunsDeferredOps: shutdown must send ops that are still inside their
// defer window, not just the ones that came due.
func TestFlushRunsDeferredOps(t *testing.T) {
	s := newTestStore(t)
	exec := &recordingExecutor{}
	d := NewDrainer(s, exec, zerolog.Nop())

	if _, err := s.Enqueue("acct-1", TypeMove, movePayload("m1"), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := d.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if runs := exec.runs(); len(runs) != 1 {
		t.Errorf("flush ran %d ops, want 1 (deferred op left behind on shutdown)", len(runs))
	}
	if n, err := s.PendingCount(); err != nil || n != 0 {
		t.Errorf("PendingCount = %d (err %v), want 0 after flush", n, err)
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	if got := backoffFor(0); got != baseBackoff {
		t.Errorf("backoffFor(0) = %v, want %v", got, baseBackoff)
	}
	if got := backoffFor(1); got != 2*baseBackoff {
		t.Errorf("backoffFor(1) = %v, want %v", got, 2*baseBackoff)
	}
	if got := backoffFor(100); got != maxBackoff {
		t.Errorf("backoffFor(100) = %v, want the cap %v", got, maxBackoff)
	}
}

// TestAbandonedOpIsCompensated is the correctness guarantee behind optimistic
// UI: when an op can never reach the server, the executor is told so it can
// walk the local change back. Without this the local store and the server
// disagree permanently — an abandoned move leaves the message parked in the
// destination locally while the server still has it in the source, and the
// next sync surfaces it in both places.
func TestAbandonedOpIsCompensated(t *testing.T) {
	s := newTestStore(t)
	exec := &recordingExecutor{failFor: 1, failWith: fmt.Errorf("%w: folder gone", ErrUnrecoverable)}
	d := NewDrainer(s, exec, zerolog.Nop())

	id, err := s.Enqueue("acct-1", TypeMove, movePayload("m1"), time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Start(ctx)
	d.Wake()

	waitForEmptyQueue(t, s)

	comps := exec.compensations()
	if len(comps) != 1 || comps[0] != id {
		t.Errorf("compensations = %v, want [%s]", comps, id)
	}
	if runs := exec.runs(); len(runs) != 0 {
		t.Errorf("executor reported %d successful runs, want 0", len(runs))
	}
}

// TestSuccessfulOpIsNotCompensated: compensation is strictly the failure path.
// Rolling back a move that actually landed would be a bug of its own.
func TestSuccessfulOpIsNotCompensated(t *testing.T) {
	s := newTestStore(t)
	exec := &recordingExecutor{}
	d := NewDrainer(s, exec, zerolog.Nop())

	if _, err := s.Enqueue("acct-1", TypeMove, movePayload("m1"), time.Now()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Start(ctx)
	d.Wake()

	waitForEmptyQueue(t, s)

	if comps := exec.compensations(); len(comps) != 0 {
		t.Errorf("compensated a successful op: %v", comps)
	}
}

// TestRetriedOpIsNotCompensatedEarly: a transient failure must retry, not
// compensate. Rolling back on the first hiccup would undo the user's action
// because the server blinked.
func TestRetriedOpIsNotCompensatedEarly(t *testing.T) {
	s := newTestStore(t)
	exec := &recordingExecutor{failFor: 1, failWith: errors.New("temporary network glitch")}
	d := NewDrainer(s, exec, zerolog.Nop())

	if _, err := s.Enqueue("acct-1", TypeMove, movePayload("m1"), time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Start(ctx)
	d.Wake()

	// First attempt fails and is rescheduled; nothing should be compensated.
	time.Sleep(200 * time.Millisecond)
	if comps := exec.compensations(); len(comps) != 0 {
		t.Fatalf("compensated on a retryable failure: %v", comps)
	}

	op, err := s.Get(findOnlyOpID(t, s))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if op == nil {
		t.Fatal("retryable op was dropped instead of rescheduled")
	}
	if op.Attempt != 1 {
		t.Errorf("attempt = %d, want 1", op.Attempt)
	}
}

func findOnlyOpID(t *testing.T, s *Store) string {
	t.Helper()
	var id string
	if err := s.db.QueryRow(`SELECT id FROM pending_ops LIMIT 1`).Scan(&id); err != nil {
		t.Fatalf("find op: %v", err)
	}
	return id
}
