package ops

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hkdb/aerion/internal/database"
)

// Store is the persistence layer for the pending_ops table.
type Store struct {
	db *database.DB
}

// NewStore creates a Store over an open database.
func NewStore(db *database.DB) *Store {
	return &Store{db: db}
}

// Enqueue persists an op and returns its id. notBefore defers execution, which
// is what gives undo a window in which cancelling is purely local.
func (s *Store) Enqueue(accountID string, opType Type, payload Payload, notBefore time.Time) (string, error) {
	encoded, err := payload.encode()
	if err != nil {
		return "", err
	}

	id := uuid.New().String()
	now := time.Now()
	_, err = s.db.Exec(`
		INSERT INTO pending_ops
			(id, account_id, op, payload_json, state, not_before_unix, attempt, created_unix)
		VALUES (?, ?, ?, ?, ?, ?, 0, ?)`,
		id, accountID, string(opType), encoded, string(StatePending),
		notBefore.Unix(), now.Unix(),
	)
	if err != nil {
		return "", fmt.Errorf("failed to enqueue op: %w", err)
	}
	return id, nil
}

// Cancel removes an op if and only if it is still pending. The conditional
// delete is the concurrency control: if the drainer claimed the op first the
// delete matches nothing and ok is false, meaning the caller must assume the
// server has already been told and reverse the operation instead.
func (s *Store) Cancel(id string) (bool, error) {
	res, err := s.db.Exec(
		`DELETE FROM pending_ops WHERE id = ? AND state = ?`,
		id, string(StatePending),
	)
	if err != nil {
		return false, fmt.Errorf("failed to cancel op: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to read cancel result: %w", err)
	}
	return n > 0, nil
}

// ClaimNext atomically takes the oldest ready op for execution and marks it
// running. Returns nil when nothing is ready.
//
// Ready means pending-or-failed, past its not_before, and — for failed ops —
// past its retry backoff. Claiming oldest-first per account preserves the
// order the user performed the actions in, which matters when a move is
// followed by a flag change on the same message.
func (s *Store) ClaimNext(now time.Time) (*Op, error) {
	row := s.db.QueryRow(`
		SELECT id, account_id, op, payload_json, state, not_before_unix,
		       attempt, COALESCE(last_attempt_unix, 0), COALESCE(last_error, ''), created_unix
		FROM pending_ops
		WHERE state IN (?, ?) AND not_before_unix <= ?
		ORDER BY created_unix ASC, rowid ASC
		LIMIT 1`,
		string(StatePending), string(StateFailed), now.Unix(),
	)

	op, err := scanOp(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// Claim it. The state guard makes the claim safe against a second drainer.
	res, err := s.db.Exec(
		`UPDATE pending_ops SET state = ? WHERE id = ? AND state IN (?, ?)`,
		string(StateRunning), op.ID, string(StatePending), string(StateFailed),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to claim op: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to read claim result: %w", err)
	}
	if n == 0 {
		// Someone else took it (or it was cancelled) between select and update.
		return nil, nil
	}

	op.State = StateRunning
	return op, nil
}

// Complete removes a successfully executed op.
func (s *Store) Complete(id string) error {
	if _, err := s.db.Exec(`DELETE FROM pending_ops WHERE id = ?`, id); err != nil {
		return fmt.Errorf("failed to complete op: %w", err)
	}
	return nil
}

// Fail records an execution failure and schedules a retry. The op returns to
// the queue rather than being dropped: a delete the server never heard about
// would otherwise come back on the next sync.
func (s *Store) Fail(id string, cause error, retryAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE pending_ops
		SET state = ?, attempt = attempt + 1, last_attempt_unix = ?,
		    last_error = ?, not_before_unix = ?
		WHERE id = ?`,
		string(StateFailed), time.Now().Unix(), cause.Error(), retryAt.Unix(), id,
	)
	if err != nil {
		return fmt.Errorf("failed to record op failure: %w", err)
	}
	return nil
}

// Abandon drops an op that can no longer succeed (its folder or account is
// gone, its payload is unreadable, or it has exhausted its retries).
func (s *Store) Abandon(id string) error {
	if _, err := s.db.Exec(`DELETE FROM pending_ops WHERE id = ?`, id); err != nil {
		return fmt.Errorf("failed to abandon op: %w", err)
	}
	return nil
}

// ReleaseRunning returns every running op to pending. Called at startup: a
// running row means the process died mid-execution, and the op must be retried
// rather than left stranded.
//
// Retrying may repeat an operation the server already applied. All op types
// here are idempotent enough for that to be safe — copying a message that is
// already in the destination, re-setting a flag that is already set, or
// deleting a UID that is already gone are all no-ops or benign.
func (s *Store) ReleaseRunning() (int, error) {
	res, err := s.db.Exec(
		`UPDATE pending_ops SET state = ? WHERE state = ?`,
		string(StatePending), string(StateRunning),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to release running ops: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to read release result: %w", err)
	}
	return int(n), nil
}

// PendingCount reports how many ops are still queued, for shutdown flushing
// and diagnostics.
func (s *Store) PendingCount() (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM pending_ops`).Scan(&n); err != nil {
		return 0, fmt.Errorf("failed to count pending ops: %w", err)
	}
	return n, nil
}

// Get returns a single op, or nil when it is no longer queued.
func (s *Store) Get(id string) (*Op, error) {
	row := s.db.QueryRow(`
		SELECT id, account_id, op, payload_json, state, not_before_unix,
		       attempt, COALESCE(last_attempt_unix, 0), COALESCE(last_error, ''), created_unix
		FROM pending_ops WHERE id = ?`, id)

	op, err := scanOp(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return op, nil
}

type scannable interface {
	Scan(dest ...interface{}) error
}

func scanOp(row scannable) (*Op, error) {
	var (
		op                             Op
		opType, state, payload         string
		notBefore, lastAttempt, create int64
	)
	err := row.Scan(
		&op.ID, &op.AccountID, &opType, &payload, &state,
		&notBefore, &op.Attempt, &lastAttempt, &op.LastError, &create,
	)
	if err != nil {
		return nil, err
	}

	decoded, err := decodePayload(payload)
	if err != nil {
		return nil, err
	}

	op.Type = Type(opType)
	op.State = State(state)
	op.Payload = decoded
	op.NotBefore = time.Unix(notBefore, 0)
	op.CreatedAt = time.Unix(create, 0)
	if lastAttempt > 0 {
		op.LastAttempt = time.Unix(lastAttempt, 0)
	}
	return &op, nil
}
