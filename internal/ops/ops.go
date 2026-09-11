// Package ops provides a durable outbox for mailbox mutations.
//
// Every action that changes server state (move, flag, delete, append) applies
// to the local store immediately and enqueues its server half here. A drainer
// executes queued ops against IMAP, in order, per account.
//
// The indirection buys three things a fire-and-forget goroutine cannot:
//
//   - Undo of a still-pending op is a local cancel. No network, no waiting for
//     a round trip that hasn't happened yet.
//   - Ops survive quit and crash. Previously a delete whose goroutine hadn't
//     run yet evaporated on exit, and the next sync resurrected the message.
//   - Offline queues instead of failing.
package ops

import (
	"encoding/json"
	"fmt"
	"time"
)

// Type identifies what an op does. Values are persisted, so they are part of
// the on-disk format — add, don't rename.
type Type string

const (
	// TypeMove copies messages to a destination folder and removes them from
	// the source. Trash, archive, spam and move-to-folder all land here.
	TypeMove Type = "move"
	// TypeFlag adds or removes a single IMAP flag on messages in one folder.
	TypeFlag Type = "flag"
	// TypeDelete permanently removes messages from a folder.
	TypeDelete Type = "delete"
	// TypeCopy copies messages to another folder, leaving the originals.
	TypeCopy Type = "copy"
	// TypeAppend uploads a message to a folder (cross-account move, drafts).
	TypeAppend Type = "append"
)

// State is an op's position in the drain lifecycle.
type State string

const (
	// StatePending is queued and not yet claimed. Only ops in this state can
	// be cancelled — the whole point of the defer window.
	StatePending State = "pending"
	// StateRunning has been claimed by the drainer and may have already
	// touched the server. Cancelling is no longer safe.
	StateRunning State = "running"
	// StateFailed errored and is waiting for a retry.
	StateFailed State = "failed"
)

// MessageRef identifies a message on both sides of the divide: the stable
// local row id, and the server UID it had when the op was created.
//
// The UID is captured at enqueue time because a move reparks the local row
// immediately — by the time the op drains, the row no longer carries the UID
// the server knows it by in the source folder.
type MessageRef struct {
	ID        string `json:"id"`
	UID       uint32 `json:"uid"`
	MessageID string `json:"messageId,omitempty"`
}

// Payload carries op-specific arguments. Fields are shared across types rather
// than split per type so the table stays one shape; each type documents which
// fields it reads.
type Payload struct {
	// Messages the op acts on. Used by every type except append.
	Messages []MessageRef `json:"messages,omitempty"`

	// SourceFolderID is the folder the messages are in on the server.
	// move, flag, delete, copy.
	SourceFolderID string `json:"sourceFolderId,omitempty"`

	// DestFolderID is where they are going. move, copy, append.
	DestFolderID string `json:"destFolderId,omitempty"`

	// FlagType ("read" or "starred") and FlagValue. flag only.
	FlagType  string `json:"flagType,omitempty"`
	FlagValue bool   `json:"flagValue,omitempty"`

	// RawPath points at a file holding the RFC822 bytes to upload. append only.
	RawPath string `json:"rawPath,omitempty"`
}

// Op is one queued mutation.
type Op struct {
	ID          string
	AccountID   string
	Type        Type
	Payload     Payload
	State       State
	NotBefore   time.Time
	Attempt     int
	LastAttempt time.Time
	LastError   string
	CreatedAt   time.Time
}

// Describe returns a short human-readable summary, for logs and errors.
func (o *Op) Describe() string {
	return fmt.Sprintf("%s op %s (%d message(s), attempt %d)",
		o.Type, o.ID, len(o.Payload.Messages), o.Attempt)
}

func (p Payload) encode() (string, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("failed to encode op payload: %w", err)
	}
	return string(b), nil
}

func decodePayload(s string) (Payload, error) {
	var p Payload
	if err := json.Unmarshal([]byte(s), &p); err != nil {
		return p, fmt.Errorf("failed to decode op payload: %w", err)
	}
	return p, nil
}

// MessageIDs returns the local row ids the op acts on.
func (p Payload) MessageIDs() []string {
	ids := make([]string, 0, len(p.Messages))
	for _, m := range p.Messages {
		ids = append(ids, m.ID)
	}
	return ids
}
