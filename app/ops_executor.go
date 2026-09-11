package app

import (
	"context"
	"fmt"
	"time"

	"github.com/hkdb/aerion/internal/logging"
	"github.com/hkdb/aerion/internal/message"
	"github.com/hkdb/aerion/internal/ops"
	"github.com/hkdb/aerion/internal/undo"
	wailsRuntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

// opDeferWindow is how long a queued mutation waits before the drainer sends
// it to the server.
//
// It exists so an immediate undo is a local cancel rather than a reversal: no
// network round trip, no waiting for a sync, and nothing to go wrong. The cost
// is that the server — and therefore the user's other devices — lag the local
// view by this much. Five seconds is Gmail's shortest default and comfortably
// covers a reflexive Ctrl+Z.
const opDeferWindow = 5 * time.Second

// enqueueOp queues a server-side mutation and nudges the drainer. Callers have
// already applied the change to the local store; this is the half that has to
// reach IMAP.
func (a *App) enqueueOp(accountID string, opType ops.Type, payload ops.Payload) (string, error) {
	id, err := a.opStore.Enqueue(accountID, opType, payload, time.Now().Add(opDeferWindow))
	if err != nil {
		return "", err
	}
	if a.opDrainer != nil {
		a.opDrainer.Wake()
	}
	return id, nil
}

// cancelOp removes a queued op if the drainer hasn't claimed it yet. Returns
// false when the op is already running or done, meaning the server has been
// told and the caller must reverse rather than cancel.
func (a *App) cancelOp(id string) (bool, error) {
	if id == "" || a.opStore == nil {
		return false, nil
	}
	return a.opStore.Cancel(id)
}

// opRefs captures the local id, server UID and Message-ID of each message an
// op will act on.
//
// The UID has to be captured here, at enqueue time: a move reparks the local
// row immediately, so by the time the op drains the row no longer carries the
// UID the server knows it by in the source folder.
func opRefs(messages []*message.Message) []ops.MessageRef {
	refs := make([]ops.MessageRef, 0, len(messages))
	for _, m := range messages {
		refs = append(refs, ops.MessageRef{
			ID:        m.ID,
			UID:       m.UID,
			MessageID: m.MessageID,
		})
	}
	return refs
}

// enqueueFlagOps queues one flag op per folder. Failures are logged rather
// than returned: the local flag change has already been applied and shown, and
// a flag that fails to reach the server self-corrects on the next sync.
func (a *App) enqueueFlagOps(byFolder map[string][]*message.Message, flagType string, value bool) {
	log := logging.WithComponent("app.ops")

	for folderID, msgs := range byFolder {
		if len(msgs) == 0 {
			continue
		}
		if _, err := a.enqueueOp(msgs[0].AccountID, ops.TypeFlag, ops.Payload{
			Messages:       opRefs(msgs),
			SourceFolderID: folderID,
			FlagType:       flagType,
			FlagValue:      value,
		}); err != nil {
			log.Error().Err(err).Str("folderID", folderID).Str("flag", flagType).Msg("Failed to queue flag change")
		}
	}
}

// undoOriginals records where each message was, and at which UID, before a
// move — everything needed to put it back if the queued op is cancelled
// before it reaches the server.
func undoOriginals(messages []*message.Message) []undo.MessageUID {
	out := make([]undo.MessageUID, 0, len(messages))
	for _, m := range messages {
		out = append(out, undo.MessageUID{ID: m.ID, UID: m.UID})
	}
	return out
}

// emitFolderCounts recomputes and broadcasts one folder's totals.
func (a *App) emitFolderCounts(folderID string) {
	log := logging.WithComponent("app.ops")

	unread, err := a.messageStore.CountUnreadByFolder(folderID)
	if err != nil {
		log.Warn().Err(err).Str("folderID", folderID).Msg("Failed to count unread messages")
		return
	}
	total, err := a.messageStore.CountByFolder(folderID)
	if err != nil {
		log.Warn().Err(err).Str("folderID", folderID).Msg("Failed to count messages")
		return
	}
	if err := a.folderStore.UpdateCounts(folderID, total, unread); err != nil {
		log.Warn().Err(err).Str("folderID", folderID).Msg("Failed to update folder counts")
		return
	}
	wailsRuntime.EventsEmit(a.ctx, "folders:countsChanged", map[string]int{folderID: unread})
}

// opMessages rebuilds the message values an IMAP helper needs from an op
// payload. Deliberately reconstructed from the payload rather than re-read
// from the store: the stored rows have moved on, and the UID recorded at
// enqueue time is the one the server still recognizes.
func opMessages(accountID string, refs []ops.MessageRef) []*message.Message {
	msgs := make([]*message.Message, 0, len(refs))
	for _, r := range refs {
		msgs = append(msgs, &message.Message{
			ID:        r.ID,
			AccountID: accountID,
			UID:       r.UID,
			MessageID: r.MessageID,
		})
	}
	return msgs
}

// Execute implements ops.Executor. Called by the drainer, one op at a time.
func (a *App) Execute(ctx context.Context, op *ops.Op) error {
	switch op.Type {
	case ops.TypeMove:
		return a.execMoveOp(ctx, op)
	case ops.TypeFlag:
		return a.execFlagOp(op)
	case ops.TypeDelete:
		return a.execDeleteOp(op)
	case ops.TypeCopy:
		return a.execCopyOp(op)
	default:
		return fmt.Errorf("%w: unknown op type %q", ops.ErrUnrecoverable, op.Type)
	}
}

// Compensate implements ops.Executor. Called when an op has been abandoned and
// will never reach the server.
//
// Every mutation here applied to the local store first, on the assumption the
// server would follow. When it can't, that assumption has to be withdrawn —
// otherwise the local store and the server disagree permanently, and the next
// sync makes it visible in the worst way: an abandoned move leaves the message
// parked in the destination folder locally while the server still has it in
// the source, so the next source-folder sync re-inserts it and the message
// shows up in two places at once.
func (a *App) Compensate(_ context.Context, op *ops.Op, cause error) {
	log := logging.WithComponent("app.ops")

	switch op.Type {
	case ops.TypeMove:
		a.compensateMove(op, cause)

	case ops.TypeFlag, ops.TypeCopy, ops.TypeDelete:
		// These self-heal. A flag the server never saw is corrected by the next
		// sync; a copy that never happened simply isn't there; a permanent
		// delete that never happened means the message comes back on sync,
		// which is the truthful outcome. Tell the user rather than silently
		// letting the state flip back under them.
		log.Error().Err(cause).Str("op", op.Describe()).Msg("Op abandoned; local state will resync")
		a.notifyOpFailed(op, cause, 0)

	default:
		log.Error().Err(cause).Str("op", op.Describe()).Msg("Op abandoned with no compensation path")
	}
}

// compensateMove puts an abandoned move's messages back where they came from.
func (a *App) compensateMove(op *ops.Op, cause error) {
	log := logging.WithComponent("app.ops")

	entries := make([]message.FolderUID, 0, len(op.Payload.Messages))
	for _, m := range op.Payload.Messages {
		entries = append(entries, message.FolderUID{
			ID:       m.ID,
			FolderID: op.Payload.SourceFolderID,
			UID:      m.UID,
		})
	}

	restored, err := a.messageStore.RestoreParkedMessages(entries, op.Payload.DestFolderID)
	if err != nil {
		log.Error().Err(err).Str("op", op.Describe()).Msg("Failed to roll back abandoned move")
		return
	}

	log.Warn().Err(cause).
		Str("op", op.Describe()).
		Int("restored", restored).
		Int("total", len(entries)).
		Msg("Move abandoned; local rows rolled back to source folder")

	if restored > 0 {
		wailsRuntime.EventsEmit(a.ctx, "messages:moved", map[string]interface{}{
			"messageIds":   op.Payload.MessageIDs(),
			"destFolderId": op.Payload.SourceFolderID,
		})
		a.emitFolderCounts(op.Payload.SourceFolderID)
		a.emitFolderCounts(op.Payload.DestFolderID)
	}

	a.notifyOpFailed(op, cause, restored)
}

// notifyOpFailed tells the frontend an action couldn't be completed, so the
// user finds out from a message rather than from the message reappearing.
func (a *App) notifyOpFailed(op *ops.Op, cause error, restored int) {
	folderName := ""
	if op.Payload.DestFolderID != "" {
		if f, err := a.folderStore.Get(op.Payload.DestFolderID); err == nil && f != nil {
			folderName = f.Name
		}
	}

	wailsRuntime.EventsEmit(a.ctx, "ops:failed", map[string]interface{}{
		"op":           string(op.Type),
		"messageCount": len(op.Payload.Messages),
		"folderName":   folderName,
		"reverted":     restored > 0,
		"error":        cause.Error(),
	})
}

// execMoveOp performs the server half of a move, then resyncs the destination
// so the moved messages pick up their real UIDs.
func (a *App) execMoveOp(ctx context.Context, op *ops.Op) error {
	log := logging.WithComponent("app.ops")

	destFolder, err := a.folderStore.Get(op.Payload.DestFolderID)
	if err != nil || destFolder == nil {
		return fmt.Errorf("%w: destination folder %s not found", ops.ErrUnrecoverable, op.Payload.DestFolderID)
	}

	msgs := opMessages(op.AccountID, op.Payload.Messages)
	if len(msgs) == 0 {
		return nil
	}

	if err := a.moveMessagesToIMAP(msgs, op.Payload.SourceFolderID, destFolder); err != nil {
		return fmt.Errorf("move to %s failed: %w", destFolder.Path, err)
	}

	// Sync the destination so the moved messages get their real UIDs. Clear the
	// debounce timestamp first so this request isn't silently dropped.
	syncKey := op.AccountID + ":" + destFolder.ID
	a.syncMu.Lock()
	delete(a.syncLastRequest, syncKey)
	a.syncMu.Unlock()

	syncErr := a.SyncFolder(op.AccountID, destFolder.ID)
	if syncErr != nil && syncErr != context.Canceled {
		log.Warn().Err(syncErr).Str("destFolderID", destFolder.ID).Msg("Failed to sync destination folder after move")
	}

	// Reap whatever is still parked at a negative UID. The sync rebinds every
	// message it can match by Message-ID, so this only catches rows that
	// couldn't be reconciled. Skipped on sync failure: the parked row is the
	// only local copy of that message, with its body and attachments.
	if syncErr == nil {
		if err := a.messageStore.DeleteTempUIDs(destFolder.ID); err != nil {
			log.Warn().Err(err).Str("destFolderID", destFolder.ID).Msg("Failed to clean up temp UIDs after move")
		}
	}

	// The IMAP move succeeded even if the follow-up sync didn't; reporting
	// failure here would retry the move and copy the message twice.
	return nil
}

func (a *App) execFlagOp(op *ops.Op) error {
	msgs := opMessages(op.AccountID, op.Payload.Messages)
	if len(msgs) == 0 {
		return nil
	}
	if err := a.syncFlagsToIMAP(msgs, op.Payload.SourceFolderID, op.Payload.FlagType, op.Payload.FlagValue); err != nil {
		return fmt.Errorf("flag %s=%v failed: %w", op.Payload.FlagType, op.Payload.FlagValue, err)
	}
	return nil
}

func (a *App) execDeleteOp(op *ops.Op) error {
	msgs := opMessages(op.AccountID, op.Payload.Messages)
	if len(msgs) == 0 {
		return nil
	}
	if err := a.deleteMessagesFromIMAP(msgs, op.Payload.SourceFolderID); err != nil {
		return fmt.Errorf("permanent delete failed: %w", err)
	}
	return nil
}

func (a *App) execCopyOp(op *ops.Op) error {
	destFolder, err := a.folderStore.Get(op.Payload.DestFolderID)
	if err != nil || destFolder == nil {
		return fmt.Errorf("%w: destination folder %s not found", ops.ErrUnrecoverable, op.Payload.DestFolderID)
	}

	msgs := opMessages(op.AccountID, op.Payload.Messages)
	if len(msgs) == 0 {
		return nil
	}
	if err := a.copyMessagesToIMAP(msgs, op.Payload.SourceFolderID, destFolder); err != nil {
		return fmt.Errorf("copy to %s failed: %w", destFolder.Path, err)
	}
	return nil
}
