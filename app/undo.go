package app

import (
	"context"
	"fmt"

	"github.com/hkdb/aerion/internal/imap"
	"github.com/hkdb/aerion/internal/message"
	"github.com/hkdb/aerion/internal/undo"
	wailsRuntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

// ============================================================================
// Undo API - Exposed to frontend via Wails bindings
// ============================================================================

// Undo reverses the most recent undoable action
// Returns the description of what was undone, or error if nothing to undo
func (a *App) Undo() (string, error) {
	cmd := a.undoStack.Pop()
	if cmd == nil {
		return "", fmt.Errorf("nothing to undo")
	}

	if err := cmd.Undo(); err != nil {
		return "", fmt.Errorf("undo failed: %w", err)
	}

	// Emit event to refresh UI
	wailsRuntime.EventsEmit(a.ctx, "undo:completed", cmd.Description())

	return cmd.Description(), nil
}

// CanUndo returns true if there's an action that can be undone
func (a *App) CanUndo() bool {
	return a.undoStack.CanUndo()
}

// GetUndoDescription returns the description of what would be undone
func (a *App) GetUndoDescription() string {
	cmd := a.undoStack.Peek()
	if cmd == nil {
		return ""
	}
	return cmd.Description()
}

// ============================================================================
// UndoContext Implementation - Required for undo.Command operations
// ============================================================================

// GetIMAPConnectionForUndo implements undo.UndoContext
func (a *App) GetIMAPConnectionForUndo(ctx context.Context, accountID string) (*imap.Client, func(), error) {
	poolConn, err := a.imapPool.GetConnection(ctx, accountID)
	if err != nil {
		return nil, nil, err
	}
	return poolConn.Client(), func() { a.imapPool.Release(poolConn) }, nil
}

// UpdateLocalFlags implements undo.UndoContext
func (a *App) UpdateLocalFlags(messageIDs []string, isRead, isStarred *bool) error {
	err := a.messageStore.UpdateFlagsBatch(messageIDs, isRead, isStarred)
	if err != nil {
		return err
	}
	// Emit a per-flag event so each listener sees the typed payload it
	// expects. UpdateLocalFlags is called by undo commands; today each
	// command flips exactly one flag (either read or starred), so in
	// practice only one branch fires. The if/if (not if/else) shape
	// covers a hypothetical future undo that combines both without
	// changing this call site.
	if isRead != nil {
		wailsRuntime.EventsEmit(a.ctx, "messages:readChanged", map[string]interface{}{
			"messageIds": messageIDs,
			"isRead":     *isRead,
		})
	}
	if isStarred != nil {
		wailsRuntime.EventsEmit(a.ctx, "messages:starredChanged", map[string]interface{}{
			"messageIds": messageIDs,
			"isStarred":  *isStarred,
		})
	}
	return nil
}

// MoveLocalMessages implements undo.UndoContext
func (a *App) MoveLocalMessages(messageIDs []string, folderID string) error {
	// Get the source folder IDs before moving (for count updates)
	messages, err := a.messageStore.GetByIDs(messageIDs)
	if err != nil {
		return fmt.Errorf("failed to get messages: %w", err)
	}

	// Group by source folder
	sourceFolderIDs := make(map[string]bool)
	for _, msg := range messages {
		sourceFolderIDs[msg.FolderID] = true
	}

	// Move messages in database
	err = a.messageStore.MoveMessages(messageIDs, folderID)
	if err != nil {
		return err
	}

	// Emit messages:moved event
	wailsRuntime.EventsEmit(a.ctx, "messages:moved", map[string]interface{}{
		"messageIds":   messageIDs,
		"destFolderId": folderID,
	})

	// Update folder counts for all affected folders (source + destination)
	go func() {
		defer recoverPanic("app.undo", "update folder counts")
		folderCounts := make(map[string]int)

		// Update source folders
		for sourceFolderID := range sourceFolderIDs {
			unreadCount, err := a.messageStore.CountUnreadByFolder(sourceFolderID)
			if err == nil {
				folderObj, err := a.folderStore.Get(sourceFolderID)
				if err == nil && folderObj != nil {
					totalCount, _ := a.messageStore.CountByFolder(sourceFolderID)
					_ = a.folderStore.UpdateCounts(sourceFolderID, totalCount, unreadCount)
					folderCounts[sourceFolderID] = unreadCount
				}
			}
		}

		// Update destination folder
		unreadCount, err := a.messageStore.CountUnreadByFolder(folderID)
		if err == nil {
			folderObj, err := a.folderStore.Get(folderID)
			if err == nil && folderObj != nil {
				totalCount, _ := a.messageStore.CountByFolder(folderID)
				_ = a.folderStore.UpdateCounts(folderID, totalCount, unreadCount)
				folderCounts[folderID] = unreadCount
			}
		}

		if len(folderCounts) > 0 {
			wailsRuntime.EventsEmit(a.ctx, "folders:countsChanged", folderCounts)
		}
	}()

	return nil
}

// DeleteLocalMessages implements undo.UndoContext
func (a *App) DeleteLocalMessages(messageIDs []string) error {
	err := a.messageStore.DeleteBatch(messageIDs)
	if err == nil {
		wailsRuntime.EventsEmit(a.ctx, "messages:deleted", messageIDs)
	}
	return err
}

// ResolveMessagesInFolder implements undo.UndoContext.
//
// The local ids are the fast path: they survive a move because the destination
// sync rebinds the parked row instead of replacing it. Only ids that no longer
// resolve to a row in folderID fall back to the RFC822 Message-ID lookup — a
// message with no Message-ID header, or one whose parked row was reaped before
// the server reported the copy.
func (a *App) ResolveMessagesInFolder(accountID, folderID string, localIDs, rfc822MessageIDs []string) ([]string, error) {
	resolved, err := a.messageStore.FilterIDsInFolder(folderID, localIDs)
	if err != nil {
		return nil, err
	}
	if len(resolved) == len(localIDs) {
		return resolved, nil
	}

	byID, err := a.messageStore.GetIDsByMessageIDs(accountID, folderID, rfc822MessageIDs)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool, len(resolved))
	for _, id := range resolved {
		seen[id] = true
	}
	for _, id := range byID {
		if !seen[id] {
			seen[id] = true
			resolved = append(resolved, id)
		}
	}
	return resolved, nil
}

// CancelPendingOp implements undo.UndoContext.
func (a *App) CancelPendingOp(opID string) (bool, error) {
	return a.cancelOp(opID)
}

// RestoreMessages implements undo.UndoContext.
//
// Only reached when the move's server op was cancelled before it ran, so the
// messages still carry these UIDs on the server and putting them back is a
// purely local edit.
func (a *App) RestoreMessages(originals []undo.MessageUID, folderID string) error {
	entries := make([]message.FolderUID, 0, len(originals))
	ids := make([]string, 0, len(originals))
	for _, o := range originals {
		entries = append(entries, message.FolderUID{ID: o.ID, FolderID: folderID, UID: o.UID})
		ids = append(ids, o.ID)
	}
	if err := a.messageStore.RestoreMessages(entries); err != nil {
		return err
	}

	wailsRuntime.EventsEmit(a.ctx, "messages:moved", map[string]interface{}{
		"messageIds":   ids,
		"destFolderId": folderID,
	})
	a.emitFolderCounts(folderID)
	return nil
}

// MoveMessagesToFolderWithoutUndo implements undo.UndoContext.
// Delegates to the standard MoveToFolder pipeline (IMAP + local DB + events)
// with undo recording suppressed — an undo must not be undoable itself.
func (a *App) MoveMessagesToFolderWithoutUndo(messageIDs []string, destFolderID string) error {
	return a.moveToFolder(messageIDs, destFolderID, false)
}
