package undo

import (
	"context"
	"fmt"

	"github.com/emersion/go-imap/v2"
	imapPkg "github.com/hkdb/aerion/internal/imap"
)

// UndoContext provides dependencies for undo operations
type UndoContext interface {
	// GetIMAPConnectionForUndo returns an IMAP client for the account
	GetIMAPConnectionForUndo(ctx context.Context, accountID string) (*imapPkg.Client, func(), error)
	// UpdateLocalFlags updates flags in local database
	UpdateLocalFlags(messageIDs []string, isRead, isStarred *bool) error
	// MoveLocalMessages moves messages in local database
	MoveLocalMessages(messageIDs []string, folderID string) error
	// DeleteLocalMessages deletes messages from local database
	DeleteLocalMessages(messageIDs []string) error
	// ResolveMessagesInFolder returns the local DB ids still present in folderID,
	// preferring the supplied local ids and falling back to RFC822 Message-ID
	// lookup for any that no longer resolve.
	ResolveMessagesInFolder(accountID, folderID string, localIDs, rfc822MessageIDs []string) ([]string, error)
	// MoveMessagesToFolderWithoutUndo moves messages using the full move pipeline
	// (IMAP + local DB) without pushing a new command onto the undo stack.
	MoveMessagesToFolderWithoutUndo(messageIDs []string, destFolderID string) error
}

// FlagChangeCommand handles read/star flag changes
type FlagChangeCommand struct {
	BaseCommand
	ctx           context.Context
	undoCtx       UndoContext
	accountID     string
	folderPath    string
	messageIDs    []string
	uids          []uint32
	flagType      string // "read" or "starred"
	previousState bool   // What was the state before
}

// NewFlagChangeCommand creates a new FlagChangeCommand
func NewFlagChangeCommand(
	ctx context.Context,
	undoCtx UndoContext,
	accountID, folderPath string,
	messageIDs []string,
	uids []uint32,
	flagType string,
	previousState bool,
	description string,
) *FlagChangeCommand {
	return &FlagChangeCommand{
		BaseCommand:   NewBaseCommand(description),
		ctx:           ctx,
		undoCtx:       undoCtx,
		accountID:     accountID,
		folderPath:    folderPath,
		messageIDs:    messageIDs,
		uids:          uids,
		flagType:      flagType,
		previousState: previousState,
	}
}

// Execute performs the action (already done at creation time)
func (c *FlagChangeCommand) Execute() error { return nil }

// Undo reverses the flag change
func (c *FlagChangeCommand) Undo() error {
	// Get IMAP connection
	client, release, err := c.undoCtx.GetIMAPConnectionForUndo(c.ctx, c.accountID)
	if err != nil {
		return fmt.Errorf("failed to get IMAP connection: %w", err)
	}
	defer release()

	// Select mailbox
	if _, err := client.SelectMailbox(c.ctx, c.folderPath); err != nil {
		return fmt.Errorf("failed to select mailbox: %w", err)
	}

	// Convert UIDs
	imapUIDs := make([]imap.UID, len(c.uids))
	for i, uid := range c.uids {
		imapUIDs[i] = imap.UID(uid)
	}

	// Determine flag
	var flag imap.Flag
	switch c.flagType {
	case "read":
		flag = imap.FlagSeen
	case "starred":
		flag = imap.FlagFlagged
	default:
		return fmt.Errorf("unknown flag type: %s", c.flagType)
	}

	// Restore previous state on IMAP
	if c.previousState {
		if err := client.AddMessageFlags(imapUIDs, []imap.Flag{flag}); err != nil {
			return fmt.Errorf("failed to add flags: %w", err)
		}
	} else {
		if err := client.RemoveMessageFlags(imapUIDs, []imap.Flag{flag}); err != nil {
			return fmt.Errorf("failed to remove flags: %w", err)
		}
	}

	// Update local database
	var isRead, isStarred *bool
	switch c.flagType {
	case "read":
		isRead = &c.previousState
	case "starred":
		isStarred = &c.previousState
	}
	if err := c.undoCtx.UpdateLocalFlags(c.messageIDs, isRead, isStarred); err != nil {
		return fmt.Errorf("failed to update local flags: %w", err)
	}

	return nil
}

// MoveCommand handles moving messages between folders
type MoveCommand struct {
	BaseCommand
	undoCtx          UndoContext
	accountID        string
	localMessageIDs  []string // Local DB ids — stable across the move (see Store.ReconcileMovedMessage)
	rfc822MessageIDs []string // Fallback lookup key for rows that couldn't be reconciled
	sourceFolderID   string
	destFolderID     string
	settled          func() error // Blocks until the move's IMAP + destination sync finished
}

// NewMoveCommand creates a new MoveCommand.
//
// settled blocks until the originating move has finished its IMAP work and the
// destination folder has synced, so Undo never tries to reverse a move the
// server hasn't been told about yet. It may be nil, in which case Undo
// proceeds immediately.
func NewMoveCommand(
	undoCtx UndoContext,
	accountID string,
	localMessageIDs []string,
	rfc822MessageIDs []string,
	sourceFolderID string,
	destFolderID string,
	description string,
	settled func() error,
) *MoveCommand {
	return &MoveCommand{
		BaseCommand:      NewBaseCommand(description),
		undoCtx:          undoCtx,
		accountID:        accountID,
		localMessageIDs:  localMessageIDs,
		rfc822MessageIDs: rfc822MessageIDs,
		sourceFolderID:   sourceFolderID,
		destFolderID:     destFolderID,
		settled:          settled,
	}
}

// Execute performs the action (already done at creation time)
func (c *MoveCommand) Execute() error { return nil }

// Undo reverses the move using the standard move pipeline.
//
// The messages are addressed by the local ids captured when the move was made.
// Those ids survive the round trip because the destination sync rebinds the
// parked row rather than replacing it; the Message-ID lookup is kept only as a
// fallback for rows that couldn't be reconciled (a message with no Message-ID
// header, or a server that never reported the copy).
func (c *MoveCommand) Undo() error {
	// Reversing a move requires a real server UID, which only exists once the
	// IMAP COPY has landed and the destination folder has synced. Undoing
	// before that point would move the local row back while leaving the
	// message in the destination folder on the server — the next sync would
	// silently undo the undo.
	if c.settled != nil {
		if err := c.settled(); err != nil {
			return fmt.Errorf("move still in flight: %w", err)
		}
	}

	localMsgIDs, err := c.undoCtx.ResolveMessagesInFolder(
		c.accountID, c.destFolderID, c.localMessageIDs, c.rfc822MessageIDs,
	)
	if err != nil {
		return fmt.Errorf("failed to find messages: %w", err)
	}
	if len(localMsgIDs) == 0 {
		return fmt.Errorf("messages not found in destination folder")
	}

	// Reuse the full move pipeline (IMAP + local DB + events). Undoing must not
	// itself become an undoable action, or a second Ctrl+Z would redo the move.
	return c.undoCtx.MoveMessagesToFolderWithoutUndo(localMsgIDs, c.sourceFolderID)
}
