package message

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/hkdb/aerion/internal/database"
)

// newIdentityTestStore opens a migrated temp DB seeded with one account and
// two folders (an inbox and a trash), mirroring the scaffolding in
// store_body_failed_test.go.
func newIdentityTestStore(t *testing.T) (*Store, *AttachmentStore, string, string, string) {
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

	const accountID = "acct-1"
	const inboxID = "folder-inbox"
	const trashID = "folder-trash"

	if _, err := db.Exec(
		`INSERT INTO accounts (id, name, email, imap_host, smtp_host, username)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		accountID, "Test", "test@example.com", "imap.example.com", "smtp.example.com", "test@example.com",
	); err != nil {
		t.Fatalf("seed account: %v", err)
	}
	for _, f := range []struct{ id, name, path, kind string }{
		{inboxID, "INBOX", "INBOX", "inbox"},
		{trashID, "Trash", "Trash", "trash"},
	} {
		if _, err := db.Exec(
			`INSERT INTO folders (id, account_id, name, path, folder_type)
			 VALUES (?, ?, ?, ?, ?)`,
			f.id, accountID, f.name, f.path, f.kind,
		); err != nil {
			t.Fatalf("seed folder %s: %v", f.id, err)
		}
	}

	return NewStore(db), NewAttachmentStore(db), accountID, inboxID, trashID
}

// seedFetchedMessage creates a message that has already had its body and one
// attachment downloaded — the state a message is in when a user deletes it.
func seedFetchedMessage(t *testing.T, s *Store, as *AttachmentStore, accountID, folderID string) *Message {
	t.Helper()

	m := &Message{
		ID:          "msg-local-1",
		AccountID:   accountID,
		FolderID:    folderID,
		UID:         42,
		MessageID:   "<original@example.com>",
		Subject:     "Quarterly report",
		Date:        time.Now().UTC(),
		BodyText:    "the cached body",
		BodyFetched: true,
	}
	if err := s.Create(m); err != nil {
		t.Fatalf("Create message: %v", err)
	}
	if err := as.Create(&Attachment{
		ID:        "att-1",
		MessageID: m.ID,
		Filename:  "report.pdf",
		Size:      1024,
	}); err != nil {
		t.Fatalf("Create attachment: %v", err)
	}
	return m
}

func countAttachments(t *testing.T, s *Store, messageID string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(
		`SELECT COUNT(*) FROM attachments WHERE message_id = ?`, messageID,
	).Scan(&n); err != nil {
		t.Fatalf("count attachments: %v", err)
	}
	return n
}

// TestReconcileMovedMessage_KeepsIdentityBodyAndAttachments is the core
// guarantee of stable identity. A delete parks the row at a negative UID; when
// the destination sync reports the real UID, the parked row must be rebound
// rather than replaced. If it were replaced, the new row would carry a new
// UUID, the cached body would be gone, and the attachment rows would have been
// cascade-deleted along with the old row.
func TestReconcileMovedMessage_KeepsIdentityBodyAndAttachments(t *testing.T) {
	s, as, accountID, inboxID, trashID := newIdentityTestStore(t)
	original := seedFetchedMessage(t, s, as, accountID, inboxID)

	// The delete: local move parks the row at uid = -rowid.
	if err := s.MoveMessages([]string{original.ID}, trashID); err != nil {
		t.Fatalf("MoveMessages: %v", err)
	}
	var parkedUID int
	if err := s.db.QueryRow(`SELECT uid FROM messages WHERE id = ?`, original.ID).Scan(&parkedUID); err != nil {
		t.Fatalf("read parked uid: %v", err)
	}
	if parkedUID >= 0 {
		t.Fatalf("expected negative parked UID, got %d", parkedUID)
	}

	// The destination sync: same message, now with its real server UID and
	// headers only (no body — that's what a header fetch produces).
	synced := &Message{
		AccountID:   accountID,
		FolderID:    trashID,
		UID:         9001,
		MessageID:   "<original@example.com>",
		Subject:     "Quarterly report",
		Date:        original.Date,
		BodyFetched: false,
	}
	reconciled, err := s.ReconcileMovedMessage(synced)
	if err != nil {
		t.Fatalf("ReconcileMovedMessage: %v", err)
	}
	if !reconciled {
		t.Fatal("expected the synced message to reconcile onto the parked row")
	}

	// Identity survived.
	if synced.ID != original.ID {
		t.Errorf("identity changed across the move: got %q, want %q", synced.ID, original.ID)
	}

	// Exactly one row for this message — not the original plus a sync-inserted copy.
	var rows int
	if err := s.db.QueryRow(
		`SELECT COUNT(*) FROM messages WHERE folder_id = ? AND message_id = ?`,
		trashID, "<original@example.com>",
	).Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 1 {
		t.Errorf("expected 1 row in destination folder, got %d", rows)
	}

	// The real UID was adopted.
	got, err := s.Get(original.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.UID != 9001 {
		t.Errorf("uid = %d, want 9001", got.UID)
	}

	// The cached body and the attachment rows are still there — no re-download.
	if got.BodyText != "the cached body" {
		t.Errorf("body_text = %q, want the cached body preserved", got.BodyText)
	}
	if !got.BodyFetched {
		t.Error("body_fetched was reset by a header-only sync")
	}
	if n := countAttachments(t, s, original.ID); n != 1 {
		t.Errorf("attachment rows = %d, want 1 (cascade-deleted with the old row?)", n)
	}
}

// TestUpsert_PreservesIdentityAndBodyOnConflict covers the same two invariants
// on the ordinary re-sync path, where a message already present is upserted
// again from a header fetch.
func TestUpsert_PreservesIdentityAndBodyOnConflict(t *testing.T) {
	s, as, accountID, inboxID, _ := newIdentityTestStore(t)
	original := seedFetchedMessage(t, s, as, accountID, inboxID)

	resynced := &Message{
		AccountID:   accountID,
		FolderID:    inboxID,
		UID:         42, // same (folder_id, uid) -> conflict
		MessageID:   "<original@example.com>",
		Subject:     "Quarterly report (edited flags)",
		Date:        original.Date,
		IsRead:      true,
		BodyFetched: false,
	}
	if err := s.Upsert(resynced); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	if resynced.ID != original.ID {
		t.Errorf("Upsert reassigned identity: got %q, want %q", resynced.ID, original.ID)
	}

	got, err := s.Get(original.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.BodyText != "the cached body" || !got.BodyFetched {
		t.Errorf("header-only upsert discarded the cached body: body_text=%q body_fetched=%v",
			got.BodyText, got.BodyFetched)
	}
	// Server-owned fields still update.
	if !got.IsRead {
		t.Error("is_read was not updated from the server")
	}
	if got.Subject != "Quarterly report (edited flags)" {
		t.Errorf("subject = %q, want the resynced subject", got.Subject)
	}
	if n := countAttachments(t, s, original.ID); n != 1 {
		t.Errorf("attachment rows = %d, want 1", n)
	}
}

// TestUpsert_CarriesBodyWhenFetched confirms the body-preserving conflict
// clause doesn't block a real body fetch from landing.
func TestUpsert_CarriesBodyWhenFetched(t *testing.T) {
	s, as, accountID, inboxID, _ := newIdentityTestStore(t)
	original := seedFetchedMessage(t, s, as, accountID, inboxID)

	withBody := &Message{
		AccountID:   accountID,
		FolderID:    inboxID,
		UID:         42,
		MessageID:   "<original@example.com>",
		Subject:     "Quarterly report",
		Date:        original.Date,
		BodyText:    "a newer body",
		BodyFetched: true,
	}
	if err := s.Upsert(withBody); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, err := s.Get(original.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.BodyText != "a newer body" {
		t.Errorf("body_text = %q, want the newly fetched body", got.BodyText)
	}
}

// TestReconcileMovedMessage_Declines covers the cases that must fall through to
// the normal insert path.
func TestReconcileMovedMessage_Declines(t *testing.T) {
	s, as, accountID, inboxID, trashID := newIdentityTestStore(t)
	original := seedFetchedMessage(t, s, as, accountID, inboxID)
	if err := s.MoveMessages([]string{original.ID}, trashID); err != nil {
		t.Fatalf("MoveMessages: %v", err)
	}

	t.Run("no Message-ID header", func(t *testing.T) {
		ok, err := s.ReconcileMovedMessage(&Message{
			AccountID: accountID, FolderID: trashID, UID: 9001, MessageID: "",
		})
		if err != nil {
			t.Fatalf("ReconcileMovedMessage: %v", err)
		}
		if ok {
			t.Error("expected no reconcile without a Message-ID")
		}
	})

	t.Run("nothing parked", func(t *testing.T) {
		ok, err := s.ReconcileMovedMessage(&Message{
			AccountID: accountID, FolderID: trashID, UID: 9002,
			MessageID: "<never-seen@example.com>",
		})
		if err != nil {
			t.Fatalf("ReconcileMovedMessage: %v", err)
		}
		if ok {
			t.Error("expected no reconcile when no parked row matches")
		}
	})

	t.Run("uid already present", func(t *testing.T) {
		// A row already holding the incoming UID means the normal upsert owns
		// it; reconciling would violate UNIQUE(folder_id, uid).
		if err := s.Create(&Message{
			ID: "msg-other", AccountID: accountID, FolderID: trashID, UID: 9003,
			MessageID: "<other@example.com>", Date: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		ok, err := s.ReconcileMovedMessage(&Message{
			AccountID: accountID, FolderID: trashID, UID: 9003,
			MessageID: "<original@example.com>",
		})
		if err != nil {
			t.Fatalf("ReconcileMovedMessage: %v", err)
		}
		if ok {
			t.Error("expected no reconcile when the UID is already taken")
		}
	})
}

func TestFilterIDsInFolder(t *testing.T) {
	s, as, accountID, inboxID, trashID := newIdentityTestStore(t)
	original := seedFetchedMessage(t, s, as, accountID, inboxID)

	got, err := s.FilterIDsInFolder(inboxID, []string{original.ID, "does-not-exist"})
	if err != nil {
		t.Fatalf("FilterIDsInFolder: %v", err)
	}
	if len(got) != 1 || got[0] != original.ID {
		t.Errorf("got %v, want [%s]", got, original.ID)
	}

	// Wrong folder -> no match.
	got, err = s.FilterIDsInFolder(trashID, []string{original.ID})
	if err != nil {
		t.Fatalf("FilterIDsInFolder: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %v, want no matches in the other folder", got)
	}

	// Empty input is a no-op, not an error.
	if got, err := s.FilterIDsInFolder(inboxID, nil); err != nil || got != nil {
		t.Errorf("FilterIDsInFolder(nil) = %v, %v; want nil, nil", got, err)
	}
}

// TestRestoreParkedMessages_OnlyTouchesUntouchedRows covers the guard on the
// compensation path. An abandoned move can be rolled back minutes after the
// fact, by which time the user may have moved the message again or undone it
// already. Only rows still parked from that move may be restored.
func TestRestoreParkedMessages_OnlyTouchesUntouchedRows(t *testing.T) {
	s, as, accountID, inboxID, trashID := newIdentityTestStore(t)
	parked := seedFetchedMessage(t, s, as, accountID, inboxID)

	// A second message the user has since moved on from.
	movedOn := &Message{
		ID: "msg-local-2", AccountID: accountID, FolderID: inboxID, UID: 43,
		MessageID: "<second@example.com>", Date: time.Now().UTC(),
	}
	if err := s.Create(movedOn); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Both were trashed together...
	if err := s.MoveMessages([]string{parked.ID, movedOn.ID}, trashID); err != nil {
		t.Fatalf("MoveMessages: %v", err)
	}
	// ...but the second one has since been reconciled to a real UID (its move
	// did reach the server), so it is no longer parked.
	if _, err := s.db.Exec(`UPDATE messages SET uid = 500 WHERE id = ?`, movedOn.ID); err != nil {
		t.Fatalf("simulate reconcile: %v", err)
	}

	entries := []FolderUID{
		{ID: parked.ID, FolderID: inboxID, UID: 42},
		{ID: movedOn.ID, FolderID: inboxID, UID: 43},
	}
	restored, err := s.RestoreParkedMessages(entries, trashID)
	if err != nil {
		t.Fatalf("RestoreParkedMessages: %v", err)
	}
	if restored != 1 {
		t.Errorf("restored = %d, want 1 (only the still-parked row)", restored)
	}

	back, err := s.Get(parked.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if back.FolderID != inboxID || back.UID != 42 {
		t.Errorf("parked message = folder %s uid %d, want %s/42", back.FolderID, back.UID, inboxID)
	}

	untouched, err := s.Get(movedOn.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if untouched.FolderID != trashID || untouched.UID != 500 {
		t.Errorf("reconciled message was clobbered: folder %s uid %d, want %s/500",
			untouched.FolderID, untouched.UID, trashID)
	}
}
