package app

import (
	"strings"
	"testing"
)

func TestParseMailtoURL_Valid(t *testing.T) {
	result := ParseMailtoURL("mailto:test@example.com")
	if result == nil {
		t.Fatal("ParseMailtoURL returned nil, want non-nil")
	}
	if len(result.To) != 1 || result.To[0] != "test@example.com" {
		t.Errorf("To = %v, want [test@example.com]", result.To)
	}
}

func TestParseMailtoURL_WithParams(t *testing.T) {
	result := ParseMailtoURL("mailto:test@example.com?subject=Hello&body=World")
	if result == nil {
		t.Fatal("ParseMailtoURL returned nil, want non-nil")
	}
	if result.Subject != "Hello" {
		t.Errorf("Subject = %q, want %q", result.Subject, "Hello")
	}
	if result.Body != "World" {
		t.Errorf("Body = %q, want %q", result.Body, "World")
	}
}

func TestParseMailtoURL_MultiRecipient(t *testing.T) {
	result := ParseMailtoURL("mailto:a@b.com,c@d.com")
	if result == nil {
		t.Fatal("ParseMailtoURL returned nil, want non-nil")
	}
	if len(result.To) != 2 {
		t.Fatalf("len(To) = %d, want 2", len(result.To))
	}
	if result.To[0] != "a@b.com" {
		t.Errorf("To[0] = %q, want %q", result.To[0], "a@b.com")
	}
	if result.To[1] != "c@d.com" {
		t.Errorf("To[1] = %q, want %q", result.To[1], "c@d.com")
	}
}

func TestParseMailtoURL_WithCcBcc(t *testing.T) {
	result := ParseMailtoURL("mailto:a@b.com?cc=c@d.com&bcc=e@f.com")
	if result == nil {
		t.Fatal("ParseMailtoURL returned nil, want non-nil")
	}
	if len(result.Cc) != 1 || result.Cc[0] != "c@d.com" {
		t.Errorf("Cc = %v, want [c@d.com]", result.Cc)
	}
	if len(result.Bcc) != 1 || result.Bcc[0] != "e@f.com" {
		t.Errorf("Bcc = %v, want [e@f.com]", result.Bcc)
	}
}

func TestParseMailtoURL_TooLong(t *testing.T) {
	longURL := "mailto:test@example.com?" + strings.Repeat("x", 3000)
	result := ParseMailtoURL(longURL)
	if result != nil {
		t.Error("ParseMailtoURL with 3000+ char URL should return nil")
	}
}

func TestParseMailtoURL_InvalidScheme(t *testing.T) {
	result := ParseMailtoURL("http://example.com")
	if result != nil {
		t.Error("ParseMailtoURL with http:// scheme should return nil")
	}
}

func TestParseMailtoURL_Empty(t *testing.T) {
	result := ParseMailtoURL("")
	if result != nil {
		t.Error("ParseMailtoURL with empty string should return nil")
	}
}

func TestParseMailtoURL_NoAddress(t *testing.T) {
	result := ParseMailtoURL("mailto:?subject=Hello")
	if result == nil {
		t.Fatal("ParseMailtoURL returned nil, want non-nil")
	}
	if len(result.To) != 0 {
		t.Errorf("To = %v, want empty", result.To)
	}
	if result.Subject != "Hello" {
		t.Errorf("Subject = %q, want %q", result.Subject, "Hello")
	}
}

// TestNewAppInitializesConcurrencyMaps guards an ordering hazard that is
// otherwise only reachable at runtime.
//
// The op drainer releases operations stranded by a previous run and starts
// executing them as soon as it is started. Those run the full mutation path —
// moveMessagesToIMAP calls noteOwnExpunge, which writes to ownExpungeAt. When
// these maps were built partway through Startup, a drainer started before that
// point panicked with "assignment to entry in nil map" on the first launch
// after any pending operation was left behind.
//
// Building them with the App removes the ordering question entirely, and this
// test fails if they ever drift back into Startup.
func TestNewAppInitializesConcurrencyMaps(t *testing.T) {
	a := NewApp(func() bool { return false }, false)

	maps := map[string]bool{
		"syncContexts":      a.syncContexts == nil,
		"syncLastRequest":   a.syncLastRequest == nil,
		"ownFlagChangeAt":   a.ownFlagChangeAt == nil,
		"ownExpungeAt":      a.ownExpungeAt == nil,
		"draftSyncContexts": a.draftSyncContexts == nil,
		"draftSyncDone":     a.draftSyncDone == nil,
	}
	for name, isNil := range maps {
		if isNil {
			t.Errorf("NewApp left %s nil; a background worker writing to it will panic", name)
		}
	}

	// The write that actually panicked, exercised directly.
	a.noteOwnExpunge("acct-1")
	if !a.recentOwnExpunge("acct-1") {
		t.Error("noteOwnExpunge did not record the account")
	}
}
