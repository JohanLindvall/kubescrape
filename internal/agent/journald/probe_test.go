package journald

import (
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// TestJournalTruncationCountedAndAttributed: a message exceeding MaxEntryBytes is
// truncated (journald.go sanitize/truncateRunes) with no counter bump and no
// attribute on the record marking the loss. A consumer cannot distinguish a
// truncated body from a complete one, and no metric surfaces that data was
// dropped — silent data loss. (CLAUDE.md flags the truncation as silent.)
// Fix: count truncations (e.g. a kubescrape_journal_truncated_total) and/or
// stamp a log.truncated / original-length attribute on the record.
func TestJournalTruncationCountedAndAttributed(t *testing.T) {
	before := obs.JournalDropped.Value()
	entries := []rawEntry{mkEntry("c00", "a.service", strings.Repeat("x", 200), "6")}
	exp, _ := startReader(t, Config{MaxEntryBytes: 20}, entries, false, 0)
	waitFor(t, "one record", func() bool { return len(exp.records()) == 1 })

	exp.mu.Lock()
	defer exp.mu.Unlock()
	lr := exp.batches[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if len(lr.Body().Str()) > 20 {
		t.Fatalf("body not truncated: %d bytes", len(lr.Body().Str()))
	}
	_, hasAttr := lr.Attributes().Get("log.truncated")
	if obs.JournalDropped.Value() == before && !hasAttr {
		t.Fatalf("body truncated from 200 to %d bytes with no counter bump and no log.truncated attribute: silent data loss",
			len(lr.Body().Str()))
	}
}
