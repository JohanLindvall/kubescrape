package journald

// Read-side repairs: what makes one journal message exportable — valid UTF-8,
// capped at MaxEntryBytes (sanitize) — and how each repair that changes the
// exported record invisibly is counted and named (reportDefect).

import (
	"strings"
	"time"
	"unicode/utf8"

	"github.com/JohanLindvall/kubescrape/internal/clip"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// defectWarnEvery throttles the read-side repair warning. The condition is a
// PRODUCER writing something the journal cannot hand over intact, which
// persists for as long as that producer runs, so the useful information is one
// line naming a unit — the rate belongs to the counter.
const defectWarnEvery = 5 * time.Minute

// utf8Replacement stands in for each invalid byte, U+FFFD.
const utf8Replacement = "�"

// sanitize makes one journal message exportable: valid UTF-8 (the journal
// stores raw bytes) and capped at MaxEntryBytes without splitting a rune.
// origLen reports the RAW journal length — captured before the UTF-8
// replacement, whose replacement runes would otherwise inflate/deflate the
// advertised original size.
//
// Two things it must not do, both on the SINGLE reader goroutine that also
// flushes, so a stall here is a stall for every unit on the node:
//
// Validate what it is about to throw away. strings.ToValidUTF8 walks a string
// rune by rune and is ~31x slower than utf8.ValidString on the overwhelmingly
// common already-valid input (1 MiB: 2.07 ms against 67 µs), so it is gated on
// a ValidString probe and, past the cap, runs only over the bytes that survive
// the cut. Same lesson as logscrub's secretKVCandidate: the admission IS the
// cost.
//
// Hand back a reslice of anything longer than the body. clip.Runes ends in
// s[:n], which pins the WHOLE string it cut for the life of the batch while
// batchBytes counts only the truncated length — so MaxBatchBytes, documented
// as "a soft bound that keeps a batch from growing large in memory", bounded
// nothing. Measured at MaxEntryBytes 1 KiB over 1024 x 64 KiB messages:
// 1.00 MB accounted, 64.15 MB live. Defaults are nearly immune (both are
// 1 MiB), so it bit exactly the operator who LOWERED the entry cap to bound
// memory. BOTH truncating branches therefore clone, which is the half that
// had to be said twice: the under-cap branch cuts the ToValidUTF8
// INTERMEDIATE rather than the raw message, so its reslice pinned the
// validated copy instead (2-3x the accounted bytes rather than 64x — a
// message of many separate invalid runs grows by two bytes per run — and
// flushRetry holds the batch in place for a whole collector outage). The
// whole-message branch cannot alias anything the batch does not already own.
func (r *Reader) sanitize(msg, unit string) (body string, origLen int) {
	raw := len(msg)
	if raw <= r.cfg.MaxEntryBytes {
		if utf8.ValidString(msg) {
			return msg, 0
		}
		r.reportDefect(defectInvalidUTF8, unit)
		msg = strings.ToValidUTF8(msg, utf8Replacement)
		if len(msg) <= r.cfg.MaxEntryBytes {
			return msg, 0
		}
		// A replacement rune is wider than the byte it replaces, so a message
		// that fit before validation need not fit after it. Cloned for the
		// reason above: msg is the validated copy, and the cut would otherwise
		// pin all of it.
		return strings.Clone(clip.Runes(msg, r.cfg.MaxEntryBytes)), raw
	}
	cut := clip.Runes(msg, r.cfg.MaxEntryBytes)
	if !utf8.ValidString(cut) {
		// Only the SURVIVING bytes are probed here (the cut already happened),
		// so an over-cap message whose invalid bytes were all past the cut
		// reports no defect — correct: the exported body is byte-identical to
		// what the producer wrote for as far as it goes, and the truncation
		// itself is already carried by log.truncated and
		// kubescrape_journal_truncated_total.
		r.reportDefect(defectInvalidUTF8, unit)
		// Fresh allocation, so the second cut aliases only itself; clone below
		// is then a cheap copy of at most MaxEntryBytes.
		cut = clip.Runes(strings.ToValidUTF8(cut, utf8Replacement), r.cfg.MaxEntryBytes)
	}
	return strings.Clone(cut), raw
}

// Defect label values for kubescrape_journal_entry_defects_total. Spelled once
// here because they are metric label values as well as log values, and the
// metric's help text enumerates exactly these.
const (
	defectInvalidUTF8 = "invalid_utf8"
	defectNoTimestamp = "no_timestamp"
)

// reportDefect counts one read-side repair and, throttled, says which unit
// produced it.
//
// The counter is unconditional (it is the rate, and a unit logging raw bytes
// does this on every message); the line is throttled keylessly PER DEFECT CLASS
// (see defectWarn), because the condition is a PRODUCER and one example unit is
// what an operator acts on — keying per unit would let a node with many
// misbehaving units flood on the same fact, while keying by nothing at all let
// one defect class hide the other. Both arguments are already-materialised
// strings, so no Enabled guard is warranted.
func (r *Reader) reportDefect(defect, unit string) {
	obs.JournalEntryDefects.WithLabelValues(defect).Inc()
	gate, ok := r.defectWarn[defect]
	if !ok || !gate.Allow(defectWarnEvery) {
		return
	}
	switch defect {
	case defectInvalidUTF8:
		r.log.Warn("journal message is not valid UTF-8; the invalid bytes are replaced before export, so the exported body differs from what the producer wrote",
			"unit", unit, "defect", defect)
	default:
		r.log.Warn("journal entry carried no timestamp; the record is dated with this agent's clock at read time, not the producer's",
			"unit", unit, "defect", defect)
	}
}
