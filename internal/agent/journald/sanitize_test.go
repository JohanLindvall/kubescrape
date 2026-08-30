package journald

// sanitize is the whole read-side per-entry cost of this pipeline and runs on
// the SINGLE reader goroutine that also flushes, so both its CPU and the
// memory it hands to the batch are load-bearing.

import (
	"runtime"
	"strings"
	"testing"
	"unicode/utf8"
	"unsafe"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// benchBody defeats dead-code elimination of sanitize's result.
var benchBody string

// sanitizer builds a Reader with just the entry cap set.
func sanitizer(maxEntry int) *Reader {
	return New(Config{MaxEntryBytes: maxEntry})
}

// A truncated body must be CLONED, not resliced. logchain.TruncateRunes ends
// in s[:n], which pins the whole original message for the life of the batch
// while batchBytes counts only the truncated length — so MaxBatchBytes, the
// "soft bound that keeps a batch from growing large in memory", bounded
// nothing. Measured at MaxEntryBytes 1 KiB over 1024 x 64 KiB messages: 1.00 MB
// accounted, 64.15 MB live.
func TestTruncatedBodyDoesNotPinTheWholeMessage(t *testing.T) {
	r := sanitizer(1 << 10)
	msg := strings.Repeat("a", 64<<10)

	body, origLen := r.sanitize(msg, "unit.service")
	if origLen != len(msg) {
		t.Fatalf("origLen = %d, want the raw length %d", origLen, len(msg))
	}
	if len(body) != 1<<10 {
		t.Fatalf("body = %d bytes, want the entry cap", len(body))
	}
	if unsafe.StringData(body) == unsafe.StringData(msg) {
		t.Fatal("the truncated body still aliases the journal message: it pins every byte the batch accounting says it dropped")
	}
	// The same must hold once the message needed UTF-8 repair as well.
	dirty := "\xff\xfe" + msg
	body, _ = r.sanitize(dirty, "unit.service")
	if unsafe.StringData(body) == unsafe.StringData(dirty) {
		t.Fatal("the repaired-and-truncated body still aliases the journal message")
	}
	if !utf8.ValidString(body) || len(body) > 1<<10 {
		t.Fatalf("body: valid=%v len=%d, want valid UTF-8 within the cap", utf8.ValidString(body), len(body))
	}
}

// The cut has to come BEFORE the validation, not after it: strings.ToValidUTF8
// walks the whole string and, on invalid input, builds a replacement copy of
// it, so a 4 MiB journal message cost a 4 MiB allocation and a full rune-by-rune
// pass to produce a 1 KiB record. Allocated bytes are the deterministic proxy
// for the pass nobody should be paying for.
func TestOverCapMessageIsValidatedOnlyUpToTheCap(t *testing.T) {
	const (
		maxEntry = 1 << 10
		msgLen   = 4 << 20
		calls    = 8
	)
	r := sanitizer(maxEntry)
	// Invalid bytes throughout, so ToValidUTF8 cannot short-circuit: run over
	// the whole message it builds a replacement copy of all 4 MiB, run over the
	// cut it touches at most MaxEntryBytes.
	msg := strings.Repeat("x\xff", msgLen/2)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for range calls {
		body, _ := r.sanitize(msg, "unit.service")
		runtime.KeepAlive(body)
	}
	runtime.ReadMemStats(&after)

	perCall := (after.TotalAlloc - before.TotalAlloc) / calls
	// One clone of at most the cap, plus slack for the test's own bookkeeping.
	if perCall > 64<<10 {
		t.Fatalf("sanitize allocated %d bytes per call for a %d-byte message capped at %d: it is validating (and copying) what it then throws away",
			perCall, len(msg), maxEntry)
	}
}

// The common case — an already-valid message under the cap — must stay
// allocation-free and hand back the message itself. A fix that cloned or
// rebuilt unconditionally would pay for every entry on the node.
func TestValidMessageUnderTheCapIsFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	r := sanitizer(1 << 20)
	msg := strings.Repeat("kubelet: pod sandbox ready\n", 64)
	if n := testing.AllocsPerRun(200, func() {
		body, _ := r.sanitize(msg, "unit.service")
		if len(body) == 0 {
			t.Fatal("empty body")
		}
	}); n != 0 {
		t.Fatalf("sanitize allocated %v times for a valid in-cap message, want 0", n)
	}
	body, origLen := r.sanitize(msg, "unit.service")
	if origLen != 0 || body != msg {
		t.Fatalf("sanitize(valid) = (%d bytes, origLen %d), want the message untouched", len(body), origLen)
	}
}

func TestSanitizeRepairsAndCaps(t *testing.T) {
	r := sanitizer(16)
	for _, tc := range []struct {
		name string
		in   string
	}{
		{"valid in cap", "hello"},
		{"invalid in cap", "he\xffllo"},
		{"valid over cap", strings.Repeat("é", 32)},
		{"invalid over cap", strings.Repeat("\xff", 64)},
		{"multibyte on the boundary", strings.Repeat("a", 15) + "é"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, origLen := r.sanitize(tc.in, "unit.service")
			if !utf8.ValidString(body) {
				t.Errorf("body %q is not valid UTF-8", body)
			}
			if len(body) > 16 {
				t.Errorf("body = %d bytes, want at most the cap", len(body))
			}
			// origLen is 0 for an untouched message and the RAW length for a
			// truncated one — never a post-repair length.
			if origLen != 0 && origLen != len(tc.in) {
				t.Errorf("origLen = %d, want 0 or the raw length %d", origLen, len(tc.in))
			}
		})
	}
}

// BenchmarkSanitize reports the per-entry read-side cost the two fixes above
// target: the valid path (a ValidString probe instead of a full ToValidUTF8
// rebuild pass) and the over-cap path (validate the cut, not the message).
func BenchmarkSanitize(b *testing.B) {
	run := func(b *testing.B, maxEntry int, msg string) {
		r := sanitizer(maxEntry)
		b.ReportAllocs()
		b.SetBytes(int64(len(msg)))
		for b.Loop() {
			benchBody, _ = r.sanitize(msg, "unit.service")
		}
	}
	b.Run("valid/small", func(b *testing.B) {
		run(b, 1<<20, strings.Repeat("kubelet: pod sandbox ready\n", 8))
	})
	b.Run("valid/1MiB", func(b *testing.B) {
		run(b, 4<<20, strings.Repeat("a", 1<<20))
	})
	b.Run("overcap/4MiB-into-1KiB", func(b *testing.B) {
		run(b, 1<<10, strings.Repeat("a", 4<<20))
	})
}
