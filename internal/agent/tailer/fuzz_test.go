package tailer

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// FuzzFeedLine pushes arbitrary byte lines (split on '\n', as consume does)
// through feedLine on a containerd file, with the trace-joining stage on and
// off and varying entry-size limits. Invariants: no panics anywhere in the
// two-stage pipeline; watermark never exceeds the total bytes fed; every
// batched entry's [start, offset) range lies within the fed bytes with
// start <= offset, and offsets are non-decreasing per stream; stopPipeline
// drains without panicking and emits only in-bounds entries.
func FuzzFeedLine(f *testing.F) {
	ts := timeNowCRI()
	seeds := []string{
		// Plain CRI traffic.
		ts + " stdout F hello\n" + ts + " stderr F world\n",
		// P/F fragment runs, including an unclosed trailing run.
		ts + " stdout P frag1\n" + ts + " stdout P frag2\n" + ts + " stdout F end\n" + ts + " stdout P dangling\n",
		// Stack-trace continuation lines (multiline join) after a CRI line.
		ts + " stderr F panic: boom\n" + ts + " stderr F \tat main.go:1\n" + ts + " stderr F \tat main.go:2\n",
		// Non-CRI passthrough, NULs, invalid UTF-8, CRI lookalikes.
		"not a cri line\n\x00\x01\x02\n\xff\xfe bad utf8\n" +
			"2026-13-45T99:99:99Z stdout F corrupt timestamp\n" +
			ts + " stdin F unknown stream\n" +
			ts + " stdoutF missing space\n" +
			ts + " stdout X bad tag\n" +
			ts + " stdout P\n" + ts + " stdout F\n",
		// Timestamps going backwards mid-run.
		"2026-07-05T10:00:01Z stdout P new\n2020-01-01T00:00:00Z stdout F old\n",
		// Empty lines and whitespace.
		"\n\n   \n\t\n",
		// Interleaved streams splitting fragment runs.
		ts + " stdout P a\n" + ts + " stderr P b\n" + ts + " stdout F c\n" + ts + " stderr F d\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s), true, byte(0))
		f.Add([]byte(s), false, byte(0))
		f.Add([]byte(s), true, byte(1)) // tiny entry cap: exercise truncation/drop paths
	}
	f.Fuzz(func(t *testing.T, data []byte, multiline bool, sizeClass byte) {
		maxEntry := 1 << 20
		if sizeClass%2 == 1 {
			maxEntry = 96 // small cap: over-limit truncation and drop paths
		}
		tl, file := benchTailer(t, Config{Multiline: multiline, MaxEntryBytes: maxEntry})
		ctx := context.Background()

		var total int64
		checkWatermark := func(when string) {
			if wm, ok := file.watermark(); ok && (wm.off < 0 || wm.off > total || wm.seg != file.tail) {
				t.Fatalf("%s: watermark %+v out of range [0, %d] (tail seg %d)", when, wm, total, file.tail)
			}
		}

		for line := range bytes.SplitSeq(data, []byte{'\n'}) {
			start := total
			total += int64(len(line)) + 1
			if len(line) == 0 {
				continue // consume drops empty physical lines but the offset advances
			}
			tl.feedLine(ctx, file, string(line), start, total, time.Now())
			checkWatermark("after feed")
		}
		file.lineStart, file.readPos = total, total

		tl.stopPipeline(ctx, file)
		checkWatermark("after stop")

		lastOffset := map[string]int64{}
		for i, e := range tl.batch {
			if e.file != file {
				t.Fatalf("entry %d: unexpected file", i)
			}
			if e.start.off < 0 || e.end.off > total || e.start.off > e.end.off {
				t.Fatalf("entry %d (stream %q body %q): range [%d, %d) outside fed bytes [0, %d]",
					i, e.stream, clip(e.body), e.start.off, e.end.off, total)
			}
			if e.start.seg != file.tail || e.end.seg != file.tail {
				t.Fatalf("entry %d: segment ids %d/%d, want tail %d", i, e.start.seg, e.end.seg, file.tail)
			}
			if prev, ok := lastOffset[e.stream]; ok && e.end.off < prev {
				t.Fatalf("entry %d (stream %q body %q): offset %d went backwards (prev %d)",
					i, e.stream, clip(e.body), e.end.off, prev)
			}
			lastOffset[e.stream] = e.end.off
		}
	})
}

// FuzzIngestChunk drives consume — the physical-line splitter from untrusted,
// tenant-written bytes at arbitrary read-chunk boundaries to the offsets
// checkpoints are built from — through ingestChunk exactly as every read loop
// does. FuzzFeedLine does its own '\n' split and never reaches it, so the
// interacting lineStart/pending/skipEnd/discarding/limited state went unfuzzed.
//
// Rate limiting runs off, in DROP mode and in PAUSE mode (readFile's retry is
// modelled: refill the bucket and consume again before the next read), with a
// small entry cap so the oversized-line discard window is reachable — the carry
// cap is MaxEntryBytes+oversizeSlack, so a seed carries a newline-free run past
// it. Invariants, after every chunk and every pause retry:
//
//   - lineStart + len(pending) == readPos (file's state invariant, file.go);
//   - skipEnd <= lineStart, and skipEnd is 0 or follows a '\n' in the data;
//   - committed <= lineStart and on a line boundary;
//   - the watermark sits at or below lineStart;
//   - limited implies pending begins with a whole, non-blank line: a pause
//     holds a LINE, never a blank line or a discarded tail, which are handled
//     ahead of the limiter precisely so they cannot defer the file's reading.
//
// After stopPipeline every batched entry's range lies on line boundaries within
// [0, lineStart]. And with rate limiting off and no oversized line in either
// run, the entries do not depend on the chunking. (An oversized line is the
// deliberate exception: consume discards a line longer than the carry cap when
// it arrives in pieces but feeds it, truncated, when one read holds it whole.)
func FuzzIngestChunk(f *testing.F) {
	ts := timeNowCRI()
	long := strings.Repeat("x", 4096+200) // past a 96-byte cap's MaxEntryBytes+oversizeSlack
	seeds := []string{
		ts + " stdout F hello\n" + ts + " stderr F world\n",
		ts + " stdout P frag1\n" + ts + " stdout P frag2\n" + ts + " stdout F end\n" + ts + " stdout P dangling",
		ts + " stderr F panic: boom\n" + ts + " stderr F \tat main.go:1\n\n\n" + ts + " stdout F after-blanks\n",
		"not a cri line\n\x00\x01\n\xff\xfe bad utf8\n\n",
		ts + " stdout F " + long + "\n" + ts + " stdout F next\n",
		long + long + "\n" + ts + " stdout F after-discard\n\n" + ts + " stdout F tail",
	}
	cutSets := [][]byte{nil, {0}, {6, 40, 255}, {199, 255, 7}}
	for _, s := range seeds {
		for i, cuts := range cutSets {
			f.Add([]byte(s), cuts, byte(i%3), i%2 == 0)
		}
	}
	f.Fuzz(func(t *testing.T, data []byte, cuts []byte, mode byte, multiline bool) {
		cfg := Config{Multiline: multiline, MaxEntryBytes: 96}
		switch mode % 3 {
		case 1:
			cfg.RateLimit, cfg.RateBurst, cfg.RateDrop = 1, 2, true
		case 2:
			cfg.RateLimit, cfg.RateBurst = 1, 2
		}
		ctx := context.Background()
		boundary := func(off int64) bool {
			return off == 0 || (off <= int64(len(data)) && data[off-1] == '\n')
		}

		run := func(chunked bool) (*Tailer, *file) {
			tl, fl := benchTailer(t, cfg)
			check := func(when string) {
				t.Helper()
				if fl.lineStart+int64(len(fl.pending)) != fl.readPos {
					t.Fatalf("%s: lineStart %d + pending %d != readPos %d", when, fl.lineStart, len(fl.pending), fl.readPos)
				}
				if fl.skipEnd > fl.lineStart || !boundary(fl.skipEnd) {
					t.Fatalf("%s: skipEnd %d (lineStart %d) is not a line boundary at or below lineStart", when, fl.skipEnd, fl.lineStart)
				}
				if fl.committed > fl.lineStart || !boundary(fl.committed) {
					t.Fatalf("%s: committed %d (lineStart %d) is not a line boundary at or below lineStart", when, fl.committed, fl.lineStart)
				}
				if wm, ok := fl.watermark(); ok && wm.off > fl.lineStart {
					t.Fatalf("%s: watermark %d above lineStart %d", when, wm.off, fl.lineStart)
				}
				if fl.limited {
					if i := bytes.IndexByte(fl.pending, '\n'); i <= 0 || fl.discarding {
						t.Fatalf("%s: paused on %q (discarding=%v), want a whole non-blank line at the head of pending",
							when, clip(string(fl.pending)), fl.discarding)
					}
				}
			}
			// readFile stops reading a paused file and retries once tokens
			// refill: model exactly that before every read.
			unpause := func() {
				for n := 0; fl.limited; n++ {
					if n > len(data)+2 {
						t.Fatal("a paused file never resumed across bucket refills")
					}
					fl.tokens = cfg.RateBurst
					if tl.consume(ctx, fl, false) {
						t.Fatal("consume reported a rewind with a null exporter")
					}
					check("after a pause retry")
				}
			}
			rest := data
			for i := 0; len(rest) > 0; i++ {
				n := len(rest)
				if chunked && len(cuts) > 0 {
					n = min(n, int(cuts[i%len(cuts)])+1)
				}
				unpause()
				if tl.ingestChunk(ctx, fl, rest[:n], false) {
					t.Fatal("ingestChunk reported a rewind with a null exporter")
				}
				rest = rest[n:]
				check("after a chunk")
			}
			unpause()
			tl.stopPipeline(ctx, fl)
			for i, e := range tl.batch {
				if e.start.off < 0 || e.start.off > e.end.off || e.end.off > fl.lineStart ||
					!boundary(e.start.off) || !boundary(e.end.off) {
					t.Fatalf("entry %d (body %q): range [%d, %d) is not line boundaries within [0, %d]",
						i, clip(e.body), e.start.off, e.end.off, fl.lineStart)
				}
			}
			return tl, fl
		}

		tlA, fA := run(true)
		if mode%3 != 0 {
			return
		}
		tlB, fB := run(false)
		if fA.oversized != 0 || fB.oversized != 0 {
			return // the documented exception: see above
		}
		if len(tlA.batch) != len(tlB.batch) {
			t.Fatalf("chunking changed the entry count: %d chunked vs %d whole", len(tlA.batch), len(tlB.batch))
		}
		for i := range tlA.batch {
			a, b := tlA.batch[i], tlB.batch[i]
			if a.body != b.body || a.start != b.start || a.end != b.end {
				t.Fatalf("entry %d depends on the chunking: %q [%d,%d) chunked vs %q [%d,%d) whole",
					i, clip(a.body), a.start.off, a.end.off, clip(b.body), b.start.off, b.end.off)
			}
		}
	})
}

func clip(s string) string {
	if len(s) > 80 {
		return s[:80] + "..."
	}
	return strings.ToValidUTF8(s, "�")
}
