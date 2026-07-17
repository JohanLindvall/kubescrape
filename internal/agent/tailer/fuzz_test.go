package tailer

import (
	"bytes"
	"context"
	"strings"
	"testing"
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

		for _, line := range bytes.Split(data, []byte{'\n'}) {
			start := total
			total += int64(len(line)) + 1
			if len(line) == 0 {
				continue // consume drops empty physical lines but the offset advances
			}
			tl.feedLine(ctx, file, string(line), start, total)
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

func clip(s string) string {
	if len(s) > 80 {
		return s[:80] + "..."
	}
	return strings.ToValidUTF8(s, "�")
}
