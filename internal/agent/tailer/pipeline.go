package tailer

// Line ingestion: the per-file two-stage multiline pipeline (CRI rejoin +
// trace joining), physical-line splitting, and the per-file rate limiter.

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/multiline"
	"github.com/JohanLindvall/multiline/cri"
)

// newPipeline (re)creates the file's aggregation stages with empty state.
// Incomplete segments (if any) are no longer present in the fresh pipeline
// and must be re-read (feedSegments) before the current inode is consumed.
func (t *Tailer) newPipeline(f *file) {
	if f.tail == 0 {
		// First pipeline for this file: issue its tail segment id. Files
		// restored from a checkpoint re-issue a higher tail in initFile,
		// above their loaded segments' ids.
		f.newTail()
	}
	f.reset()
	f.keyStdout = f.containerID + "/stdout"
	f.keyStderr = f.containerID + "/stderr"
	if f.source.containerd {
		f.stStdout = f.state(f.keyStdout)
		f.stStderr = f.state(f.keyStderr)
		f.stPlain = f.state(f.containerID) // non-CRI passthrough lines
	} else {
		f.stStdout, f.stStderr = nil, nil
		f.stPlain = f.state(plainKey)
	}

	if f.source.multiline {
		f.traces = multiline.New(t.traceEmitFunc(f),
			multiline.WithMaxBytes(t.cfg.MaxEntryBytes), multiline.WithMaxLines(512))
	} else {
		f.traces = nil
	}

	// Containerd files run stage 1 (CRI P/F rejoin) ahead of the trace stage;
	// plain files feed the trace stage (or emit) directly from feedLine.
	// Emission is synchronous inside Add/Flush*, so the state's lastEnd is
	// exactly the end offset of the line's last fragment.
	if f.source.containerd {
		f.criStage = cri.New(t.criEmitFunc(f), multiline.WithMaxBytes(t.cfg.MaxEntryBytes))
	} else {
		f.criStage = nil
	}
}

// traceEmitFunc builds the trace (multiline) stage's emission callback for
// one file: it maps the emitted logical entry back to the byte ranges owed by
// the per-stream offset FIFO and appends the entry to the batch.
func (t *Tailer) traceEmitFunc(f *file) func(context.Context, multiline.Entry[time.Time]) error {
	return func(_ context.Context, e multiline.Entry[time.Time]) error {
		st := f.stateFor(e.Key)
		items := st.live()
		// The multiline stage's line/byte caps can drop over-limit lines
		// without ever emitting them (their runs never complete), leaving
		// orphaned items that would freeze the watermark — and with it
		// this file's checkpoint — forever. The entry's first-line time
		// identifies the true head: timestamps are monotonic per stream,
		// so strictly-older leading items belong to dropped lines.
		dropped := 0
		for dropped < len(items) && items[dropped].when.Before(e.Data) {
			dropped++
			obs.LogFifoDropped.Inc()
		}
		if dropped > 0 {
			st.pop(dropped) // persist the drops even if nothing is emitted below
			items = st.live()
		}
		n := min(e.Lines, len(items)) // Lines > len(items) must not happen; defensive
		if n == 0 {
			return nil
		}
		start, end := items[0].start, items[n-1].end
		st.pop(n)
		t.emit(f, entry{
			time: e.Data, stream: st.stream, body: e.Text,
			truncated: e.Truncated, match: e.Match, start: start, end: end,
		})
		return nil
	}
}

// criEmitFunc builds the CRI stage's emission callback for one file: it
// resolves the emitted line's byte range from the per-stream run bookkeeping
// (deferred F-closed runs included) and either emits directly or hands the
// line to the trace stage with its offsets pushed onto the FIFO.
func (t *Tailer) criEmitFunc(f *file) func(context.Context, string, string, time.Time, int64) error {
	return func(ctx context.Context, key, line string, when time.Time, rawStart int64) error {
		st := f.stateFor(key)
		var start, end pos
		if st.closed {
			// Deferred emission of an F-closed run: its boundaries were
			// pinned when the F line was fed (lastEnd has since moved on
			// to the line that triggered this flush).
			start, end = st.closedStart, st.closedEnd
			st.closed = false
			if st.hasNext {
				// Hand coverage over to the line that triggered the
				// flush; it is still buffered in the stage.
				st.runStart, st.hasRun, st.hasNext = st.nextStart, true, false
			} else {
				st.hasRun = false
			}
		} else {
			// Emission within the fed line's own AddParsed (single F,
			// passthrough) or a flush of an unclosed run: runStart is the
			// run's first position, lastEnd the newest line's end.
			if st.hasRun {
				start = st.runStart
			} else {
				start = pos{seg: f.curSeg(), off: rawStart} // defensive; hasRun is set before AddParsed
			}
			st.hasRun = false
			end = st.lastEnd
		}
		if f.traces == nil {
			t.emit(f, entry{time: when, stream: st.stream, body: line, start: start, end: end})
			return nil
		}
		st.push(logItem{start: start, end: end, when: when})
		return f.traces.AddAt(ctx, key, line, when, when)
	}
}

// emit appends one completed entry to the batch.
func (t *Tailer) emit(f *file, e entry) {
	e.file = f
	t.batch = append(t.batch, e)
}

// streamOf extracts the stream from a pipeline key ("<id>/<stream>"); ""
// for non-CRI passthrough lines.
func streamOf(key string) string {
	if i := strings.LastIndexByte(key, '/'); i >= 0 {
		return key[i+1:]
	}
	return ""
}

// plainKey keys a plain file's single logical stream. It has no '/', so
// streamOf yields "" (plain files have no CRI stream). Each file owns its own
// pipeline, so one key per file is enough.
const plainKey = "line"

// feedLine pushes one raw physical line spanning [start, end) into the
// pipeline. Containerd files go through the CRI stage; plain files feed the
// trace stage (or emit) directly, sharing the same offset accounting so
// rotation and cross-rotation multi-line joining work identically.
func (t *Tailer) feedLine(ctx context.Context, f *file, raw string, start, end int64) {
	if !f.source.containerd {
		t.feedPlainLine(ctx, f, raw, start, end)
		return
	}
	st := f.stPlain // non-CRI passthrough
	l, ok := cri.Parse(raw)
	if ok {
		switch l.Stream {
		case "stdout":
			st = f.stStdout
		case "stderr":
			st = f.stStderr
		default:
			st = f.state(f.containerID + "/" + l.Stream)
		}
	}
	seg := f.curSeg()
	startPos, endPos := pos{seg, start}, pos{seg, end}
	wasOpen := st.hasRun && !st.closed
	st.lastEnd = endPos
	if st.closed {
		// The pending closed run flushes inside this AddParsed; its callback
		// installs this line's registration afterwards (runStart must keep
		// pointing at the older, watermark-clamping position until then).
		st.nextStart, st.hasNext = startPos, true
	} else if !st.hasRun {
		st.runStart, st.hasRun = startPos, true
	}
	if ok && !l.Partial && wasOpen {
		// The F line closes an open multi-fragment run. The stage defers the
		// emission to the next line fed for this key, so pin the run's own
		// boundaries now — at callback time lastEnd already belongs to that
		// next line.
		st.closed, st.closedStart, st.closedEnd = true, st.runStart, endPos
	}
	// AddParsed reuses this parse — the only one on the whole line's path.
	if err := f.criStage.AddParsed(ctx, f.containerID, raw, l, ok, start); err != nil {
		t.log.Warn("log pipeline", "path", f.path, "error", err)
	}
}

// feedPlainLine feeds one line of a non-containerd file. The record timestamp
// is the ingest time (enrich may override it from the line in flush). There is
// no stage-1 (CRI) buffer, so the fifo alone tracks the buffered lines and no
// runStart bookkeeping is needed: the line lands in the fifo before it is fed,
// so the watermark covers it until the trace stage emits it.
func (t *Tailer) feedPlainLine(ctx context.Context, f *file, raw string, start, end int64) {
	when := time.Now()
	seg := f.curSeg()
	if f.traces == nil {
		t.emit(f, entry{time: when, body: raw, start: pos{seg, start}, end: pos{seg, end}})
		return
	}
	f.stPlain.push(logItem{start: pos{seg, start}, end: pos{seg, end}, when: when})
	if err := f.traces.AddAt(ctx, plainKey, raw, when, when); err != nil {
		t.log.Warn("log pipeline", "path", f.path, "error", err)
	}
}

// stopPipeline drains both stages into the batch.
func (t *Tailer) stopPipeline(ctx context.Context, f *file) {
	if f.criStage != nil {
		_ = f.criStage.Stop(ctx)
	}
	if f.traces != nil {
		_ = f.traces.Stop(ctx)
	}
}

// ingestChunk accounts one read chunk (byte counter, pending buffer, read
// position) and consumes it — the shared body of every read/drain loop.
func (t *Tailer) ingestChunk(ctx context.Context, f *file, chunk []byte, unlimited bool) {
	obs.LogBytes.Add(float64(len(chunk)))
	f.pending = append(f.pending, chunk...)
	f.readPos += int64(len(chunk))
	t.consume(ctx, f, unlimited)
}

// consume splits pending bytes into physical lines and feeds the pipeline.
// unlimited bypasses the per-file rate limit (rotation drains, where pausing
// would lose the remainder of the rotated-away inode).
func (t *Tailer) consume(ctx context.Context, f *file, unlimited bool) {
	for {
		i := bytes.IndexByte(f.pending, '\n')
		if i < 0 {
			// Bound the carried incomplete physical line.
			if len(f.pending) > t.cfg.MaxEntryBytes+4096 {
				f.lineStart += int64(len(f.pending))
				f.pending = f.pending[:0]
				// The line's REMAINDER (everything up to its eventual newline)
				// is part of the same oversized line: without this flag it
				// would be fed as a "line" of its own — an arbitrary mid-line
				// suffix, exported as a garbage record.
				f.discarding = true
				obs.LogOversizedDropped.Inc()
			}
			return
		}
		if !unlimited && !t.allowLine(f) {
			if !t.cfg.RateDrop {
				// Pause: keep pending, stop reading until tokens refill.
				if !f.limited {
					f.limited = true
					obs.LogRateLimited.WithLabelValues("pause").Inc()
				}
				return
			}
			// Drop: discard the line, keep consuming.
			f.pending = f.pending[i+1:]
			f.lineStart += int64(i + 1)
			obs.LogRateLimited.WithLabelValues("drop").Inc()
			continue
		}
		f.limited = false
		line := f.pending[:i]
		start := f.lineStart
		f.pending = f.pending[i+1:]
		f.lineStart += int64(i + 1)

		if f.discarding {
			// The tail of an oversized discarded line; its newline ends the
			// discard window. Offsets advanced above, nothing is fed.
			f.discarding = false
			continue
		}
		if len(line) == 0 {
			continue
		}
		t.feedLine(ctx, f, string(line), start, f.lineStart)
	}
}

// allowLine takes one token from the file's rate-limit bucket, refilling it by
// elapsed time first. Always true when rate limiting is off.
func (t *Tailer) allowLine(f *file) bool {
	if t.cfg.RateLimit <= 0 {
		return true
	}
	now := time.Now()
	if f.lastRefill.IsZero() {
		f.tokens = t.cfg.RateBurst
	} else {
		f.tokens = min(t.cfg.RateBurst, f.tokens+now.Sub(f.lastRefill).Seconds()*t.cfg.RateLimit)
	}
	f.lastRefill = now
	if f.tokens < 1 {
		return false
	}
	f.tokens--
	return true
}
