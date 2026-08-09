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

// maxGroupLines is the trace stage's retained-lines cap (WithMaxLines): a
// group keeps at most this many lines; further continuation lines are
// consumed but dropped from retention.
const maxGroupLines = 512

// maxGroupBuffered bounds a NEVER-COMPLETING group. A workload continuously
// emitting continuation-shaped lines (an exception header followed by endless
// frame-shaped output) refreshes the group's staleness stamp on every line,
// so FlushBefore never fires, while the stage keeps consuming past its caps —
// the offset FIFO then grows by one item (~56 B) per line forever and
// `committed` stays pinned at the group's start: the checkpoint freezes (a
// crash re-ingests the whole window), idle-close is blocked and the lag
// gauges climb. Past DOUBLE the retention cap the group has already been
// truncated for maxGroupLines lines, so further buffering buys nothing:
// boundGroup flushes it — the same truncated entry an eventual flush would
// emit — and the remainder starts a fresh group. Stage 1 has the same wedge
// through endless P fragments that never reach this FIFO at all; feedLine
// bounds that run by CONSUMED BYTES (the force-close beside forceFinal).
const maxGroupBuffered = 2 * maxGroupLines

// boundGroup force-flushes a stream's trace-stage group once its offset FIFO
// shows it consuming past maxGroupBuffered. Called after every trace-stage
// feed; the comparison is the entire steady-state cost.
func (t *Tailer) boundGroup(ctx context.Context, f *file, st *streamState, key string) error {
	if len(st.live()) < maxGroupBuffered {
		return nil
	}
	return f.traces.Flush(ctx, key)
}

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

	ml := f.source.multiline
	if f.multiline != nil {
		ml = *f.multiline // pod annotation override
	}
	if ml {
		f.traces = multiline.New(t.traceEmitFunc(f),
			multiline.WithMaxBytes(t.cfg.MaxEntryBytes), multiline.WithMaxLines(maxGroupLines))
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
// the per-stream offset FIFO and appends the entry to the batch. Entry.Lines
// charges even cap-dropped lines (multiline >= v0.0.11, sum(Lines) == lines
// consumed, pinned upstream by FuzzCappedConservation), so the pop below
// covers every pushed item exactly — the orphan head-drop and reconcileFifos
// that compensated for the old undercount are gone with it.
func (t *Tailer) traceEmitFunc(f *file) func(context.Context, multiline.Entry[time.Time]) error {
	return func(_ context.Context, e multiline.Entry[time.Time]) error {
		st := f.stateFor(e.Key)
		items := st.live()
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
// and either emits directly or hands the line to the trace stage with its
// offsets pushed onto the FIFO.
//
// Every emission happens within the fed line's own AddParsed (single F,
// passthrough, and — since multiline v0.0.11's FinalMatcher — an F-closed
// run) or within a Flush* of an unclosed run, so runStart/lastEnd always
// still describe the emitted run: the old deferred-to-the-next-line emission
// (closed/closedStart/closedEnd/nextStart/hasNext) is gone with the library
// behaviour that needed it.
func (t *Tailer) criEmitFunc(f *file) func(context.Context, string, string, time.Time, pos) error {
	return func(ctx context.Context, key, line string, when time.Time, rawStart pos) error {
		st := f.stateFor(key)
		var start pos
		if st.hasRun {
			start = st.runStart
		} else {
			// A never-F-closed P-run flushes its retained lines one by
			// one, and the first of those emissions spends hasRun — the
			// rest resolve from the line's own feed-time position, which
			// the stage carried as the payload. The position must be the
			// one captured WHEN THE LINE WAS FED: a rotation carried
			// since then has moved the tail id, and stamping the current
			// tail here put the fragment in a segment newer than its
			// bytes — the watermark clamp then read the old segment as
			// unconstrained and retired it with lines still buffered.
			start = rawStart
		}
		st.hasRun = false
		end := st.lastEnd
		if f.traces == nil {
			t.emit(f, entry{time: when, stream: st.stream, body: line, start: start, end: end})
			return nil
		}
		st.push(logItem{start: start, end: end})
		if err := f.traces.AddAt(ctx, key, line, when, when); err != nil {
			return err
		}
		return t.boundGroup(ctx, f, st, key)
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
		// The clocks the multi-line age-out needs: the newest LOG timestamp
		// this file fed, and the wall-clock instant it was fed at. sweep
		// compares its cutoff against the former while lines are arriving and
		// falls back to the latter once they stop. Stamped at FEED time, for
		// every CRI line: a buffered P fragment reaches no emission callback,
		// and either stage may hold lines with Multiline on or off — a
		// wall-clock cutoff against the lines' own timestamps tears any run
		// buffered across a sweep boundary during a backlog catch-up.
		f.lastLineTime, f.lastFed = l.Time, time.Now()
	}
	seg := f.curSeg()
	startPos, endPos := pos{seg, start}, pos{seg, end}
	st.lastEnd = endPos
	if !st.hasRun {
		st.runStart, st.hasRun = startPos, true
		st.runBytes = 0
	}
	st.runBytes += end - start
	// Bound a NEVER-COMPLETING stage-1 run — maxGroupBuffered's sibling one
	// stage down. A workload writing one endless logical line (P fragments
	// forever, no F) wedges the file without this: nothing reaches stage 2, so
	// boundGroup sees nothing; every fragment refreshes the age-out stamps, so
	// FlushBefore never fires; and the watermark pins at runStart — the
	// checkpoint freezes (a crash re-ingests the whole window), idle-close is
	// blocked, and every rotation carried adds a segment that can never
	// retire. Past DOUBLE the retention cap the run has been truncating for a
	// full cap of bytes and further buffering buys nothing: the fragment's tag
	// flips to F so the stage closes the run ITSELF — one joined (truncated)
	// entry, emitted synchronously inside this AddParsed where runStart and
	// lastEnd still describe the run — and the remainder starts a fresh run,
	// exactly as after a FlushBefore. The flip must rewrite the RAW line, not
	// just the parse: with a run pending the stage classifies by re-splitting
	// raw, and Line.Partial alone is consulted only on the buffer-free fast
	// path.
	if ok && l.Partial && st.runBytes >= 2*int64(t.cfg.MaxEntryBytes) {
		raw = forceFinal(raw)
		l.Partial = false
	}
	// AddParsed reuses this parse — the only one on the whole line's path.
	// The payload is the SEGMENT-QUALIFIED start position: the stage may hold
	// this line across a carried rotation and emit it under a newer tail id.
	if err := f.criStage.AddParsed(ctx, f.containerID, raw, l, ok, startPos); err != nil {
		t.log.Warn("log pipeline", "path", f.path, "error", err)
	}
}

// forceFinal rewrites a parsed CRI "P" fragment as the "F" line that closes
// its run: the tag field of the "<ts> <stream> <tag> <content>" header flips.
// Only called on a line cri.Parse accepted as Partial, so the two spaces and
// the single-byte tag are guaranteed present; the guards keep a malformed
// line unchanged (the wedge is preferable to corrupting a record).
func forceFinal(raw string) string {
	i := strings.IndexByte(raw, ' ')
	if i < 0 {
		return raw
	}
	j := strings.IndexByte(raw[i+1:], ' ')
	if j < 0 {
		return raw
	}
	k := i + 1 + j + 1
	if k >= len(raw) || raw[k] != 'P' {
		return raw
	}
	b := []byte(raw)
	b[k] = 'F'
	return string(b)
}

// feedPlainLine feeds one line of a non-containerd file. The record timestamp
// is the ingest time (enrich may override it from the line in flush). There is
// no stage-1 (CRI) buffer, so the fifo alone tracks the buffered lines and no
// runStart bookkeeping is needed: the line lands in the fifo before it is fed,
// so the watermark covers it until the trace stage emits it.
func (t *Tailer) feedPlainLine(ctx context.Context, f *file, raw string, start, end int64) {
	when := time.Now()
	seg := f.curSeg()
	endPos := pos{seg, end}
	// Advance the fed boundary exactly as feedLine does for CRI streams.
	// fedEnd() (max lastEnd at the tail) is what a rename rotation reads to
	// decide whether to record a segment for the drained-away inode; without
	// this the plain stream's lastEnd stayed at zero, fedEnd()==committed, no
	// segment was recorded, and any fed-but-uncommitted plain lines were lost
	// on a rotation whose export failed (the old inode being the only copy).
	f.stPlain.lastEnd = endPos
	if f.traces == nil {
		t.emit(f, entry{time: when, body: raw, start: pos{seg, start}, end: endPos})
		return
	}
	f.stPlain.push(logItem{start: pos{seg, start}, end: endPos})
	// A plain file's line time IS the feed time, so the two clocks coincide and
	// the age-out behaves exactly as before; recorded anyway so sweep needs no
	// special case.
	f.lastLineTime, f.lastFed = when, when
	if err := f.traces.AddAt(ctx, plainKey, raw, when, when); err != nil {
		t.log.Warn("log pipeline", "path", f.path, "error", err)
	}
	if err := t.boundGroup(ctx, f, f.stPlain, plainKey); err != nil {
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

// oversizeSlack is the grace past MaxEntryBytes before an unterminated
// physical line's accumulated prefix is discarded. consume (the live path) and
// replaySegment (the checkpoint-replay path) must apply the SAME bound: a line
// capped and discarded live is re-read on replay, and a different bound there
// would feed as a record what the live path discarded (or vice versa).
const oversizeSlack = 4096

// maxIdlePendingBytes caps the carry buffer a file keeps between reads. One
// oversized line grows it to MaxEntryBytes+oversizeSlack plus a chunk; pinning
// that per file forever (a node tracks thousands) costs more than re-growing it
// the one time another such line shows up. Two 64 KiB read chunks is the
// steady-state working set, so normal files never hit this path.
const maxIdlePendingBytes = 128 * 1024

// appendPending appends one read chunk to the file's carry-over buffer,
// reusing ONE array per file: the unconsumed remainder is moved back to the
// front of pendingBase (a memmove — the two slices alias) so the chunk lands
// in the capacity that frees up, rather than in a freshly allocated array.
func (f *file) appendPending(chunk []byte) {
	buf := append(f.pendingBase[:0], f.pending...) // in-place when it aliases
	buf = append(buf, chunk...)
	f.pendingBase = buf[:0:cap(buf)]
	f.pending = buf
}

// ingestChunk accounts one read chunk (byte counter, pending buffer, read
// position) and consumes it — the shared body of every read/drain loop.
func (t *Tailer) ingestChunk(ctx context.Context, f *file, chunk []byte, unlimited bool) {
	obs.LogBytes.Add(float64(len(chunk)))
	f.appendPending(chunk)
	f.readPos += int64(len(chunk))
	t.consume(ctx, f, unlimited)
	if len(f.pending) == 0 && cap(f.pendingBase) > maxIdlePendingBytes {
		// Everything read has been consumed and the buffer is oversized (an
		// over-long line grew it): give it back instead of holding it for the
		// life of the file.
		f.pending, f.pendingBase = nil, nil
	}
}

// consume splits pending bytes into physical lines and feeds the pipeline.
// unlimited bypasses the per-file rate limit (rotation drains, where pausing
// would lose the remainder of the rotated-away inode).
func (t *Tailer) consume(ctx context.Context, f *file, unlimited bool) {
	for {
		i := bytes.IndexByte(f.pending, '\n')
		if i < 0 {
			// Bound the carried incomplete physical line.
			if len(f.pending) > t.cfg.MaxEntryBytes+oversizeSlack {
				f.lineStart += int64(len(f.pending))
				f.pending = f.pending[:0]
				// The line's REMAINDER (everything up to its eventual newline)
				// is part of the same oversized line: without this flag it
				// would be fed as a "line" of its own — an arbitrary mid-line
				// suffix, exported as a garbage record.
				//
				// Which is also why the counter is bumped only on the FIRST
				// slab: consume runs per 64 KiB read chunk, so counting each
				// over-cap flush reported one 10 MiB line as ~10 dropped LINES,
				// which is what the metric says it counts.
				if !f.discarding {
					obs.LogOversizedDropped.Inc()
				}
				f.discarding = true
			}
			return
		}
		if f.discarding {
			// The tail of an oversized discarded line: its newline ends the
			// discard window. Handled BEFORE the rate limiter, because this is
			// not a line — it must neither spend a token nor be deferred by a
			// pause, and in DROP mode the limiter's branch below consumed it
			// while leaving f.discarding set, so the next GOOD line was
			// swallowed by the discard window instead.
			f.pending = f.pending[i+1:]
			f.lineStart += int64(i + 1)
			f.discarding = false
			continue
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
