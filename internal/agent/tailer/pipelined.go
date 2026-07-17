package tailer

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Pipelined export (Config.PipelinedExport, opt-in): flush hands the built
// payload to a single worker goroutine and the sweep keeps reading while the
// export (with retries) is in flight. At most ONE export is outstanding; the
// next flush first settles the previous one and applies its result — commit
// on success, rewind-and-purge on failure — so the at-least-once invariants
// are unchanged, just applied one flush later.
//
// The rotation/truncation machinery must never interleave with an in-flight
// export for the same file (a failure rewinds to the committed offset, which
// only works while the file's byte-offset world is intact), so readFile
// settles the in-flight export before handling a detected rotation, and drop
// settles before draining a removed file. Offsets are gen-checked at apply
// time as belt-and-braces.
type inflight struct {
	ctx     context.Context // the sweep's ctx at handoff, as the sync path uses
	ld      plog.Logs
	kept    int
	offsets map[*file]int64
	// highs is the per-file UNCLAMPED batch max offset (current-gen entries
	// plus any re-offered exportedHigh): what committed could reach once
	// nothing is buffered. Recorded as file.exportedHigh on successful commit
	// where the watermark clamp withheld it.
	highs map[*file]int64
	// gens (keyed by every file the batch touches) records each file's
	// rotation generation at BUILD time; commit/fail apply only where the gen
	// is unchanged.
	gens map[*file]int
	// carriedDone holds the touched files whose carried rotation prefix was
	// already fully drained into this batch at BUILD time; only those may have
	// their carried fds released once the batch exports (see flush).
	carriedDone map[*file]struct{}
	err         error
	done        chan struct{}
}

// exportWorker delivers handed-off payloads; inf.err is visible to the sweep
// goroutine after done closes.
func (t *Tailer) exportWorker() {
	for inf := range t.exportCh {
		inf.err = t.exportWithRetry(inf.ctx, inf.ld)
		close(inf.done)
	}
}

// settleInflight waits for the outstanding export (if any) and applies its
// result. Runs on the sweep goroutine.
func (t *Tailer) settleInflight() {
	inf := t.inflight
	if inf == nil {
		return
	}
	<-inf.done
	t.inflight = nil
	if inf.err != nil {
		t.failBatch(inf)
	} else {
		t.commitBatch(inf)
	}
}

// pollInflight applies the outstanding export's result if it has completed,
// without blocking — housekeeping calls it every tick so a FAILED export
// rewinds (and its data gets re-read) even when no new lines arrive to
// trigger a flush.
func (t *Tailer) pollInflight() {
	if t.inflight == nil {
		return
	}
	select {
	case <-t.inflight.done:
		t.settleInflight()
	default:
	}
}

// settle waits for the outstanding export when it touches f — called before
// rotation handling or dropping so those always see settled state.
func (t *Tailer) settle(f *file) {
	if t.inflight == nil {
		return
	}
	if _, ok := t.inflight.gens[f]; ok {
		t.settleInflight()
	}
}

// commitBatch advances the committed offsets of a successfully exported
// batch: never past lines still buffered in the pipeline, and only when the
// file's rotation generation is unchanged since the batch was built.
func (t *Tailer) commitBatch(inf *inflight) {
	obs.LogEntries.Add(float64(inf.kept))
	for f, gen := range inf.gens {
		if gen != f.gen {
			continue // rotated since build; offsets are stale
		}
		// offsets was already clamped to the BUILD-time watermark in flush, so it
		// never names a line still buffered when this batch was built. Re-reading
		// f.watermark() here would be wrong in pipelined mode: the commit is
		// applied a flush later, when those lines may sit in the next, unexported
		// batch and the live watermark no longer holds committed back for them.
		if off, ok := inf.offsets[f]; ok && off > f.committed {
			f.committed = off
		}
		// Entries past the committed offset were DELIVERED but their commit
		// was withheld by the build-time watermark clamp; remember the high
		// so a later flush can re-offer it once nothing is buffered.
		if hi := inf.highs[f]; hi > f.committed {
			f.exportedHigh, f.exportedHighGen = hi, f.gen
		}
		// Release the carried rotation prefix only if its group was fully drained
		// into THIS batch at build time — i.e. it really made it into the exported
		// payload. Judging by the live watermark would fire prematurely when a
		// concurrently-buffered stream is momentarily quiet.
		if f.carried != nil {
			if _, done := inf.carriedDone[f]; done {
				f.closeCarried() // exported: the rotated inodes' fds can go
			}
		}
	}
	t.lastFlush = time.Now()
}

// failBatch rewinds a failed batch's files to their committed offsets and
// purges any read-ahead entries of those files from the current batch (their
// bytes will be re-read after the rewind).
func (t *Tailer) failBatch(inf *inflight) {
	t.log.Error("exporting logs failed, rewinding", "records", inf.kept, "error", inf.err)
	obs.LogExportFailures.Inc()
	rewound := make(map[*file]bool, len(inf.gens))
	for f, gen := range inf.gens {
		if gen != f.gen {
			continue
		}
		t.rewind(f)
		rewound[f] = true
	}
	if len(t.batch) == 0 || len(rewound) == 0 {
		return
	}
	kept := t.batch[:0]
	for _, e := range t.batch {
		if !rewound[e.file] {
			kept = append(kept, e)
		}
	}
	t.batch = kept
}
