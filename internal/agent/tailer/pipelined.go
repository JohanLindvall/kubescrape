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
	gens    map[*file]int
	touched map[*file]struct{}
	err     error
	done    chan struct{}
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
	if _, ok := t.inflight.touched[f]; ok {
		t.settleInflight()
	}
}

// commitBatch advances the committed offsets of a successfully exported
// batch: never past lines still buffered in the pipeline, and only when the
// file's rotation generation is unchanged since the batch was built.
func (t *Tailer) commitBatch(inf *inflight) {
	obs.LogEntries.Add(float64(inf.kept))
	for f := range inf.touched {
		if inf.gens[f] != f.gen {
			continue // rotated since build; offsets are stale
		}
		if off, ok := inf.offsets[f]; ok {
			if wm, wok := f.watermark(); wok && wm < off {
				off = wm
			}
			if off > f.committed {
				f.committed = off
			}
		}
		// Once the carried group has fully drained (nothing buffered), its
		// record has been exported, so the rotated-away prefix is no longer
		// needed for recovery.
		if f.carried != nil {
			if _, wok := f.watermark(); !wok {
				f.carried = nil
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
	rewound := make(map[*file]bool, len(inf.touched))
	for f := range inf.touched {
		if inf.gens[f] != f.gen {
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
