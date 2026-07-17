package tailer

import (
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// batchInfo carries a flushed batch's commit information from build to apply:
// per-file committed-offset candidates (clamped to the build-time watermark),
// the unclamped highs behind them, the files' rotation generations, and which
// carried rotation prefixes were fully drained into the batch.
type batchInfo struct {
	kept    int
	offsets map[*file]int64
	// highs is the per-file UNCLAMPED batch max offset (current-gen entries
	// plus any re-offered exportedHigh): what committed could reach once
	// nothing is buffered. Recorded as file.exportedHigh on successful commit
	// where the watermark clamp withheld it.
	highs map[*file]int64
	// gens (keyed by every file the batch touches) records each file's
	// rotation generation at BUILD time; commit/fail apply only where the gen
	// is unchanged (a backstop: flush applies synchronously, and the per-entry
	// gen filter already excludes pre-rotation offsets at build).
	gens map[*file]int
	// carriedDone holds the touched files whose carried rotation prefix was
	// already fully drained into this batch at BUILD time; only those may have
	// their carried fds released once the batch exports (see flush).
	carriedDone map[*file]struct{}
}

// commitBatch advances the committed offsets of a successfully exported
// batch: never past lines still buffered in the pipeline, and only when the
// file's rotation generation is unchanged since the batch was built.
func (t *Tailer) commitBatch(inf *batchInfo) {
	obs.LogEntries.Add(float64(inf.kept))
	for f, gen := range inf.gens {
		if gen != f.gen {
			continue // rotated since build; offsets are stale
		}
		// offsets was already clamped to the BUILD-time watermark in flush, so
		// it never names a line still buffered when this batch was built.
		if off, ok := inf.offsets[f]; ok && off > f.committed {
			f.committed = off
		}
		// Entries past the committed offset were DELIVERED but their commit
		// was withheld by the build-time watermark clamp; remember the high
		// so a later flush can re-offer it once nothing is buffered.
		if hi := inf.highs[f]; hi > f.committed {
			f.exportedHigh, f.exportedHighGen = hi, f.gen
		}
		// Release the carried rotation prefix only if its group was fully
		// drained into THIS batch at build time — i.e. it really made it into
		// the exported payload. Judging by the live watermark would fire
		// prematurely when a concurrently-buffered stream is momentarily
		// quiet.
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
func (t *Tailer) failBatch(inf *batchInfo, err error) {
	t.log.Error("exporting logs failed, rewinding", "records", inf.kept, "error", err)
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
