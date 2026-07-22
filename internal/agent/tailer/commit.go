package tailer

import (
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// batchInfo carries a flushed batch's commit information from build to apply:
// per-file, per-segment committed-offset candidates (already clamped to the
// build-time watermark) and the unclamped high position behind them.
type batchInfo struct {
	kept int
	// cands maps each touched file to its per-segment commit candidates. A
	// segment id that no longer resolves (a truncated-away incarnation, or a
	// segment that completed earlier) commits nothing — the segment-qualified
	// position IS the staleness check.
	cands map[*file]map[int]int64
	// highs is the per-file UNCLAMPED max end position: what could commit
	// once nothing is buffered. Recorded as file.exportedHigh on successful
	// commit where the watermark clamp withheld it.
	highs map[*file]pos
}

// commitBatch advances the committed offsets of a successfully exported
// batch: the tail candidate advances the file checkpoint, older segments'
// candidates advance their own records, and a segment whose whole range is
// now committed retires (fd closed, checkpoint entry gone).
func (t *Tailer) commitBatch(inf *batchInfo) {
	obs.LogEntries.Add(float64(inf.kept))
	for f, c := range inf.cands {
		for seg, off := range c {
			if seg == f.tail {
				if off > f.committed {
					f.committed = off
				}
				continue
			}
			if s := f.segmentByID(seg); s != nil && off > s.committed {
				s.committed = off
				if s.committed >= s.to {
					f.retire(s)
				}
			}
		}
		// Entries past the committed positions were DELIVERED but their
		// commit was withheld by the build-time watermark clamp; remember the
		// high so a later flush can re-offer it once nothing is buffered.
		if hi := inf.highs[f]; f.committedPos().less(hi) {
			f.exportedHigh = hi
		} else if !f.committedPos().less(f.exportedHigh) {
			// The commit frontier reached the remembered high: the re-offer
			// is spent; clear it so later flushes stop proposing it.
			f.exportedHigh = pos{}
		}
	}
}

// committedPos is the file's overall commit frontier: the oldest incomplete
// segment's progress, or the tail's committed offset when none remain.
func (f *file) committedPos() pos {
	if len(f.segments) > 0 {
		s := f.segments[0]
		return pos{s.id, s.committed}
	}
	return pos{f.tail, f.committed}
}

// failBatch rewinds a failed batch's files to their committed offsets; their
// bytes are re-read after the rewind. (t.batch is always empty here: flush
// clears it before the synchronous export, and nothing appends during it.)
func (t *Tailer) failBatch(inf *batchInfo, err error) {
	t.log.Error("exporting logs failed, rewinding", "records", inf.kept, "error", err)
	obs.LogExportFailures.Inc()
	for f := range inf.cands {
		t.rewind(f)
	}
}
