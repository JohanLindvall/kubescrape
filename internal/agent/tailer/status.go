package tailer

import (
	"cmp"
	"os"
	"slices"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// FileStatus is one tracked file's position snapshot, for the agent's
// /debug/tailer endpoint. Lag is the backlog: bytes on disk not yet exported
// and committed (size - committed, 0 when the file shrank/rotated).
type FileStatus struct {
	Path        string `json:"path"`
	Source      string `json:"source,omitempty"`
	ContainerID string `json:"containerId,omitempty"`
	Size        int64  `json:"size"`
	ReadPos     int64  `json:"readPos"`
	Committed   int64  `json:"committed"`
	Lag         int64  `json:"lag"`
	Resolved    bool   `json:"resolved"`
	Compressed  bool   `json:"compressed,omitempty"`
	Segments    int    `json:"segments,omitempty"`
	RateLimited bool   `json:"rateLimited,omitempty"`
	// Stalled: the live tail is NOT being read because a rotated segment's
	// replay CANNOT PROCEED (readFile's gate, held by a segment whose stall
	// clock is running). Lag grows while it holds, nothing is lost, and the
	// aggregate is kubescrape_log_segments_stalled — this field is which FILE.
	// A replay merely UNFINISHED is not stalled: see file.stalledReplay.
	Stalled bool `json:"stalled,omitempty"`
	// PodConfigError: the pod's kubescrape.io/logs annotation failed to parse
	// and was ignored (aggregate: kubescrape_log_pod_config_invalid_total).
	PodConfigError string `json:"podConfigError,omitempty"`
}

// stalledReplay reports whether this file's live tail is gated behind a
// rotated segment that is not PROGRESSING — the one state where a file stops
// collecting without losing anything, which is what the gauge is alerted on.
//
// "Unfinished" is the wrong test and was the one used: a replay walking a 10
// MiB rotated log at MaxBytesPerSweep is unfinished for its whole (healthy)
// duration, so a node recovering from an outage held the gauge up throughout
// and a sustained-nonzero alert paged on a recovery that was working. The
// stall CLOCK is the honest signal — chargeStall arms it on a pass that fed
// nothing, discarded nothing and was not purged by a rewind, and clears it on
// any of the three — so a budget-cut replay never lights it, a collector
// outage never lights it, and a source that will not open lights it on its
// second pass and stays lit until the segment is given up on.
func (f *file) stalledReplay() bool {
	if f.segmentsFed {
		return false
	}
	for _, sg := range f.segments {
		if !sg.stalledSince.IsZero() {
			return true
		}
	}
	return false
}

// Status returns the most recently published per-file snapshot (refreshed on
// the checkpoint cadence, ~10s). Safe to call from any goroutine.
func (t *Tailer) Status() []FileStatus {
	if s := t.status.Load(); s != nil {
		return *s
	}
	return nil
}

// publishStatus snapshots every tracked file (one stat each), updates the lag
// gauges, and publishes the snapshot for Status. Runs on the sweep goroutine.
func (t *Tailer) publishStatus() {
	out := make([]FileStatus, 0, len(t.files))
	var maxLag, totalLag int64
	var stalled int
	for _, f := range t.files {
		if f.excluded {
			continue // annotation opt-out: nothing is read, lag is not real
		}
		fs := FileStatus{
			Path:        f.path,
			ContainerID: f.containerID,
			ReadPos:     f.readPos,
			Committed:   f.committed,
			Resolved:    f.resolved,
			Compressed:  f.compressed,
			Segments:    len(f.segments),
			RateLimited: f.limited,
			Stalled:     f.stalledReplay(),

			PodConfigError: f.podConfigErr,
		}
		if fs.Stalled {
			stalled++
		}
		if f.source != nil {
			fs.Source = f.source.name
		}
		if st, err := os.Stat(f.path); err == nil {
			fs.Size = st.Size()
		} else {
			fs.Size = f.readPos // gone/rotating; best effort
		}
		lag := fs.Size - fs.Committed
		if f.compressed {
			// Size is compressed on-disk bytes but the offsets live in
			// DECOMPRESSED space — their difference is meaningless. Report
			// the read-but-uncommitted backlog instead (the unread remainder
			// is unknowable without decompressing).
			lag = fs.ReadPos - fs.Committed
		}
		if lag > 0 {
			fs.Lag = lag
			totalLag += lag
			if lag > maxLag {
				maxLag = lag
			}
		}
		out = append(out, fs)
	}
	slices.SortFunc(out, func(a, b FileStatus) int { return cmp.Compare(b.Lag, a.Lag) })
	obs.LogLagMaxBytes.Set(float64(maxLag))
	obs.LogLagTotalBytes.Set(float64(totalLag))
	obs.LogSegmentsStalled.Set(float64(stalled))
	t.status.Store(&out)
	t.lastStatus = time.Now()
}
