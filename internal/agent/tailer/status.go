package tailer

import (
	"cmp"
	"context"
	"log/slog"
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
	// Oversized: unterminated lines this file had discarded for exceeding
	// MaxEntryBytes. The aggregate (kubescrape_log_oversized_dropped_total) is
	// a real loss counter that cannot name a file; this is which one.
	Oversized int `json:"oversized,omitempty"`
	// UnresolvedFor: how long this file has been tracked without its metadata
	// resolving, so nothing is being read from it. Omitted once resolved.
	UnresolvedFor string `json:"unresolvedFor,omitempty"`
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
	var stalled, unresolved, limited, oversized int
	// The file that has been waiting for metadata longest, and for how long:
	// the one file an operator should look at when logs are missing, since the
	// oldest is the one whose failure is least likely to be a container that
	// merely started a moment ago.
	var oldestPath string
	var oldestFor time.Duration
	now := time.Now()
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
			Oversized:   f.oversized,

			PodConfigError: f.podConfigErr,
		}
		if !f.resolved && !f.discovered.IsZero() {
			fs.UnresolvedFor = now.Sub(f.discovered).Round(time.Second).String()
		}
		if fs.Stalled {
			stalled++
		}
		if !f.resolved {
			unresolved++
			if !f.discovered.IsZero() {
				if d := now.Sub(f.discovered); d > oldestFor {
					oldestFor, oldestPath = d, f.path
				}
			}
		}
		if f.limited {
			limited++
		}
		oversized += f.oversized
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
	obs.LogFilesUnresolved.Set(float64(unresolved))
	t.status.Store(&out)
	t.lastStatus = now
	t.reportStatus(out, unresolved, limited, oversized, stalled, totalLag, maxLag, oldestPath, oldestFor)
}

// reportStatus is publishStatus's human-readable half: the periodic summary a
// first live run needs, and the one condition in it worth waking someone for.
//
// /debug/tailer has all of this per file, but it has to be ASKED, and the two
// things an operator has during an incident are the logs and the metrics. The
// summary is Debug because it repeats every statusEvery (~10s) and says
// nothing an alert would fire on; the guard is there because slog evaluates
// arguments eagerly and a Duration rendering is not free.
//
// The WARN is the unattributed-file condition, which is the one state in this
// package where a file is tracked, nothing is read from it, nothing is lost and
// no counter used to move — see obs.LogFilesUnresolved. It is age-gated
// (unresolvedWarnAfter) because every file is unresolved for its first sweep,
// and throttled because it persists for as long as its cause does.
func (t *Tailer) reportStatus(out []FileStatus, unresolved, limited, oversized, stalled int,
	totalLag, maxLag int64, oldestPath string, oldestFor time.Duration,
) {
	if t.log.Enabled(context.Background(), slog.LevelDebug) {
		t.log.Debug("tailer status",
			"files", len(out), "unresolved", unresolved, "bytes", totalLag,
			"maxBytes", maxLag, "stalled", stalled, "rateLimited", limited,
			"oversized", oversized)
	}
	if oldestFor < unresolvedWarnAfter || !t.unresolvedWarn.Allow(unresolvedWarnEvery) {
		return
	}
	t.log.Warn("log files have been tracked without resolving their metadata, so nothing is being read from them",
		"files", unresolved, "path", oldestPath, "wait", oldestFor.Round(time.Second),
		"note", "the files are intact on disk and nothing is lost; check that the metadata service is reachable")
}
