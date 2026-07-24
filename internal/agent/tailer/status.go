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
	obs.LogLagBytes.Set(float64(maxLag))
	obs.LogLagBytesTotal.Set(float64(totalLag))
	t.status.Store(&out)
	t.lastStatus = time.Now()
}
