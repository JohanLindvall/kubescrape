package tailer

// Offset persistence through the shared positions store.

import (
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
)

// checkpoint is one file's persisted position (shared shape with the
// unified positions store).
type checkpoint = positions.LogPos

func (t *Tailer) loadCheckpoints() map[string]checkpoint {
	if t.cfg.Positions == nil {
		return nil
	}
	return t.cfg.Positions.Logs()
}

// checkpointing reports whether any checkpoint store is configured.
func (t *Tailer) checkpointing() bool {
	return t.cfg.Positions != nil
}

func (t *Tailer) saveCheckpoints() {
	t.lastCheckpoint = time.Now()
	if !t.checkpointing() {
		return
	}
	cps := make(map[string]checkpoint, len(t.files))
	for path, f := range t.files {
		t.extendFingerprint(f)
		cp := checkpoint{
			Offset: f.committed, Inode: f.inode,
			FingerprintLen: f.fp.Len, FingerprintHash: f.fp.Hash,
		}
		for _, c := range f.segments {
			// From is the segment's commit PROGRESS: a restart re-reads only
			// the owed [From, To) remainder.
			cp.Pending = append(cp.Pending, positions.Prefix{
				Inode:           c.inode,
				FingerprintLen:  c.fp.Len,
				FingerprintHash: c.fp.Hash,
				From:            c.committed,
				To:              c.to,
			})
		}
		cps[path] = cp
	}
	if err := t.cfg.Positions.SetLogs(cps); err != nil {
		t.log.Warn("writing positions file", "error", err)
	}
}
