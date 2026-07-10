package journald

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/go-systemd/v22/sdjournal"
)

// waitTimeout bounds a blocking journal wait so context cancellation is noticed
// promptly between waits.
const waitTimeout = time.Second

// sdSource reads the systemd journal through libsystemd.
type sdSource struct {
	j *sdjournal.Journal
	// skipCursor, when set, is the resume cursor whose (already-exported) entry
	// must be skipped once — but only if it is still present (it may have been
	// rotated away, in which case the first entry is genuinely new).
	skipCursor string
}

// openJournal opens the journal positioned just after afterCursor, or at the
// tail when afterCursor is empty. It is the default Reader.open.
func openJournal(cfg Config, afterCursor string) (source, error) {
	var (
		j   *sdjournal.Journal
		err error
	)
	if cfg.Dir != "" {
		j, err = sdjournal.NewJournalFromDir(cfg.Dir)
	} else {
		j, err = sdjournal.NewJournal()
	}
	if err != nil {
		return nil, fmt.Errorf("opening journal: %w", err)
	}

	// Matches on the same field are OR'd by systemd, so multiple units become a
	// disjunction automatically.
	for _, unit := range cfg.Units {
		if err := j.AddMatch("_SYSTEMD_UNIT=" + unit); err != nil {
			_ = j.Close()
			return nil, fmt.Errorf("adding unit match %q: %w", unit, err)
		}
	}

	s := &sdSource{j: j}
	if afterCursor != "" {
		if err := j.SeekCursor(afterCursor); err != nil {
			_ = j.Close()
			return nil, fmt.Errorf("seeking cursor: %w", err)
		}
		s.skipCursor = afterCursor
	} else {
		// Start at the tail: SeekTail anchors past the end; Previous positions on
		// the last existing entry so the first Next moves to genuinely new ones.
		if err := j.SeekTail(); err != nil {
			_ = j.Close()
			return nil, fmt.Errorf("seeking tail: %w", err)
		}
		if _, err := j.Previous(); err != nil {
			_ = j.Close()
			return nil, fmt.Errorf("anchoring tail: %w", err)
		}
	}
	return s, nil
}

func (s *sdSource) next(ctx context.Context) (rawEntry, bool, error) {
	for {
		if ctx.Err() != nil {
			return rawEntry{}, false, ctx.Err()
		}
		n, err := s.j.Next()
		if err != nil {
			return rawEntry{}, false, err
		}
		if n == 0 {
			// Caught up; block for new entries, then re-check.
			s.j.Wait(waitTimeout)
			continue
		}
		e, err := s.j.GetEntry()
		if err != nil {
			return rawEntry{}, false, err
		}
		if s.skipCursor != "" {
			skip := e.Cursor == s.skipCursor
			s.skipCursor = ""
			if skip {
				continue // the already-exported resume entry
			}
		}
		return rawEntry{
			fields:   e.Fields,
			cursor:   e.Cursor,
			realtime: time.UnixMicro(int64(e.RealtimeTimestamp)),
		}, true, nil
	}
}

func (s *sdSource) close() error { return s.j.Close() }
