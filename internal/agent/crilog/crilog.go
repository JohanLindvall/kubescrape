// Package crilog parses containerd/CRI container log files.
//
// Each physical line has the form
//
//	2016-10-06T00:17:09.669794202Z stdout F log content
//
// where the third field is F for a full (or final) line and P for a partial
// line that continues in the next physical line.
package crilog

import (
	"bytes"
	"errors"
	"time"
)

// Line is one parsed physical log line.
type Line struct {
	Time    time.Time
	Stream  string // "stdout" or "stderr"
	Partial bool   // P flag: continued on the next line
	Content []byte // not retained; copy if kept beyond the call
}

var errMalformed = errors.New("malformed CRI log line")

// Parse parses one physical line (without the trailing newline). The
// returned Content aliases data.
func Parse(data []byte) (Line, error) {
	var l Line
	// Timestamp.
	sp := bytes.IndexByte(data, ' ')
	if sp <= 0 {
		return l, errMalformed
	}
	t, err := time.Parse(time.RFC3339Nano, string(data[:sp]))
	if err != nil {
		return l, errMalformed
	}
	l.Time = t
	data = data[sp+1:]
	// Stream.
	sp = bytes.IndexByte(data, ' ')
	if sp <= 0 {
		return l, errMalformed
	}
	switch string(data[:sp]) {
	case "stdout":
		l.Stream = "stdout"
	case "stderr":
		l.Stream = "stderr"
	default:
		return l, errMalformed
	}
	data = data[sp+1:]
	// Flags (P/F, possibly extended with ':'-separated tags in future).
	sp = bytes.IndexByte(data, ' ')
	if sp < 0 {
		// A full line with empty content has no trailing space.
		if string(data) == "F" {
			l.Content = nil
			return l, nil
		}
		return l, errMalformed
	}
	switch string(data[:sp]) {
	case "F":
	case "P":
		l.Partial = true
	default:
		return l, errMalformed
	}
	l.Content = data[sp+1:]
	return l, nil
}

// Assembler reassembles logical log entries from partial (P) lines. It is
// bounded: an entry growing beyond maxSize is emitted truncated.
type Assembler struct {
	maxSize int

	buf       []byte
	start     time.Time
	stream    string
	active    bool
	truncated bool
}

// Entry is one logical log entry (one application-level line).
type Entry struct {
	Time      time.Time
	Stream    string
	Body      []byte // not retained; copy if kept beyond the call
	Truncated bool
}

// NewAssembler creates an assembler that caps entries at maxSize bytes.
func NewAssembler(maxSize int) *Assembler {
	return &Assembler{maxSize: maxSize}
}

// Add consumes one parsed line and returns a completed entry, if any.
func (a *Assembler) Add(l Line) (Entry, bool) {
	if !a.active {
		if !l.Partial {
			// Common case: complete line, no buffering.
			return Entry{Time: l.Time, Stream: l.Stream, Body: l.Content}, true
		}
		a.active = true
		a.start = l.Time
		a.stream = l.Stream
		a.truncated = false
		a.buf = append(a.buf[:0], l.Content...)
		if len(a.buf) > a.maxSize {
			a.buf = a.buf[:a.maxSize]
			a.truncated = true
		}
		return Entry{}, false
	}
	// Continuation. A stream switch means we lost the terminating line;
	// flush what we have and restart.
	if l.Stream != a.stream {
		done := a.finish()
		_, _ = a.Add(l)
		return done, true
	}
	if !a.truncated {
		room := a.maxSize - len(a.buf)
		if room > 0 {
			take := min(room, len(l.Content))
			a.buf = append(a.buf, l.Content[:take]...)
		}
		if len(l.Content) > room {
			a.truncated = true
		}
	}
	if l.Partial {
		return Entry{}, false
	}
	return a.finish(), true
}

// Flush returns the buffered incomplete entry, if any. Use on shutdown or
// when a file is removed.
func (a *Assembler) Flush() (Entry, bool) {
	if !a.active {
		return Entry{}, false
	}
	return a.finish(), true
}

// Pending reports whether a partial entry is buffered.
func (a *Assembler) Pending() bool { return a.active }

func (a *Assembler) finish() Entry {
	a.active = false
	return Entry{Time: a.start, Stream: a.stream, Body: a.buf, Truncated: a.truncated}
}
