package spool

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// FuzzSpoolCorruption builds a valid spool (appends, partial consumption),
// corrupts bytes at fuzzed offsets in segment/cursor files, and checks the
// spool stays usable: Open never fails on content corruption, Pop never
// panics and terminates, Append still works, and — whenever the corruption
// could not have forged frame boundaries (cursor damage, truncation, a byte
// flipped inside a payload) — every delivered record is one of the appended
// payloads (possibly duplicated or with the one flipped byte). Frames carry
// no checksums by design, so corruption that rewrites framing (length
// prefixes, appended garbage) may surface garbage records; those cases only
// assert the structural invariants.
func FuzzSpoolCorruption(f *testing.F) {
	f.Add(uint8(6), uint8(2), uint8(0), uint8(0), uint32(5), uint8(0xff), uint16(0))
	f.Add(uint8(6), uint8(2), uint8(0), uint8(1), uint32(9), uint8(0), uint16(64))
	f.Add(uint8(6), uint8(2), uint8(1), uint8(2), uint32(3), uint8(0x41), uint16(100))
	f.Add(uint8(12), uint8(0), uint8(2), uint8(0), uint32(0), uint8(1), uint16(500))
	f.Add(uint8(1), uint8(1), uint8(0), uint8(1), uint32(1<<20), uint8(7), uint16(33))
	f.Add(uint8(3), uint8(3), uint8(3), uint8(2), uint32(17), uint8(0x80), uint16(80))

	f.Fuzz(func(t *testing.T, nRecords, commitN, target, op uint8, off uint32, val uint8, segHint uint16) {
		dir := t.TempDir()
		segSize := int64(48 + int64(segHint)%512) // small: force several segments
		opts := Options{SegmentBytes: segSize, MaxBytes: 1 << 20}

		sp, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("initial Open: %v", err)
		}
		n := 1 + int(nRecords)%12
		appended := map[string]bool{}
		for i := 0; i < n; i++ {
			payload := []byte(fmt.Sprintf("record-%02d-%s", i, strings.Repeat("x", i*7%40)))
			if err := sp.Append(payload); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
			appended[string(payload)] = true
		}
		for i := 0; i < int(commitN)%(n+1); i++ {
			data, commit, ok, err := sp.Pop()
			if err != nil || !ok {
				t.Fatalf("pre-corruption Pop %d: ok=%v err=%v", i, ok, err)
			}
			if !appended[string(data)] {
				t.Fatalf("pre-corruption Pop returned unknown payload %q", data)
			}
			commit()
		}
		_ = sp.Close()

		// Snapshot the frame layout so a corrupted offset can be classified as
		// payload vs framing.
		files, seq := spoolFiles(t, dir)

		// Corrupt one file.
		framingIntact := true
		if len(files) > 0 {
			path := files[int(target)%len(files)]
			isCursor := filepath.Base(path) == cursorName
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch op % 3 {
			case 0: // overwrite one byte
				if len(raw) > 0 {
					pos := int(off) % len(raw)
					if raw[pos] != val {
						if !isCursor && !inPayload(raw, pos) {
							framingIntact = false
						}
						raw[pos] = val
						mustWrite(t, path, raw)
					}
				}
			case 1: // truncate
				raw = raw[:int(off)%(len(raw)+1)]
				mustWrite(t, path, raw)
			case 2: // append garbage
				raw = append(raw, bytes.Repeat([]byte{val}, 1+int(off)%9)...)
				if !isCursor {
					framingIntact = false
				}
				mustWrite(t, path, raw)
			}
		}
		_ = seq

		// Reopen: content corruption must never fail Open.
		sp2, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("Open after corruption: %v", err)
		}
		defer func() { _ = sp2.Close() }()

		// Append must still work.
		sentinel := "sentinel-after-corruption"
		if err := sp2.Append([]byte(sentinel)); err != nil {
			t.Fatalf("Append after corruption: %v", err)
		}

		// Drain: must terminate within a bounded number of Pops, never panic,
		// and (with framing intact) deliver only appended payloads, the single
		// flipped-byte variant, or the sentinel.
		sawSentinel := false
		limit := n + 16
		for i := 0; ; i++ {
			if i > limit {
				t.Fatalf("Pop did not drain within %d iterations", limit)
			}
			data, commit, ok, err := sp2.Pop()
			if err != nil {
				continue // surfaced error; the spool must have made progress (bounded by limit)
			}
			if !ok {
				break // empty
			}
			if framingIntact && !appended[string(data)] && string(data) != sentinel && !offByOneByte(appended, data) {
				t.Fatalf("Pop delivered a never-appended record %q (framing intact)", data)
			}
			if string(data) == sentinel {
				sawSentinel = true
			}
			commit()
		}
		if framingIntact && !sawSentinel {
			t.Fatalf("sentinel appended after corruption was never delivered (framing intact)")
		}

		// The spool must heal: within a few probe appends it must round-trip a
		// fresh record again (a desynchronized tail may cost the first probe —
		// Pop skips the corrupt segment — but the replacement segment is
		// clean). With framing intact there is no desync, so the very first
		// probe must survive.
		healed := -1
	probes:
		for attempt := 0; attempt < 3; attempt++ {
			probe := fmt.Sprintf("probe-%d", attempt)
			if err := sp2.Append([]byte(probe)); err != nil {
				t.Fatalf("Append probe %d: %v", attempt, err)
			}
			for i := 0; ; i++ {
				if i > limit {
					t.Fatalf("probe %d: Pop did not terminate within %d iterations", attempt, limit)
				}
				data, commit, ok, err := sp2.Pop()
				if err != nil {
					continue
				}
				if !ok {
					break // probe lost to a segment skip; try a fresh one
				}
				got := string(data)
				commit()
				if got == probe {
					healed = attempt
					break probes
				}
			}
		}
		if healed < 0 {
			t.Fatalf("spool did not heal: three probe records in a row were lost")
		}
		if framingIntact && healed != 0 {
			t.Fatalf("lost %d probe record(s) with framing intact", healed)
		}
	})
}

// spoolFiles lists the spool's files (segments sorted by seq, then the
// cursor) and the segment seqs.
func spoolFiles(t *testing.T, dir string) ([]string, []int64) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var segs []string
	var seqs []int64
	cursor := ""
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), segSuffix) {
			segs = append(segs, filepath.Join(dir, e.Name()))
		} else if e.Name() == cursorName {
			cursor = filepath.Join(dir, e.Name())
		}
	}
	sort.Strings(segs)
	files := segs
	if cursor != "" {
		files = append(files, cursor)
	}
	return files, seqs
}

// inPayload reports whether pos lies inside a frame's payload bytes (not a
// length prefix) when walking the segment's frames from the start.
func inPayload(seg []byte, pos int) bool {
	off := 0
	for off+frameHeader <= len(seg) {
		n := int(binary.BigEndian.Uint32(seg[off:]))
		end := off + frameHeader + n
		if end > len(seg) {
			return false // torn tail region
		}
		if pos >= off+frameHeader && pos < end {
			return true
		}
		if pos < off+frameHeader {
			return false // in this frame's header
		}
		off = end
	}
	return false
}

// offByOneByte reports whether data equals some appended payload with exactly
// one byte changed (the corruption we injected).
func offByOneByte(appended map[string]bool, data []byte) bool {
	for p := range appended {
		if len(p) != len(data) {
			continue
		}
		diff := 0
		for i := 0; i < len(p); i++ {
			if p[i] != data[i] {
				diff++
			}
		}
		if diff == 1 {
			return true
		}
	}
	return false
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
