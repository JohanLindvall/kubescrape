package spool

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// FuzzSpoolCorruption builds a valid spool (appends, partial consumption),
// corrupts bytes at fuzzed offsets in segment/cursor files, and checks the
// spool stays usable: Open never fails on content corruption, Pop never panics
// and terminates, Append still works, the spool heals, and — because every
// frame is checksummed over its length and payload together — a damaged record
// is NEVER delivered. Corruption may cost data (a dropped frame, or a skipped
// segment when the framing itself is no longer trustworthy), but whatever Pop
// returns is a record that was really appended.
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

		files, seq := spoolFiles(t, dir)

		// Corrupt one file.
		if len(files) > 0 {
			path := files[int(target)%len(files)]
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch op % 3 {
			case 0: // overwrite one byte
				if len(raw) > 0 {
					pos := int(off) % len(raw)
					raw[pos] = val
					mustWrite(t, path, raw)
				}
			case 1: // truncate
				raw = raw[:int(off)%(len(raw)+1)]
				mustWrite(t, path, raw)
			case 2: // append garbage
				raw = append(raw, bytes.Repeat([]byte{val}, 1+int(off)%9)...)
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
		// and — the checksum guarantee — deliver ONLY records that were really
		// appended. A damaged frame is dropped, never handed back mangled.
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
			if !appended[string(data)] && string(data) != sentinel {
				t.Fatalf("Pop delivered a record that was never appended: %q", data)
			}
			commit()
		}

		// The spool must heal: within a few probe appends it must round-trip a
		// fresh record again (a corrupt tail may cost the first probe — Pop
		// skips the segment — but the replacement segment is clean).
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
				if got != probe && !appended[got] && got != sentinel && !strings.HasPrefix(got, "probe-") {
					t.Fatalf("Pop delivered a record that was never appended: %q", got)
				}
				if got == probe {
					healed = attempt
					break probes
				}
			}
		}
		if healed < 0 {
			t.Fatalf("spool did not heal: three probe records in a row were lost")
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

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
