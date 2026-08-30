package positions

import (
	"encoding/json"
	"fmt"
	"maps"
	"path/filepath"
	"testing"
)

// The positions store is written from the tailer's SINGLE sweep goroutine, the
// one that serves every log file on the node, and it is written from seven call
// sites — including one per discovery pass and one per rotation sweep. Every
// millisecond a save spends is a millisecond in which no log file on the node
// is read, so the cost of a save is a whole-node stall and belongs in a
// benchmark rather than in a comment.
//
// Run without -race: the allocation figures are what these report. The wall
// clock is dominated by two fsyncs (the file, then the directory) and is a
// property of the filesystem under /tmp, not of this code — quote the
// allocation counts, and quote timings as "~".

// benchLogs builds a plausible per-file position map: containerd log paths are
// long, and a file mid-rotation carries a Pending segment.
func benchLogs(n int) map[string]LogPos {
	m := make(map[string]LogPos, n)
	for i := range n {
		p := fmt.Sprintf("/var/log/containers/some-workload-%d-7c9f8b6d54-abcde_production-namespace_container-name-%016x.log", i, i)
		lp := LogPos{Offset: int64(i) * 4096, Inode: uint64(i) + 100000, FingerprintLen: 256, FingerprintHash: uint64(i) * 2654435761}
		if i%10 == 0 {
			lp.Pending = []Prefix{{Inode: uint64(i) + 90000, FingerprintLen: 256, FingerprintHash: uint64(i), From: 1024, To: 65536}}
		}
		m[p] = lp
	}
	return m
}

func benchStore(b *testing.B, n int) (*Store, map[string]LogPos, string) {
	b.Helper()
	s, err := Open(filepath.Join(b.TempDir(), "positions.json"))
	if err != nil {
		b.Fatal(err)
	}
	m := benchLogs(n)
	var key string
	for k := range m {
		key = k
		break
	}
	return s, m, key
}

// BenchmarkSetLogs is one save of a CHANGED document — one file's offset
// advanced, which is what every sweep on a live node produces. It is the whole
// cost the sweep goroutine pays: copy, marshal, temp file, fsync, rename,
// directory fsync.
func BenchmarkSetLogs(b *testing.B) {
	for _, n := range []int{100, 1000, 3000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s, m, key := benchStore(b, n)
			i := 0
			b.ReportAllocs()
			for b.Loop() {
				i++
				cur := maps.Clone(m)
				cur[key] = LogPos{Offset: int64(i)}
				if err := s.SetLogs(cur); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSetLogsOwned is the same changed save through the ownership
// transfer the tailer uses. Both arms build the caller's map first, because
// saveCheckpoints does: the difference between them is exactly the defensive
// copy SetLogs makes of a map its caller drops on return.
func BenchmarkSetLogsOwned(b *testing.B) {
	for _, n := range []int{100, 1000, 3000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s, m, key := benchStore(b, n)
			i := 0
			b.ReportAllocs()
			for b.Loop() {
				i++
				cur := maps.Clone(m)
				cur[key] = LogPos{Offset: int64(i)}
				if err := s.SetLogsOwned(cur); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSetLogsUnchanged is the same call when nothing about the document
// changed since the last save: the tailer's 10-second cadence on a quiet node,
// and every repeat save inside one sweep. The write is skipped, so what is left
// is the marshal and the hash — no fsync, no rename.
func BenchmarkSetLogsUnchanged(b *testing.B) {
	for _, n := range []int{100, 3000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s, m, _ := benchStore(b, n)
			if err := s.SetLogs(m); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if err := s.SetLogs(m); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMarshal isolates the encode from the I/O, so a change to either can
// be attributed.
func BenchmarkMarshal(b *testing.B) {
	for _, n := range []int{100, 3000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s, m, _ := benchStore(b, n)
			if err := s.SetLogs(m); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := json.Marshal(&s.doc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkLogs is the load side: the tailer reads it once per save while the
// last listing has failed, and once at startup.
func BenchmarkLogs(b *testing.B) {
	s, m, _ := benchStore(b, 3000)
	if err := s.SetLogs(m); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = s.Logs()
	}
}
