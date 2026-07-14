package spool

import (
	"encoding/binary"
	"testing"
)

func benchPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

func sizeName(n int) string {
	switch {
	case n >= 1<<20:
		return "1MiB"
	case n >= 64<<10:
		return "64KiB"
	case n >= 4<<10:
		return "4KiB"
	default:
		return "256B"
	}
}

// BenchmarkFrameSum isolates the per-frame checksum: the work Append adds on
// top of its write+fsync. It must stay allocation-free — the digest is
// stack-held — and it is ~3 orders of magnitude cheaper than the fsync it rides
// along with (compare against BenchmarkAppend), so integrity here is close to
// free. Read the two together before trading the checksum away for throughput.
func BenchmarkFrameSum(b *testing.B) {
	for _, size := range []int{256, 4 << 10, 64 << 10, 1 << 20} {
		data := benchPayload(size)
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], uint32(size))
		b.Run(sizeName(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			var sink uint64
			for i := 0; i < b.N; i++ {
				sink = frameSum(hdr[:], data)
			}
			_ = sink
		})
	}
}

// BenchmarkAppend measures the full durable append (frame + write + fsync). The
// fsync dominates by design: Append must not return until the record is on
// disk, since the tailer advances its checkpoint on the strength of it.
func BenchmarkAppend(b *testing.B) {
	for _, size := range []int{256, 4 << 10, 64 << 10} {
		data := benchPayload(size)
		b.Run(sizeName(size), func(b *testing.B) {
			s, err := Open(b.TempDir(), Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Append(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
