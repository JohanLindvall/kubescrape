package otlpexport

// The warm gzip-writer slot: what it guarantees, and what it costs.

import (
	"bytes"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/klauspost/compress/gzip"
)

// A gzip writer must survive a GC between exports. It could not before the warm
// slot existed: sync.Pool is drained by every collection, and the agent's GC
// cadence (1.9-2.5 s under load) is the same as the tailer's flush interval, so
// the pool missed on ~81% of a live node's compressions and each miss rebuilt a
// level-5 encoder — 1,076,536 B and 17 allocations against a hit's ~2.4 kB and
// 1. This is the budget that fails the build if the slot stops holding.
func TestGzipWriterSurvivesGCAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	c := &gzipCodec{}
	body := benchBody()
	compress := func() {
		w, err := c.Compress(io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(body); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	compress() // fill the warm slot

	got := testing.AllocsPerRun(10, func() {
		// Two collections: the first moves the sync.Pool's contents to its
		// victim cache, the second drops them. Only the warm slot can carry a
		// writer across both, which is the whole point.
		runtime.GC()
		runtime.GC()
		compress()
	})
	// One allocation is the per-message pooledGzipWriter wrapper. The ceiling
	// leaves room for the runtime's own accounting around the two collections
	// and nothing like a rebuilt encoder (17).
	if want := 6.0; got > want {
		t.Errorf("a compression after two GCs allocates %.1f times, want <= %.1f "+
			"(a rebuilt klauspost encoder is 17 allocations and ~1 MiB)", got, want)
	}
}

// The reader half of the same guarantee. It is the one paid on the SEND path
// as well as the receive path: grpc-go compresses its responses, so every
// export decompresses a two-byte OTLP ExportResponse, and a pool miss there
// built a 32 KiB LZ77 window plus huffman tables to read two bytes — 41,496 B
// and 10 allocations against a hit's 62 B and 1.
func TestGzipReaderSurvivesGCAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	c := &gzipCodec{}
	gz := gzipTestBytes(t, []byte{0x0a, 0x00}, nil) // an OTLP ExportResponse
	src := bytes.NewReader(nil)
	var sink [64]byte
	decompress := func() {
		src.Reset(gz)
		r, err := c.Decompress(src)
		if err != nil {
			t.Fatal(err)
		}
		for {
			if _, err := r.Read(sink[:]); err != nil {
				break
			}
		}
		if err := r.(io.Closer).Close(); err != nil {
			t.Fatal(err)
		}
	}
	decompress() // fill the warm slot

	got := testing.AllocsPerRun(10, func() {
		runtime.GC()
		runtime.GC()
		decompress()
	})
	// One allocation is the per-message pooledGzipReader wrapper (plus the one
	// bufio the first release of a given reader builds, which is why the
	// ceiling is not 1). A rebuilt gzip.Reader is 10.
	if want := 6.0; got > want {
		t.Errorf("a decompression after two GCs allocates %.1f times, want <= %.1f "+
			"(a rebuilt gzip.Reader is 10 allocations and ~37 kB)", got, want)
	}
}

// A writer taken from the warm slot must be usable — Reset onto a new sink and
// producing a stream the reader half accepts. The cheap way to get this wrong
// is to hand back a writer that is still pointing at the previous sink.
func TestWarmGzipWriterIsReusable(t *testing.T) {
	c := &gzipCodec{}
	for i := 0; i < 3; i++ {
		var sink countingWriter
		w, err := c.Compress(&sink)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte("hello gzip")); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if sink.n == 0 {
			t.Fatalf("round %d: the writer wrote nothing to its own sink", i)
		}
	}
}

type countingWriter struct{ n int }

func (w *countingWriter) Write(p []byte) (int, error) { w.n += len(p); return len(p), nil }

// finalizedSink is a writer big enough (and pointer-bearing enough) to get its
// own heap object, so a finalizer on it observes exactly its own reachability.
type finalizedSink struct{ buf []byte }

func (w *finalizedSink) Write(p []byte) (int, error) { w.buf = append(w.buf, p...); return len(p), nil }

// A pooled writer must not hold the sink it just wrote to. The warm slot is
// what makes this matter: a sync.Pool entry was dropped by the next collection,
// so the reference lived a cycle at most; a warm slot holds it for the process'
// lifetime, and on the gRPC path that sink is grpc-go's assembly writer over
// its own recycled buffers while on the HTTP path it is a bufpool.Buffer the
// caller Releases immediately afterwards. Measured with a finalizer, which is
// the same instrument (and the same argument) as
// TestPooledGzipReaderReleasesTheMessagesSource.
func TestPooledGzipWriterReleasesItsSink(t *testing.T) {
	c := &gzipCodec{}
	var collected atomic.Bool
	func() {
		// NOT countingWriter: an 8-byte pointer-free struct goes through the
		// runtime's tiny allocator, where several objects share one block and a
		// finalizer waits for ALL of them — which made this pass alone and fail
		// inside the package's full run. The buffer field is what keeps it out
		// of that path.
		sink := &finalizedSink{}
		runtime.SetFinalizer(sink, func(any) { collected.Store(true) })
		w, err := c.Compress(sink)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte("a message worth compressing")); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil { // the writer is pooled here
			t.Fatal(err)
		}
	}()
	// Take the writer back out and hold it ALIVE: the sink must be unreachable
	// while the writer that wrote to it is still around, or the test would
	// prove only that both were collected together.
	z := c.writers.get()
	if z == nil {
		t.Fatal("the writer never reached the cache; this test would prove nothing")
	}
	for i := 0; i < 50 && !collected.Load(); i++ {
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
	runtime.KeepAlive(z)
	if !collected.Load() {
		t.Fatal("a pooled gzip writer still references the sink it wrote to, and the warm slot holds that reference for the life of the process")
	}
}

// What the warm slot PINS. The doc on gzipWriterCache quotes this figure to
// justify holding exactly one writer per level; a klauspost or level change
// that made a warm writer an order of magnitude larger would turn "about a
// MiB on a 71 MiB DaemonSet" into a claim worth re-arguing, so it is asserted
// rather than remembered.
func TestWarmGzipWriterRetainedSize(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the detector's own bookkeeping is charged to the heap this measures")
	}
	const writers = 8
	body := benchBody()
	keep := make([]*gzip.Writer, 0, writers)
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc
	for i := 0; i < writers; i++ {
		z := newGzipWriter(gzip.DefaultCompression)
		z.Reset(io.Discard)
		if _, err := z.Write(body); err != nil {
			t.Fatal(err)
		}
		if err := z.Close(); err != nil {
			t.Fatal(err)
		}
		keep = append(keep, z)
	}
	runtime.GC()
	runtime.ReadMemStats(&ms)
	per := float64(ms.HeapAlloc-base) / writers
	runtime.KeepAlive(keep)
	t.Logf("a warm gzip writer retains %.0f KiB", per/1024)
	// Measured 996 KiB. The bounds are deliberately wide — this is an alarm on
	// an order-of-magnitude change, not a pin on an exact allocator layout.
	if per < 128<<10 || per > 4<<20 {
		t.Errorf("a warm gzip writer retains %.0f KiB, want roughly 1 MiB "+
			"(the gzipWriterCache doc's one-slot argument is sized on it)", per/1024)
	}
}
