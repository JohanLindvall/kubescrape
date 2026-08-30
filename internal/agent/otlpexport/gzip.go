package otlpexport

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/JohanLindvall/bufpool"
	"github.com/klauspost/compress/gzip"
	"google.golang.org/grpc/encoding"
)

// The gRPC "gzip" compressor is registered here backed by klauspost/compress
// (wire-compatible with the stdlib codec grpc ships, roughly twice as fast).
// Do NOT also import google.golang.org/grpc/encoding/gzip — whichever init
// runs last wins the registration.
func init() {
	encoding.RegisterCompressor(&gzipCodec{})
}

const gzipName = "gzip"

// assertGzipCodec verifies our klauspost-backed codec still owns the gRPC
// "gzip" registration. A future dependency importing grpc/encoding/gzip
// with a later-running init would silently displace it — wire-compatible,
// but -otlp-compression-level and the pooled writers would be bypassed.
// Called at client construction so the fragility is a startup error, not a
// silent behavior change.
func assertGzipCodec() error {
	if _, ok := encoding.GetCompressor(gzipName).(*gzipCodec); !ok {
		return fmt.Errorf("gRPC %q compressor displaced (another package registered one after ours; do not import google.golang.org/grpc/encoding/gzip)", gzipName)
	}
	return nil
}

type gzipCodec struct {
	writers gzipWriterCache // see warmPool: a warm slot in front of a sync.Pool
	readers gzipReaderCache // the same, for Decompress
}

type (
	gzipWriterCache = warmPool[gzip.Writer]
	gzipReaderCache = warmPool[gzip.Reader]
)

// warmPool is a sync.Pool with ONE slot in front of it that the garbage
// collector cannot take back.
//
// The pool alone could not keep either half of a gzip codec, and the reason is
// structural rather than a tuning accident: sync.Pool is drained by every GC
// (the local set becomes the victim cache and the previous victim set is
// dropped), so an object survives at most one cycle. An agent GCs every
// 1.9-2.5 s under load while the tailer flushes every 2 s — the same period, so
// the pool was being asked to bridge a gap it structurally cannot. Measured on
// a live node: 447 klauspost flate.NewWriter constructions against ~550 gRPC
// compressions in 1000 s, an ~81% miss rate on a pool whose whole job is that
// object.
//
// What a miss costs, both halves, quoted in ALLOCATIONS rather than
// nanoseconds — the nanoseconds are swamped by the deflate/inflate itself and
// did not resolve on a loaded machine:
//
//	compress a 460 kB body           1 alloc  ->  1,078,328 B / 17 allocs
//	decompress the 2-byte response   1 alloc  ->     43,288 B / 10 allocs
//
// (BenchmarkGzipCompressGCBetween and BenchmarkGzipDecompressResponseGCBetween,
// against a pristine pre-warm-slot tree; re-measured 2026-08-29 on go1.26.6 at
// -benchtime 3000x.)
//
// DO NOT QUOTE A WARM-PATH B/op HERE, which two earlier revisions of this
// comment did with two different numbers. The benchmark still pays ONE cold
// construction before the slot is warm, so its warm B/op is that construction
// divided by the iteration count and nothing else: the same compress path
// reports 3,610 B/op at 3e2 iterations, 375 at 3e3 and 70 at 2e4. The
// allocation COUNT is the stable figure and it has been 1 every time.
//
// The writer figure is the larger because klauspost
// builds the compressor lazily — gzip.NewWriterLevel itself is 160 B — and the
// first Write then materialises a level-5 encoder's window and hash tables.
// That was ~5% of everything the agent allocated, spent rebuilding it twice a
// second. The reader half is smaller per message but is paid on the SEND path
// too: grpc-go compresses its responses, so every export decompresses two
// bytes.
//
// ONE slot, not a free list, because the price is retained memory and the
// benefit is not linear in the count. A warm writer holds 996 KiB
// (TestWarmGzipWriterRetainedSize pins it) and a reader ~37 kB, while the
// agent's steady state is SERIAL: the tailer's single sweep goroutine exports
// inline, the scraper's cycle exports after it, self-metrics once a minute. One
// slot converts that stream from a miss per export to a hit per export; a
// second would buy only the rarer overlap, which the sync.Pool behind it
// already serves — within a burst there is no GC to drain it.
//
// So what this pins is ONE object per cache that has been used, which is a
// ceiling rather than a growth path: the gRPC codec is one cache pair, and the
// HTTP body path is one per gzip LEVEL, of which a deployment uses one (the
// flag is a single value; only routes configured at differing levels can reach
// more). That is ~1 MiB on a DaemonSet running the default pipelines, whose
// RSS is ~70 MiB (the same process reads 66.9 MiB in internal/cli/memlimit.go's
// measurement). README's "~28 MB agent RSS" is a DIFFERENT configuration — a
// metrics-only agent with the tailer off, scraping one 100k-series target — and
// the two are not in conflict; each names its shape. The metadata
// service links this too and pushes self-metrics once a minute against a 9-11
// MiB live heap, where the same MiB is a larger relative cost — and still the
// right trade, since at 39 collections a second that pool misses every single
// time, and a slightly larger live heap raises the GC goal rather than the
// footprint.
//
// get takes the warm slot first and falls back to the pool; put fills the warm
// slot first and falls back to the pool, which is exactly what this did before
// the slot existed. Both are lock-free: the slot is one atomic swap.
type warmPool[T any] struct {
	warm atomic.Pointer[T]
	pool sync.Pool // *T, the overflow for concurrent use
}

// get returns an object to reuse, or nil when neither the warm slot nor the
// pool holds one. The caller Resets it.
func (c *warmPool[T]) get() *T {
	if v := c.warm.Swap(nil); v != nil {
		return v
	}
	v, _ := c.pool.Get().(*T)
	return v
}

// put offers a finished object back. The warm slot wins when it is empty —
// that is the one a GC cannot reclaim — and everything else goes to the pool.
func (c *warmPool[T]) put(v *T) {
	if c.warm.CompareAndSwap(nil, v) {
		return
	}
	c.pool.Put(v)
}

// effectiveGzipLevel maps Config.CompressionLevel (0 = "the library default")
// onto a real gzip level, so two destinations spelling the same intent
// differently are never treated as a conflict.
func effectiveGzipLevel(level int) int {
	if level == 0 {
		return gzip.DefaultCompression
	}
	return level
}

// codecGzipLevel is the level the gRPC codec compresses at, and it is the ONE
// piece of this that cannot be per-client: gRPC resolves a compressor by NAME
// from a process-global registry and Compress(io.Writer) carries no per-call
// context, so a single registered codec serves every gRPC destination in the
// process. New is called once per destination (per-signal x3 plus the default,
// plus one per route), so letting the last construction win would silently
// give one destination's level to all of them, in construction order.
// pinGzipLevel therefore records the first gzip-over-gRPC client's level and
// REFUSES a later one that disagrees. The HTTP body path has no such
// constraint and takes its level as a parameter.
var (
	codecGzipLevel  atomic.Int32 // read by the codec's writer pool
	gzipPinMu       sync.Mutex
	gzipPinned      bool
	gzipPinnedLevel int
)

func init() { codecGzipLevel.Store(gzip.DefaultCompression) }

func pinGzipLevel(level int) error {
	gzipPinMu.Lock()
	defer gzipPinMu.Unlock()
	if gzipPinned && gzipPinnedLevel != level {
		return fmt.Errorf("gzip compression level %d conflicts with level %d already in use by another gRPC destination in this process: "+
			"the gRPC %q compressor is registered by name process-wide and cannot differ per destination (use one level for every gRPC destination, or the http protocol, whose level is per-destination)",
			level, gzipPinnedLevel, gzipName)
	}
	gzipPinned, gzipPinnedLevel = true, level
	codecGzipLevel.Store(int32(level))
	return nil
}

func newGzipWriter(level int) *gzip.Writer {
	w, err := gzip.NewWriterLevel(nil, level)
	if err != nil {
		return gzip.NewWriter(nil)
	}
	return w
}

func (c *gzipCodec) Compress(w io.Writer) (io.WriteCloser, error) {
	z := c.writers.get()
	if z == nil {
		z = newGzipWriter(int(codecGzipLevel.Load()))
	}
	z.Reset(w)
	return &pooledGzipWriter{Writer: z, cache: &c.writers}, nil
}

// Decompress hands back a POOLED reader. A gzip reader carries a fixed 32 KiB
// LZ77 window plus its huffman tables — 36.7 kB in 5 allocations, measured per
// gzip.NewReader — and this is paid per RECEIVED MESSAGE on both halves of
// every gRPC hop: the ingest receiver decompresses each pushed payload, and the
// exporting client decompresses the collector's response to every export (an
// OTLP ExportResponse is two bytes on the wire, and grpc-go compresses and
// decompresses it like any other message). The writer side was pooled from the
// start and the read side was not.
//
// A pooled reader is reused via Reset, which keeps the decompressor (window and
// all) and re-points it at the new source.
//
// The reader is returned to the pool by pooledGzipReader, never here: the
// contract that makes pooling safe is that nothing can still read from a reader
// that has gone back. See its doc.
func (c *gzipCodec) Decompress(r io.Reader) (io.Reader, error) {
	z := c.readers.get()
	if z == nil {
		z = new(gzip.Reader) // a zero Reader + Reset IS gzip.NewReader
	}
	if err := z.Reset(r); err != nil {
		// A malformed header is what an unauthenticated sender produces at
		// will, so this path must not leak the reader out of the pool — a
		// hostile stream would otherwise turn pooling off for the process.
		// A Reset reader is reusable whatever Reset returned.
		putGzipReader(&c.readers, z)
		return nil, err
	}
	return &pooledGzipReader{z: z, pool: &c.readers}, nil
}

// putGzipReader scrubs a reader of everything belonging to the message it just
// read and pools it. Two things have to go, and one Reset does not remove both.
//
// The HEADER is sender-controlled — FEXTRA up to 64 KiB, FNAME and FCOMMENT up
// to 512 bytes each, allocated fresh per message — and every Reset zeroes it
// (gunzip.go's `*z = Reader{...}`). Pooling a reader with its header intact
// would park memory whose SIZE the pusher picks, on listeners that are
// unauthenticated by design.
//
// The SOURCE is the other one, and it survives a Reset onto an empty reader —
// which is what this used to do, under a comment claiming the opposite.
// gunzip.go re-points the flate decompressor at the new source at line 250, the
// LAST statement of readHeader, so a Reset whose header read fails (an empty
// source has no header) leaves the decompressor — and, for a source gzip had to
// wrap, the retained bufio.Reader — pointing at the message that just finished.
// Measured with a finalizer on the source
// (TestPooledGzipReaderReleasesTheMessagesSource, and it fails on the
// empty-source form): retained for both source shapes, released for neither.
//
// What that reference COSTS is a property of the source, and on the paths this
// codec actually serves it is not bytes. grpc-go hands the codec a *mem.Reader
// over the message's own buffers (rpc_util.go's decompress) and frees each
// buffer as it is read — mem.buffer.Free nils the data, returns the bytes to
// its BufferPool and the buffer object to its bufferObjectPool — and Closes the
// reader outright on the paths that stop short of the end. So by the time we
// pool, the source a Reset onto an empty reader would have kept is DRAINED:
// Remaining() == 0, no compressed bytes parked at all
// (TestGRPCSourceIsDrainedWhenTheReaderIsPooled). An earlier version of this
// comment claimed the old form parked "up to MaxRecvMsgSize per pooled reader"
// on that path; it did not, and nothing measured said it did.
//
// The release is therefore worth doing for the REFERENCE, not for a quantity:
// a pooled reader can live for the process' lifetime, and what it would hold
// for that long is a pointer into another package's recycled allocator objects,
// on the strength of a buffer lifecycle grpc-go does not promise us and a
// source shape it is free to change. Cheap insurance is the whole argument —
// see below for what the insurance costs.
//
// So the reader parks on a complete, valid, EMPTY gzip stream instead. Its
// header PARSES, readHeader runs to the end, and every reference to the
// message's own memory is replaced by a reference to a 20-byte package
// constant. idleGzipSource is deliberately NOT an io.ByteReader: that is what
// makes gzip re-point its retained bufio.Reader (the one shape a ByteReader
// idle source cannot reach) at the constant too. The price is paid on the
// gRPC path, whose *mem.Reader IS a ByteReader and for which gzip therefore
// never allocated a bufio to reuse: the first release of a given reader
// allocates one (measured, 4192 bytes — a 4 KiB buffer and its struct) and
// every later release of that reader allocates nothing, against the 36.7 kB
// the pool saves per message. What a pooled reader can still hold of any
// message is then bounded by that bufio's fixed 4 KiB of scratch — our size,
// not the sender's.
func putGzipReader(pool *gzipReaderCache, z *gzip.Reader) {
	if err := z.Reset(idleGzipSource{}); err != nil {
		// Unreachable: TestIdleGzipStreamParses pins the constant against the
		// same parser. A reader whose state we cannot vouch for is dropped
		// rather than handed to the next message.
		return
	}
	pool.put(z)
}

// idleGzipStream is a complete gzip stream of no bytes: the 10-byte header
// (magic, deflate, no flags, no mtime, no extra fields), one empty final
// deflate block, then a CRC32 and ISIZE of zero. Only the header is ever read —
// readHeader is all a Reset performs — but the trailer costs 10 static bytes and
// keeps the constant a thing the parser would accept in full.
var idleGzipStream = [20]byte{
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff,
	0x03, 0x00,
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
}

// idleGzipSource is the source a pooled reader points at while it waits: the
// idle stream, statelessly, as many times as it is asked for it. Stateless
// because a reader with a position would have to be allocated per release or
// pooled alongside the gzip.Reader, and nothing ever reads past the header
// anyway — a pooled reader is Reset onto the next message before its first
// Read.
//
// It must NOT implement io.ByteReader: gzip stores a ByteReader source
// directly and leaves any bufio.Reader it retains pointing wherever it pointed
// before, which is the message we are trying to let go of.
type idleGzipSource struct{}

func (idleGzipSource) Read(p []byte) (int, error) { return copy(p, idleGzipStream[:]), nil }

func (*gzipCodec) Name() string { return gzipName }

// pooledGzipReader owns the one decision pooling a READER turns on: when the
// reader may go back, given that a reader another goroutine can still read from
// is a reader two consumers will share.
//
// The wrapper is allocated per message and the POOL holds the bare *gzip.Reader,
// deliberately (the writer side is built the same way): pooling the wrapper
// would let a caller holding a stale handle read a reader that has since been
// handed to someone else, which is exactly the hazard the release rules exist
// to prevent. The wrapper is 48 bytes against the 36.7 kB it saves.
//
// Release happens on whichever comes first, and exactly once:
//   - a terminal Read (io.EOF, or a corrupt-stream error) — nobody reads a
//     reader again after it has ended, so this is the proof the pooling needs;
//   - Close, which grpc-go calls (`defer closer.Close()` in its decompress)
//     after it has finished reading. That covers the message it stops reading
//     SHORT of EOF, which is every message over the receive limit: EOF alone
//     would send exactly the payloads a hostile sender picks to the GC.
//
// Both run under the mutex, so a Close racing an in-flight Read waits for that
// Read to return rather than pooling the reader underneath it. After release
// the reader is unreachable from here — z is nil and every later Read answers
// from the recorded terminal error, touching nothing that now belongs to
// another consumer.
type pooledGzipReader struct {
	mu   sync.Mutex
	z    *gzip.Reader // nil once released; never touched again after that
	err  error        // the terminal error, repeated to every later Read
	pool *gzipReaderCache
}

func (p *pooledGzipReader) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.z == nil {
		return 0, p.err
	}
	n, err := p.z.Read(b)
	if err != nil {
		p.err = err
		p.releaseLocked()
	}
	return n, err
}

// Close releases the reader. A reader closed before it ended is NOT recorded as
// having ended: a later Read gets errGzipReadAfterClose rather than io.EOF,
// because a truncated message reported as a clean EOF is a partial payload that
// decodes as a whole one.
func (p *pooledGzipReader) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err == nil {
		p.err = errGzipReadAfterClose
	}
	p.releaseLocked()
	return nil
}

// releaseLocked is idempotent: grpc-go's Close runs on a reader that a terminal
// Read may already have released, and a second Put would hand ONE reader to two
// consumers, whose messages would then interleave through a single window.
func (p *pooledGzipReader) releaseLocked() {
	if p.z == nil {
		return
	}
	z := p.z
	p.z = nil
	putGzipReader(p.pool, z)
}

var errGzipReadAfterClose = errors.New("otlpexport: gzip reader read after close")

// putGzipWriter releases a finished writer's SINK before pooling it, for the
// same reason putGzipReader releases a finished reader's source — and it became
// load-bearing when the warm slot did, because the sync.Pool it used to go into
// was emptied by the next collection and a warm slot is not.
//
// A gzip.Writer holds the io.Writer it was Reset onto, in two places (its own
// z.w and the compressor's huffmanBitWriter), and Close does not clear either.
// On the gRPC path that sink is grpc-go's message-assembly writer over its
// pooled mem buffers; on the HTTP path it is a bufpool.Buffer that the caller
// RELEASES straight afterwards. Pooling the writer as-is would therefore pin
// another package's recycled allocator objects for the process' lifetime — the
// one thing a cache that outlives the GC must not do.
//
// Reset(nil) is the whole release: klauspost's init re-points both references
// and PRESERVES the compressor (gzip.go:98-112), so the window and hash tables
// this cache exists to keep are kept, and nothing sender-controlled travels
// with them. It is also what makes the writer ready for its next Reset.
func putGzipWriter(cache *gzipWriterCache, z *gzip.Writer) {
	z.Reset(nil)
	cache.put(z)
}

// pooledGzipWriter returns the writer to the cache on Close.
type pooledGzipWriter struct {
	*gzip.Writer
	cache *gzipWriterCache
}

func (p *pooledGzipWriter) Close() error {
	err := p.Writer.Close()
	putGzipWriter(p.cache, p.Writer)
	return err
}

// httpGzipWriters pools writers for the OTLP/HTTP body path, ONE POOL PER
// LEVEL: a pooled writer carries its level, so a shared pool would hand a
// level-1 writer to a destination configured for level 9. httpGzipBufs pools
// the compressed-body buffers (bufpool's strike heuristic bounds how long an
// oversized, under-utilized backing array stays pooled) and is level-agnostic.
var (
	// Index 0 is gzip.DefaultCompression, 1..9 are the explicit levels.
	// warmPool's zero value is usable; get returns nil when neither its warm
	// slot nor its sync.Pool holds a writer, which gzipBody handles (a
	// sync.Pool New func cannot close over a level from an array literal, and
	// the warm slot has no New to give it one).
	httpGzipWriters [10]gzipWriterCache
	// bufpool.Pool's zero value is ready to use (and must not be copied).
	httpGzipBufs bufpool.Pool
)

// gzipWriterPool selects the cache for an effective gzip level. Each level has
// its own warm slot, so what this can pin is one writer per level actually
// USED — a destination compresses at one level, so in practice one.
func gzipWriterPool(level int) *gzipWriterCache {
	if level < 1 || level > 9 {
		return &httpGzipWriters[0] // DefaultCompression (and any level gzip refuses)
	}
	return &httpGzipWriters[level]
}

// gzipBody compresses an OTLP/HTTP request body into a pooled buffer at the
// CALLER's level — the HTTP path has no process-global registry to work
// around, so a per-destination level actually takes effect here.
//
// The returned buffer must NOT be handed to net/http as the request body:
// bufpool's Close IS Release, net/http closes a request body twice on a
// redirect, and the second close lands while the transport may still be
// reading — a package-level pool hand-off under an in-flight read. Wrap it in
// a pooledBody (see its doc), whose Close is a no-op and whose release() pools
// the buffer only once it has been read to EOF. A caller that never reaches
// the transport releases it itself.
func gzipBody(body []byte, level int) (*bufpool.Buffer, error) {
	buf := httpGzipBufs.Get()
	pool := gzipWriterPool(level)
	z := pool.get()
	if z == nil {
		z = newGzipWriter(level)
	}
	z.Reset(buf)
	if _, err := z.Write(body); err != nil {
		buf.Release()
		// A Reset writer is safe to reuse after a Write/Close error (the next
		// Reset clears its state); returning it avoids leaking the pooled
		// writer on the (rare) error path. putGzipWriter is what keeps the
		// just-Released buffer from being pinned by the pooled writer.
		putGzipWriter(pool, z)
		return nil, err
	}
	if err := z.Close(); err != nil {
		buf.Release()
		putGzipWriter(pool, z)
		return nil, err
	}
	putGzipWriter(pool, z)
	return buf, nil
}
