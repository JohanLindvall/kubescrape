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
	writers sync.Pool // *gzip.Writer
	readers sync.Pool // *gzip.Reader, see Decompress
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
	z, ok := c.writers.Get().(*gzip.Writer)
	if !ok {
		z = newGzipWriter(int(codecGzipLevel.Load()))
	}
	z.Reset(w)
	return &pooledGzipWriter{Writer: z, pool: &c.writers}, nil
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
	z, ok := c.readers.Get().(*gzip.Reader)
	if !ok {
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
func putGzipReader(pool *sync.Pool, z *gzip.Reader) {
	if err := z.Reset(idleGzipSource{}); err != nil {
		// Unreachable: TestIdleGzipStreamParses pins the constant against the
		// same parser. A reader whose state we cannot vouch for is dropped
		// rather than handed to the next message.
		return
	}
	pool.Put(z)
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
	pool *sync.Pool
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

// pooledGzipWriter returns the writer to the pool on Close.
type pooledGzipWriter struct {
	*gzip.Writer
	pool *sync.Pool
}

func (p *pooledGzipWriter) Close() error {
	err := p.Writer.Close()
	p.pool.Put(p.Writer)
	return err
}

// httpGzipWriters pools writers for the OTLP/HTTP body path, ONE POOL PER
// LEVEL: a pooled writer carries its level, so a shared pool would hand a
// level-1 writer to a destination configured for level 9. httpGzipBufs pools
// the compressed-body buffers (bufpool's strike heuristic bounds how long an
// oversized, under-utilized backing array stays pooled) and is level-agnostic.
var (
	// Index 0 is gzip.DefaultCompression, 1..9 are the explicit levels.
	// sync.Pool's zero value is usable; Get returns nil when the pool is empty
	// and no New is set, which gzipBody handles (New cannot close over a level
	// from an array literal).
	httpGzipWriters [10]sync.Pool
	// bufpool.Pool's zero value is ready to use (and must not be copied).
	httpGzipBufs bufpool.Pool
)

// gzipWriterPool selects the pool for an effective gzip level.
func gzipWriterPool(level int) *sync.Pool {
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
	z, ok := pool.Get().(*gzip.Writer)
	if !ok {
		z = newGzipWriter(level)
	}
	z.Reset(buf)
	if _, err := z.Write(body); err != nil {
		buf.Release()
		// A Reset writer is safe to reuse after a Write/Close error (the next
		// Reset clears its state); returning it avoids leaking the pooled
		// writer on the (rare) error path.
		pool.Put(z)
		return nil, err
	}
	if err := z.Close(); err != nil {
		buf.Release()
		pool.Put(z)
		return nil, err
	}
	pool.Put(z)
	return buf, nil
}
