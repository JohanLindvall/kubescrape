package otlpexport

import (
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

func (c *gzipCodec) Decompress(r io.Reader) (io.Reader, error) {
	return gzip.NewReader(r)
}

func (*gzipCodec) Name() string { return gzipName }

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
