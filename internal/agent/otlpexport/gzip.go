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

// gzipLevel is the process-wide gzip level for both the gRPC codec and the
// HTTP body path (the writer pools are shared, so the level is a process
// setting; it is fixed at client construction, before the pools warm up).
var gzipLevel atomic.Int32

func init() { gzipLevel.Store(gzip.DefaultCompression) }

func setGzipLevel(level int) { gzipLevel.Store(int32(level)) }

func newGzipWriter() *gzip.Writer {
	w, err := gzip.NewWriterLevel(nil, int(gzipLevel.Load()))
	if err != nil {
		return gzip.NewWriter(nil)
	}
	return w
}

func (c *gzipCodec) Compress(w io.Writer) (io.WriteCloser, error) {
	z, ok := c.writers.Get().(*gzip.Writer)
	if !ok {
		z = newGzipWriter()
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

// httpGzipWriters pools writers for the OTLP/HTTP body path; httpGzipBufs
// pools the compressed-body buffers (bufpool's strike heuristic bounds how
// long an oversized, under-utilized backing array stays pooled).
var (
	httpGzipWriters = sync.Pool{New: func() any { return newGzipWriter() }}
	httpGzipBufs    = bufpool.New()
)

// gzipBody compresses an OTLP/HTTP request body into a pooled buffer. The
// buffer returns to its pool on Close, so it can be handed to the HTTP
// transport as the request body (the transport always closes the body, even
// on errors); a caller that never reaches the transport must Recycle it.
func gzipBody(body []byte) (*bufpool.Buffer, error) {
	buf := httpGzipBufs.Get()
	z := httpGzipWriters.Get().(*gzip.Writer)
	z.Reset(buf)
	if _, err := z.Write(body); err != nil {
		buf.Recycle()
		return nil, err
	}
	if err := z.Close(); err != nil {
		buf.Recycle()
		return nil, err
	}
	httpGzipWriters.Put(z)
	return buf, nil
}
