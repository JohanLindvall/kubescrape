package otlpexport

import (
	"io"
	"sync"

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

type gzipCodec struct {
	writers sync.Pool // *gzip.Writer
}

func (c *gzipCodec) Compress(w io.Writer) (io.WriteCloser, error) {
	z, ok := c.writers.Get().(*gzip.Writer)
	if !ok {
		z = gzip.NewWriter(nil)
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
	httpGzipWriters = sync.Pool{New: func() any { return gzip.NewWriter(nil) }}
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
