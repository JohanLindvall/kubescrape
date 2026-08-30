package otlpexport

import (
	"bytes"
	"io"
	"runtime"
	"testing"
)

// What one gzip WRITER costs to build, which is what a pool miss buys.
func BenchmarkGzipNewWriter(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = newGzipWriter(-1)
	}
}

// The gRPC codec's compress path, warm: every iteration finds a pooled writer.
func BenchmarkGzipCompressWarm(b *testing.B) {
	c := &gzipCodec{}
	body := benchBody()
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for b.Loop() {
		w, err := c.Compress(io.Discard)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := w.Write(body); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// The same path with a GC between exports, which is the LIVE agent's shape: the
// profile counted 447 flate.NewWriter constructions against ~550 gRPC
// compressions in 1000 s, because sync.Pool is drained by the collector and the
// agent GCs every 1.9-2.5 s while logs flush every 2 s.
func BenchmarkGzipCompressGCBetween(b *testing.B) {
	c := &gzipCodec{}
	body := benchBody()
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		runtime.GC()
		runtime.GC() // two, to clear the victim cache as well
		b.StartTimer()
		w, err := c.Compress(io.Discard)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := w.Write(body); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// The reader half of the GC-drain case: an OTLP ExportResponse decompressed
// with a collection between messages, which is the SEND path's shape (one
// response per export, one GC every couple of seconds).
func BenchmarkGzipDecompressResponseGCBetween(b *testing.B) {
	c := &gzipCodec{}
	gz := gzipTestBytes(b, []byte{0x0a, 0x00}, nil)
	src := bytes.NewReader(nil)
	var sink [64]byte
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		runtime.GC()
		runtime.GC()
		b.StartTimer()
		src.Reset(gz)
		r, err := c.Decompress(src)
		if err != nil {
			b.Fatal(err)
		}
		for {
			if _, err := r.Read(sink[:]); err != nil {
				break
			}
		}
		_ = r.(io.Closer).Close()
	}
}
