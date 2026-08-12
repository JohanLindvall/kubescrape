package otlpexport

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

// A representative OTLP-ish body: repetitive structure with per-record unique
// values (ids, timestamps, latencies), like real telemetry.
func benchBody() []byte {
	var sb strings.Builder
	for i := 0; i < 2000; i++ {
		fmt.Fprintf(&sb,
			`{"resource":{"k8s.pod.name":"payments-6f7b9c%03d","k8s.namespace.name":"prod"},"body":"2026-07-13T10:%02d:%02d.%06dZ INFO handled request path=/api/v1/orders/%d trace=%016x status=200 latency=%dms","severity":"info"}`,
			i%40, i/60%60, i%60, i*7919%1000000, i, uint64(i)*0x9e3779b97f4a7c15, i%250)
	}
	return []byte(sb.String())
}

// The two shapes the gRPC codec DECOMPRESSES, both of which used to mint a
// gzip reader (a fixed 32 KiB window plus huffman tables) and drop it. A/B
// measured here, the baseline being the code this replaced — a Decompress whose
// whole body is `return gzip.NewReader(r)`, with no wrapper and no pool:
//
//	push (460 KB body):  17 allocs  ->  1
//	response (2 bytes):   5 allocs  ->  1
//
// ALLOCATION counts, which are machine-independent, and deliberately not the
// wall-clock figures this comment used to carry: they could not be reproduced
// on the same CPU family, and a number nobody can reproduce reads as a
// regression that has not happened. For scale, in the same machine-independent
// terms: the reader is a per-MESSAGE cost and not a per-byte one, so the
// two-byte response allocated 36.7 kB to be read and now allocates 48 B.
//
// The baseline is the unpooled FUNCTION and not this one with the pool
// bypassed, which is what an earlier version of these figures measured — that
// arm still paid putGzipReader's scrub and the per-message wrapper, and so
// credited the pool with 3 allocations per message it never saved.
//
// The one remaining allocation is the per-message wrapper.
// TestGzipDecompressAllocationBudget is what fails the build if this moves;
// these only report it.
func BenchmarkGzipDecompressPush(b *testing.B) {
	c := &gzipCodec{}
	gz := gzipTestBytes(b, benchBody(), nil)
	src := bytes.NewReader(nil)
	sink := make([]byte, 32*1024)
	b.SetBytes(int64(len(gz)))
	b.ReportAllocs()
	for b.Loop() {
		src.Reset(gz)
		r, err := c.Decompress(src)
		if err != nil {
			b.Fatal(err)
		}
		for {
			if _, err := r.Read(sink); err != nil {
				break
			}
		}
		_ = r.(io.Closer).Close()
	}
}

// An OTLP ExportResponse is two bytes on the wire, and grpc-go compresses the
// response like any other message — so this is paid once per EXPORT, on the
// send path, by every gRPC destination in the fleet.
func BenchmarkGzipDecompressResponse(b *testing.B) {
	c := &gzipCodec{}
	gz := gzipTestBytes(b, []byte{0x0a, 0x00}, nil)
	src := bytes.NewReader(nil)
	var sink [64]byte
	b.ReportAllocs()
	for b.Loop() {
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

func BenchmarkGzipBody(b *testing.B) {
	body := benchBody()
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for b.Loop() {
		buf, err := gzipBody(body, effectiveGzipLevel(0))
		if err != nil {
			b.Fatal(err)
		}
		buf.Release()
	}
}
