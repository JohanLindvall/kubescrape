package otlpexport

import (
	"fmt"
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

func BenchmarkGzipBody(b *testing.B) {
	body := benchBody()
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf, err := gzipBody(body)
		if err != nil {
			b.Fatal(err)
		}
		buf.Recycle()
	}
}
