package promscrape

import (
	"bytes"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/promparse"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// fuzzSeedBodies are representative and adversarial exposition bodies shared
// by the parser and converter fuzz targets.
var fuzzSeedBodies = []string{
	// Representative classic exposition.
	"# HELP http_requests_total Total requests.\n" +
		"# TYPE http_requests_total counter\n" +
		"http_requests_total{code=\"200\",method=\"get\"} 1027 1395066363000\n" +
		"http_requests_total{code=\"400\",method=\"post\"} 3\n" +
		"# TYPE temp gauge\n" +
		"temp{host=\"a\"} -17.5\n",
	// Histogram + summary families.
	"# TYPE http_duration histogram\n" +
		"http_duration_bucket{le=\"0.1\"} 100\n" +
		"http_duration_bucket{le=\"0.5\"} 140\n" +
		"http_duration_bucket{le=\"+Inf\"} 144\n" +
		"http_duration_sum 53.4\n" +
		"http_duration_count 144\n" +
		"# TYPE rpc summary\n" +
		"rpc{quantile=\"0.5\"} 0.05\n" +
		"rpc{quantile=\"0.99\"} 0.9\n" +
		"rpc_sum 8000\n" +
		"rpc_count 100000\n",
	// OpenMetrics with exemplar and EOF.
	"# TYPE foo counter\n" +
		"foo_total 17.0 1520879607.789 # {trace_id=\"4bf92f3577b34da6a3ce929d0e0e4736\",span_id=\"00f067aa0ba902b7\"} 0.67 1520879607.789\n" +
		"# EOF\n",
	// Escapes, empty label block, NaN/Inf values, missing values.
	"a{b=\"c\\n\\\"d\\\\\"} 1\nempty{} 2\nnan NaN\ninf +Inf\nneg -Inf\nnoval\n",
	// TYPE redeclaration mid-exposition (converter order-key edge).
	"# TYPE x histogram\nx_bucket{le=\"1\"} 1\n# TYPE x summary\nx{quantile=\"0.5\"} 2\nx_count 3\n",
	// Buckets without le, summaries without quantile, decreasing cumulative
	// counts, duplicate le values.
	"# TYPE h histogram\nh_bucket 5\nh_bucket{le=\"2\"} 9\nh_bucket{le=\"2\"} 4\nh_bucket{le=\"1\"} 7\n# TYPE s summary\ns 3\n",
	// Malformed lines, control bytes, non-UTF8.
	"metric{a=\"unterminated\nm\x00etric 1\n\xff\xfe 2\nname{=\"v\"} 1\nname{a=} 1\nname{a=\"v\" 1\n",
	// Whitespace-heavy and comment-only.
	"   \n\t\n#\n# TYPE\n# TYPE t\n# TYPE t counter extra\n  m  1  \n",
	// Exemplar edge cases.
	"om_total 1 # {} 2\nom_total 1 #{a=\"b\"} 2 3 4\nom 2 1.5 # {a=\"b\"} 1\n",
	// Timestamp extremes.
	"m 1 9223372036854775807\nm 1 -9223372036854775808\nm 1 1e300\nm 1 0.0001\n",
}

// FuzzConverter pipes fuzzed parses through the converter and batcher
// (including mid-parse chunking via take) and requires that every produced
// pmetric.Metrics marshals cleanly.
func FuzzConverter(f *testing.F) {
	for _, body := range fuzzSeedBodies {
		f.Add([]byte(body), byte(0))
		f.Add([]byte(body), byte(3))
	}
	f.Fuzz(func(t *testing.T, data []byte, mode byte) {
		openMetrics := mode&1 != 0
		exemplars := mode&2 != 0
		limit := 1 + int(mode>>2) // small chunk limit exercises take() mid-parse

		marshaler := &pmetric.ProtoMarshaler{}
		checkTaken := func(md pmetric.Metrics) {
			if md.ResourceMetrics().Len() != 1 {
				t.Fatalf("batch has %d ResourceMetrics, want 1", md.ResourceMetrics().Len())
			}
			if _, err := marshaler.MarshalMetrics(md); err != nil {
				t.Fatalf("MarshalProto: %v", err)
			}
		}

		b := newBatcher(func(res pcommon.Resource) {
			res.Attributes().PutStr("url.full", "http://fuzz.local/metrics")
		}, limit, time.Unix(1e9, 0), time.Unix(1e9+60, 0))
		conv := newConverter(b, nil)
		pp := promparse.Get(promparse.Options{MaxLineBytes: 1 << 20, OpenMetrics: openMetrics, Exemplars: exemplars})
		_, err := pp.Parse(bytes.NewReader(data), func(s Sample) error {
			_ = conv.add(s)
			if b.count() >= limit {
				checkTaken(b.take())
			}
			return nil
		})
		promparse.Put(pp)
		if err != nil {
			t.Fatalf("parse returned error: %v", err)
		}
		_ = conv.finish()
		if conv.malformed < 0 {
			t.Fatalf("converter malformed count negative: %d", conv.malformed)
		}
		checkTaken(b.take())
	})
}

// FuzzCgroupIdentity feeds arbitrary strings through the cadvisor cgroup-path
// parser. Invariants: no panics; a non-empty pod UID is always canonical
// 8-4-4-4-12 form; a non-empty container ID is always 64 hex characters.
func FuzzCgroupIdentity(f *testing.F) {
	seeds := []string{
		"/kubepods/burstable/pod12345678-1234-1234-1234-123456789012/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod12345678_1234_1234_1234_123456789012.slice/cri-containerd-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.scope",
		"/kubepods/pod12345678-1234-1234-1234-123456789012",
		"/", "", "//", "pod", "podX", "/system.slice/docker-.scope",
		"/kubepods.slice/kubepods-pod_.slice", "pod\x00", "/kubepods/pod£züß/…",
		"/a/pod12345678-1234-1234-1234-12345678901G/b",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, id string) {
		podUID, containerID := cgroupIdentity(id)
		if podUID != "" && !isPodUID(podUID) {
			t.Fatalf("cgroupIdentity(%q) returned non-canonical pod UID %q", id, podUID)
		}
		if containerID != "" && !isContainerID(containerID) {
			t.Fatalf("cgroupIdentity(%q) returned non-hex container ID %q", id, containerID)
		}
	})
}
