package tailbuffer

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// What one trace costs as a function of how many PUSHES it arrives in — the
// measurement behind the decision NOT to intern resources (see add's comment).
//
// A trace collects one ResourceSpans, with its own copy of the sender's resource
// attributes, per (pushed payload, source resource) that carried spans for it.
// Merging across payloads would mean comparing attribute maps on the receive
// path, under the buffer's mutex, for every group of every push. These
// sub-benchmarks price what that would buy: the same 20-span trace, differing
// only in how many pushes it arrives in, with a minimal resource and with a
// realistically enriched one.
//
// Measured 2026-08-02 (Go 1.25, amd64, Ryzen 7 8840HS, -benchtime 5000x); B/op
// is one WHOLE 20-span trace, so the rows differ only in duplicated resources:
//
//	pushes   minimal resource (1 attr)   enriched resource (13 attrs)
//	     1    7178 B    95 allocs         7896 B   107 allocs
//	     2    7496 B   105 allocs         8936 B   129 allocs
//	     4    8136 B   122 allocs        11016 B   170 allocs
//	    10    9960 B   158 allocs        17160 B   234 allocs
//	    20   13256 B   209 allocs        27656 B   449 allocs
//
// So a duplicated group costs ~320 B with a bare service.name and ~1040 B with
// the attribute set the tier's enricher actually stamps — the 470 B in
// bench_test.go was measured against a minimal resource and understates the
// enriched case by more than 2x. The bottom row is a real 3.5x.
//
// Interning is still NOT worth doing, and the deciding argument is that the
// bottom rows are unreachable rather than that they are cheap:
//
//   - Groups multiply per (payload, RESOURCE), and the resources of one trace
//     are mostly DISTINCT. A trace crossing five services carries five different
//     service.name/pod/namespace sets; no interning merges those. Only the SAME
//     sender delivering spans of the SAME trace in SEPARATE pushes duplicates
//     anything.
//   - How often that happens is bounded by decisionWait. A sender's spans for
//     one trace are emitted within that trace's own duration and land in one SDK
//     batch; two batches means a batch boundary fell inside the buffer's window,
//     and the default window is 5s against a default SDK schedule delay of 5s.
//     Spans arriving later than that are past the decision and never buffered at
//     all — they take the late path. The reachable range is the top two rows:
//     +13% on an enriched trace, not +250%.
//   - The price of merging is paid on the receive path, under the mutex that
//     serialises every sender on the shard: a hash of the source resource per
//     push plus a full attribute comparison per candidate group (a 64-bit hash
//     alone would merge two distinct resources on collision, which is silently
//     mislabelled telemetry). Per-trace group bookkeeping also costs the
//     ONE-group common case something, where today it costs nothing.
//
// If it ever does become a shard's problem, the symptom is
// kubescrape_tail_sampling_buffered_spans far below what the process's RSS
// implies at the ~1 KiB/span budget in memory.go, and the first remedy is a
// SHORTER decisionWait — fewer batch boundaries inside the window — which needs
// no code at all.
func BenchmarkAssembleByPushSize(b *testing.B) {
	for _, rich := range []bool{false, true} {
		for _, per := range []int{20, 10, 5, 2, 1} {
			b.Run(fmt.Sprintf("%s/%dPerPush", resLabel(rich), per), func(b *testing.B) {
				payloads := buildPushes(20, per, rich)
				buf := benchBuffer(b, 1<<20)
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, p := range payloads {
						if err := buf.ExportTraces(ctx, p); err != nil {
							b.Fatal(err)
						}
					}
					buf.reset()
				}
			})
		}
	}
}

func resLabel(rich bool) string {
	if rich {
		return "EnrichedResource"
	}
	return "MinimalResource"
}

// buildPushes chops one `spans`-span trace into pushes of `per` spans.
func buildPushes(spans, per int, rich bool) []ptrace.Traces {
	out := make([]ptrace.Traces, 0, spans/per)
	for i := 0; i < spans; i += per {
		specs := make([]spanSpec, 0, per)
		for j := 0; j < per; j++ {
			specs = append(specs, spanSpec{trace: 1, span: uint64(i + j + 1), end: 10, attrs: map[string]any{
				"http.route": "/api/v1/orders", "http.status_code": 200,
			}})
		}
		td := payload("checkout", specs...)
		if rich {
			enrichResource(td.ResourceSpans().At(0).Resource().Attributes())
		}
		out = append(out, td)
	}
	return out
}

// enrichResource is what the tier's ingest enricher actually stamps on a pushed
// resource, so the cost of copying one is measured against the real thing.
func enrichResource(m pcommon.Map) {
	for k, v := range map[string]string{
		"k8s.namespace.name":  "shop",
		"k8s.pod.name":        "checkout-7d9f8b6c4d-x2k9p",
		"k8s.pod.uid":         "3f2a1c8e-9b4d-4e7a-8c1f-2d5e6a7b8c9d",
		"k8s.container.name":  "checkout",
		"k8s.node.name":       "ip-10-0-3-217.eu-west-1.compute.internal",
		"k8s.deployment.name": "checkout",
		"k8s.pod.ip":          "10.244.3.17",
		"service.namespace":   "shop",
		"service.instance.id": "3f2a1c8e-9b4d-4e7a-8c1f-2d5e6a7b8c9d/checkout",
		"service.version":     "1.42.0",
		"cloud.region":        "eu-west-1",
		"telemetry.sdk.name":  "opentelemetry",
	} {
		m.PutStr(k, v)
	}
}
