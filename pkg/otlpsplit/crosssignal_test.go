package otlpsplit

// One table-driven test asserting the FIVE properties every splitter shares,
// against all three of them. The three splitters are deliberately not
// genericized — pdata's three type families would need an adapter layer that
// costs more than the duplication — so the shared CONTRACT is enforced here
// instead: a fix (or a regression) landing in one splitter fails this test for
// the others.
//
// The five properties:
//
//  1. flush-before-append at the cap: packing under-cap resources never
//     produces an over-cap part (a resource that would overflow the current
//     chunk flushes it first);
//  2. a scope-less over-cap resource ships WHOLE (rejected and counted at the
//     collector, never silently dropped);
//  3. an empty scope's identity survives a big-resource split (it carries
//     scope name/attributes the under-cap path preserves);
//  4. a non-empty input never yields zero parts;
//  5. every part's encoded size is <= the cap, EXCEPT a single leaf that alone
//     exceeds it, which goes alone (nothing can shrink it).

import (
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// crossSignal adapts one splitter to the shared contract. Payloads and parts
// travel as `any`; each closure owns the type assertion for its signal.
type crossSignal struct {
	name string
	// build returns a payload of `resources` resources, each with ONE scope of
	// `itemsPer` leaf items carrying ~itemBytes of payload each.
	build func(resources, itemsPer, itemBytes int) any
	// buildScopeless returns ONE resource with NO scopes and ~attrBytes of
	// resource attributes.
	buildScopeless func(attrBytes int) any
	// buildEmptyScope returns ONE resource with two scopes: the FIRST empty
	// but named "empty-scope", the SECOND with `itemsPer` items of ~itemBytes
	// each (sized by the caller to exceed the cap, forcing the big split).
	buildEmptyScope func(itemsPer, itemBytes int) any
	// split runs the splitter.
	split func(payload any, maxBytes int) []any
	// size is the exact encoded size of one payload or part.
	size func(payload any) int
	// leaves counts leaf items (records/data points/spans) in a payload/part.
	leaves func(payload any) int
	// resources counts resources in a payload/part.
	resources func(payload any) int
	// hasScopeNamed reports whether any scope in the part carries the name.
	hasScopeNamed func(payload any, name string) bool
}

func crossSignals() []crossSignal {
	body := func(n int) string { return strings.Repeat("x", n) }
	return []crossSignal{
		{
			name: "logs",
			build: func(resources, itemsPer, itemBytes int) any {
				ld := plog.NewLogs()
				for r := 0; r < resources; r++ {
					rl := ld.ResourceLogs().AppendEmpty()
					rl.Resource().Attributes().PutStr("k8s.pod.name", "pod")
					sl := rl.ScopeLogs().AppendEmpty()
					sl.Scope().SetName("cross")
					for i := 0; i < itemsPer; i++ {
						sl.LogRecords().AppendEmpty().Body().SetStr(body(itemBytes))
					}
				}
				return ld
			},
			buildScopeless: func(attrBytes int) any {
				ld := plog.NewLogs()
				ld.ResourceLogs().AppendEmpty().Resource().Attributes().PutStr("fat", body(attrBytes))
				return ld
			},
			buildEmptyScope: func(itemsPer, itemBytes int) any {
				ld := plog.NewLogs()
				rl := ld.ResourceLogs().AppendEmpty()
				empty := rl.ScopeLogs().AppendEmpty()
				empty.Scope().SetName("empty-scope")
				big := rl.ScopeLogs().AppendEmpty()
				big.Scope().SetName("big")
				for i := 0; i < itemsPer; i++ {
					big.LogRecords().AppendEmpty().Body().SetStr(body(itemBytes))
				}
				return ld
			},
			split: func(payload any, maxBytes int) []any {
				parts := Logs(payload.(plog.Logs), maxBytes)
				out := make([]any, len(parts))
				for i, p := range parts {
					out[i] = p
				}
				return out
			},
			size:   func(p any) int { return logMarshaler.LogsSize(p.(plog.Logs)) },
			leaves: func(p any) int { return p.(plog.Logs).LogRecordCount() },
			resources: func(p any) int {
				return p.(plog.Logs).ResourceLogs().Len()
			},
			hasScopeNamed: func(p any, name string) bool {
				rls := p.(plog.Logs).ResourceLogs()
				for i := 0; i < rls.Len(); i++ {
					sls := rls.At(i).ScopeLogs()
					for j := 0; j < sls.Len(); j++ {
						if sls.At(j).Scope().Name() == name {
							return true
						}
					}
				}
				return false
			},
		},
		{
			name: "metrics",
			build: func(resources, itemsPer, itemBytes int) any {
				md := pmetric.NewMetrics()
				for r := 0; r < resources; r++ {
					rm := md.ResourceMetrics().AppendEmpty()
					rm.Resource().Attributes().PutStr("k8s.pod.name", "pod")
					sm := rm.ScopeMetrics().AppendEmpty()
					sm.Scope().SetName("cross")
					for i := 0; i < itemsPer; i++ {
						m := sm.Metrics().AppendEmpty()
						m.SetName("m")
						dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
						dp.SetDoubleValue(1)
						dp.Attributes().PutStr("pad", body(itemBytes))
					}
				}
				return md
			},
			buildScopeless: func(attrBytes int) any {
				md := pmetric.NewMetrics()
				md.ResourceMetrics().AppendEmpty().Resource().Attributes().PutStr("fat", body(attrBytes))
				return md
			},
			buildEmptyScope: func(itemsPer, itemBytes int) any {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				empty := rm.ScopeMetrics().AppendEmpty()
				empty.Scope().SetName("empty-scope")
				big := rm.ScopeMetrics().AppendEmpty()
				big.Scope().SetName("big")
				for i := 0; i < itemsPer; i++ {
					m := big.Metrics().AppendEmpty()
					m.SetName("m")
					dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
					dp.SetDoubleValue(1)
					dp.Attributes().PutStr("pad", body(itemBytes))
				}
				return md
			},
			split: func(payload any, maxBytes int) []any {
				parts := Metrics(payload.(pmetric.Metrics), maxBytes)
				out := make([]any, len(parts))
				for i, p := range parts {
					out[i] = p
				}
				return out
			},
			size:   func(p any) int { return metricMarshaler.MetricsSize(p.(pmetric.Metrics)) },
			leaves: func(p any) int { return p.(pmetric.Metrics).DataPointCount() },
			resources: func(p any) int {
				return p.(pmetric.Metrics).ResourceMetrics().Len()
			},
			hasScopeNamed: func(p any, name string) bool {
				rms := p.(pmetric.Metrics).ResourceMetrics()
				for i := 0; i < rms.Len(); i++ {
					sms := rms.At(i).ScopeMetrics()
					for j := 0; j < sms.Len(); j++ {
						if sms.At(j).Scope().Name() == name {
							return true
						}
					}
				}
				return false
			},
		},
		{
			name: "traces",
			build: func(resources, itemsPer, itemBytes int) any {
				td := ptrace.NewTraces()
				for r := 0; r < resources; r++ {
					rs := td.ResourceSpans().AppendEmpty()
					rs.Resource().Attributes().PutStr("k8s.pod.name", "pod")
					ss := rs.ScopeSpans().AppendEmpty()
					ss.Scope().SetName("cross")
					for i := 0; i < itemsPer; i++ {
						sp := ss.Spans().AppendEmpty()
						sp.SetName("span")
						sp.SetTraceID(pcommon.TraceID{1})
						sp.Attributes().PutStr("pad", body(itemBytes))
					}
				}
				return td
			},
			buildScopeless: func(attrBytes int) any {
				td := ptrace.NewTraces()
				td.ResourceSpans().AppendEmpty().Resource().Attributes().PutStr("fat", body(attrBytes))
				return td
			},
			buildEmptyScope: func(itemsPer, itemBytes int) any {
				td := ptrace.NewTraces()
				rs := td.ResourceSpans().AppendEmpty()
				empty := rs.ScopeSpans().AppendEmpty()
				empty.Scope().SetName("empty-scope")
				big := rs.ScopeSpans().AppendEmpty()
				big.Scope().SetName("big")
				for i := 0; i < itemsPer; i++ {
					sp := big.Spans().AppendEmpty()
					sp.SetName("span")
					sp.Attributes().PutStr("pad", body(itemBytes))
				}
				return td
			},
			split: func(payload any, maxBytes int) []any {
				parts := Traces(payload.(ptrace.Traces), maxBytes)
				out := make([]any, len(parts))
				for i, p := range parts {
					out[i] = p
				}
				return out
			},
			size:   func(p any) int { return traceMarshaler.TracesSize(p.(ptrace.Traces)) },
			leaves: func(p any) int { return p.(ptrace.Traces).SpanCount() },
			resources: func(p any) int {
				return p.(ptrace.Traces).ResourceSpans().Len()
			},
			hasScopeNamed: func(p any, name string) bool {
				rss := p.(ptrace.Traces).ResourceSpans()
				for i := 0; i < rss.Len(); i++ {
					sss := rss.At(i).ScopeSpans()
					for j := 0; j < sss.Len(); j++ {
						if sss.At(j).Scope().Name() == name {
							return true
						}
					}
				}
				return false
			},
		},
	}
}

func TestCrossSignalSplitInvariants(t *testing.T) {
	const capBytes = 4 << 10 // small, so every shape is cheap to build
	for _, sig := range crossSignals() {
		t.Run(sig.name, func(t *testing.T) {
			t.Run("flush-before-append keeps every part within the cap", func(t *testing.T) {
				// Many under-cap resources that together exceed the cap: the
				// packer must flush before appending, never after.
				payload := sig.build(12, 3, 256)
				if sig.size(payload) <= capBytes {
					t.Fatalf("test payload (%d bytes) does not exceed the cap; grow it", sig.size(payload))
				}
				parts := sig.split(payload, capBytes)
				if len(parts) < 2 {
					t.Fatalf("expected a real split, got %d part(s)", len(parts))
				}
				total := 0
				for i, p := range parts {
					if got := sig.size(p); got > capBytes {
						t.Errorf("part %d is %d bytes, over the %d cap", i, got, capBytes)
					}
					total += sig.leaves(p)
				}
				if want := sig.leaves(payload); total != want {
					t.Errorf("parts carry %d leaves, input had %d", total, want)
				}
			})

			t.Run("scope-less over-cap resource ships whole", func(t *testing.T) {
				payload := sig.buildScopeless(2 * capBytes)
				parts := sig.split(payload, capBytes)
				if len(parts) == 0 {
					t.Fatal("a non-empty input yielded zero parts")
				}
				got := 0
				for _, p := range parts {
					got += sig.resources(p)
				}
				if got != 1 {
					t.Fatalf("the scope-less resource must ship in exactly one part; found it %d times", got)
				}
			})

			t.Run("empty-scope identity survives a big-resource split", func(t *testing.T) {
				// The resource exceeds the cap, so the big split runs; the empty
				// scope carries identity the under-cap path would preserve.
				payload := sig.buildEmptyScope(24, 512)
				if sig.size(payload) <= capBytes {
					t.Fatalf("test payload (%d bytes) does not exceed the cap; grow it", sig.size(payload))
				}
				parts := sig.split(payload, capBytes)
				found := false
				for _, p := range parts {
					if sig.hasScopeNamed(p, "empty-scope") {
						found = true
						break
					}
				}
				if !found {
					t.Error("the empty scope's identity was dropped by the big-resource split")
				}
			})

			t.Run("non-empty input never yields zero parts", func(t *testing.T) {
				for name, payload := range map[string]any{
					"under-cap":          sig.build(1, 1, 16),
					"over-cap":           sig.build(8, 4, 512),
					"scope-less-fat":     sig.buildScopeless(2 * capBytes),
					"empty-scope-plus":   sig.buildEmptyScope(24, 512),
					"single-leaf-BIGGER": sig.build(1, 1, 4*capBytes),
				} {
					if parts := sig.split(payload, capBytes); len(parts) == 0 {
						t.Errorf("%s: zero parts for a non-empty input", name)
					}
				}
			})

			t.Run("single leaf over the cap goes alone", func(t *testing.T) {
				payload := sig.build(1, 3, 2*capBytes) // three leaves, each alone over the capBytes
				parts := sig.split(payload, capBytes)
				if len(parts) != 3 {
					t.Fatalf("3 over-cap leaves must yield 3 single-leaf parts, got %d", len(parts))
				}
				for i, p := range parts {
					if got := sig.leaves(p); got != 1 {
						t.Errorf("part %d carries %d leaves; an over-cap leaf must go alone", i, got)
					}
					// The one legitimate over-cap part shape: a single leaf
					// nothing can shrink.
					if sig.size(p) <= capBytes {
						t.Errorf("part %d unexpectedly fits the cap; the fixture leaf is too small", i)
					}
				}
			})
		})
	}
}
