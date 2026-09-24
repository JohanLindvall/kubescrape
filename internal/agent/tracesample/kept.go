package tracesample

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/tracehash"
)

// The questions a layer ABOVE this sampler asks it. The trace tier's two
// aggregators (agent/spanmetrics, agent/servicegraph) sit above it so they can
// count every span, and they attach exemplars — links to traces. A link to a
// trace this sampler drops resolves to nothing, so each asks before it records
// one. Both are safe for concurrent use (they read only what New set) and
// allocate nothing, since they run on the per-span / per-request path.

// SpanKept reports whether sp would ship: keep, i.e. the probability plus the
// keepErrors / keepSlowerThan guard rails, so an error or slow fragment this
// sampler rescues still anchors an exemplar. The rate cap is deliberately OUT:
// it is billed per payload at export time, and spending a token to answer a
// question would charge the bucket for spans that never ship.
func (s *Sampler) SpanKept(sp ptrace.Span) bool { return s.keep(sp) }

// TraceKept is keep's probability half alone — whether this sampler keeps the
// trace as a WHOLE — for a caller holding only the id: the service graph's
// paired edge carries no span status or duration. It errs toward false (a trace
// a guard rail rescued fragments of reads as dropped), which is the right
// direction for an exemplar: a missing link costs nothing, a dead one sends an
// operator to a trace that does not exist.
func (s *Sampler) TraceKept(id pcommon.TraceID) bool { return tracehash.Keep(id, s.threshold) }
