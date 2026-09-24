package otlpingest

// The auto metrics mode's decision (MetricsAuto): whether enriching each
// ResourceMetrics from its own resource attributes attributes everything
// correctly, or the push has to be split per data point (split.go).

import (
	"context"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// resourceModeSuffices reports whether enriching each ResourceMetrics from its
// own resource attributes attributes everything correctly — i.e. no data point
// carries an id naming a different object than its resource's, and no
// id-LESS resource has a point that carries one.
//
// The data-point half is not optional. A resource-level container.id is set
// automatically by every SDK container detector (Go's resource.WithContainerID,
// Java's ContainerResource, the collector's resourcedetection/container), and it
// is in the default -ingest-container-id-keys. An exporter that DESCRIBES other
// objects — the kube-state-metrics shape this mode exists for — therefore has a
// resource ID naming ITSELF while each data point names a different pod. Asking
// only about resources sent that straight down the resource branch and stamped
// every point with the exporter's own pod and service.name, silently, with
// kubescrape_ingest_resources_total{enriched} reading healthy. The same payload
// in explicit datapoint mode split correctly, which is what
// TestSplitResourceUsesDescribedObjectIdentity pins.
//
// An id-less resource whose points carry NO id either does not demote. It
// used to — "every resource carries an ID" was the first test — and the split
// it bought was the resource branch's answer at the cost of a copy of every
// point: that resource's points all land in the splitter's "" group, whose
// copied resource takes the same peer fallback and the same merge
// (enrichAttrs' no-token arm), counted once per resource either way. The
// ordinary plain-SDK sender with no container detector and no point labels is
// exactly this shape, and it is the auto mode's (the default's) common case.
//
// The two halves run as two PASSES, cheapest first. Interleaving them let an
// early resource's point walk run to completion before a later resource that
// alone forces false was ever looked at, and that walk spends the request's
// lookup budget on probes whose answer cannot change the outcome. The first
// pass issues no lookup at all: an id-less resource demotes the push exactly
// when one of its points names SOME object (anyPointHasID — presence only,
// since there is no resource id for it to be foreign to).
func (e *Enricher) resourceModeSuffices(ctx context.Context, cache *reqCache, md pmetric.Metrics) bool {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		if !e.hasID(rm.Resource().Attributes()) && e.anyPointHasID(rm) {
			return false
		}
	}
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		// The id the resource branch would attribute it BY — the one token
		// chooser every path shares, so the decision compares points against
		// the same object enrichment then names. For a resource carrying both
		// kinds this issues the attribution lookup enrichment issues next
		// anyway, memoised in cache — not a probe.
		resID := e.resolvableToken(ctx, cache, rm.Resource().Attributes())
		if resID == "" {
			continue // id-less, and the first pass proved its points are too
		}
		// FOREIGN, not merely present. A point ID equal to the resource's own
		// describes the sender itself — an app labelling its metrics with its
		// container id, which SDK metric views do — and the resource branch
		// attributes it identically while leaving the sender authoritative
		// about itself. Demoting it to the split path instead regrouped its
		// points and OVERWROTE its service.name/k8s.* with the derived ones
		// (overwriteAttrs, correct only for a describing exporter), so an
		// ordinary sender silently changed job identity by adding a label.
		if e.anyForeignDataPointID(ctx, cache, rm, resID) {
			return false
		}
	}
	return true
}

// anyPointHasID reports whether any data point in rm carries an id attribute
// at all. Allocation-free and lookup-free: idValue builds no token.
func (e *Enricher) anyPointHasID(rm pmetric.ResourceMetrics) bool {
	sms := rm.ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		ms := sms.At(i).Metrics()
		for j := 0; j < ms.Len(); j++ {
			for a := range dataPointAttrs(ms.At(j)) {
				if e.hasID(a) {
					return true
				}
			}
		}
	}
	return false
}

// anyForeignDataPointID reports whether any data point in rm carries an ID
// attribute naming a DIFFERENT object than resID (one pass, first hit wins).
func (e *Enricher) anyForeignDataPointID(ctx context.Context, cache *reqCache, rm pmetric.ResourceMetrics, resID string) bool {
	sms := rm.ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		ms := sms.At(i).Metrics()
		for j := 0; j < ms.Len(); j++ {
			if e.metricPointsHaveForeignID(ctx, cache, ms.At(j), resID) {
				return true
			}
		}
	}
	return false
}

func (e *Enricher) metricPointsHaveForeignID(ctx context.Context, cache *reqCache, m pmetric.Metric, resID string) bool {
	for a := range dataPointAttrs(m) {
		if prefix, val, ok := e.idValue(a); ok && e.foreignPointID(ctx, cache, prefix, val, resID) {
			return true
		}
	}
	return false
}

// foreignID reports whether a data-point token names a DIFFERENT OBJECT than
// the resource's token — the question the auto-mode decision actually needs.
//
// An UNRESOLVABLE point token is not evidence of a foreign object: it used to
// demote the payload from the resource path (which would have enriched the
// sender correctly) to the split path, where a group whose id resolves to
// nothing has its copied resource CLEARED — deleting every attribute the
// sender set, so service.name vanished and the Prometheus job became
// unknown_service. A token that DOES resolve is foreign exactly when it names
// a different object than the resource's — one predicate, sameObject, shared
// with the split path (see its comment for the drift this repaired).
func (e *Enricher) foreignID(ctx context.Context, cache *reqCache, tok, resID string) bool {
	if tok == resID {
		return false
	}
	if !e.resolves(ctx, cache, tok) {
		return false // unresolvable: not evidence of anything
	}
	return !e.sameObject(ctx, cache, tok, resID)
}

// foreignPointID is foreignID for a data point's id, taking the id in PARTS.
// Same answer, same order of decisions — but the kind-tagged token is only
// materialised on a MEMO MISS, i.e. at most once per distinct id per push,
// instead of once per data point. Every earlier exit reads the memo through
// map[string(buf)], which does not copy the key.
func (e *Enricher) foreignPointID(ctx context.Context, cache *reqCache, prefix, val, resID string) bool {
	if tokenIs(resID, prefix, val) {
		return false // the sender's own id: the resource branch attributes it
	}
	buf := append(cache.tokBuf[:0], prefix...)
	buf = append(buf, val...)
	cache.tokBuf = buf
	if r, ok := cache.ids[string(buf)]; ok {
		if !r.resolved {
			return false // unresolvable: not evidence of anything
		}
		return !sameResolved(r, e.attrsFor(ctx, cache, resID))
	}
	if resolved, ok := cache.probes[string(buf)]; ok && !resolved {
		return false
	}
	return e.foreignID(ctx, cache, string(buf), resID)
}

// tokenIs reports whether the kind-tagged token tok is prefix+val, comparing in
// place rather than building the concatenation to compare it against.
func tokenIs(tok, prefix, val string) bool {
	return len(tok) == len(prefix)+len(val) && tok[:len(prefix)] == prefix && tok[len(prefix):] == val
}
