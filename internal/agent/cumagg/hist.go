package cumagg

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Hist is one cumulative latency histogram with per-bucket exemplars: the
// primitive both aggregators are made of (spanmetrics' duration, servicegraph's
// client and server sides). It was written out twice — the fields, the fold,
// the set-only snapshot copy and the data point — while BucketIndex,
// RecordExemplar, ClearExemplars, PutExemplar and HistMetric already lived
// here.
//
// Buckets stays nil until the histogram is first observed: a virtual-node edge
// never has a server side, and rendering a zero-count point for it would put a
// latency series on the graph for a measurement nobody took (HistSnap.Present
// carries that into the render). Callers hold the Store's mutex for every
// method.
type Hist struct {
	Count   uint64
	Sum     float64
	Buckets []uint64 // nbuckets (len(bounds)+1) once observed
	// Ex holds at most one exemplar per bucket — the latest observed since the
	// last DELIVERED export — and stays nil until one is recorded (exemplars
	// off, or requests whose spans carried no trace id).
	Ex []Exemplar
}

// Observe folds one measurement v, already placed in bucket idx (BucketIndex),
// into a histogram of nbuckets buckets. The index is the caller's so it can
// attach an exemplar to that same bucket without walking the bounds twice, and
// compute it outside the lock. The first observation allocates the buckets —
// on a series' admission path, never on the warm one.
func (h *Hist) Observe(idx, nbuckets int, v float64) {
	if h.Buckets == nil {
		h.Buckets = make([]uint64, nbuckets)
	}
	h.Count++
	h.Sum += v
	h.Buckets[idx]++
}

// SetExemplar keeps this sample as bucket idx's exemplar (RecordExemplar holds
// the one-per-bucket, latest-wins and skip-without-a-trace-id rules; both
// aggregators' exemplars mean the same thing).
func (h *Hist) SetExemplar(idx, nbuckets int, v float64, ts pcommon.Timestamp, tid pcommon.TraceID, sid pcommon.SpanID) {
	h.Ex = RecordExemplar(h.Ex, nbuckets, idx, v, ts, tid, sid)
}

// ClearExemplars is ClearExemplars on h's exemplars: the Store's after-DELIVERY
// hook (a failed send keeps them for the retry).
func (h *Hist) ClearExemplars() { ClearExemplars(h.Ex) }

// HistSnap is a Hist as of the instant a render copied it (see Snapshotter):
// its own arrays, reused across renders, which the build reads lock-free.
type HistSnap struct {
	Present bool // the histogram was observed at all (Buckets != nil)
	Count   uint64
	Sum     float64
	Buckets []uint64
	// Ex holds only the exemplars that are SET, in bucket order — which is
	// exactly what PutHistPoint renders, and typically one or two per series
	// per interval rather than one slot per bucket. Copying the full
	// per-bucket array instead put ~34 MB of memcpy under the series mutex at
	// servicegraph's cardinality cap, which is the stall the snapshot exists
	// to remove.
	Ex []Exemplar
}

// Fit pre-sizes the snapshot's arrays for nbuckets buckets, so CopyFrom under
// the mutex is pure memmove. Called without the lock (Snapshotter.Fit).
func (h *HistSnap) Fit(nbuckets int) {
	if cap(h.Buckets) < nbuckets {
		h.Buckets = make([]uint64, 0, nbuckets)
		h.Ex = make([]Exemplar, 0, nbuckets)
	}
}

// CopyFrom copies src, reusing this snapshot's arrays. The caller holds the
// Store's lock: everything read here is written under it.
func (h *HistSnap) CopyFrom(src *Hist) {
	h.Present = src.Buckets != nil
	h.Count, h.Sum = src.Count, src.Sum
	h.Buckets = append(h.Buckets[:0], src.Buckets...)
	h.Ex = h.Ex[:0]
	for i := range src.Ex {
		if src.Ex[i].Set {
			h.Ex = append(h.Ex, src.Ex[i])
		}
	}
}

// PutHistPoint writes h into p — the two stamps, count, sum, bounds, bucket
// counts and one exemplar per occupied bucket in bucket order. The attributes
// are the caller's (its label set).
func PutHistPoint(p pmetric.HistogramDataPoint, h *HistSnap, bounds []float64, start, ts pcommon.Timestamp) {
	p.SetStartTimestamp(start)
	p.SetTimestamp(ts)
	p.SetCount(h.Count)
	p.SetSum(h.Sum)
	p.ExplicitBounds().FromRaw(bounds)
	p.BucketCounts().FromRaw(h.Buckets)
	// Pre-sized: appending grows the slice one exemplar at a time, per point,
	// per export.
	if len(h.Ex) > 0 {
		p.Exemplars().EnsureCapacity(len(h.Ex))
	}
	for i := range h.Ex {
		PutExemplar(p.Exemplars(), h.Ex[i])
	}
}
