package promscrape

// Prometheus protobuf exposition parsing — the only format that carries
// NATIVE histograms, which convert to OTLP exponential histograms. Opt-in
// (-scrape-native-histograms): the target scrape then offers the protobuf
// Accept and this path handles a protobuf response; text responses keep the
// streaming text parser. Native histogram fields (schema, zero bucket,
// span/delta-encoded buckets) map 1:1 onto OTLP's exponential histogram
// (same base-2 scheme); classic families in the same response convert
// through the ordinary Sample path. A family carrying BOTH native and
// classic data uses the native representation (Prometheus's own preference
// when scraping native histograms).

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"unicode/utf8"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// protoContentType is the media type of the protobuf exposition, in both the
// Accept offer and the response check — one constant so the two cannot drift.
const protoContentType = "application/vnd.google.protobuf"

// maxProtoMessageBytes bounds one delimited MetricFamily message on the WIRE.
// proto.Unmarshal materialises the ENTIRE message (there is no streaming decode
// like the text front's). The body is already gunzipped by the transport, so
// this is a bound on the DECOMPRESSED size. Kept at the OTLP/gRPC 4 MiB
// convention: a single family (one metric name's series, or one native
// histogram) fits comfortably.
//
// It is NOT a bound on the decode's heap, and this comment used to say it was.
// Every submessage decodes into its own allocated struct, and an empty Metric
// costs two bytes on the wire, so one in-cap message of empty Metric entries —
// ~4 KiB gzipped — measured 265 MiB of live heap (~63x); a dense but realistic
// classic-histogram family (19,001 metrics, 3 labels, 11 buckets) measured
// 36.8 MiB (~9x). maxProtoDecodedBytes is the bound on that, checked before the
// decode. Reachability is bounded by the opt-in too: only an operator who
// enabled -scrape-native-histograms decodes protobuf at all (scraper.go).
const maxProtoMessageBytes = 4 << 20

// maxProtoDecodedBytes bounds the heap proto.Unmarshal may materialise for ONE
// message, ESTIMATED from the wire by protoDecodedSize before the decode runs,
// so a message past it is refused without ever being built. 16x the wire cap:
// the realistic dense family above estimates at ~38 MiB (its ~304k submessages
// x protoSubmessageBytes), so a legitimate 4 MiB family fits with room, while
// the hostile empty-Metric shape estimates at 256 MiB and is refused. It is per
// DECODE, and one runs per in-flight protobuf scrape, so the process-wide worst
// case is -scrape-concurrency times this — the same multiplication
// maxFamilyAccBytes states for the converter.
const maxProtoDecodedBytes = 16 * maxProtoMessageBytes

// protoSubmessageBytes is the estimated heap of one decoded submessage: the
// generated struct (message state, size cache, unknown-field slice and one
// pointer per field, or a proto2 scalar's own boxed allocation), its pointer in
// the parent's repeated field and that slice's growth slack. Measured against
// the hostile shape (an empty Metric: ~128 B struct + slice slot), and deliberately
// not refined per message kind — this bounds an order of magnitude, it does not
// account bytes. String and bytes payloads are not charged: they decode to
// copies no larger than the wire bytes maxProtoMessageBytes already bounds.
const protoSubmessageBytes = 128

// protoDeltaBytes is one decoded element of a native histogram's delta list
// (an int64 in a []int64), which can cost a single byte on the wire.
const protoDeltaBytes = 8

// protoPresizeBytes bounds what an UNVERIFIED length prefix may allocate before
// the target has delivered a single byte of the message. See readDelimited.
const protoPresizeBytes = 64 << 10

// readDelimited reads the n-byte message body into dst, growing the buffer only
// as the target actually produces bytes.
//
// The length is the TARGET's claim, and this scrape is issued to whatever a
// pod annotation or a ServiceMonitor points at — not necessarily something the
// cluster's operator wrote. Sizing the destination from that claim let a target
// declare the whole maxProtoMessageBytes and send nothing, allocating it per
// concurrent scrape for free (the scraper runs targets in parallel, so it
// multiplies). The same reasoning, and the same shape, as otlpingest.readAllCapped and its
// maxPresizeBytes — the ingest path fixed this for bodies it receives; a scrape
// response is the identical hazard pointing outward.
//
// Growth doubles from a bounded head, so a legitimate large family still costs
// O(log n) allocations, and the buffer is returned for reuse across messages.
func readDelimited(r io.Reader, dst []byte, n int) ([]byte, error) {
	start := min(n, protoPresizeBytes)
	if cap(dst) < start {
		dst = make([]byte, 0, start)
	}
	dst = dst[:0]
	for len(dst) < n {
		if len(dst) == cap(dst) {
			grown := make([]byte, len(dst), min(cap(dst)*2, n))
			copy(grown, dst)
			dst = grown
		}
		m, err := io.ReadFull(r, dst[len(dst):min(cap(dst), n)])
		dst = dst[:len(dst)+m]
		if err != nil {
			// ReadFull says io.EOF when it read NOTHING, which here still means
			// the body ended inside a message whose length prefix promised more
			// — a truncation (reason=body), never the clean end of the
			// exposition the caller's io.EOF test would take it for.
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return dst, err
		}
	}
	return dst, nil
}

// maxExpBuckets bounds the dense expansion of span-encoded buckets: spans
// can declare arbitrary gaps, and a hostile exposition must not allocate
// unbounded bucket slices.
const maxExpBuckets = 4096

// parseProtoAndExport consumes a delimited-protobuf exposition. Classic
// families flow through the same converter/filter machinery as text samples
// (ss.accept); native histograms go straight to the batcher as exponential
// histogram points, filtered and relabeled under their FAMILY name (ss.keep):
// they have no `_bucket`/`_sum`/`_count` component series for a rule to name,
// which is Prometheus' own relabeling behaviour too. The per-scrape policy — filter/relabel drops, MaxSamples,
// chunk flushing, the salvage-on-abort — is the shared scrapeSession's; only
// the format loop lives here. malformed includes the converter's count on
// every return path.
func (ss *scrapeSession) parseProtoAndExport(body io.Reader) (malformed int, err error) {
	defer func() {
		malformed += ss.conv.malformed
		if err != nil {
			// Salvage on an abort (sample limit, truncated body, over-cap or
			// over-budget message), exactly as the text path does. A TIMEOUT is
			// the exception on both fronts: the salvage export would share the
			// expired scrape context, so only the chunks exported during the
			// parse ship (see salvage).
			ss.salvage()
		}
	}()

	br := bufio.NewReaderSize(body, 64*1024)
	var buf []byte
	var mf dto.MetricFamily
	// meta is this exposition's HELP/UNIT budget; see protoMetaBudget.
	var meta protoMetaBudget
	for {
		n, rerr := binary.ReadUvarint(br)
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return malformed, rerr
		}
		if n > maxProtoMessageBytes {
			// Classified as body — "a response body over this pipeline's cap" is
			// exactly this — rather than left to fall through to reason=other,
			// which the metric's own help reads as a gap in the classifier. Only
			// THIS refusal: the read errors either side of it carry the body
			// reader's own timeouts and resets, and an explicit wrapper would
			// win over their timeout/connect classification.
			return malformed, classify(reasonBody, fmt.Errorf("proto message of %d bytes exceeds the %d-byte cap", n, maxProtoMessageBytes))
		}
		var rerr2 error
		if buf, rerr2 = readDelimited(br, buf, int(n)); rerr2 != nil {
			return malformed, rerr2
		}
		if est := protoDecodedSize(buf, maxProtoDecodedBytes); est > maxProtoDecodedBytes {
			// Refused BEFORE the decode, never partially decoded: the whole
			// point is that the heap is never built. The family is counted
			// malformed (it is dropped), and the scrape aborts like the wire cap
			// above — what was converted before it still ships (salvage).
			malformed++
			return malformed, classify(reasonBody, fmt.Errorf("proto message of %d bytes would decode to over %d bytes of heap (estimated at least %d)", n, maxProtoDecodedBytes, est))
		}
		mf.Reset()
		if perr := proto.Unmarshal(buf, &mf); perr != nil {
			malformed++
			continue
		}
		bad, ferr := ss.protoFamily(&mf, &meta)
		malformed += bad
		if ferr != nil {
			return malformed, ferr
		}
		if ferr := ss.flushIfFull(); ferr != nil {
			return malformed, ferr
		}
	}
	if ferr := ss.conv.finish(); ferr != nil {
		return malformed, ferr
	}
	return malformed, nil
}

// protoMetaBudget is the protobuf front's copy of the text parser's
// per-exposition HELP/UNIT accounting (promparse's setMeta), charged the same
// way: the same MaxMetaBytes budget, the family NAME charged once on its first
// admitted field, then each field's text, and at most MaxTrackedFamilies
// families described. The text front charges the name because it keeps it as
// a table key; this front keeps nothing and charges it anyway, because the
// property is that a target is described the same whichever format it
// negotiated. -scrape-native-histograms moves every client_golang target to
// this front at once, and that must not change which series carry a
// Description or a Unit. Charging only the text described all 20,000 families
// of TestProtoFrontDescribesTheSameFamiliesAsTheTextFront's first exposition
// here, while the text front described 12,433 of them.
//
// Parity is exact for a family declared once with HELP before UNIT, which is
// the order client_golang's OpenMetrics encoder writes and the only shape a
// MetricFamily message can carry. The one case it cannot match is a family
// REPEATED within one exposition: the text front charges a same-text
// redeclaration nothing and a changed one only its growth, while this front,
// holding no per-family table to recognise the repeat, charges it again. A
// conforming protobuf exposition never repeats a family (client_golang's
// Gather merges them), so only a target that does pays for it, in its own
// descriptions.
type protoMetaBudget struct {
	bytes    int // charged against maxMetaBytes
	families int // families described, against maxTrackedFamilies
}

// admit returns the part of a family's HELP and UNIT the budget still has room
// for, and charges it. HELP is charged before UNIT and a field that does not
// fit is dropped alone, as the text front does for a HELP line followed by a
// UNIT line.
func (b *protoMetaBudget) admit(family, help, unit string) (string, string) {
	described := false
	if help != "" && !b.charge(family, len(help), &described) {
		help = ""
	}
	if unit != "" && !b.charge(family, len(unit), &described) {
		unit = ""
	}
	return help, unit
}

// charge spends n bytes of text, plus the family name and a family slot when
// the family has no admitted field yet, or spends nothing and reports false.
func (b *protoMetaBudget) charge(family string, n int, described *bool) bool {
	if !*described {
		if b.families >= maxTrackedFamilies {
			return false
		}
		n += len(family)
	}
	if b.bytes+n > maxMetaBytes {
		return false
	}
	b.bytes += n
	if !*described {
		b.families++
		*described = true
	}
	return true
}

// protoFamily converts one MetricFamily. Native points bypass ss.accept (they
// do not flow through the converter), so they charge MaxSamples via
// ss.countNative and flush a full batch between points via ss.flushIfFull.
//
// It checks the scrape context itself, once per Metric and once per component
// row, and that is load-bearing rather than tidy: the whole message is resident
// before the first row, so no socket read gives the deadline a place to land,
// and the per-Metric label cap bounds ONE metric, not a message. Measured
// against a 200 ms deadline before the checks: one 4096-label histogram of
// 265k buckets ran 35.8 s, 132 4096-label gauges 12.0 s, both returning err=nil
// while cycle() waited on them. ctx.Err is an atomic load and allocates
// nothing; a context error classifies as reason=timeout, and salvage declines
// to export once the context is done.
//
// meta is the exposition's HELP/UNIT budget, shared across its families.
func (ss *scrapeSession) protoFamily(mf *dto.MetricFamily, meta *protoMetaBudget) (int, error) {
	malformed := 0
	name := mf.GetName()
	// The text front structurally cannot produce a nameless sample (the
	// grammar rejects the line), but an unset proto `name` decodes cleanly —
	// with type defaulting to COUNTER — and would export an empty-named OTLP
	// metric that a downstream OTLP→Prometheus translation rejects
	// per-series. Reject the family as malformed, mirroring the text front.
	if name == "" {
		return len(mf.GetMetric()), nil
	}
	// The family's HELP/UNIT ride on every sample, exactly as the text path
	// carries them from the "# HELP"/"# UNIT" comments, and under the text
	// front's per-exposition accounting (protoMetaBudget): a field that would
	// pass it ships without its description or unit. That is parity, not a
	// guarantee against an oversized OTLP leaf: a target can make its own
	// chunk over-cap through bucket count alone, on either front.
	help, unit := meta.admit(name, mf.GetHelp(), mf.GetUnit())
	// Every row of the family is emitted through e, whose base carries the
	// fields the rows share; the per-Metric timestamp is set on it below.
	e := protoEmit{ss: ss, base: Sample{Family: name, Help: help, Unit: unit}}
	// One reusable exemplar per family: a Sample only borrows it for the emit
	// call (the converter deep-copies the ones it keeps).
	var ex Exemplar
	typ := mf.GetType()
	// The component names are FAMILY-invariant, so they are built once here
	// rather than once per Metric — a 500-metric family rebuilt three of them
	// 500 times each, for nothing.
	var nameBucket, nameSum, nameCount, nameGSum, nameGCount string
	switch typ {
	case dto.MetricType_SUMMARY:
		nameSum, nameCount = name+"_sum", name+"_count"
	case dto.MetricType_HISTOGRAM:
		nameBucket, nameSum, nameCount = name+"_bucket", name+"_sum", name+"_count"
	case dto.MetricType_GAUGE_HISTOGRAM:
		nameBucket, nameGSum, nameGCount = name+"_bucket", name+"_gsum", name+"_gcount"
	}
	// Bucket bounds and quantiles repeat verbatim on every Metric of the
	// family; the memo renders each distinct value once.
	var floats floatStrings
	// comp is a component row's label set — the metric's labels plus the
	// synthesized le/quantile — built ONCE per Metric in one scratch slice
	// reused across the family, with only its last slot rewritten per row. A
	// Sample only borrows its Labels for the accept call (promparse.Sample's
	// contract, which the text parser relies on too): hist/summ copy them, the
	// batchers PutStr them, the filter and relabel chains only read them. The
	// per-row append it replaced copied the whole label slice for every bucket.
	var comp []Label
	// Resolved ONCE per family, never per Metric: see familyIsNative.
	native := typ == dto.MetricType_HISTOGRAM && familyIsNative(mf)
	var droppedClassic, droppedNHCB int
	defer func() {
		if droppedClassic > 0 {
			obs.ScrapeHistogramMixed.WithLabelValues(ss.pipeline, "classic").Add(float64(droppedClassic))
		}
		if droppedNHCB > 0 {
			obs.ScrapeHistogramMixed.WithLabelValues(ss.pipeline, "nhcb").Add(float64(droppedNHCB))
		}
		if droppedClassic > 0 || droppedNHCB > 0 {
			// The counter says a representation lost; only the line says WHICH
			// FAMILY and on which target, which is the whole diagnosis — a
			// family with mixed representations is the exporter's bug and the
			// operator has to go and find it. Per family per target, once: an
			// exposition does not change shape between cycles.
			// Keyed on the TARGET alone, never on the family name: the name
			// comes off the scraped body, the warnOnce table never expires,
			// and a target minting fresh family names would fill it and
			// silence every other complaint in this package for the life of
			// the process (see warnOnce). So the first mixed family on a
			// target is the one that speaks — which is enough to send an
			// operator to that exporter — and the counter carries the rest.
			ss.s.warnOnce("histmixed:"+ss.warnKey,
				"a protobuf histogram family carries more than one representation; the metrics in the losing one are dropped",
				"target", ss.what, "metric", clipForLog(name), "native", native,
				"droppedClassic", droppedClassic, "droppedNHCB", droppedNHCB)
		}
	}()
	for _, m := range mf.GetMetric() {
		if err := ss.ctx.Err(); err != nil {
			return malformed, err
		}
		labels, ok := protoLabels(m)
		if !ok {
			malformed++
			continue
		}
		ts := m.GetTimestampMs()
		e.base.TimestampMs = ts
		switch typ {
		case dto.MetricType_COUNTER:
			cnt := m.GetCounter()
			if err := e.emit(name, RoleCounter, labels, cnt.GetValue(), ss.protoExemplar(cnt.GetExemplar(), &ex)); err != nil {
				return malformed, err
			}
		case dto.MetricType_GAUGE, dto.MetricType_UNTYPED:
			v := m.GetGauge().GetValue()
			if typ == dto.MetricType_UNTYPED {
				v = m.GetUntyped().GetValue()
			}
			if err := e.emit(name, RoleGauge, labels, v, nil); err != nil {
				return malformed, err
			}
		case dto.MetricType_SUMMARY:
			sum := m.GetSummary()
			if comp, ok = withComponent(comp, labels, "quantile", len(sum.GetQuantile())); !ok {
				malformed++
				continue
			}
			for _, q := range sum.GetQuantile() {
				if err := ss.ctx.Err(); err != nil {
					return malformed, err
				}
				comp[len(comp)-1].Value = floats.get(q.GetQuantile())
				if err := e.emit(name, RoleSummaryQuantile, comp, q.GetValue(), nil); err != nil {
					return malformed, err
				}
			}
			if err := e.emit(nameSum, RoleSummarySum, labels, sum.GetSampleSum(), nil); err != nil {
				return malformed, err
			}
			if err := e.emit(nameCount, RoleSummaryCount, labels, float64(sum.GetSampleCount()), nil); err != nil {
				return malformed, err
			}
		case dto.MetricType_GAUGE_HISTOGRAM:
			// OTLP has no gauge-histogram shape, so every mapping is lossy and
			// the question is only which loss lies. A gauge histogram's buckets
			// are a CURRENT snapshot and may DECREASE, so shipping them as the
			// cumulative monotonic histogram (or exponential point) the
			// HISTOGRAM path builds misdeclares their temporality and makes
			// every downstream rate()/delta read nonsense. The components ship
			// as GAUGES under the OpenMetrics names instead — which is exactly
			// what the text front produces for `# TYPE x gaugehistogram` (an
			// unrecognised TYPE degrades to untyped, i.e. gauges), so the two
			// formats describe one target the same way. A NATIVE gauge histogram
			// keeps only its _gcount/_gsum: its distribution has no OTLP shape
			// that would not lie about temporality.
			h := m.GetHistogram()
			if comp, ok = withComponent(comp, labels, "le", len(h.GetBucket())); !ok {
				malformed++
				continue
			}
			for _, b := range h.GetBucket() {
				if err := ss.ctx.Err(); err != nil {
					return malformed, err
				}
				comp[len(comp)-1].Value = floats.get(b.GetUpperBound())
				if err := e.emit(nameBucket, RoleGauge, comp, bucketCount(b), nil); err != nil {
					return malformed, err
				}
			}
			if err := e.emit(nameGSum, RoleGauge, labels, h.GetSampleSum(), nil); err != nil {
				return malformed, err
			}
			if err := e.emit(nameGCount, RoleGauge, labels, sampleCount(h), nil); err != nil {
				return malformed, err
			}
		case dto.MetricType_HISTOGRAM:
			h := m.GetHistogram()
			if native {
				if r := reprOf(h); r != reprNative {
					// The family ships as ONE exponential histogram metric, and
					// this child is not carrying that shape: its points would be
					// refused by the batcher's type guard anyway. Counted so the
					// discard has an attributable signal.
					if r == reprNHCB {
						droppedNHCB++
					} else {
						droppedClassic++
					}
					continue
				}
				if err := ss.countNative(); err != nil {
					return malformed, err
				}
				if !ss.keep(name, labels) {
					continue
				}
				if !ss.addNativeHistogram(name, metricMeta{help: help, unit: unit}, labels, h, ts) {
					malformed++
					continue
				}
				if err := ss.flushIfFull(); err != nil {
					return malformed, err
				}
				continue
			}
			if isNHCB(h) {
				// An NHCB message's bounds live in custom_values, and
				// client_model v0.6.2 — the version Prometheus itself pins —
				// does not generate that field: they are not reachable from this
				// message at all, so there is nothing to convert. Falling through
				// to the classic branch shipped a point with NO bounds and a
				// single overflow bucket, i.e. a correct count and sum beside a
				// distribution every histogram_quantile reads as nonsense, and
				// counted nothing. Refused instead, so the loss is visible.
				malformed++
				continue
			}
			// The native branch above synthesizes no component — there `le` is
			// an ordinary label — so withComponent's clash check belongs on the
			// classic path only.
			if comp, ok = withComponent(comp, labels, "le", len(h.GetBucket())); !ok {
				malformed++
				continue
			}
			for _, b := range h.GetBucket() {
				if err := ss.ctx.Err(); err != nil {
					return malformed, err
				}
				comp[len(comp)-1].Value = floats.get(b.GetUpperBound())
				if err := e.emit(nameBucket, RoleHistogramBucket, comp, bucketCount(b), ss.protoExemplar(b.GetExemplar(), &ex)); err != nil {
					return malformed, err
				}
			}
			if err := e.emit(nameSum, RoleHistogramSum, labels, h.GetSampleSum(), nil); err != nil {
				return malformed, err
			}
			if err := e.emit(nameCount, RoleHistogramCount, labels, sampleCount(h), nil); err != nil {
				return malformed, err
			}
		default:
			malformed++
		}
	}
	return malformed, nil
}

// protoEmit emits the Samples of one protobuf family. base carries the fields
// every row shares — Family, Help and Unit for the whole family, TimestampMs per
// Metric — so no arm spells them and none can drop one. Help and Unit are
// load-bearing, not decorative: converter.hist/summ take an accumulator's meta
// from whichever component sample creates it, so one component row without
// them strips the family's description and unit.
type protoEmit struct {
	ss   *scrapeSession
	base Sample
}

// emit accepts one row: base plus the row's own name, role, labels, value and
// exemplar (nil for none). The Sample only borrows labels and ex for the call.
func (e *protoEmit) emit(name string, role SampleRole, labels []Label, v float64, ex *Exemplar) error {
	smp := e.base
	smp.Name, smp.Role, smp.Labels, smp.Value, smp.Exemplar = name, role, labels, v, ex
	return e.ss.accept(smp)
}

// protoExemplar converts a protobuf exemplar into the shape the text path
// produces, reusing the caller's scratch (a Sample only borrows its exemplar
// for the emit call). nil when the target sent none or -scrape-exemplars is
// off — the SAME gate the OpenMetrics text path uses, so the two formats agree
// on what the flag means.
//
// The label block is bounded by the OpenMetrics exemplar rule — 128 code
// points of names plus values (promparse.MaxExemplarLabelSetRunes, Prometheus's
// exemplar.ExemplarMaxLabelSetLength) — and against a worse consumer than
// protoLabels'. An exemplar's labels land in pmetric.Exemplar.FilteredAttributes
// via setExemplar's PutStr, which probes the map linearly before every insert,
// so the WRITE alone is O(labels²) inside one uninterruptible call on the
// scrape goroutine — measured 10.5s at 80k labels, and ~420k of them fit inside
// maxProtoMessageBytes. The per-sample label CAP this block used to take was
// not enough either: 4096 labels still cost ~50 ms per exemplar in the
// duplicate scan, and with one exemplar per bucket row, one metric of 85
// exemplar-bearing buckets ran 4 s past its deadline. The rune bound is the
// spec's own and caps the label count at 128, so both scans are trivial.
// The text front applies the identical rule in its parseExemplar, so this is
// the two fronts agreeing rather than a new rule. A refused exemplar is a BAD
// exemplar, never malformed: its sample is still exported, exactly as the text
// path's badExemplars means.
func (ss *scrapeSession) protoExemplar(pe *dto.Exemplar, scratch *Exemplar) *Exemplar {
	if pe == nil || !ss.s.cfg.Exemplars {
		return nil
	}
	lps := pe.GetLabel()
	// Every accepted label has a non-empty name, so each costs at least one
	// rune: a longer list cannot fit, and is refused before anything is read.
	if len(lps) > maxExemplarLabelSetRunes {
		ss.badExemplars++
		return nil
	}
	runes := 0
	for _, lp := range lps {
		runes += utf8.RuneCountInString(lp.GetName()) + utf8.RuneCountInString(lp.GetValue())
		if runes > maxExemplarLabelSetRunes {
			ss.badExemplars++
			return nil
		}
	}
	// An empty or repeated name is refused whole, as the text front refuses
	// the exemplar suffix that carries one (see appendProtoLabels).
	var ok bool
	if scratch.Labels, ok = appendProtoLabels(scratch.Labels[:0], lps); !ok {
		ss.badExemplars++
		return nil
	}
	scratch.Value = pe.GetValue()
	scratch.TimestampMs = 0
	if t := pe.GetTimestamp(); t.IsValid() {
		scratch.TimestampMs = t.AsTime().UnixMilli()
	}
	return scratch
}

// histRepr is the representation one HISTOGRAM message carries its
// distribution in. Each maps to a DIFFERENT OTLP metric type (or, for NHCB, to
// none at all), which is why the choice cannot be made per data point.
type histRepr uint8

const (
	reprClassic histRepr = iota // per-bucket `bucket` rows -> OTLP Histogram
	reprNative                  // span-encoded exponential -> OTLP ExponentialHistogram
	reprNHCB                    // custom-bucket native (schema -53) -> nothing, see isNHCB
)

func reprOf(h *dto.Histogram) histRepr {
	switch {
	case isNative(h):
		return reprNative
	case isNHCB(h):
		return reprNHCB
	default:
		return reprClassic
	}
}

// familyIsNative resolves native-vs-classic ONCE per family.
//
// The decision cannot be per Metric: one metric NAME carries exactly one OTLP
// type, so a family whose children mix representations had its type fixed by
// whichever child was converted first and the other's points were then dropped
// by the batcher's type guard — visible only as obs.ScrapeCollisions, a bare
// counter with no pipeline or target label, so a whole classic child could
// vanish with nothing naming it. Native wins the family, which is Prometheus's
// own preference; the loser is counted in obs.ScrapeHistogramMixed.
func familyIsNative(mf *dto.MetricFamily) bool {
	for _, m := range mf.GetMetric() {
		if isNative(m.GetHistogram()) {
			return true
		}
	}
	return false
}

// isNHCB reports whether a histogram carries CUSTOM-BUCKET native data: schema
// -53, span-encoded counts, and bounds that live in the message's custom_values
// field — which client_model v0.6.2 (the version prometheus/prometheus itself
// pins, and whose own protobuf parser therefore does not read NHCB either) does
// not generate. The bounds are unreachable, so such a message is refused and
// counted rather than converted: see the HISTOGRAM branch of protoFamily.
//
// A message that ALSO carries classic `bucket` rows is not NHCB by this test —
// those bounds ARE readable, and the classic path uses them.
func isNHCB(h *dto.Histogram) bool {
	if h == nil || h.GetSchema() != -53 || len(h.GetBucket()) > 0 {
		return false
	}
	return len(h.GetPositiveSpan()) > 0 || len(h.GetNegativeSpan()) > 0
}

// isNative reports whether a histogram carries native (exponential) data:
// span-encoded buckets or a non-empty zero bucket. NHCB (custom bounds,
// schema -53) is NOT native-exponential — see isNHCB for what happens to it.
//
// The test is on the VALUES, mirroring Prometheus's own isNativeHistogram
// (model/textparse/protobufparse.go). Testing FIELD PRESENCE instead read a
// plain classic histogram from an exporter that materialises default fields —
// schema/zero_threshold/zero_count all set, all zero — as an EMPTY native one:
// the classic buckets were never converted and the point that shipped carried
// count and sum with no buckets at all. The ecosystem contract runs the other
// way: a native histogram with no observations yet declares itself with a
// zero-length no-op span (client_golang emits one) precisely so that a
// value-based test still finds it.
//
// A NIL histogram is not native. That case is reachable from the wire: a
// HISTOGRAM (or GAUGE_HISTOGRAM) family whose Metric omits the histogram
// submessage yields nil from GetHistogram(), and the raw field reads below
// (h.Schema, h.ZeroThreshold, h.ZeroCount) would dereference it — the generated
// GetSchema() above is nil-safe and hides that. Nothing in the agent recovers a
// panic, so a scraped target could crash the process with a few bytes of
// malformed protobuf and hold the node's DaemonSet in CrashLoopBackOff, since
// the same target is re-scraped every cycle. The classic fallback below is all
// nil-safe getters and degrades to an empty histogram, which is what a family
// carrying no data should produce.
func isNative(h *dto.Histogram) bool {
	if h == nil {
		return false
	}
	if h.GetSchema() == -53 {
		return false
	}
	return len(h.GetPositiveSpan()) > 0 || len(h.GetNegativeSpan()) > 0 ||
		h.GetZeroThreshold() > 0 || h.GetZeroCount() > 0
}

// sampleCount, bucketCount and zeroBucketCount read a histogram's observation
// counts. dto's sample_count_float / cumulative_count_float / zero_count_float
// OVERRIDE their integer counterparts when > 0 ("Overrides sample_count if > 0",
// says the message) — that is how a FLOAT histogram carries them, classic
// buckets included, and it is the pair Prometheus keys its own float variant on
// (model/textparse/protobufparse.go). EVERY count read goes through these:
// reading the integer field of a float exposition yields a zero for every
// observation the target made, with no counter moving.
func sampleCount(h *dto.Histogram) float64 {
	if f := h.GetSampleCountFloat(); f > 0 {
		return f
	}
	return float64(h.GetSampleCount())
}

func bucketCount(b *dto.Bucket) float64 {
	if f := b.GetCumulativeCountFloat(); f > 0 {
		return f
	}
	return float64(b.GetCumulativeCount())
}

func zeroBucketCount(h *dto.Histogram) float64 {
	if f := h.GetZeroCountFloat(); f > 0 {
		return f
	}
	return float64(h.GetZeroCount())
}

// bucketPopulation is the number of observations a native histogram's buckets
// carry — the quantity OTLP requires the point's count to equal. false on
// overflow: a population that does not fit a uint64 cannot be expressed by any
// valid point, so the caller refuses the point as malformed rather than
// shipping a wrapped one, which is countOf's own discipline.
func bucketPopulation(zero uint64, pos, neg []uint64) (uint64, bool) {
	total := zero
	for _, counts := range [2][]uint64{pos, neg} {
		for _, v := range counts {
			sum := total + v
			if sum < total {
				return 0, false
			}
			total = sum
		}
	}
	return total, true
}

// addNativeHistogram appends one exponential histogram point to the
// batcher; false = undecodable, or a value no valid OTLP point can carry
// (counted malformed by the caller).
func (ss *scrapeSession) addNativeHistogram(name string, meta metricMeta, labels []Label, h *dto.Histogram, ts int64) bool {
	eb, ok := ss.cb.(expSink)
	if !ok {
		return false // batcher variant without exponential support
	}
	// schema and zero_threshold are stamped VERBATIM onto the OTLP point
	// (fillExponentialPoint: scale, zero region), and both are the TARGET's
	// values: Prometheus defines standard exponential schemas only in [-4, 8]
	// (-53 is NHCB and never reaches this path), and a zero region cannot be
	// negative or non-finite. Out of range they make a spec-invalid point a
	// strict backend rejects PER BATCH — the target's co-batched metrics with
	// it, every cycle, with only a generic export error as signal — so they
	// are refused like the hostile span shapes below (no classic fallback:
	// decodeSpans failures get none either), never clamped, which would hide
	// the producer bug behind rewritten data.
	if sch := h.GetSchema(); sch < -4 || sch > 8 {
		return false
	}
	if zt := h.GetZeroThreshold(); math.IsNaN(zt) || math.IsInf(zt, 0) || zt < 0 {
		return false
	}
	pos, posOff, ok := decodeSpans(h.GetPositiveSpan(), h.GetPositiveDelta(), h.GetPositiveCount())
	if !ok {
		return false
	}
	neg, negOff, ok := decodeSpans(h.GetNegativeSpan(), h.GetNegativeDelta(), h.GetNegativeCount())
	if !ok {
		return false
	}
	count, ok := countOf(sampleCount(h))
	if !ok {
		return false
	}
	zero, ok := countOf(zeroBucketCount(h))
	if !ok {
		return false
	}
	// OTLP requires count == zero_count + sum(positive) + sum(negative), and on
	// the FLOAT encoding nothing upstream of here can hold it: the sample count,
	// the zero count and every absolute bucket count go through countOf
	// INDEPENDENTLY, so a message that is CONSISTENT on the wire
	// (sample_count_float: 1, positive_count: [0.5, 0.5]) converts to count=1
	// against buckets [1 1] — buckets claiming twice the observations their
	// declared population has. The overclaim is not a ±1 artifact either: it
	// scales with the number of buckets, up to ~2x the population. This is the
	// exponential sibling of the [run, total] clamp fillHistogramPoint applies
	// so the classic shape holds the identity BY CONSTRUCTION.
	//
	// The reconciliation is ONE-DIRECTIONAL and must stay so. Raising count to
	// the population its buckets carry is the conservative reading — a
	// population cannot be smaller than what was observed into it — while
	// lowering it (or clamping the buckets down, the classic path's move) would
	// destroy a legitimate shape: a NaN observation increments a Prometheus
	// histogram's count without entering any bucket (client_golang's own
	// validateCount permits population <= count for exactly that reason), so
	// count > bucket sum is the TARGET's truth passed through faithfully, not
	// something to repair here.
	population, ok := bucketPopulation(zero, pos, neg)
	if !ok {
		return false
	}
	count = max(count, population)
	// A native histogram carries its exemplars on the family message rather
	// than per bucket; they are point-scoped either way.
	var exemplars []Exemplar
	if ss.s.cfg.Exemplars {
		var scratch Exemplar
		for _, pe := range h.GetExemplars() {
			if len(exemplars) >= maxExemplarsPerPoint {
				break
			}
			if e := ss.protoExemplar(pe, &scratch); e != nil {
				exemplars = append(exemplars, copyExemplar(*e))
			}
		}
	}
	eb.addExponential(name, expPoint{
		labels:    labels,
		meta:      meta,
		exemplars: exemplars,
		ts:        ts,
		schema:    h.GetSchema(),
		zeroCount: zero,
		zeroTh:    h.GetZeroThreshold(),
		count:     count,
		sum:       h.GetSampleSum(),
		hasSum:    h.SampleSum != nil,
		pos:       pos, posOffset: posOff,
		neg: neg, negOffset: negOff,
	})
	return true
}

// decodeSpans expands Prometheus span/delta bucket encoding into the dense
// absolute counts OTLP wants. Prometheus indexes are 1-based upper-bound
// indexes (bucket i covers (base^(i-1), base^i]); OTLP buckets are 0-based
// lower-bound (index j covers (base^(offset+j), base^(offset+j+1)]), so the
// OTLP offset is the first Prometheus index minus one.
//
// An INTEGER histogram delta-encodes its bucket counts in deltas; a FLOAT one
// carries them ABSOLUTELY in the parallel *_count field ("Absolute count of
// each bucket", says the message), which is what Prometheus reads into a
// FloatHistogram. Both are well-formed, so the encoding follows what the
// message actually carries: reading only deltas made every float histogram
// look like a span run with missing deltas — counted malformed and dropped
// WHOLE, count and sum included.
//
// The running index is int64 because it is driven by int32 offsets and uint32
// lengths the TARGET chooses: in int32 arithmetic a hostile span pair wrapped
// past the gap guard and produced a point whose buckets sat at a
// mathematically impossible index. An index (or an OTLP offset) leaving int32
// range is refused, never wrapped.
func decodeSpans(spans []*dto.BucketSpan, deltas []int64, absolute []float64) (counts []uint64, offset int32, ok bool) {
	if len(spans) == 0 {
		return nil, 0, true
	}
	// Deltas win when a message carries both: that is the integer encoding.
	floats := len(deltas) == 0 && len(absolute) > 0
	var idx, start int64
	first := true
	var cur int64
	di := 0
	for _, sp := range spans {
		idx += int64(sp.GetOffset())
		if idx < math.MinInt32 || idx > math.MaxInt32 {
			return nil, 0, false
		}
		if first {
			start = idx
			first = false
		} else {
			gap := idx - start - int64(len(counts))
			if gap < 0 || int64(len(counts))+gap > maxExpBuckets {
				return nil, 0, false
			}
			counts = append(counts, make([]uint64, gap)...)
		}
		for i := uint32(0); i < sp.GetLength(); i++ {
			var v uint64
			if floats {
				if di >= len(absolute) {
					return nil, 0, false
				}
				var vok bool
				if v, vok = countOf(absolute[di]); !vok {
					return nil, 0, false
				}
			} else {
				if di >= len(deltas) {
					return nil, 0, false
				}
				cur += deltas[di]
				if cur < 0 {
					return nil, 0, false
				}
				v = uint64(cur)
			}
			di++
			if len(counts) >= maxExpBuckets {
				return nil, 0, false
			}
			counts = append(counts, v)
		}
		// The next span's offset is relative to the index AFTER this span.
		idx += int64(sp.GetLength())
		if idx < math.MinInt32 || idx > math.MaxInt32 {
			return nil, 0, false
		}
	}
	if start-1 < math.MinInt32 {
		return nil, 0, false
	}
	return counts, int32(start - 1), true
}

// protoLabels converts a metric's label pairs; ok is false for a set
// appendProtoLabels refuses, and the caller drops the metric as malformed,
// mirroring the text front's whole-line reject.
func protoLabels(m *dto.Metric) ([]Label, bool) {
	lps := m.GetLabel()
	if len(lps) == 0 {
		return nil, true
	}
	out, ok := appendProtoLabels(nil, lps)
	if !ok {
		return nil, false
	}
	return out, true
}

// appendProtoLabels appends wire label pairs to dst — a metric's own labels
// (protoLabels) or an exemplar's (protoExemplar), the two doors a protobuf
// label set arrives through — refusing the set whole (ok false, dst returned
// at its original length so a caller's scratch keeps its backing array) for
// any of three shapes:
//
// An empty NAME: an unset proto field the text grammar cannot express, which
// would otherwise become an empty OTLP attribute key.
//
// A REPEATED name. `repeated LabelPair` carries duplicates by construction, so
// this is the one shape the wire format admits and the text grammar does not
// (the text front fails the whole line, or the exemplar suffix, parser.go's
// parseLabels). It must be refused here for the same reason: every reader in
// this package resolves a name through labelValue, which returns the FIRST
// match, while pcommon.Map.PutStr upserts, so the LAST pair is what ships — a
// keep/drop rule then matches a value the export does not carry, an exemplar is
// attributed to a value nothing else in the pipeline agrees on, and a histogram
// whose own `le` repeats collapses every bucket row into one accumulator with
// malformed=0. The O(n²) scan runs over a few labels.
//
// More than maxLabelsPerSample pairs: the SAME ceiling the text front applies,
// for the same reason and more urgently. The duplicate-name scan is O(labels²)
// and runs inside one uninterruptible call; the text path survives a hostile
// line only because its parse is interleaved with 64 KiB socket reads, so the
// scrape deadline lands between them. Here the whole message is already
// resident (readDelimited + proto.Unmarshal) before the first comparison, so
// nothing interrupts it: 120k labels in one metric — 1.38 MiB, well inside
// maxProtoMessageBytes — measured 80s against a 1s scrape timeout, and cycle()
// waits for every scrape it starts. The check comes before anything is
// allocated. (An exemplar is held far below it, by protoExemplar's rune bound.)
//
// The cap bounds ONE label set's scan (tens of milliseconds at the ceiling),
// never a MESSAGE: ~100 metrics at the ceiling fit in maxProtoMessageBytes and
// ran seconds past the deadline. What bounds the message is protoFamily's
// per-Metric context check, which this cap makes fine-grained enough to land
// within one metric of the deadline.
func appendProtoLabels(dst []Label, lps []*dto.LabelPair) ([]Label, bool) {
	if len(lps) > maxLabelsPerSample {
		return dst, false
	}
	base := len(dst)
	dst = slices.Grow(dst, len(lps))
	for _, lp := range lps {
		name := lp.GetName()
		if name == "" || hasLabel(dst[base:], name) {
			return dst[:base], false
		}
		dst = append(dst, Label{Name: name, Value: lp.GetValue()})
	}
	return dst, true
}

// withComponent builds a component row's label set into dst: the metric's own
// labels and one trailing slot named component (`le` or `quantile`), whose VALUE
// the caller sets per row.
//
// ok is false, dst returned untouched, when the metric will emit rows (rows >
// 0) and ALREADY carries a label of that name; the caller drops the metric as
// malformed. The component label is appended AFTER the metric's own, so a
// target that already carries it creates exactly the duplicate protoLabels
// refuses — only where protoLabels can no longer see it. The consequence is the
// same and worse: every reader resolves `quantile`/`le` to the FIRST (the
// target's) pair while the export carries the last, so a histogram's bucket
// rows all read one bound and collapse into a single accumulator with
// malformed=0. The text grammar cannot express it either
// (`x_bucket{le="1",le="2"}` fails the line), so the metric is malformed here
// too. The check lives here rather than at each call so that no component arm
// can build its rows without it.
func withComponent(dst, labels []Label, component string, rows int) ([]Label, bool) {
	if rows > 0 && hasLabel(labels, component) {
		return dst, false
	}
	dst = append(dst[:0], labels...)
	return append(dst, Label{Name: component}), true
}

// protoMsg is a MetricFamily submessage kind, for the pre-decode size walk.
type protoMsg uint8

const (
	pmScalar    protoMsg = iota // a string, a packed scalar list or unknown bytes: no submessage
	pmLeaf                      // LabelPair, Gauge, Untyped, Quantile, BucketSpan, Timestamp: scalar fields only
	pmFamily                    // MetricFamily
	pmMetric                    // Metric
	pmCounter                   // Counter
	pmSummary                   // Summary
	pmHistogram                 // Histogram
	pmBucket                    // Bucket
	pmExemplar                  // Exemplar
)

// protoChild names the submessage kind a length-delimited field of parent
// decodes into, per client_model's io.prometheus.client schema (field numbers
// as in metrics.pb.go). The schema is not recursive, so the walk below is at
// most five levels deep; a field it does not know is walked as opaque bytes,
// which is also what the decoder keeps it as (unknown fields).
func protoChild(parent protoMsg, field protowire.Number) protoMsg {
	switch parent {
	case pmFamily:
		if field == 4 { // metric
			return pmMetric
		}
	case pmMetric:
		switch field {
		case 1, 2, 5: // label, gauge, untyped
			return pmLeaf
		case 3:
			return pmCounter
		case 4:
			return pmSummary
		case 7:
			return pmHistogram
		}
	case pmCounter:
		switch field {
		case 2:
			return pmExemplar
		case 3: // created_timestamp
			return pmLeaf
		}
	case pmSummary:
		if field == 3 || field == 4 { // quantile, created_timestamp
			return pmLeaf
		}
	case pmHistogram:
		switch field {
		case 3:
			return pmBucket
		case 9, 12, 15: // negative_span, positive_span, created_timestamp
			return pmLeaf
		case 16: // exemplars
			return pmExemplar
		}
	case pmBucket:
		if field == 3 {
			return pmExemplar
		}
	case pmExemplar:
		if field == 1 || field == 3 { // label, timestamp
			return pmLeaf
		}
	}
	return pmScalar
}

// protoDecodedSize estimates, from the WIRE, the heap proto.Unmarshal will
// materialise for one MetricFamily message: protoSubmessageBytes per
// submessage and protoDeltaBytes per native-histogram delta. It stops as soon
// as the estimate passes limit, so a refusal costs at most one walk to the
// point of refusal. O(len(b)) and allocation-free (protowire parses in place).
// Malformed wire just ends the walk: the decode then fails on it and counts it.
func protoDecodedSize(b []byte, limit int) int {
	w := protoSizeWalk{limit: limit}
	w.walk(b, pmFamily)
	return w.n
}

type protoSizeWalk struct{ n, limit int }

func (w *protoSizeWalk) walk(b []byte, kind protoMsg) bool {
	for len(b) > 0 {
		if w.n > w.limit {
			return false
		}
		num, typ, tl := protowire.ConsumeTag(b)
		if tl < 0 {
			return false
		}
		b = b[tl:]
		// negative_delta, positive_delta: packed (or not) sint64 lists, one
		// int64 per element however few wire bytes the element took.
		deltas := kind == pmHistogram && (num == 10 || num == 13)
		if typ != protowire.BytesType {
			if deltas && typ == protowire.VarintType {
				w.n += protoDeltaBytes
			}
			vl := protowire.ConsumeFieldValue(num, typ, b)
			if vl < 0 {
				return false
			}
			b = b[vl:]
			continue
		}
		v, vl := protowire.ConsumeBytes(b)
		if vl < 0 {
			return false
		}
		b = b[vl:]
		switch child := protoChild(kind, num); child {
		case pmScalar:
			if deltas {
				w.n += protoDeltaBytes * varintCount(v)
			}
		case pmLeaf:
			w.n += protoSubmessageBytes
		default:
			w.n += protoSubmessageBytes
			if !w.walk(v, child) {
				return false
			}
		}
	}
	return true
}

// varintCount is the number of varints in a packed run: every varint ends in
// exactly one byte with the continuation bit clear.
func varintCount(b []byte) int {
	n := 0
	for _, c := range b {
		if c < 0x80 {
			n++
		}
	}
	return n
}

// floatStrings renders the float64 label values of one family — bucket bounds
// and summary quantiles — once per distinct value.
//
// Both are FAMILY-invariant (every Metric of a family repeats the same bound
// set), and the fmt.Sprintf this replaces cost a string plus the interface
// boxing of its argument on every bucket of every metric. The rendering is
// unchanged: fmt's %v for a float64 IS %g at the default precision, so the old
// integral-vs-not branch produced the same shortest-round-trip form on both
// sides, which is what strconv writes here.
type floatStrings struct {
	buf  []byte
	memo map[float64]string
}

func (f *floatStrings) get(v float64) string {
	if s, ok := f.memo[v]; ok {
		return s
	}
	f.buf = strconv.AppendFloat(f.buf[:0], v, 'g', -1, 64)
	s := string(f.buf)
	if f.memo == nil {
		f.memo = make(map[float64]string, 16)
	}
	// NaN never equals itself, so a bound set of NaNs would insert one entry per
	// row: bounded like the text parser's own interning.
	if len(f.memo) < maxInternedValues {
		f.memo[v] = s
	}
	return s
}
