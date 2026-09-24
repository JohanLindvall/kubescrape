// Package debugtap is the agent's on-demand equivalent of a collector debug
// exporter: GET /debug/otlp streams a text (OTLP JSON Lines) representation
// of the payloads flowing through the export chain to any attached HTTP
// client, filtered by resource attributes (path.Match globs on key and
// value, with '/' an ordinary character — see compileGlob) and downsampled by a
// percentage — both query parameters, so the cost is chosen per session, not
// per deployment. /debug/otlp/ui is a minimal built-in page driving the same
// endpoint.
//
// The tap sits between the transforms and the router (producers → transform
// → tap → router → buffer → client), so it shows payloads as they will ship
// — post-transform, pre-fan-out, routed destinations included. The agent's
// own self-metrics chain deliberately bypasses the router and the tap.
//
// Cost discipline: with no subscriber the tap is one atomic load per export.
// With subscribers attached, filtering and marshaling run on the exporting
// goroutine — the debugging session pays while it looks — but a SLOW READER
// NEVER BLOCKS an export: each subscriber has a bounded channel and an
// over-full one drops the payload for that subscriber, counted and reported
// on its own stream at the end.
package debugtap

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
)

// signal selects which payloads a subscriber sees.
type signal uint8

const (
	sigLogs signal = 1 << iota
	sigMetrics
	sigTraces
	sigAll = sigLogs | sigMetrics | sigTraces
)

// attrFilter is one resource-attribute condition: a resource matches when ANY
// of its attributes has a key matching key and a SCALAR value (rendered as
// string) matching value — both path.Match globs, with '/' an ordinary
// character (see compileGlob). A subscriber's filters are ANDed. Built by
// newAttrFilter, which compiles both halves once for the stream's life.
type attrFilter struct {
	key   glob
	value glob
}

// newAttrFilter compiles a validated key=value filter (ServeHTTP runs
// config.Glob over both halves before this).
func newAttrFilter(key, value string) attrFilter {
	return attrFilter{key: compileGlob(key), value: compileGlob(value)}
}

func (f attrFilter) matches(attrs pcommon.Map) bool {
	for k, v := range attrs.All() {
		if f.key.match(k) {
			if s, ok := filterValue(v); ok && f.value.match(s) {
				return true
			}
		}
	}
	return false
}

// filterValue renders an attribute value for the VALUE half of a filter, and
// refuses the kinds whose rendering is unbounded — the size half of the
// ceiling maxAttrFilters and maxAttrFilterBytes bound on the PATTERN side, and
// which they cannot express because it is the payload, not the query, that
// chooses it.
//
// pcommon.Value.AsString JSON-marshals a Map or Slice value in full (building
// a map[string]any first) and base64-encodes a Bytes one, allocating
// proportionally to the value — once per key-matching filter, per resource,
// per export, on the EXPORTING goroutine, which for logs is the tailer's
// single sweep goroutine serving every log file on the node. A resource
// attribute of those kinds cannot come from this agent (everything attrs.Build
// stamps is a scalar); it can only arrive from a push on the unauthenticated
// -ingest or trace-tier listeners, bounded only by the 16 MiB message cap. So
// a wide-key filter over a hostile payload is the same multiplier
// maxAttrFilters exists to refuse, reached through the value side.
//
// They are therefore treated as NON-MATCHING rather than truncated: a
// truncated render would match a glob the whole value does not, which is the
// one thing worse than not matching on a debugging stream. What is lost is the
// ability to filter on a pushed structured attribute; what is kept is that
// matching never RENDERS what the sender chose.
//
// A Str value is still the sender's length, and nothing bounds it but the
// message cap: its cost is matching, not rendering, which is why the matcher
// (compileGlob) compares a literal with ==, a `*`-only glob with a substring
// scan, and copies the value only for a `?`, class or escape glob that meets a
// '/' in it — an operand cap would bound nothing, since a sender spreads the
// same bytes across many resources, and it would stop legitimate filters on
// long values.
func filterValue(v pcommon.Value) (string, bool) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str(), true
	case pcommon.ValueTypeEmpty, pcommon.ValueTypeBool, pcommon.ValueTypeInt, pcommon.ValueTypeDouble:
		return v.AsString(), true
	default: // Map, Slice, Bytes — rendered size is the sender's choice
		return "", false
	}
}

// glob is one half of an attribute filter, compiled once when the stream
// attaches: path.Match syntax with the SEPARATOR NEUTRALIZED. Neither half of
// a filter is a path, but path.Match's `*` and `?` stop at '/', so
// `container.image.name=*nginx*` could never match
// "docker.io/library/nginx:1.25" and `k8s.pod.label.*` could never match the
// `app.kubernetes.io/name` key attrs stamps for the commonest Kubernetes label
// form. The result is an empty stream indistinguishable from "this agent is
// exporting nothing" — the very outcome the pattern validation at the query
// seam exists to prevent.
//
// Matching runs on the EXPORTING goroutine, per attribute per resource per
// export, against operands a sender chose, so the shapes are split by cost:
//
//   - a LITERAL (no `*?[\`) is ==: no copy, and a length mismatch fails in O(1);
//   - a `*`-only glob (the `*nginx*` shape) is a prefix/suffix check plus a
//     substring scan per literal run — `*` matches any bytes, '/' included, so
//     nothing needs neutralizing and nothing is copied;
//   - anything else (`?`, a class, an escape) is path.Match with '/' replaced
//     by NUL in both operands: the pattern once, here; the name per comparison,
//     and only when it holds a '/' (strings.ReplaceAll returns a '/'-free
//     input unchanged). NUL is an ordinary byte to path.Match, so classes,
//     escapes and config.Glob's validity probe are unaffected.
//
// The first two are also more exact than the neutralized path.Match they
// replace: that aliased a NUL in the name to a '/' in the pattern.
type glob struct {
	kind globKind
	lit  string   // globLiteral: the whole pattern
	segs []string // globStars: the literal runs around the '*'s, len >= 2
	neut string   // globGeneral: the pattern with '/' neutralized
}

type globKind uint8

const (
	globLiteral globKind = iota
	globStars
	globGeneral
)

func compileGlob(pat string) glob {
	switch {
	case !strings.ContainsAny(pat, `*?[\`):
		return glob{kind: globLiteral, lit: pat}
	case !strings.ContainsAny(pat, `?[\`):
		return glob{kind: globStars, segs: strings.Split(pat, "*")}
	default:
		return glob{kind: globGeneral, neut: strings.ReplaceAll(pat, "/", "\x00")}
	}
}

func (g glob) match(s string) bool {
	switch g.kind {
	case globLiteral:
		return s == g.lit
	case globStars:
		return starMatch(g.segs, s)
	default:
		ok, _ := path.Match(g.neut, strings.ReplaceAll(s, "/", "\x00"))
		return ok
	}
}

// starMatch matches a glob whose only metacharacter is '*', given as the
// literal runs between the stars (strings.Split(pat, "*"), so at least two):
// the first must be a prefix, the last a suffix of what the prefix leaves, and
// each middle run must occur, leftmost, after the previous one. Leftmost is
// exact for '*' — any later occurrence leaves strictly less for the rest.
func starMatch(segs []string, s string) bool {
	first, last := segs[0], segs[len(segs)-1]
	if !strings.HasPrefix(s, first) {
		return false
	}
	s = s[len(first):]
	if !strings.HasSuffix(s, last) {
		return false
	}
	s = s[:len(s)-len(last)]
	for _, mid := range segs[1 : len(segs)-1] {
		i := strings.Index(s, mid)
		if i < 0 {
			return false
		}
		s = s[i+len(mid):]
	}
	return true
}

// maxSubscribers bounds concurrent debug streams. Every subscriber costs a
// render per export ON THE EXPORTING GOROUTINE (~70 ms for a 16 MiB payload),
// which for the tailer is the single sweep goroutine serving every log file on
// the node — so N idle curls must not be able to multiply that unboundedly.
// The bound holds for callers who PASS the agent's debug gate too: a local
// connection or a valid -debug-token-file token says who may read the stream,
// not how many copies of it the node can afford. Over the cap the
// stream is refused with 503; four concurrent debugging humans is already a
// crowded incident call.
const maxSubscribers = 4

// maxAttrFilters bounds ONE stream's filter list, which maxSubscribers cannot:
// the subscriber cap bounds how many renders an export pays for, this bounds
// how many GLOBS each of those renders is walked against (how much each glob
// then costs is maxAttrFilterBytes, below — the count alone was documented
// here as bounding the matching cost, and a count is not a bound on bytes).
// Filters are ANDed and matchAll
// short-circuits only on a non-match, so filters an attacker picks to all
// match (`attr=*/*=*`) each walk the resource's whole attribute map — the cost
// is len(filters) x attributes x resources, per export, on the exporting
// goroutine. Go's net/url caps a request at 10,000 query parameters, which is
// three orders of magnitude above any real debugging session; a human ANDing
// more than a handful of resource-attribute globs has already lost the thread,
// and the UI's textarea is one glob per line.
const maxAttrFilters = 16

// maxAttrFilterBytes bounds ONE filter's `key=value` text — the SIZE half of
// the ceiling above, which the count half cannot express.
//
// Matching a glob costs in proportion to its length — path.Match is
// O(len(pattern) x len(name)), and a `*` glob's substring scans are linear in
// the pattern too — paid per resource attribute, per resource, per export, on
// the exporting goroutine: for logs the tailer's single sweep goroutine
// serving every log file on the node. With only the count bounded, the ceiling
// on one glob was net/http's 1 MiB request line, i.e. the caller's choice: 16
// filters of a megabyte each is the same multiplier the count bound was
// written against, spelled with fewer, larger patterns. Together the two
// ceilings bound the PATTERN text a stream carries at 16 x 512 B = 8 KiB — the
// query's half of the cost. The other half is the payload's (the names and
// values it is matched against); filterValue says what bounds that.
//
// The bound lives HERE and not in config.Glob, which owns the parse-time
// validity probe for this door as well as the routing globs and the tailer's
// source namespaces. Those two are OPERATOR-authored startup config, read once
// at boot with no request driving them, where a length ceiling would refuse a
// legitimate long route glob and buy nothing. This one arrives per REQUEST,
// from whoever passed the debug gate, and is re-walked on every export for the
// life of the stream — which is the difference the bound is about.
//
// 512 bytes is far above any real filter: an attribute KEY is a semconv name
// (tens of bytes), and the longest value a Kubernetes object can put in an
// attribute is a 253-byte DNS name.
const maxAttrFilterBytes = 512

// maxQueuedBytes bounds what ONE subscriber's channel may hold. The channel's
// slot count (256) bounds nothing by itself — 256 slots of 16 MiB renders is
// 4 GB pinned by a reader that connected and stopped reading — so the queue
// is bounded by BYTES and an over-budget payload is dropped for that stream
// (counted, reported on the stream) exactly like a full channel.
//
// A render LARGER than the whole budget is a different drop with a different
// remedy: no reader is fast enough for it, so it is counted apart
// (subscriber.oversized) and reported as such rather than as a slow reader.
// The tap sits above the router and sees whole ingest and trace-tier pushes,
// up to the 16 MiB HTTP body cap and past it with
// -ingest-grpc-max-recv-bytes raised, and JSON roughly doubles them.
const maxQueuedBytes = 32 << 20

// dropReportEvery paces the stream's own drop report when NOTHING is being
// delivered. The report used to ride on a delivery, so a stream whose every
// payload was dropped — each one over the budget, say — showed its banner and
// then nothing, which is exactly what "nothing matched" looks like.
const dropReportEvery = 2 * time.Second

// subscriber is one attached debug stream.
type subscriber struct {
	signals signal
	filters []attrFilter
	sample  float64 // percent of matching RESOURCES kept, 0-100
	ch      chan []byte
	dropped atomic.Int64
	// oversized counts payloads whose render alone exceeded the queue budget
	// (maxQueuedBytes) since the stream last reported them — never deliverable
	// however fast the reader, so reported apart from the slow-reader drops.
	// Swung to zero on report like dropped; droppedAll carries both.
	oversized atomic.Int64
	// droppedAll is the same tally the stream never resets. dropped is SWUNG
	// TO ZERO each time the stream reports it to its reader, so it cannot also
	// serve the detach line — which would then say 0 for a session that spent
	// its whole life dropping.
	droppedAll atomic.Int64
	// queued tracks the bytes sitting in ch: reserved before the send (and
	// released again when the send loses to a full channel), released by the
	// reader after receive.
	queued atomic.Int64
	// since is when this stream attached. It exists for the refusal line: that
	// line is throttled to one per window, so it has to carry the whole story
	// on its own, and "the oldest session has been open for 3h" is the story
	// ("someone left a curl running") that a bare count is not.
	since time.Time
}

// Tap wraps the export chain and fans matching payloads out to subscribers.
type Tap struct {
	inner  otlpexport.Exporter
	traces otlpexport.TracesExporter // nil when the chain has no trace capability

	active atomic.Int32 // len(subs); the zero-subscriber fast path
	mu     sync.Mutex
	subs   map[int]*subscriber
	next   int

	// randFloat is rand.Float64, injectable for deterministic tests.
	randFloat func() float64
	// queueBudget is maxQueuedBytes and reportEvery is dropReportEvery;
	// fields rather than constants read in place so a test can reach the
	// over-budget and idle-report paths without 32 MiB renders or 2s waits.
	queueBudget int64
	reportEvery time.Duration
}

// New wraps inner. Trace capability follows inner's, exactly as the
// transform wrapper's does.
func New(inner otlpexport.Exporter) *Tap {
	traces, _ := inner.(otlpexport.TracesExporter)
	return &Tap{
		inner: inner, traces: traces, subs: map[int]*subscriber{}, randFloat: rand.Float64,
		queueBudget: maxQueuedBytes, reportEvery: dropReportEvery,
	}
}

var logsMarshaler = &plog.JSONMarshaler{}
var metricsMarshaler = &pmetric.JSONMarshaler{}
var tracesMarshaler = &ptrace.JSONMarshaler{}

// ExportLogs offers the payload to the attached log streams (one atomic load
// when there are none) and forwards it unchanged.
func (t *Tap) ExportLogs(ctx context.Context, ld plog.Logs) error {
	if t.active.Load() > 0 {
		t.offer(sigLogs, func(sub *subscriber) ([]byte, *renderFailure) { return t.renderLogs(ld, sub) })
	}
	return t.inner.ExportLogs(ctx, ld)
}

// ExportMetrics is ExportLogs for metrics.
func (t *Tap) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	if t.active.Load() > 0 {
		t.offer(sigMetrics, func(sub *subscriber) ([]byte, *renderFailure) { return t.renderMetrics(md, sub) })
	}
	return t.inner.ExportMetrics(ctx, md)
}

// ExportTraces is ExportLogs for traces; it fails when the wrapped chain has
// no trace capability, after the streams have seen the payload.
func (t *Tap) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if t.active.Load() > 0 {
		t.offer(sigTraces, func(sub *subscriber) ([]byte, *renderFailure) { return t.renderTraces(td, sub) })
	}
	if t.traces == nil {
		return errors.New("the export chain has no trace capability")
	}
	return t.traces.ExportTraces(ctx, td)
}

// renderFailure is a marshal error, kept apart from "nothing matched" so the
// caller can report the one and stay silent about the other. Both spell the
// same thing on the wire — no line for this subscriber — which is why they had
// to be told apart here rather than at the stream.
type renderFailure struct {
	signal string
	err    error
}

// marshalWarns throttles that report. It is keyless: a payload that fails to
// marshal fails on every export and for every subscriber, so the useful
// information is one line, and the flood would otherwise run at the export
// rate times the subscriber count.
var marshalWarns = &logdedupe.Throttle{}

const marshalWarnEvery = time.Minute

// The three renderers are the same shape spelled thrice — pdata's generated
// slices share methods but no interface, and three plain loops read better
// than the generics needed to unify them.
//
// A stream that keeps EVERY resource (keepsAll) marshals the payload itself
// rather than a deep copy of all of it: the JSON marshaler only reads, and
// offer renders synchronously while the exporter's caller still holds the
// payload. That is the default /debug/otlp request and the UI's, and the copy
// was ~70% of such a render's allocations and bytes, on the exporting
// goroutine.

func (t *Tap) renderLogs(ld plog.Logs, sub *subscriber) ([]byte, *renderFailure) {
	out := ld
	if !sub.keepsAll() {
		out = plog.NewLogs()
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			if sub.keeps(rl.Resource().Attributes(), t.randFloat) {
				rl.CopyTo(out.ResourceLogs().AppendEmpty())
			}
		}
	}
	if out.ResourceLogs().Len() == 0 {
		return nil, nil
	}
	b, err := logsMarshaler.MarshalLogs(out)
	if err != nil {
		return nil, &renderFailure{signal: "logs", err: err}
	}
	return b, nil
}

func (t *Tap) renderMetrics(md pmetric.Metrics, sub *subscriber) ([]byte, *renderFailure) {
	out := md
	if !sub.keepsAll() {
		out = pmetric.NewMetrics()
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			rm := md.ResourceMetrics().At(i)
			if sub.keeps(rm.Resource().Attributes(), t.randFloat) {
				rm.CopyTo(out.ResourceMetrics().AppendEmpty())
			}
		}
	}
	if out.ResourceMetrics().Len() == 0 {
		return nil, nil
	}
	b, err := metricsMarshaler.MarshalMetrics(out)
	if err != nil {
		return nil, &renderFailure{signal: "metrics", err: err}
	}
	return b, nil
}

func (t *Tap) renderTraces(td ptrace.Traces, sub *subscriber) ([]byte, *renderFailure) {
	out := td
	if !sub.keepsAll() {
		out = ptrace.NewTraces()
		for i := 0; i < td.ResourceSpans().Len(); i++ {
			rs := td.ResourceSpans().At(i)
			if sub.keeps(rs.Resource().Attributes(), t.randFloat) {
				rs.CopyTo(out.ResourceSpans().AppendEmpty())
			}
		}
	}
	if out.ResourceSpans().Len() == 0 {
		return nil, nil
	}
	b, err := tracesMarshaler.MarshalTraces(out)
	if err != nil {
		return nil, &renderFailure{signal: "traces", err: err}
	}
	return b, nil
}

// drop records one payload this subscriber did not get: once for the stream's
// own next report, once for the lifetime tally the detach line carries.
func (s *subscriber) drop() {
	s.dropped.Add(1)
	s.droppedAll.Add(1)
}

// dropOversized records one payload whose render was larger than the whole
// queue budget: counted apart for the stream's report, and in the same
// lifetime tally as every other drop.
func (s *subscriber) dropOversized() {
	s.oversized.Add(1)
	s.droppedAll.Add(1)
}

// keepsAll reports whether this stream keeps every resource of every payload:
// no filter, no sampling. The renderers then skip the per-resource walk and
// the copy it feeds.
func (s *subscriber) keepsAll() bool { return len(s.filters) == 0 && s.sample >= 100 }

// keeps applies the subscriber's filters and sample to one resource.
func (s *subscriber) keeps(attrs pcommon.Map, randFloat func() float64) bool {
	if !matchAll(attrs, s.filters) {
		return false
	}
	return s.sample >= 100 || randFloat()*100 < s.sample
}

// offer renders the payload once per subscriber (their filters differ) and
// delivers without blocking: an over-budget or full channel counts a drop
// instead. The subscriber set is snapshotted and t.mu released BEFORE any
// rendering: a render is a JSON marshal, after a deep copy of the kept
// resources for a filtered or sampled stream (~70 ms for a
// 16 MiB payload), and holding the lock across it would serialise every
// concurrent export in the process — the ingest handlers, the scrape
// goroutines, the tailer's single sweep — behind one debug session's
// renders. A snapshotted subscriber is safe to use unlocked: its fields are
// set once before publication, its counters are atomics, and unsubscribe
// only removes it from the map — the channel is never closed, so a send
// racing an unsubscribe at worst parks the payload in a channel nobody
// reads until the subscriber is collected.
func (t *Tap) offer(sig signal, render func(*subscriber) ([]byte, *renderFailure)) {
	t.mu.Lock()
	subs := make([]*subscriber, 0, len(t.subs))
	for _, sub := range t.subs {
		if sub.signals&sig != 0 {
			subs = append(subs, sub)
		}
	}
	t.mu.Unlock()
	for _, sub := range subs {
		b, failed := render(sub)
		if b == nil {
			// A nil render is either "no resource matched this subscriber's
			// filters" (the ordinary case, silent) or a MARSHAL failure. The
			// second cannot happen for pdata this process built and would be
			// invisible if it did: the stream simply shows nothing, which is
			// what a filter that matches nothing looks like — the exact
			// confusion the query-time glob validation exists to prevent. So
			// it is reported, throttled, naming the signal.
			if failed != nil && marshalWarns.Allow(marshalWarnEvery) {
				slog.Warn("rendering a payload for a /debug/otlp stream failed, so that stream silently "+
					"skips it; the export itself is unaffected", "signal", failed.signal, "error", failed.err)
			}
			continue
		}
		// Reserve the bytes before the send: offers are no longer serialised
		// by t.mu, and load-then-add would let racing exports pass the budget
		// check together and overshoot the cap.
		n := int64(len(b))
		if n > t.queueBudget {
			// Undeliverable at any reading speed: not a slow reader.
			sub.dropOversized()
			continue
		}
		if sub.queued.Add(n) > t.queueBudget {
			sub.queued.Add(-n)
			sub.drop()
			continue
		}
		select {
		case sub.ch <- b:
		default:
			sub.queued.Add(-n)
			sub.drop()
		}
	}
}

func matchAll(attrs pcommon.Map, filters []attrFilter) bool {
	for _, f := range filters {
		if !f.matches(attrs) {
			return false
		}
	}
	return true
}

// subscribe attaches a stream; the returned unsubscribe is idempotent enough
// for a defer. nil means the subscriber cap is reached.
func (t *Tap) subscribe(sig signal, filters []attrFilter, sample float64) (*subscriber, func()) {
	sub := &subscriber{signals: sig, filters: filters, sample: sample, ch: make(chan []byte, 256), since: time.Now()}
	t.mu.Lock()
	if len(t.subs) >= maxSubscribers {
		t.mu.Unlock()
		return nil, nil
	}
	id := t.next
	t.next++
	t.subs[id] = sub
	t.active.Store(int32(len(t.subs)))
	t.mu.Unlock()
	return sub, func() {
		t.mu.Lock()
		delete(t.subs, id)
		t.active.Store(int32(len(t.subs)))
		t.mu.Unlock()
	}
}

// Streams reports how many debug streams are attached. Tests drive the
// subscriber cap through it; it is not a metric, because an attached stream is
// a person and the attach line already names them.
func (t *Tap) Streams() int { return int(t.active.Load()) }

// oldestStream reports how long the longest-held stream has been attached, and
// how many are attached. Zero streams reports a zero age.
func (t *Tap) oldestStream() (age time.Duration, streams int) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, sub := range t.subs {
		if d := now.Sub(sub.since); d > age {
			age = d
		}
	}
	return age, len(t.subs)
}
