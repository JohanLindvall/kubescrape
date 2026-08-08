// Package debugtap is the agent's on-demand equivalent of a collector debug
// exporter: GET /debug/otlp streams a text (OTLP JSON Lines) representation
// of the payloads flowing through the export chain to any attached HTTP
// client, filtered by resource attributes (path.Match globs on key and
// value, with '/' an ordinary character — see globMatch) and downsampled by a
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
	"math/rand/v2"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
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
// of its attributes has a key matching Key and a value (rendered as string)
// matching Value — both path.Match globs, with '/' an ordinary character (see
// globMatch). A subscriber's filters are ANDed.
type attrFilter struct {
	Key   string
	Value string
}

func (f attrFilter) matches(attrs pcommon.Map) bool {
	found := false
	attrs.Range(func(k string, v pcommon.Value) bool {
		if globMatch(f.Key, k) {
			if globMatch(f.Value, v.AsString()) {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

// globMatch is path.Match with the SEPARATOR NEUTRALIZED: neither half of an
// attribute filter is a path, but path.Match's `*` and `?` stop at '/', so
// `container.image.name=*nginx*` could never match
// "docker.io/library/nginx:1.25" and `k8s.pod.label.*` could never match the
// `app.kubernetes.io/name` key attrs stamps for the commonest Kubernetes label
// form. The result is an empty stream indistinguishable from "this agent is
// exporting nothing" — the very outcome the pattern validation at the query
// seam exists to prevent. NUL is an ordinary byte to path.Match and occurs in
// neither operand, so classes, escapes and config.Glob's validity probe are
// unaffected; strings.ReplaceAll returns its input unchanged when there is no
// '/' to replace.
func globMatch(pat, s string) bool {
	ok, _ := path.Match(strings.ReplaceAll(pat, "/", "\x00"), strings.ReplaceAll(s, "/", "\x00"))
	return ok
}

// maxSubscribers bounds concurrent debug streams. The port is
// unauthenticated and every subscriber costs a render per export ON THE
// EXPORTING GOROUTINE (~70 ms for a 16 MiB payload), which for the tailer is
// the single sweep goroutine serving every log file on the node — so N idle
// curls must not be able to multiply that unboundedly. Over the cap the
// stream is refused with 503; four concurrent debugging humans is already a
// crowded incident call.
const maxSubscribers = 4

// maxQueuedBytes bounds what ONE subscriber's channel may hold. The channel's
// slot count (256) bounds nothing by itself — 256 slots of 16 MiB renders is
// 4 GB pinned by a reader that connected and stopped reading — so the queue
// is bounded by BYTES and an over-budget payload is dropped for that stream
// (counted, reported on the stream) exactly like a full channel.
const maxQueuedBytes = 32 << 20

// subscriber is one attached debug stream.
type subscriber struct {
	signals signal
	filters []attrFilter
	sample  float64 // percent of matching RESOURCES kept, 0-100
	ch      chan []byte
	dropped atomic.Int64
	// queued tracks the bytes sitting in ch: charged on send, released by the
	// reader after receive.
	queued atomic.Int64
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
}

// New wraps inner. Trace capability follows inner's, exactly as the
// transform wrapper's does.
func New(inner otlpexport.Exporter) *Tap {
	traces, _ := inner.(otlpexport.TracesExporter)
	return &Tap{inner: inner, traces: traces, subs: map[int]*subscriber{}, randFloat: rand.Float64}
}

var logsMarshaler = &plog.JSONMarshaler{}
var metricsMarshaler = &pmetric.JSONMarshaler{}
var tracesMarshaler = &ptrace.JSONMarshaler{}

func (t *Tap) ExportLogs(ctx context.Context, ld plog.Logs) error {
	if t.active.Load() > 0 {
		t.offer(sigLogs, func(sub *subscriber) ([]byte, bool) { return t.renderLogs(ld, sub) })
	}
	return t.inner.ExportLogs(ctx, ld)
}

func (t *Tap) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	if t.active.Load() > 0 {
		t.offer(sigMetrics, func(sub *subscriber) ([]byte, bool) { return t.renderMetrics(md, sub) })
	}
	return t.inner.ExportMetrics(ctx, md)
}

func (t *Tap) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if t.active.Load() > 0 {
		t.offer(sigTraces, func(sub *subscriber) ([]byte, bool) { return t.renderTraces(td, sub) })
	}
	if t.traces == nil {
		return errors.New("the export chain has no trace capability")
	}
	return t.traces.ExportTraces(ctx, td)
}

// The three renderers are the same shape spelled thrice — pdata's generated
// slices share methods but no interface, and three plain loops read better
// than the generics needed to unify them.

func (t *Tap) renderLogs(ld plog.Logs, sub *subscriber) ([]byte, bool) {
	out := plog.NewLogs()
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		if sub.keeps(rl.Resource().Attributes(), t.randFloat) {
			rl.CopyTo(out.ResourceLogs().AppendEmpty())
		}
	}
	if out.ResourceLogs().Len() == 0 {
		return nil, false
	}
	b, err := logsMarshaler.MarshalLogs(out)
	return b, err == nil
}

func (t *Tap) renderMetrics(md pmetric.Metrics, sub *subscriber) ([]byte, bool) {
	out := pmetric.NewMetrics()
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		if sub.keeps(rm.Resource().Attributes(), t.randFloat) {
			rm.CopyTo(out.ResourceMetrics().AppendEmpty())
		}
	}
	if out.ResourceMetrics().Len() == 0 {
		return nil, false
	}
	b, err := metricsMarshaler.MarshalMetrics(out)
	return b, err == nil
}

func (t *Tap) renderTraces(td ptrace.Traces, sub *subscriber) ([]byte, bool) {
	out := ptrace.NewTraces()
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		if sub.keeps(rs.Resource().Attributes(), t.randFloat) {
			rs.CopyTo(out.ResourceSpans().AppendEmpty())
		}
	}
	if out.ResourceSpans().Len() == 0 {
		return nil, false
	}
	b, err := tracesMarshaler.MarshalTraces(out)
	return b, err == nil
}

// keeps applies the subscriber's filters and sample to one resource.
func (s *subscriber) keeps(attrs pcommon.Map, randFloat func() float64) bool {
	if !matchAll(attrs, s.filters) {
		return false
	}
	return s.sample >= 100 || randFloat()*100 < s.sample
}

// offer renders the payload once per subscriber (their filters differ) and
// delivers without blocking: a full channel counts a drop instead.
func (t *Tap) offer(sig signal, render func(*subscriber) ([]byte, bool)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, sub := range t.subs {
		if sub.signals&sig == 0 {
			continue
		}
		b, ok := render(sub)
		if !ok {
			continue
		}
		if sub.queued.Load()+int64(len(b)) > maxQueuedBytes {
			sub.dropped.Add(1)
			continue
		}
		select {
		case sub.ch <- b:
			sub.queued.Add(int64(len(b)))
		default:
			sub.dropped.Add(1)
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
	sub := &subscriber{signals: sig, filters: filters, sample: sample, ch: make(chan []byte, 256)}
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
