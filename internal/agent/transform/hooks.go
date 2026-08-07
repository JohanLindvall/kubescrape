package transform

// The four hook points beyond the per-signal batch transforms, each an
// optional section of the same hot-reloaded transforms file:
//
//	ingest:  | def admit(resource): ...     -> False rejects the resource
//	targets: | def target(t): ...           -> t.drop() / t.path = "/metrics2"
//	sample:  | def decide(trace): ...       -> True/False/None (tail sampling)
//	parse:   | def parse(line): ...         -> {"body": ..., "severity_text": ...}
//
// Each accessor below FAILS OPEN: a script error must degrade to "the hook
// did nothing" — admitting the push, keeping the target, abstaining from the
// decision, leaving the line unparsed — never to data loss, because the hook
// runs on paths where an error cannot be retried into correctness the way a
// batch transform's can. Errors are counted per hook in
// kubescrape_transform_errors_total{signal} and logged throttled.

import (
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.starlark.net/starlark"

	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// hookErr counts and (throttled) logs a hook's script error; every caller
// then takes its fail-open path.
var hookWarnGates struct {
	ingest, targets, sample, parse logdedupe.Throttle
}

func hookErr(gate *logdedupe.Throttle, signal string, err error) {
	obs.TransformErrors.WithLabelValues(signal).Inc()
	if gate.Allow(time.Minute) {
		slog.Warn("transform hook failed (failing open)", "hook", signal, "error", err)
	}
}

// roAttrsView is the read-only attribute view hooks hand to scripts whose
// subject they must not mutate (a sampler's buffered spans are owned by the
// buffer; a target's pod labels are the store's).
type roAttrsView struct{ m pcommon.Map }

func (a roAttrsView) String() string        { return "attributes" }
func (a roAttrsView) Type() string          { return "attributes" }
func (a roAttrsView) Freeze()               {}
func (a roAttrsView) Truth() starlark.Bool  { return a.m.Len() > 0 }
func (a roAttrsView) Hash() (uint32, error) { return 0, fmt.Errorf("attributes are unhashable") }
func (a roAttrsView) Get(k starlark.Value) (starlark.Value, bool, error) {
	return attrsView(a).Get(k)
}
func (a roAttrsView) Has(k starlark.Value) (bool, error) { return attrsView(a).Has(k) }

// --- ingest admission ---

// AdmitResource runs the ingest hook's admit(resource) for one pushed
// resource: false means the receiver removes the resource before enrichment
// (the operator's per-sender policy — the honest mitigation for senders a
// built-in bound can only slow down). No hook, a non-False return, or a
// script error all admit.
func (w *Wrapper) AdmitResource(attrs pcommon.Map) bool {
	p := w.program.Load()
	if p == nil || p.ingest == nil {
		return true
	}
	v, err := p.ingest.call(attrsView{attrs})
	if err != nil {
		hookErr(&hookWarnGates.ingest, "ingest", err)
		return true
	}
	return v != starlark.False
}

// HasAdmit reports whether an ingest admission hook is configured (so the
// receiver can skip the per-resource call entirely).
func (w *Wrapper) HasAdmit() bool {
	p := w.program.Load()
	return p != nil && p.ingest != nil
}

// --- scrape-target hook ---

// TransformTargets runs target(t) over each fetched scrape target: t.drop()
// removes it, t.path is writable (the URL re-renders). Errors keep the
// target untouched.
func (w *Wrapper) TransformTargets(ts []kubemeta.ScrapeTarget) []kubemeta.ScrapeTarget {
	p := w.program.Load()
	if p == nil || p.targets == nil {
		return ts
	}
	out := ts[:0]
	for i := range ts {
		tgt := &targetObj{t: &ts[i]}
		if _, err := p.targets.call(tgt); err != nil {
			hookErr(&hookWarnGates.targets, "targets", err)
			out = append(out, ts[i])
			continue
		}
		if tgt.mutatedPath {
			ts[i].URL = ts[i].Scheme + "://" + ts[i].Address + ts[i].Path
		}
		if !tgt.dropped {
			out = append(out, ts[i])
		}
	}
	return out
}

// targetObj is one scrape target: identity read-only, path writable.
type targetObj struct {
	t           *kubemeta.ScrapeTarget
	dropped     bool
	mutatedPath bool
}

func (o *targetObj) String() string        { return "target" }
func (o *targetObj) Type() string          { return "target" }
func (o *targetObj) Freeze()               {}
func (o *targetObj) Truth() starlark.Bool  { return true }
func (o *targetObj) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

func (o *targetObj) AttrNames() []string {
	return []string{"drop", "labels", "monitor", "namespace", "path", "pod", "source", "url"}
}

func (o *targetObj) Attr(name string) (starlark.Value, error) {
	switch name {
	case "url":
		return starlark.String(o.t.URL), nil
	case "path":
		return starlark.String(o.t.Path), nil
	case "source":
		return starlark.String(o.t.Source), nil
	case "monitor":
		return starlark.String(o.t.Monitor), nil
	case "namespace":
		return starlark.String(o.t.Pod.Namespace), nil
	case "pod":
		return starlark.String(o.t.Pod.Name), nil
	case "labels":
		return stringMapView{m: o.t.Pod.Labels}, nil
	case "drop":
		return dropFn{mark: func() { o.dropped = true }}, nil
	}
	return nil, nil
}

func (o *targetObj) SetField(name string, v starlark.Value) error {
	if name != "path" {
		return fmt.Errorf("cannot set %s", name)
	}
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("path must be a string")
	}
	if s == "" || s[0] != '/' {
		s = "/" + s
	}
	o.t.Path = s
	o.mutatedPath = true
	return nil
}

// stringMapView is a read-only dict view over a Go string map (pod labels).
type stringMapView struct{ m map[string]string }

func (s stringMapView) String() string        { return "labels" }
func (s stringMapView) Type() string          { return "labels" }
func (s stringMapView) Freeze()               {}
func (s stringMapView) Truth() starlark.Bool  { return len(s.m) > 0 }
func (s stringMapView) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }
func (s stringMapView) Get(k starlark.Value) (starlark.Value, bool, error) {
	key, ok := starlark.AsString(k)
	if !ok {
		return nil, false, fmt.Errorf("label key must be a string")
	}
	v, found := s.m[key]
	if !found {
		return starlark.None, true, nil
	}
	return starlark.String(v), true, nil
}
func (s stringMapView) Has(k starlark.Value) (bool, error) {
	key, ok := starlark.AsString(k)
	if !ok {
		return false, fmt.Errorf("label key must be a string")
	}
	_, found := s.m[key]
	return found, nil
}

// --- tail-sampling script policy ---

// SampleDecider adapts the sample hook to tailsample's injected script
// policy: decide(trace) -> True samples, False drops, None (or an error)
// abstains to the next policy. nil when the section is absent — the policy
// compiler then refuses `type: script`, at config time.
func (w *Wrapper) SampleDecider() func(tailsample.Trace) (sample, abstain bool) {
	if p := w.program.Load(); p == nil || p.sample == nil {
		return nil
	}
	return func(t tailsample.Trace) (bool, bool) {
		p := w.program.Load() // hot reload: resolve per decision
		if p == nil || p.sample == nil {
			return false, true
		}
		v, err := p.sample.call(&traceView{t: t})
		if err != nil {
			hookErr(&hookWarnGates.sample, "sample", err)
			return false, true
		}
		switch v {
		case starlark.True:
			return true, false
		case starlark.False:
			return false, false
		default:
			return false, true
		}
	}
}

// traceView is a whole buffered trace, read-only: the buffer still owns the
// spans, and the evaluator's contract is that policies never mutate them.
type traceView struct{ t tailsample.Trace }

func (v *traceView) String() string        { return "trace" }
func (v *traceView) Type() string          { return "trace" }
func (v *traceView) Freeze()               {}
func (v *traceView) Truth() starlark.Bool  { return true }
func (v *traceView) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

func (v *traceView) AttrNames() []string { return []string{"span_count", "spans", "trace_id"} }

func (v *traceView) Attr(name string) (starlark.Value, error) {
	switch name {
	case "trace_id":
		return hexID(v.t.TraceID.String(), v.t.TraceID.IsEmpty()), nil
	case "span_count":
		return starlark.MakeInt(len(v.t.Spans)), nil
	case "spans":
		return &sampleSpans{spans: v.t.Spans}, nil
	}
	return nil, nil
}

type sampleSpans struct{ spans []tailsample.Span }

func (s *sampleSpans) String() string        { return "spans" }
func (s *sampleSpans) Type() string          { return "spans" }
func (s *sampleSpans) Freeze()               {}
func (s *sampleSpans) Truth() starlark.Bool  { return len(s.spans) > 0 }
func (s *sampleSpans) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }
func (s *sampleSpans) Len() int              { return len(s.spans) }
func (s *sampleSpans) Iterate() starlark.Iterator {
	return &sampleSpanIter{spans: s.spans}
}

type sampleSpanIter struct {
	spans []tailsample.Span
	i     int
}

func (it *sampleSpanIter) Next(v *starlark.Value) bool {
	if it.i >= len(it.spans) {
		return false
	}
	*v = &sampleSpanObj{s: it.spans[it.i]}
	it.i++
	return true
}
func (it *sampleSpanIter) Done() {}

// sampleSpanObj is one buffered span, READ-ONLY (unlike the traces batch
// transform's spanObj): drop/route/mutation make no sense on a whole-trace
// decision, and the buffer still owns the pdata.
type sampleSpanObj struct{ s tailsample.Span }

func (o *sampleSpanObj) String() string        { return "span" }
func (o *sampleSpanObj) Type() string          { return "span" }
func (o *sampleSpanObj) Freeze()               {}
func (o *sampleSpanObj) Truth() starlark.Bool  { return true }
func (o *sampleSpanObj) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

func (o *sampleSpanObj) AttrNames() []string {
	return []string{"attributes", "duration_ms", "kind", "name", "resource", "status_code"}
}

func (o *sampleSpanObj) Attr(name string) (starlark.Value, error) {
	switch name {
	case "name":
		return starlark.String(o.s.Span.Name()), nil
	case "kind":
		return starlark.String(spanKind(o.s.Span.Kind())), nil
	case "status_code":
		return starlark.MakeInt(int(o.s.Span.Status().Code())), nil
	case "duration_ms":
		d := int64(o.s.Span.EndTimestamp()) - int64(o.s.Span.StartTimestamp())
		return starlark.Float(float64(d) / 1e6), nil
	case "attributes":
		return roAttrsView{o.s.Span.Attributes()}, nil
	case "resource":
		return roAttrsView{o.s.Resource}, nil
	}
	return nil, nil
}

// --- plain-source parse hook ---

// Parsed is what parse(line) produced for one plain-file line.
type Parsed struct {
	Body         string
	HasBody      bool
	SeverityText string
	TimeUnixNano int64
}

// ParseLine runs the parse hook on one plain-source line. ok=false — no
// hook, a None return, or a script error — leaves the line untouched.
func (w *Wrapper) ParseLine(line string) (Parsed, bool) {
	p := w.program.Load()
	if p == nil || p.parse == nil {
		return Parsed{}, false
	}
	v, err := p.parse.call(starlark.String(line))
	if err != nil {
		hookErr(&hookWarnGates.parse, "parse", err)
		return Parsed{}, false
	}
	d, ok := v.(*starlark.Dict)
	if !ok {
		return Parsed{}, false // None (or anything else): unparsed
	}
	var out Parsed
	if bv, found, _ := d.Get(starlark.String("body")); found {
		if s, ok := starlark.AsString(bv); ok {
			out.Body, out.HasBody = s, true
		}
	}
	if sv, found, _ := d.Get(starlark.String("severity_text")); found {
		out.SeverityText, _ = starlark.AsString(sv)
	}
	if tv, found, _ := d.Get(starlark.String("time_unix_nano")); found {
		if n, ok := tv.(starlark.Int); ok {
			out.TimeUnixNano, _ = n.Int64()
		}
	}
	return out, true
}

// HasParse reports whether a parse hook is configured.
func (w *Wrapper) HasParse() bool {
	p := w.program.Load()
	return p != nil && p.parse != nil
}
