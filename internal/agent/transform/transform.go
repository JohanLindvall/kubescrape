// Package transform runs user-defined transformations over OTLP payloads at
// the exporter seam: every batch a pipeline exports passes through the active
// program before buffering, so spooled bytes are final and replays are
// deterministic across reloads.
//
// Programs are Starlark (pure-Go, hermetic by construction — no I/O, no
// imports; see engine.go). The transforms file is SEPARATE from the agent
// config and hot-reloads: edits compile-then-commit atomically (a broken
// edit keeps the last good program running, counted and warned), so
// transformation logic changes without a pod restart.
//
// Cost model: one Starlark invocation per exported BATCH per signal, PLUS —
// for a payload NOT marked with Handoff — one deep copy of that batch, and
// the copy, not the script, is what the copying path costs. Measured on a
// 1024-record log batch with a `def transform(batch): return` script: ~249µs
// total, of which ld.CopyTo is ~226µs (91%) and the whole Starlark call is
// 245ns; a 10,000-point metrics payload (one promscrape chunk) is ~2.05ms and
// 2.3 MB for the same no-op. A payload marked Handoff(ctx) — the producer's
// promise that it rebuilds from source on failure rather than re-offering the
// object (see handoff.go for the roster) — runs the script IN PLACE and pays
// only the invocation; the copy remains the CORRECTNESS requirement for every
// unmarked producer, which re-exports the SAME object on retry (see the
// comment above ExportLogs). The tailer takes a third shape: it transforms
// its just-built batch once via TransformLogs and retries through Inner(),
// so its retry loop neither re-copies nor re-runs the script.
//
// Within a batch, records ARE lazy: the iterators walk positions rather than
// materializing host objects, and each object resolves only the fields the
// script touches (~1µs per touched record), so a script that breaks out early
// pays only for what it visited. The per-line/per-sample hot paths never see
// any of this — pipelines without transforms don't even get the wrapper
// installed.
package transform

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"sigs.k8s.io/yaml"
)

// Config is the transforms file: one optional program per signal.
type Config struct {
	// Logs/Metrics/Traces each hold one Starlark script defining
	// transform(batch). Empty = passthrough for that signal.
	Logs    string `json:"logs,omitempty"`
	Metrics string `json:"metrics,omitempty"`
	Traces  string `json:"traces,omitempty"`

	// The hook sections (hooks.go), each optional and each defining its own
	// function; all fail open on script errors:
	//
	// Ingest defines admit(resource) — per pushed resource, before
	// enrichment; False removes it (the operator's per-sender policy).
	Ingest string `json:"ingest,omitempty"`
	// Targets defines target(t) — per fetched scrape target; t.drop()
	// removes it, t.path is writable.
	Targets string `json:"targets,omitempty"`
	// Sample defines decide(trace) — the tail-sampling `script` policy body
	// (True samples, False drops, None abstains).
	Sample string `json:"sample,omitempty"`
	// Parse defines parse(line) — plain log sources flagged parseScript run
	// it per line; a dict return may set body/severity_text/time_unix_nano.
	Parse string `json:"parse,omitempty"`
}

// Program is a compiled, immutable set of per-signal transforms. Swapped
// atomically on reload; in-flight batches finish on the program they started
// with.
type Program struct {
	logs    *starlarkProgram
	metrics *starlarkProgram
	traces  *starlarkProgram
	ingest  *starlarkProgram
	targets *starlarkProgram
	sample  *starlarkProgram
	parse   *starlarkProgram
	// Hash identifies the compiled config (content hash of the file), served on
	// /debug/transforms so per-node convergence after a reload is observable.
	Hash string
}

// Compile parses and compiles the whole config; any error rejects the WHOLE
// config (never "half the signals applied").
func Compile(raw []byte) (*Program, error) {
	var cfg Config
	if err := yaml.UnmarshalStrict(raw, &cfg); err != nil {
		return nil, fmt.Errorf("transforms file: %w", err)
	}
	p := &Program{Hash: contentHash(raw)}
	var err error
	if cfg.Logs != "" {
		if p.logs, err = compileStarlark("logs", cfg.Logs); err != nil {
			return nil, err
		}
	}
	if cfg.Metrics != "" {
		if p.metrics, err = compileStarlark("metrics", cfg.Metrics); err != nil {
			return nil, err
		}
	}
	if cfg.Traces != "" {
		if p.traces, err = compileStarlark("traces", cfg.Traces); err != nil {
			return nil, err
		}
	}
	for _, h := range []struct {
		src, signal, fn string
		dst             **starlarkProgram
	}{
		{cfg.Ingest, "ingest", "admit", &p.ingest},
		{cfg.Targets, "targets", "target", &p.targets},
		{cfg.Sample, "sample", "decide", &p.sample},
		{cfg.Parse, "parse", "parse", &p.parse},
	} {
		if h.src == "" {
			continue
		}
		if *h.dst, err = compileStarlarkFn(h.signal, h.src, h.fn); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// CompileFile reads and compiles path.
func CompileFile(path string) (*Program, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Compile(data)
}

// Empty reports a program with no transforms at all.
func (p *Program) Empty() bool {
	return p == nil || (p.logs == nil && p.metrics == nil && p.traces == nil &&
		p.ingest == nil && p.targets == nil && p.sample == nil && p.parse == nil)
}

// Exporter is the downstream the wrapper forwards to (otlpexport.Client and
// Buffered both satisfy it; traces are optional at wrap time).
type Exporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// TracesExporter is the optional traces downstream.
type TracesExporter interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
}

// MetricEmitter is the emit_metric bridge target: the logMetrics set's
// EmitDirect. An interface so transform needs no metrics import in its API.
type MetricEmitter interface {
	EmitDirect(name string, value float64, labels map[string]string, resource pcommon.Map) error
}

// Wrapper applies the active program to every batch, then forwards. The
// program pointer is swapped by the reloader; each Export loads it once.
type Wrapper struct {
	next       Exporter
	nextTraces TracesExporter
	// emitter is the emit_metric target (unset = the builtin errors, naming
	// the missing logMetrics section). A shared POINTER for the same reason
	// program is one: Fork used to copy the interface VALUE, and main builds
	// the self-chain fork before it wires the emitter, so the fork's stayed
	// nil forever and a metrics script's emit_metric failed the self chain's
	// every export. Atomic because SetMetricEmitter may run after forks exist
	// (and, in principle, after traffic flows); the holder indirection is
	// because an interface cannot ride an atomic.Pointer directly.
	emitter *atomic.Pointer[emitterHolder]
	// program is a POINTER so forks can share one reloaded program (see Fork):
	// a second wrapper over a different downstream must not need a second
	// reloader, or a broken edit could leave the two chains on different
	// programs.
	program *atomic.Pointer[Program]
}

// emitterHolder boxes the MetricEmitter interface so the wrapper (and every
// fork aliasing the same pointer) can load it atomically.
type emitterHolder struct{ m MetricEmitter }

// Wrap builds a Wrapper forwarding to next (nextTraces may be nil when the
// exporter cannot ship traces).
func Wrap(next Exporter, nextTraces TracesExporter, initial *Program) *Wrapper {
	w := &Wrapper{
		next:       next,
		nextTraces: nextTraces,
		program:    &atomic.Pointer[Program]{},
		emitter:    &atomic.Pointer[emitterHolder]{},
	}
	w.program.Store(initial)
	return w
}

// Fork returns a wrapper over a DIFFERENT downstream that shares this one's
// program AND its emit_metric target: one reloader keeps both current, and a
// SetMetricEmitter on either reaches both — they can never diverge. The agent
// uses it for the chain carrying its own metrics, which skips the namespace
// router but must still see the operator's transforms.
func (w *Wrapper) Fork(next Exporter, nextTraces TracesExporter) *Wrapper {
	return &Wrapper{next: next, nextTraces: nextTraces, program: w.program, emitter: w.emitter}
}

// SetMetricEmitter wires the emit_metric bridge. It may be called after Fork —
// forks alias the same slot — and the caller keeps the typed-value guard: a
// nil *DynamicMetricSet boxed into the interface would defeat the builtin's
// own nil check, so main only calls this with a non-nil concrete value.
func (w *Wrapper) SetMetricEmitter(m MetricEmitter) { w.emitter.Store(&emitterHolder{m: m}) }

// metricEmitter loads the wired emit_metric target; nil until SetMetricEmitter
// has run (the builtin then errors, naming the missing logMetrics section).
func (w *Wrapper) metricEmitter() MetricEmitter {
	if h := w.emitter.Load(); h != nil {
		return h.m
	}
	return nil
}

// TransformLogs runs the active logs program on ld IN PLACE (drop marks
// swept, empty groups pruned) and reports the script's error; no program means
// no-op. It is the tailer's seam: the tailer's retry loop re-sends the SAME
// batch, so it can neither mark Handoff (the contract forbids the re-send)
// nor export through the wrapper (every attempt would re-copy and re-run the
// script) — instead it transforms the batch it just built exactly once, here,
// and retries through Inner(). The caller decides what an emptied payload
// means (the tailer commits its offsets without a send) and treats an error
// like a failed export, so a re-read re-runs the — possibly hot-reloaded —
// program.
func (w *Wrapper) TransformLogs(ld plog.Logs) error {
	p := w.program.Load()
	if p == nil || p.logs == nil {
		return nil
	}
	// Counted at transform time: this seam runs ONCE per built batch (the
	// tailer's retry loop re-sends the same already-transformed object), so
	// there is no per-attempt re-run to over-count. A sweep-level rewind
	// rebuilds the batch from source, which is a fresh transform by design.
	dropped, err := p.logs.runLogs(ld, w.metricEmitter())
	if err != nil {
		return err
	}
	p.logs.countDropped(dropped)
	return nil
}

// Inner is the exporter below the transform layer, for a producer that
// applies the transform itself (TransformLogs) and must not pay it again on
// the send path.
func (w *Wrapper) Inner() Exporter { return w.next }

// Swap installs a new program (compile-then-commit: callers only pass
// programs that compiled whole).
func (w *Wrapper) Swap(p *Program) { w.program.Store(p) }

// Active returns the current program (for /debug/transforms).
func (w *Wrapper) Active() *Program { return w.program.Load() }

// An UNMARKED payload is transformed on a COPY, never the caller's object: the
// scripts mutate pdata in place (lazy host objects alias it), while unmarked
// producers retry the SAME object on a transient failure (logchain.Pending,
// tailbuffer) and the spanmetrics tap Consumes it after forwarding. Mutating
// those in place would double-apply a non-idempotent script on retry and feed
// the tap the post-transform payload. A payload marked Handoff(ctx) — whose
// producer rebuilds from source on failure and never re-reads the object
// (handoff.go) — is transformed IN PLACE: a script error there may leave it
// half-transformed, which the marker's contract makes unobservable. The
// no-script fast path forwards the original uncopied (matching route and
// tracesample, which were hardened the same way).

// ExportLogs transforms then forwards.
func (w *Wrapper) ExportLogs(ctx context.Context, ld plog.Logs) error {
	if p := w.program.Load(); p != nil && p.logs != nil {
		out := ld
		if !HandedOff(ctx) {
			out = plog.NewLogs()
			ld.CopyTo(out)
		}
		dropped, err := p.logs.runLogs(out, w.metricEmitter())
		if err != nil {
			return err
		}
		if out.ResourceLogs().Len() == 0 {
			p.logs.countDropped(dropped)
			return nil // everything dropped: acked, nothing to send
		}
		if err := w.next.ExportLogs(ctx, out); err != nil {
			// Not counted: the producer re-offers the batch and the retry
			// re-runs the script — see run*'s doc.
			return err
		}
		p.logs.countDropped(dropped)
		return nil
	}
	return w.next.ExportLogs(ctx, ld)
}

// ExportMetrics transforms then forwards.
func (w *Wrapper) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	if p := w.program.Load(); p != nil && p.metrics != nil {
		out := md
		if !HandedOff(ctx) {
			out = pmetric.NewMetrics()
			md.CopyTo(out)
		}
		dropped, err := p.metrics.runMetrics(out, w.metricEmitter())
		if err != nil {
			return err
		}
		if out.ResourceMetrics().Len() == 0 {
			p.metrics.countDropped(dropped)
			return nil
		}
		if err := w.next.ExportMetrics(ctx, out); err != nil {
			return err
		}
		p.metrics.countDropped(dropped)
		return nil
	}
	return w.next.ExportMetrics(ctx, md)
}

// ExportTraces transforms then forwards.
func (w *Wrapper) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if p := w.program.Load(); p != nil && p.traces != nil {
		if w.nextTraces == nil {
			return errors.New("trace transform configured but the exporter does not support traces")
		}
		out := td
		if !HandedOff(ctx) {
			out = ptrace.NewTraces()
			td.CopyTo(out)
		}
		dropped, err := p.traces.runTraces(out, w.metricEmitter())
		if err != nil {
			return err
		}
		if out.ResourceSpans().Len() == 0 {
			p.traces.countDropped(dropped)
			return nil
		}
		if err := w.nextTraces.ExportTraces(ctx, out); err != nil {
			return err
		}
		p.traces.countDropped(dropped)
		return nil
	}
	// No traces script: pass through. Require traces capability only when a
	// script actually exists, so a logs-only transforms file never forces the
	// trace path to need a traces-capable downstream.
	if w.nextTraces == nil {
		return errors.New("exporter does not support traces")
	}
	return w.nextTraces.ExportTraces(ctx, td)
}

// HasSample reports whether the program carries a sample: section (the
// `type: script` tail-sampling body); config validation cross-checks it.
func (p *Program) HasSample() bool { return p != nil && p.sample != nil }
