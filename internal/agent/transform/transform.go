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
// Cost model: one Starlark invocation per exported BATCH per signal, PLUS a
// post-script prune sweep, PLUS — for a payload NOT marked with Handoff — one
// deep copy of that batch, and the copy, not the script, is what the copying
// path costs. Measured on a 1024-record log batch with a
// `def transform(batch): return` script: ~249µs total, of which ld.CopyTo is
// ~226µs (91%) and the whole Starlark call is 245ns; a 10,000-point metrics
// payload (one promscrape chunk) is ~2.05ms and 2.3 MB for the same no-op. A
// payload marked Handoff(ctx) — the producer's promise that it rebuilds from
// source on failure rather than re-offering the object (see handoff.go for the
// roster) — runs the script IN PLACE and skips the copy, but NOT the prune:
// that sweep visits every record, span or data point and looks the drop
// marker up in its attributes whether or not the script called drop(), so it
// costs O(elements x attributes) and, on the in-place path, dwarfs the
// invocation (a 10k-point, 8-attribute chunk: 0.2-0.38 ms of prune against
// ~0.4µs of call, some 5-8% of that chunk's proto marshal). It cannot be
// skipped when no drop() ran: the prune is PRESENCE-based, and a marker that
// reached the payload through a door other than drop() (a logAttributes lift,
// for one) must be pruned all the same — gating the sweep would make that
// record's fate depend on whether another record in the batch was dropped.
// The copy remains the CORRECTNESS requirement for every
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
	// One row per section: the batch transforms all define transform(batch),
	// the hooks each their own function (hooks.go).
	for _, sec := range []struct {
		src, signal, fn string
		dst             **starlarkProgram
	}{
		{cfg.Logs, "logs", "transform", &p.logs},
		{cfg.Metrics, "metrics", "transform", &p.metrics},
		{cfg.Traces, "traces", "transform", &p.traces},
		{cfg.Ingest, "ingest", "admit", &p.ingest},
		{cfg.Targets, "targets", "target", &p.targets},
		{cfg.Sample, "sample", "decide", &p.sample},
		{cfg.Parse, "parse", "parse", &p.parse},
	} {
		if sec.src == "" {
			continue
		}
		var err error
		if *sec.dst, err = compileStarlarkFn(sec.signal, sec.src, sec.fn); err != nil {
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
// swept, empty groups pruned) and reports how many records the script dropped,
// or its error; no program means no-op. It is the tailer's seam: the tailer's
// retry loop re-sends the SAME batch, so it can neither mark Handoff (the
// contract forbids the re-send) nor export through the wrapper (every attempt
// would re-copy and re-run the script) — instead it transforms the batch it
// just built exactly once, here, and retries through Inner(). The caller
// decides what an emptied payload means (the tailer commits its offsets
// without a send) and treats an error like a failed export, so a re-read
// re-runs the — possibly hot-reloaded — program.
//
// The drops are REPORTED, not counted: the caller counts them into
// kubescrape_transform_dropped_total{signal="logs"} once the batch's records
// are settled (its offsets commit, or it is dropped as permanently rejected).
// Counting here counted every rewind: a failed flush rewinds the files, the
// next sweep rebuilds the batch from the same bytes and the script drops the
// same records again, so one intended drop read as one per failed flush of
// the outage.
func (w *Wrapper) TransformLogs(ld plog.Logs) (dropped int, err error) {
	p := w.program.Load()
	if p == nil || p.logs == nil {
		return 0, nil
	}
	return p.logs.runLogs(ld, w.metricEmitter())
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

// settle counts what the script dropped from a batch once the batch's records
// are SETTLED, and passes the forward's err through. err == nil covers both
// acks: a delivered forward, and a payload the script emptied, which is acked
// without a send.
//
// A FAILED forward counts only when the failure is final for these RECORDS
// (Consumed, handoff.go). A copy-path producer re-offers the same object and
// the retry re-runs the script over a fresh copy; an ingest sender retransmits
// the same records, which a plain Handoff still describes; the log-metrics set
// re-renders a failed chunk's samples as the same points. Counting any of
// those here multiplied one batch's drops by the length of an outage (see
// run*'s doc). A CONSUMED payload's records never come back — promscrape
// take()s its chunk and latches exportFailed, cgroupstats renders from a
// snapshot() that reset each window, and the cumulative renders' next export
// is a new point — so not counting them here counted them nowhere,
// under-reporting drop volume during exactly the collector outage in which an
// operator reads this counter to tell an intentional script drop from a
// delivery failure.
func (p *starlarkProgram) settle(ctx context.Context, dropped int, err error) error {
	if err == nil || IsConsumed(ctx) {
		p.countDropped(dropped)
	}
	return err
}

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
			return p.logs.settle(ctx, dropped, nil) // everything dropped: acked, nothing to send
		}
		return p.logs.settle(ctx, dropped, w.next.ExportLogs(ctx, out))
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
			return p.metrics.settle(ctx, dropped, nil)
		}
		return p.metrics.settle(ctx, dropped, w.next.ExportMetrics(ctx, out))
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
			return p.traces.settle(ctx, dropped, nil)
		}
		return p.traces.settle(ctx, dropped, w.nextTraces.ExportTraces(ctx, out))
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

// HasParse reports whether the program carries a parse: section (the hook a
// plain log source opts into with parseScript); config validation reports a
// source that opts in with no hook to run.
func (p *Program) HasParse() bool { return p != nil && p.parse != nil }
