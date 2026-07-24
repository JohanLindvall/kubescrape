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
// Cost model: one Starlark invocation per exported BATCH per signal;
// records are exposed as lazy host objects, so a script pays only for the
// fields it touches (~1µs per touched record). The per-line/per-sample hot
// paths never see any of this — pipelines without transforms don't even get
// the wrapper installed.
package transform

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

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
}

// Program is a compiled, immutable set of per-signal transforms. Swapped
// atomically on reload; in-flight batches finish on the program they started
// with.
type Program struct {
	logs    *starlarkProgram
	metrics *starlarkProgram
	traces  *starlarkProgram
	// Hash identifies the compiled config (content hash of the file), exposed
	// on /debug/transforms and as a gauge so per-node convergence after a
	// reload is observable.
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
	return p == nil || (p.logs == nil && p.metrics == nil && p.traces == nil)
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

// Wrapper applies the active program to every batch, then forwards. The
// program pointer is swapped by the reloader; each Export loads it once.
type Wrapper struct {
	next       Exporter
	nextTraces TracesExporter
	program    atomic.Pointer[Program]
}

// Wrap builds a Wrapper forwarding to next (nextTraces may be nil when the
// exporter cannot ship traces).
func Wrap(next Exporter, nextTraces TracesExporter, initial *Program) *Wrapper {
	w := &Wrapper{next: next, nextTraces: nextTraces}
	w.program.Store(initial)
	return w
}

// Swap installs a new program (compile-then-commit: callers only pass
// programs that compiled whole).
func (w *Wrapper) Swap(p *Program) { w.program.Store(p) }

// Active returns the current program (for /debug/transforms).
func (w *Wrapper) Active() *Program { return w.program.Load() }

// ExportLogs transforms then forwards.
func (w *Wrapper) ExportLogs(ctx context.Context, ld plog.Logs) error {
	if p := w.program.Load(); p != nil && p.logs != nil {
		if err := p.logs.runLogs(ld); err != nil {
			return err
		}
		if ld.ResourceLogs().Len() == 0 {
			return nil // everything dropped: acked, nothing to send
		}
	}
	return w.next.ExportLogs(ctx, ld)
}

// ExportMetrics transforms then forwards.
func (w *Wrapper) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	if p := w.program.Load(); p != nil && p.metrics != nil {
		if err := p.metrics.runMetrics(md); err != nil {
			return err
		}
		if md.ResourceMetrics().Len() == 0 {
			return nil
		}
	}
	return w.next.ExportMetrics(ctx, md)
}

// ExportTraces transforms then forwards.
func (w *Wrapper) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if p := w.program.Load(); p != nil && p.traces != nil {
		if err := p.traces.runTraces(td); err != nil {
			return err
		}
		if td.ResourceSpans().Len() == 0 {
			return nil
		}
	}
	if w.nextTraces == nil {
		return fmt.Errorf("trace transform configured but the exporter does not support traces")
	}
	return w.nextTraces.ExportTraces(ctx, td)
}
