package main

// -test-config: offline unit tests for the log pipeline configuration. An
// operator editing a drop rule or scrub regex for a whole fleet gets CI proof
// of what it does to sample lines — today the first evidence of a too-greedy
// regex is missing production logs. Cases run through the same compiled
// chains a real start builds, in the same order the tailer applies them
// (scrub → logAttributes → enrich → logMetrics → rules → transforms), with
// nothing acquired: no listeners, no files but the configs, no network.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailer"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// configTests is the -test-config file: named cases of one input line each.
type configTests struct {
	Tests []configTestCase `json:"tests"`
}

type configTestCase struct {
	Name string `json:"name"`
	// Line is the raw log line fed through the pipeline.
	Line string `json:"line"`
	// Resource are resource attributes visible to selectors/labels (the pod's
	// k8s metadata in production; whatever the case needs here).
	Resource map[string]string `json:"resource,omitempty"`
	Expect   configExpect      `json:"expect"`
}

type configExpect struct {
	// Kept asserts whether the record survives logs.rules AND the logs
	// transform (nil = must be kept).
	Kept *bool `json:"kept,omitempty"`
	// Body asserts the exact post-scrub body.
	Body string `json:"body,omitempty"`
	// Severity asserts the enriched severity text, case-insensitively.
	Severity string `json:"severity,omitempty"`
	// Attributes assert a SUBSET of the record's attributes (enriched +
	// logAttributes-lifted), compared as strings.
	Attributes map[string]string `json:"attributes,omitempty"`
	// Metrics assert that each named logMetrics rule observed the line (a
	// subset — unlisted metrics are not constrained).
	Metrics []string `json:"metrics,omitempty"`
}

// runConfigTests loads and executes the cases, reporting each failure; a
// non-nil error means at least one case failed (the process exits non-zero).
func runConfigTests(cfg agentConfig, transformsFile, testsFile string, log *slog.Logger) error {
	raw, err := os.ReadFile(testsFile)
	if err != nil {
		return err
	}
	var tests configTests
	if err := yaml.UnmarshalStrict(raw, &tests); err != nil {
		return fmt.Errorf("%s: %w", testsFile, err)
	}
	if len(tests.Tests) == 0 {
		return fmt.Errorf("%s: no tests", testsFile)
	}

	// The same compile helpers a real start (and validateConfig) uses, so the
	// harness proves the chains production builds, not variants of them.
	scrubber, err := compileScrub(cfg.LogScrubbing)
	if err != nil {
		return err
	}
	extractor, err := compileLogAttrs(cfg.LogAttributes)
	if err != nil {
		return err
	}
	rules, err := compileLogRules(cfg.Logs)
	if err != nil {
		return err
	}
	transforms, err := compileTransforms(transformsFile)
	if err != nil {
		return err
	}

	failed := 0
	for i, tc := range tests.Tests {
		name := tc.Name
		if name == "" {
			name = fmt.Sprintf("case %d", i)
		}
		problems := runConfigCase(cfg, scrubber, extractor, rules, transforms, tc)
		if len(problems) == 0 {
			log.Info("test-config PASS", "case", name)
			continue
		}
		failed++
		for _, p := range problems {
			log.Error("test-config FAIL", "case", name, "reason", p)
		}
	}
	if failed > 0 {
		return fmt.Errorf("test-config: %d of %d cases failed", failed, len(tests.Tests))
	}
	log.Info("test-config: all cases passed", "cases", len(tests.Tests))
	return nil
}

// captureProducer is the harness's half of the chain (logchain.Producer): a
// kept record lands in dest, and Stamp writes the one thing the harness knows
// about the line — its body. Production stamps timestamps and per-entry
// stream facts here; a test case has none (see the not-modelled note in
// runConfigCase).
type captureProducer struct {
	dest plog.LogRecordSlice
	body string
}

func (c *captureProducer) Dest() plog.LogRecordSlice { return c.dest }
func (c *captureProducer) Stamp(lr plog.LogRecord)   { lr.Body().SetStr(c.body) }

// runConfigCase pushes one line through the SAME per-record chain every log
// producer runs (logchain.Chain: scrub → lift line attributes → stamp →
// enrich → logMetrics → rules) and returns the assertion failures. The chain
// used to be re-implemented here step by step, which could only prove what the
// re-implementation does; now the harness drives Chain.Line + Chain.Emit and
// keeps only the producer half every pipeline keeps (the resource, and where
// records land).
//
// The chain runs twice: once WITHOUT the rules, so the record is still there
// to assert body/severity/attributes/metrics on even when the rules drop it
// (the assertions are about the line, not the verdict), and once WITH them for
// the verdict — same Config otherwise, so the second pass is the production
// evaluation (rules after enrichment, __severity__ from the enriched severity,
// lifted resource attributes ranked between record and resource). Transforms
// stay in the harness AFTER the chain: they are an exporter-seam feature, not
// part of the per-record chain.
func runConfigCase(cfg agentConfig, scrubber *logscrub.Scrubber, extractor *logattrs.Extractor,
	rules *logline.LineFilter, transforms *transform.Program, tc configTestCase) []string {
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	// Metrics observe EVERY line (before rules), exactly as in production; a
	// fresh set per case keeps cases independent. Compiled through the shared
	// helper so the prefix participates exactly as it does at a real start.
	//
	// Whenever the config HAS the section, not only when a case asserts on it:
	// the set is also the emit_metric target the transform seam needs below,
	// and compiling it on demand meant a transforms file using that sanctioned
	// verb failed every case with "no logMetrics section is configured" —
	// naming the one section that was in fact present, for a pair a real start
	// runs correctly.
	var set *metrics.DynamicMetricSet
	if cfg.LogMetrics != nil {
		var err error
		if set, err = compileLogMetrics(cfg.LogMetrics); err != nil {
			fail("compiling logMetrics: %v", err)
		}
	}
	if len(tc.Expect.Metrics) > 0 && set == nil {
		fail("expect.metrics set but the config has no logMetrics section")
	}

	base := logchain.Config{Scrub: scrubber, LogAttrs: extractor, Enrich: *enrichOn}
	observe := base
	observe.LogMetrics = set
	chain := logchain.NewChain[struct{}](observe, false)

	// Scrub + extract precede grouping, as in every producer; the body
	// assertion reads the post-scrub line.
	body, ext := chain.Line(tc.Line)
	if tc.Expect.Body != "" && body != tc.Expect.Body {
		fail("body = %q, want %q", body, tc.Expect.Body)
	}

	// Build the payload the way the tailer's grouper does: the case's resource
	// plus this line's lifted resource attributes, the tailer's own scope
	// (name and version, so the harness models what production ships rather
	// than a nameless scope of its own), the lifted scope attributes. Not
	// modelled: the tailer's per-ENTRY facts (log.iostream, log.truncated,
	// log.multiline.match, log.file.*) and pod-annotation rules. Those are
	// runtime data rather than config, so a case cannot produce them — a rule
	// keyed on one of them is outside what this harness can prove.
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	for k, v := range tc.Resource {
		rl.Resource().Attributes().PutStr(k, v)
	}
	logattrs.Put(rl.Resource().Attributes(), ext.Resource)
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(tailer.ScopeName)
	sl.Scope().SetVersion(obs.ScopeVersion)
	logattrs.Put(sl.Scope().Attributes(), ext.Scope)

	// The group's resource is what metric labels and rule keys resolve against
	// (after the record's attributes and the line's lifted values) and what
	// log-metrics bind to — the case resource plus the lifted resource
	// attributes, as in every producer.
	in := logchain.Input[struct{}]{Body: body, Lifted: ext, Resource: rl.Resource().Attributes()}
	chain.Emit(&captureProducer{dest: sl.LogRecords(), body: body}, in)
	lr := sl.LogRecords().At(0)

	if tc.Expect.Severity != "" && !strings.EqualFold(lr.SeverityText(), tc.Expect.Severity) {
		fail("severity = %q, want %q", lr.SeverityText(), tc.Expect.Severity)
	}
	for k, want := range tc.Expect.Attributes {
		v, ok := lr.Attributes().Get(k)
		if !ok {
			fail("attribute %q missing", k)
		} else if v.AsString() != want {
			fail("attribute %q = %q, want %q", k, v.AsString(), want)
		}
	}

	// The verdict pass, then the logs transform — kept reflects both.
	kept := true
	if rules != nil {
		ruled := base
		ruled.Rules = rules
		verdict := logchain.NewChain[struct{}](ruled, false)
		kept = verdict.Emit(&captureProducer{dest: plog.NewLogRecordSlice(), body: body}, in)
	}
	if kept && transforms != nil {
		sink := &captureLogs{}
		w := transform.Wrap(sink, nil, transforms)
		if set != nil {
			// The emit_metric bridge, wired exactly as run() wires it
			// (main.go: transforms.SetMetricEmitter(logMetrics)), including
			// the typed-value guard — a nil *DynamicMetricSet boxed into the
			// interface would defeat the builtin's own nil check. Without this
			// the harness rejected every case of a script using the verb,
			// which is the opposite of what a CI proof of a transform edit is
			// for.
			w.SetMetricEmitter(set)
		}
		if err := w.ExportLogs(context.Background(), ld); err != nil {
			fail("logs transform: %v", err)
		} else {
			kept = sink.records > 0
		}
	}

	// AFTER the transform arm, so expect.metrics can also assert a series a
	// script emitted — the export firedMetricNames runs consumes the set, so
	// asserting before it would have made emit_metric unobservable even once
	// the emitter is wired. Ordinary chain observations happened further up and
	// are unaffected by the move.
	if set != nil {
		fired := firedMetricNames(set)
		for _, want := range tc.Expect.Metrics {
			if !fired[*logsMetricsPrefix+want] && !fired[want] {
				fail("metric %q did not observe the line (fired: %v)", want, slices.Sorted(maps.Keys(fired)))
			}
		}
	}
	wantKept := tc.Expect.Kept == nil || *tc.Expect.Kept
	if kept != wantKept {
		fail("kept = %v, want %v", kept, wantKept)
	}
	return problems
}

// firedMetricNames exports the set once and returns the metric names that
// carry data points.
func firedMetricNames(set *metrics.DynamicMetricSet) map[string]bool {
	sink := &captureMetrics{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = set.Export(ctx, sink, 0)
	return sink.names
}

// captureLogs counts records "exported" by the transform wrapper.
type captureLogs struct{ records int }

func (c *captureLogs) ExportLogs(_ context.Context, ld plog.Logs) error {
	c.records += ld.LogRecordCount()
	return nil
}

func (c *captureLogs) ExportMetrics(context.Context, pmetric.Metrics) error {
	return errors.New("unused")
}

// captureMetrics records exported metric names.
type captureMetrics struct{ names map[string]bool }

func (c *captureMetrics) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	if c.names == nil {
		c.names = map[string]bool{}
	}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				c.names[ms.At(k).Name()] = true
			}
		}
	}
	return nil
}
