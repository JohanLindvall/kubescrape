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
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/agent/logenrich"
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
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

	// The same compiled chains a real start builds.
	var scrubber *logscrub.Scrubber
	if cfg.LogScrubbing != nil {
		if scrubber, err = logscrub.New(*cfg.LogScrubbing); err != nil {
			return err
		}
	}
	extractor, err := logattrs.New(cfg.LogAttributes)
	if err != nil {
		return err
	}
	var rules *logline.LineFilter
	if cfg.Logs != nil {
		if rules, err = logline.NewLineFilter(cfg.Logs.Rules); err != nil {
			return err
		}
	}
	var transforms *transform.Program
	if transformsFile != "" {
		if transforms, err = transform.CompileFile(transformsFile); err != nil {
			return err
		}
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
			log.Error("test-config FAIL", "case", name, "problem", p)
		}
	}
	if failed > 0 {
		return fmt.Errorf("test-config: %d of %d cases failed", failed, len(tests.Tests))
	}
	log.Info("test-config: all cases passed", "cases", len(tests.Tests))
	return nil
}

// runConfigCase pushes one line through the pipeline stages in the tailer's
// order and returns the assertion failures.
func runConfigCase(cfg agentConfig, scrubber *logscrub.Scrubber, extractor *logattrs.Extractor,
	rules *logline.LineFilter, transforms *transform.Program, tc configTestCase) []string {
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	res := pcommon.NewMap()
	for k, v := range tc.Resource {
		res.PutStr(k, v)
	}

	// Scrub FIRST, exactly as the tailer does — everything downstream copies
	// from the body.
	body := tc.Line
	if scrubber != nil {
		body = scrubber.Scrub(body)
	}
	if tc.Expect.Body != "" && body != tc.Expect.Body {
		fail("body = %q, want %q", body, tc.Expect.Body)
	}

	// Build the record the way flush.go does: line attrs, then enrichment.
	// Not modelled: the tailer's per-ENTRY facts (log.iostream, log.truncated,
	// log.multiline.match, log.file.*) and pod-annotation rules. Those are
	// runtime data rather than config, so a case cannot produce them — a rule
	// keyed on one of them is outside what this harness can prove.
	extracted := extractor.Extract(body)
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	res.CopyTo(rl.Resource().Attributes())
	logattrs.Put(rl.Resource().Attributes(), extracted.Resource)
	sl := rl.ScopeLogs().AppendEmpty()
	// Named and versioned like the tailer's, so the harness models what
	// production ships rather than a nameless scope of its own.
	sl.Scope().SetName("github.com/JohanLindvall/kubescrape/agent/tailer")
	sl.Scope().SetVersion(obs.ScopeVersion)
	logattrs.Put(sl.Scope().Attributes(), extracted.Scope)
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr(body)
	logattrs.Put(lr.Attributes(), extracted.Log)
	if *enrichOn {
		logenrich.Apply(lr, body)
	}

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

	// Resolution is the PRODUCTION resolver itself (internal/agent/logchain),
	// not a mirror of it: this harness exists to prove what a rule or metric
	// edit does to real lines, and a re-implementation can only prove what the
	// re-implementation does. It reads the CASE's resource map plus THIS
	// line's lifted resource attributes, which is what every producer now
	// resolves against — without SetLifted the harness agreed with no
	// pipeline at all, and its own comment about resource-target attributes
	// being invisible described behaviour the chain no longer has.
	resolver := logchain.New()
	resolver.Set(lr.Attributes(), res, logchain.LowerSeverity(lr.SeverityText()))
	resolver.SetLifted(extracted.Resource)
	labelFn, valueFn, ruleFn := resolver.LabelFn(), resolver.ValueFn(), resolver.RuleFn()

	// Metrics observe EVERY line (before rules), exactly as in production; a
	// fresh set per case keeps cases independent.
	if len(tc.Expect.Metrics) > 0 {
		if cfg.LogMetrics == nil || len(cfg.LogMetrics.Metrics) == 0 {
			fail("expect.metrics set but the config has no logMetrics section")
		} else if set, err := metrics.NewDynamicMetricSet(cfg.LogMetrics.Metrics,
			metrics.WithNamePrefix(*logsMetricsPrefix)); err != nil {
			fail("compiling logMetrics: %v", err)
		} else {
			set.Bind(res).Add(valueFn, labelFn, body)
			fired := firedMetricNames(set)
			for _, want := range tc.Expect.Metrics {
				if !fired[*logsMetricsPrefix+want] && !fired[want] {
					fail("metric %q did not observe the line (fired: %v)", want, keys(fired))
				}
			}
		}
	}

	// Rules, then the logs transform — kept reflects both.
	kept := rules.Keep(ruleFn, body)
	if kept && transforms != nil {
		sink := &captureLogs{}
		w := transform.Wrap(sink, nil, transforms)
		if err := w.ExportLogs(context.Background(), ld); err != nil {
			fail("logs transform: %v", err)
		} else {
			kept = sink.records > 0
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

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
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
