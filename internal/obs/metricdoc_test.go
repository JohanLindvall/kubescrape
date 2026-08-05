package obs

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
)

var updateMetricsDoc = flag.Bool("update-metrics-doc", false, "rewrite docs/METRICS.md from the registrations in obs.go")

const metricsDocPath = "../../docs/METRICS.md"

// docs/METRICS.md is generated from the registrations rather than maintained
// by hand: 49 metrics is well past what stays accurate through manual edits,
// and the one that had already drifted (a documented label name that did not
// exist) was invisible precisely because nothing checked.
//
// Regenerate with:
//
//	go test ./internal/obs/ -run TestMetricsDocIsCurrent -update-metrics-doc
func TestMetricsDocIsCurrent(t *testing.T) {
	docs, err := ParseMetricDocs("obs.go")
	if err != nil {
		t.Fatalf("ParseMetricDocs: %v", err)
	}
	if len(docs) == 0 {
		t.Fatal("parsed no registrations — the extractor is broken, not the doc")
	}
	want := renderMetricsDoc(docs)

	if *updateMetricsDoc {
		if err := os.WriteFile(metricsDocPath, []byte(want), 0o644); err != nil {
			t.Fatalf("writing %s: %v", metricsDocPath, err)
		}
		t.Logf("wrote %s (%d metrics)", metricsDocPath, len(docs))
		return
	}

	got, err := os.ReadFile(metricsDocPath)
	if err != nil {
		t.Fatalf("reading %s: %v", metricsDocPath, err)
	}
	if string(got) != want {
		t.Errorf("%s is stale; regenerate with:\n"+
			"\tgo test ./internal/obs/ -run TestMetricsDocIsCurrent -update-metrics-doc", metricsDocPath)
	}
}

func renderMetricsDoc(docs []MetricDoc) string {
	var b strings.Builder
	b.WriteString("# Metrics\n\n")
	b.WriteString("kubescrape's own metrics (`kubescrape_*`) are **pushed over OTLP** by\n")
	b.WriteString("default: exported on `-self-metrics-interval` (default 1m) under the\n")
	b.WriteString("exporting process's own resource identity — `service.name=kubescrape` plus\n")
	b.WriteString("hostname for the metadata service, `service.name=kubescrape-agent` plus\n")
	b.WriteString("`k8s.node.name` for the agent, both stamped with `service.version`.\n\n")
	b.WriteString("With `-self-metrics-interval=0` the push is off and the same metrics are\n")
	b.WriteString("served on the `-metrics-listen` port's `/metrics` instead, beside the Go\n")
	b.WriteString("runtime and process collectors (`go_*`, `process_*`) that always live\n")
	b.WriteString("there. One knob selects the delivery modality, so the same series never\n")
	b.WriteString("ship over both paths.\n\n")
	b.WriteString("Every ScopeMetrics/ScopeLogs kubescrape emits now carries an\n")
	b.WriteString("instrumentation-scope NAME and VERSION (the build version, the same string\n")
	b.WriteString("as `service.version`). Log-derived metrics — the `logMetrics` section's\n")
	b.WriteString("output — shipped with an empty scope name until now; every other producer\n")
	b.WriteString("already named one. **Wire-visible**: a translation that turns the scope into\n")
	b.WriteString("labels (`otel_scope_name`, `otel_scope_version`) sees new label values, so\n")
	b.WriteString("those streams split at this upgrade — and, because the version is part of\n")
	b.WriteString("it, at every release boundary thereafter. Group on the metric name and its\n")
	b.WriteString("own labels if you need continuity.\n\n")
	b.WriteString("Cumulative points (every counter, summary and histogram here) now carry a\n")
	b.WriteString("real `StartTimeUnixNano`: the moment that stream began accumulating, not the\n")
	b.WriteString("export time. `StartTimeUnixNano == TimeUnixNano` is the OTLP encoding for a\n")
	b.WriteString("point that just RESET, and stamping it on every push made cumulative-to-delta\n")
	b.WriteString("consumers (Datadog, Dynatrace, AWS EMF) report the whole running total as\n")
	b.WriteString("each interval's delta, while Google Cloud rejected the points outright.\n")
	b.WriteString("Prometheus/Mimir ignore the field by default and are unaffected.\n\n")
	b.WriteString("This file is generated from `internal/obs/obs.go`. Regenerate with\n")
	b.WriteString("`go test ./internal/obs/ -run TestMetricsDocIsCurrent -update-metrics-doc`;\n")
	b.WriteString("`TestDocumentedMetricsExist` additionally fails if prose in `README.md`,\n")
	b.WriteString("`CLAUDE.md` or any `docs/*.md` names a metric or a label that is not\n")
	b.WriteString("registered.\n\n")
	b.WriteString("## Renamed in this release\n\n")
	b.WriteString("All of these are **wire-visible**: dashboards and alert rules selecting the\n")
	b.WriteString("old names match nothing, silently. Names here are written WITHOUT their\n")
	b.WriteString("`kubescrape_` prefix, so that the doc-check which fails on prose naming an\n")
	b.WriteString("unregistered metric stays strict; the table below carries the full names.\n\n")
	b.WriteString("| Was | Is | Why |\n|---|---|---|\n")
	b.WriteString("| `log_lag_bytes` (per-file max) | `log_lag_max_bytes` | `_total` is reserved for counters and both are gauges, so the name promised a counter — and the name WITHOUT the qualifier was the one that was not the total. |\n")
	b.WriteString("| `log_lag_total_bytes` (sum) | `log_lag_bytes` | see above |\n")
	b.WriteString("| `buffer_dropped_total` | `buffer_dropped_batches_total` + `buffer_dropped_records_total` | a batch is 1..1024 records, so the counter operators are told to alert on could not size the loss |\n")
	b.WriteString("| `events_dropped_total` | `events_dropped_batches_total` + `events_dropped_records_total` | same |\n")
	b.WriteString("| `azure_dropped_total` | `azure_dropped_batches_total` + `azure_dropped_records_total` | same |\n")
	b.WriteString("| (new) | `journal_dropped_records_total` | beside the existing `journal_dropped_batches_total` |\n")
	b.WriteString("| `azure_records_total{kind=\"log\"/\"metric\"}` | `azure_records_total{signal=\"logs\"/\"metrics\"}` | every other producer dimensions by `signal` with plural values; the decoded and exported counts of one pipeline shared no label to join on |\n")
	b.WriteString("| `log_metrics_dropped_capped_by_metric` (gauge) and the unlabeled `log_metrics_dropped_capped_total` (counter) | `log_metrics_dropped_capped_total{metric}` | the label name belonged in a label, not inside the metric name, and a gauge carrying a monotonic since-start total hides the reset at a restart. `sum()` over the label is the old aggregate — but the family is now ABSENT rather than 0 until something is dropped, the label set being data-driven. |\n\n")
	b.WriteString("`log_permanent_dropped_total` is unchanged: it already counts RECORDS, and it\n")
	b.WriteString("is the documented alert for the non-buffered tailer.\n\n")
	b.WriteString("## All metrics\n\n")
	b.WriteString("| Metric | Labels | Description |\n|---|---|---|\n")
	for _, d := range docs {
		labels := "—"
		if len(d.Labels) > 0 {
			q := make([]string, len(d.Labels))
			for i, l := range d.Labels {
				q[i] = "`" + l + "`"
			}
			labels = strings.Join(q, ", ")
		}
		help := strings.Join(strings.Fields(d.Help), " ")
		help = strings.ReplaceAll(help, "|", `\|`)
		fmt.Fprintf(&b, "| `%s` | %s | %s |\n", d.Name, labels, help)
	}
	fmt.Fprintf(&b, "\n%d metrics.\n", len(docs))
	return b.String()
}

// Every registered metric must document itself. A help string too long for one
// line is written as `"a" + "b"`, and the parser used to read only the first
// BasicLit — yielding an EMPTY help that docs/METRICS.md dutifully published as
// a blank cell, invisible to TestMetricsDocIsCurrent (which compares the
// generated doc against the same generator).
func TestEveryMetricHasHelp(t *testing.T) {
	docs, err := ParseMetricDocs("obs.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) == 0 {
		t.Fatal("no metrics parsed")
	}
	for _, d := range docs {
		if strings.TrimSpace(d.Help) == "" {
			t.Errorf("%s has no help text; docs/METRICS.md publishes a blank cell for it", d.Name)
		}
	}
}
