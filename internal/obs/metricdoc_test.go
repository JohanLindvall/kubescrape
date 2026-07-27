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
	b.WriteString("kubescrape's own metrics (`kubescrape_*`) are **pushed over OTLP**, not\n")
	b.WriteString("scraped. They are exported on `-self-metrics-interval` (default 1m, 0\n")
	b.WriteString("disables) under the exporting process's own resource identity:\n")
	b.WriteString("`service.name=kubescrape` plus hostname for the metadata service,\n")
	b.WriteString("`service.name=kubescrape-agent` plus `k8s.node.name` for the agent, both\n")
	b.WriteString("stamped with `service.version`.\n\n")
	b.WriteString("They are deliberately **not** served on `/metrics`. That endpoint carries only\n")
	b.WriteString("the Go runtime and process collectors (`go_*`, `process_*`) on its own\n")
	b.WriteString("`-metrics-listen` port, because `Registry.snapshot()` consumes the interval\n")
	b.WriteString("state the push path depends on — a scrape would steal samples from the\n")
	b.WriteString("exporter.\n\n")
	b.WriteString("This file is generated from `internal/obs/obs.go`. Regenerate with\n")
	b.WriteString("`go test ./internal/obs/ -run TestMetricsDocIsCurrent -update-metrics-doc`;\n")
	b.WriteString("`TestDocumentedMetricsExist` additionally fails if prose anywhere in the repo\n")
	b.WriteString("names a metric or a label that is not registered.\n\n")
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
