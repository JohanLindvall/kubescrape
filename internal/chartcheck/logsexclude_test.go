package chartcheck

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
)

// The feedback-loop guard derives -logs-exclude-namespaces from the ONE
// in-cluster LOGS destination: agent.config.export.logs.endpoint when it names
// one, the -otlp-endpoint flag base otherwise. That mirrors
// otlpexport.ExportOverride.merged, where a non-empty per-signal endpoint
// REPLACES the base rather than adding to it, and both halves of the rule are
// load-bearing in opposite directions:
//
//   - miss the override and the collectorless arrangement tails the very
//     gateway it ships to, which amplifies exactly when that gateway is slow;
//   - keep the base ALONGSIDE the override and a namespace that receives no
//     logs at all is excluded anyway — with all three signals overridden, the
//     base is an address BuildExporter never even dials.
//
// Both failures are SILENT in the same way: the files are dropped at DISCOVERY
// (internal/agent/tailer/discover.go, claim-and-skip), so no counter moves and
// nothing warns — an operator sees a whole namespace with no logs and no
// explanation anywhere.
//
// TestChartGolden/export-logs-replaces-base pins the rendered bytes for the
// second case. This pins the INTENT across the whole shape space, because a
// golden can be regenerated: widen the derivation back and the goldens simply
// record the wider answer, which is how the over-exclusion survived a review in
// the first place. The base endpoint's namespace is deliberately never the
// release namespace here — every other fixture renders into `monitoring`, which
// is also the default endpoint's namespace, and there the two terms collapse
// under `uniq` so a wrong derivation renders identically to a right one.
func TestLogsExcludeNamespacesFollowsTheOneLogsDestination(t *testing.T) {
	helm := helmBin(t)
	const base = "otel-collector.observability.svc:4317" // namespace: observability
	const loki = "http://loki-gateway.logging.svc:4318"  // namespace: logging

	for _, tc := range []struct {
		name   string
		values string
		want   string
		why    string
	}{
		{
			name:   "base-is-the-logs-destination",
			values: "agent:\n  otlp:\n    endpoint: " + base + "\n",
			want:   "obs,observability",
			why:    "with no export.logs the flag base receives the logs, so its namespace must be excluded",
		},
		{
			name: "override-replaces-the-base",
			values: "agent:\n  otlp:\n    endpoint: " + base + "\n" +
				"  config:\n    export:\n      logs:\n        endpoint: " + loki + "\n        protocol: http\n",
			want: "obs,logging",
			why:  "export.logs REPLACES the base for logs, so the base is a metrics/traces-only address and must not contribute",
		},
		{
			name: "override-without-an-endpoint-inherits-the-base",
			values: "agent:\n  otlp:\n    endpoint: " + base + "\n" +
				"  config:\n    export:\n      logs:\n        compression: none\n",
			want: "obs,observability",
			why:  "an export.logs section with an empty endpoint inherits the base, so the base is still the logs destination",
		},
		{
			name: "all-three-overridden-leaves-the-base-undialled",
			values: "agent:\n  otlp:\n    endpoint: " + base + "\n" +
				"  config:\n    export:\n      logs:\n        endpoint: " + loki + "\n        protocol: http\n" +
				"      metrics:\n        endpoint: http://mimir-gateway.metrics.svc:4318\n        protocol: http\n" +
				"      traces:\n        endpoint: http://tempo-gateway.tracing.svc:4318\n        protocol: http\n",
			want: "obs,logging",
			why:  "BuildExporter constructs no Default when every signal is overridden — nothing dials the base at all",
		},
		{
			name: "metrics-only-override-keeps-the-base",
			values: "agent:\n  otlp:\n    endpoint: " + base + "\n" +
				"  config:\n    export:\n      metrics:\n        endpoint: http://mimir-gateway.metrics.svc:4318\n        protocol: http\n",
			want: "obs,observability",
			why:  "a metrics override says nothing about where logs go; the base still carries them",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			vals := filepath.Join(dir, "values.yaml")
			if err := os.WriteFile(vals, []byte(tc.values), 0o600); err != nil {
				t.Fatal(err)
			}
			// The release namespace is `obs`, never the base endpoint's, so the
			// release term and the endpoint term cannot mask one another.
			out, err := exec.Command(helm, "template", "kubescrape", "../../charts/kubescrape",
				"--namespace", "obs", "-f", vals, "--show-only", "templates/agent.yaml").CombinedOutput()
			if err != nil {
				t.Fatalf("helm template failed: %v\n%s", err, out)
			}
			m := regexp.MustCompile(`-logs-exclude-namespaces=(\S*)`).FindSubmatch(out)
			if m == nil {
				t.Fatalf("no -logs-exclude-namespaces in the rendered DaemonSet:\n%s", out)
			}
			if got := string(m[1]); got != tc.want {
				t.Errorf("-logs-exclude-namespaces=%s, want %s — %s", got, tc.want, tc.why)
			}
		})
	}
}
