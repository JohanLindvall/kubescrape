// Package obs holds the internal (self-observability) metrics of both
// binaries. They are produced through internal/metrics' Registry and, by
// default, pushed over OTLP alongside everything else (Registry.Run). With
// -self-metrics-interval=0 the push is off and the same series are served
// instead on -metrics-listen's Prometheus /metrics endpoint, through the
// non-mutating Registry.Dump and the registryCollector bridge (runtime.go):
// one knob selects the delivery modality, so the two never double-deliver.
// New failure paths should count into an existing metric here or add one.
package obs

import (
	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// Registry collects every metric this package declares (the metrics_*.go
// files, one per subsystem). The binaries push it periodically via
// metrics.Registry.Run with their own resource identity, or — when
// -self-metrics-interval=0 turns that push off — serve it on /metrics through
// RuntimeHandler's Dump bridge.
var Registry = metrics.NewRegistry()

// durationBuckets are the bounds of the two histograms timing an operation a
// TIMEOUT bounds — a scrape (-scrape-timeout) and an OTLP export attempt
// (-otlp-timeout), both 15s by default. metrics' default buckets stop at 10s,
// so every attempt between 10s and the timeout, and every timed-out one, landed
// in +Inf, where histogram_quantile answers with the highest FINITE bound: the
// documented "p90 approaching the timeout" alert flatlined at 10 and could
// never cross a threshold near 15. The bounds run past both defaults to a
// minute, so a raised timeout still resolves.
var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 20, 30, 60}

func init() {
	// The build version reaches internal/metrics' own exported scopes through
	// here: it owns BuildVersion and imports that package, so the value is
	// pushed down rather than imported back.
	metrics.SetScopeVersion(BuildVersion())

	// Registered here rather than through a Register* hook because it is a
	// property of THIS registry, which always exists — there is no wiring
	// decision to condition it on, so a published 0 always means "nothing was
	// skipped". The value is read at export time from the registry's own
	// atomic; while Dump is serving a scrape it is one dump behind (the func
	// metrics render after the stored series), which costs a scrape and never a
	// count.
	Registry.CounterFunc("kubescrape_self_metrics_points_skipped_total",
		"Data points of this process's OWN metrics that were left out of the Prometheus /metrics response — either "+
			"because their stored label set failed to parse back (metrics.Registry.Dump) or because the "+
			"const-metric construction that turns a dumped point into an exposition refused it (obs's "+
			"registryCollector: an invalid metric or label NAME, a label VALUE that is not valid UTF-8, a "+
			"label-count mismatch, a series kind the bridge has no mapping for). ONE counter for both layers "+
			"because both drop a point from the SAME response and the remedy is the same; the throttled WARN "+
			"beside it names the metric and says which layer refused. It matters because of WHERE the loss lands "+
			"— the Prometheus /metrics exposition this process serves for itself, which is the delivery path when "+
			"-self-metrics-interval=0 and the signal an operator uses to diagnose everything else. A skipped point "+
			"is simply ABSENT from the response, so without this counter the operator's own telemetry shrinks "+
			"invisibly. There is no second copy to fall back on: /metrics carries kubescrape_* metrics exactly when "+
			"the OTLP self-metrics push is off, so a skipped point is not delivered anywhere until its cause is "+
			"fixed. This should never move: the names of these "+
			"series come from code, and so do most label values. The exceptions are operator-supplied strings, such "+
			"as a readiness gate named after a flag value (argv is not guaranteed to be UTF-8), which is what a "+
			"non-UTF-8 refusal points at; any other nonzero value is a bug in kubescrape — memory corruption in the "+
			"label round-trip, or a name the exposition cannot carry.",
		func() float64 { return float64(Registry.SkippedPoints()) })
}
