package metrics

import (
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// The existing DynamicAdd* benchmarks all observe into a series map holding a
// SINGLE label combination: one resource, one status, one method. That measures
// the fold and the rule machinery honestly and the MAP not at all — a one-entry
// map lives in L1 and its probe is a best case that no node ever sees.
//
// A node agent runs ONE DynamicMetricSet for every pod on it (see series.db's
// note on the cap being one shared pool), so the real db holds
// pods x label-combinations entries: 40 pods x 20 (status, method) pairs is
// 800, and a dense node with a chattier rule reaches thousands against the
// 10000 default cap. The access pattern is a flush: bind one pod's resource,
// push that file's lines through it, move to the next pod — so there is real
// locality within a file and none across the node.
//
// These benchmarks exist so a change to the series map's shape is measured
// where its cost actually lives.

// fleetPods builds n distinct pod resources of the width the agent stamps.
func fleetPods(n int) []pcommon.Map {
	out := make([]pcommon.Map, n)
	for i := range out {
		m := pcommon.NewMap()
		m.PutStr("k8s.namespace.name", "prod-payments")
		m.PutStr("k8s.pod.name", fmt.Sprintf("payments-6f7b9c%03d", i))
		m.PutStr("k8s.container.name", "app")
		m.PutStr("k8s.node.name", "node-7")
		m.PutStr("service.name", "payments")
		m.PutStr("service.namespace", "prod-payments")
		m.PutStr("service.instance.id", fmt.Sprintf("abc123def%03d", i))
		out[i] = m
	}
	return out
}

var (
	fleetStatuses = []string{"200", "201", "301", "400", "404", "429", "500", "503"}
	fleetMethods  = []string{"GET", "POST", "PUT", "DELETE"}
)

// benchFleet observes into a set whose db holds pods x 32 combinations,
// walking a file's worth of lines per pod before moving on.
func benchFleet(b *testing.B, pods int) {
	b.Helper()
	setTimeForTest(time.Unix(1_700_400_000, 0))
	defer testEpoch.Store(0)
	set := benchRules(b)
	res := fleetPods(pods)
	// Pre-resolved attribute lookups: the tailer's common case (record and
	// resource attributes answer the keys, no line parsing).
	attrs := map[string]string{"level": "info", "http_status": "200", "method": "GET", "latency_ms": "42.5"}
	lookup := func(k string) string { return attrs[k] }
	values := func(k string) (float64, bool, bool) {
		if k == "latency_ms" {
			return 42.5, true, true
		}
		return 0, false, false
	}
	line := `GET /api/v1/orders 200 42.5ms`

	// Warm every combination in, so the steady state is probes and not admits.
	for _, r := range res {
		bound := set.Bind(r)
		for _, st := range fleetStatuses {
			for _, mt := range fleetMethods {
				attrs["http_status"], attrs["method"] = st, mt
				bound.Add(values, lookup, line)
			}
		}
	}

	// linesPerFile is a flush's worth of one file's lines: the resource is
	// bound once and reused, which is what tailer.flush does.
	const linesPerFile = 32
	pod, i := 0, 0
	bound := set.Bind(res[0])
	b.ReportAllocs()
	for b.Loop() {
		if i == linesPerFile {
			i = 0
			pod++
			if pod == len(res) {
				pod = 0
			}
			bound = set.Bind(res[pod])
		}
		attrs["http_status"] = fleetStatuses[i%len(fleetStatuses)]
		attrs["method"] = fleetMethods[(i/len(fleetStatuses))%len(fleetMethods)]
		bound.Add(values, lookup, line)
		i++
	}
}

func BenchmarkDynamicAddFleet(b *testing.B) {
	for _, pods := range []int{1, 40, 200} {
		b.Run(fmt.Sprintf("pods=%d", pods), func(b *testing.B) { benchFleet(b, pods) })
	}
}

// benchFleetHistogram is the same shape against a histogram metric, whose
// sample carries the whole bucket distribution: one map probe, then the counts
// fold.
func benchFleetHistogram(b *testing.B, pods int) {
	b.Helper()
	setTimeForTest(time.Unix(1_700_400_400, 0))
	defer testEpoch.Store(0)
	set := benchHistogramRules(b)
	res := fleetPods(pods)
	attrs := map[string]string{"level": "info", "http_status": "200", "method": "GET"}
	lookup := func(k string) string { return attrs[k] }
	values := func(k string) (float64, bool, bool) {
		if k == "latency_s" {
			return 0.42, true, true
		}
		return 0, false, false
	}
	line := `GET /api/v1/orders 200 0.42s`
	for _, r := range res {
		bound := set.Bind(r)
		for _, st := range fleetStatuses {
			for _, mt := range fleetMethods {
				attrs["http_status"], attrs["method"] = st, mt
				bound.Add(values, lookup, line)
			}
		}
	}
	const linesPerFile = 32
	pod, i := 0, 0
	bound := set.Bind(res[0])
	b.ReportAllocs()
	for b.Loop() {
		if i == linesPerFile {
			i = 0
			pod++
			if pod == len(res) {
				pod = 0
			}
			bound = set.Bind(res[pod])
		}
		attrs["http_status"] = fleetStatuses[i%len(fleetStatuses)]
		attrs["method"] = fleetMethods[(i/len(fleetStatuses))%len(fleetMethods)]
		bound.Add(values, lookup, line)
		i++
	}
}

func BenchmarkDynamicAddFleetHistogram(b *testing.B) {
	for _, pods := range []int{40, 200} {
		b.Run(fmt.Sprintf("pods=%d", pods), func(b *testing.B) { benchFleetHistogram(b, pods) })
	}
}
