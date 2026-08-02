// Package metrics holds TWO products that share one storage type. Neither is
// the other, and the differences are exactly the ones a reader trips over:
//
//   - DynamicMetricSet (dynamic.go, compile.go, export.go) builds
//     Prometheus-style metrics (counter/gauge/histogram/summary) FROM LOG
//     LINES. It is configured declaratively (see Dynamic): each rule says which
//     lines it matches, the labels it carries and the value it observes. Its
//     labels are DATA-driven, so its series expire after inactivity and are
//     capped by cardinality, and the observations it refuses are counted (see
//     drops) and published by obs.RegisterLogMetricsDrops.
//
//   - Registry (registry.go, dump.go) is a process's OWN self-telemetry, with a
//     prometheus-client-shaped API (Inc/Add/Set/Observe, WithLabelValues). Its
//     labels come from CODE, so it has no expiry and no cardinality cap — the
//     200-year registryExpiration is how it spells "never" through storage the
//     other product needs expiry from.
//
// What they share is series (series.go, labels.go, resource.go): the expiring,
// hash-keyed sample store. Both export over OTLP; there is no Prometheus
// exposition from this package (obs bridges Registry onto a scrape separately).
package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// epochSeconds is a coarse wall clock refreshed every ten seconds so the hot
// observe path avoids a time.Now syscall per log line.
//
// The ticker starts LAZILY, on the first series built in this process, rather
// than from an init(): the package used to impose an unstoppable goroutine on
// every importer — including every test binary that merely links something that
// links it — for a clock most of them never read. Nothing stops it once
// started, deliberately: it is one goroutine for the process lifetime, and the
// alternative (a stoppable clock owned by each set) puts a shutdown obligation
// on a hot-path detail.
var (
	epochSeconds atomic.Int64
	epochOnce    sync.Once
)

// startEpochClock is called by newSeries. Idempotent.
func startEpochClock() {
	epochOnce.Do(func() {
		epochSeconds.Store(time.Now().Unix())
		go func() {
			for t := range time.Tick(10 * time.Second) {
				epochSeconds.Store(t.Unix())
			}
		}()
	})
}

// coarseEpoch is the default clock a series reads. A series with its own
// seriesSpec.now (tests) never reaches it — there is no process-global test
// override any more, and hence no atomic load per observation paying for one.
func coarseEpoch() int64 { return epochSeconds.Load() }
