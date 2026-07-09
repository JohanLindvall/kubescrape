// Package metrics builds Prometheus-style metrics
// (counter/gauge/histogram/summary) from log lines and exports them over OTLP.
// A DynamicMetricSet is configured declaratively (see Dynamic): each rule says
// which lines it matches, the labels it carries and the value it observes.
// Series expire after inactivity and are capped by cardinality. There is no
// Prometheus exposition — OTLP only.
package metrics

import (
	"sync/atomic"
	"time"
)

// epochSeconds is a coarse wall clock refreshed every ten seconds so the hot
// observe path avoids a time.Now syscall per log line. testEpoch, when nonzero,
// overrides it in tests.
var (
	epochSeconds atomic.Int64
	testEpoch    atomic.Int64
)

func init() {
	epochSeconds.Store(time.Now().Unix())
	go func() {
		for t := range time.Tick(10 * time.Second) {
			epochSeconds.Store(t.Unix())
		}
	}()
}

func loadEpoch() int64 {
	if t := testEpoch.Load(); t != 0 {
		return t
	}
	return epochSeconds.Load()
}

// setTimeForTest pins the clock (tests only).
func setTimeForTest(t time.Time) { testEpoch.Store(t.Unix()) }
