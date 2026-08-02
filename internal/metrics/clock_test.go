package metrics

import (
	"sync/atomic"
	"testing"
	"time"
)

// The clock the tests pin. It is declared HERE, in a _test file, rather than in
// metrics.go: the production series reads its clock through an injectable field
// (series.now, the store.now pattern), so pinning it costs the production
// observe path nothing — it used to cost an atomic load per observation, on the
// per-log-line hot path, purely so tests could override it.
//
// Every construction helper below installs it, which is what keeps the
// setTimeForTest/advance calls in the test bodies reading as they did.
var testEpoch atomic.Int64

// setTimeForTest pins the clock every set/series built through the helpers
// below reads.
func setTimeForTest(t time.Time) { testEpoch.Store(t.Unix()) }

// testNow is the injected clock.
func testNow() int64 { return testEpoch.Load() }

// newTestSet builds a DynamicMetricSet on the pinned clock. Tests use it in
// place of NewDynamicMetricSet; the signature is identical.
func newTestSet(metrics []Dynamic, opts ...Option) (*DynamicMetricSet, error) {
	return NewDynamicMetricSet(metrics, append(opts, withClock(testNow))...)
}

// newTestSeries builds a bare series on the pinned clock.
func newTestSeries(spec seriesSpec) *series {
	spec.now = testNow
	return newSeries(spec)
}

// The production observe path must not read a test hook. A series built without
// an injected clock takes the package's coarse one, and there is no global
// override for it to consult — which is the point of the field.
func TestProductionSeriesTakesTheCoarseClock(t *testing.T) {
	setTimeForTest(time.Unix(1, 0))
	defer testEpoch.Store(0)
	s := newSeries(seriesSpec{name: "x", kind: kindCounter, maxSize: 10})
	if s.now != nil {
		t.Fatal("a series built without seriesSpec.now carries a clock")
	}
	if got := s.epoch(); got == 1 {
		t.Fatal("the production clock consulted the test override; the hot path is paying for the tests again")
	}
}
