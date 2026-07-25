package metrics

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// valueRegexp both extracts a value AND filters. For inc/dec/count — the gauge
// actions that need no value at all — only the MATCH may gate the observation.
// Gating them on the extraction succeeding meant a regex used as a pure content
// filter, or one capturing a non-numeric group, rejected every line it matched:
// the rule compiled without error (those actions need no value source) and then
// silently reported nothing for the life of the process.
func TestValueRegexpFiltersWithoutRequiringANumber(t *testing.T) {
	setTimeForTest(time.Unix(1_700_500_000, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{
		// Numeric capture: extracts AND filters (2 of 4 lines match).
		{Name: "n_count", Type: GaugeType, Action: "count", ValueRegexp: `latency=(\d+)`, Match: []string{"m=1"}},
		// Pure content filter, no capture group at all.
		{Name: "n_inc", Type: GaugeType, Action: "inc", ValueRegexp: `ERROR`, Match: []string{"m=1"}},
		// Non-numeric capture group.
		{Name: "n_word", Type: GaugeType, Action: "count", ValueRegexp: `user=(\w+)`, Match: []string{"m=1"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	lookup := func(k string) string {
		if k == "m" {
			return "1"
		}
		return ""
	}
	lines := []string{
		"latency=10 ok",
		"no numbers here",
		"latency=20 ok",
		"ERROR something user=bob",
	}
	for _, ln := range lines {
		set.Add(nil, lookup, pcommon.NewMap(), ln)
	}

	want := map[string]float64{
		"n_count": 2, // two latency= lines
		"n_inc":   1, // one ERROR line, value ignored
		"n_word":  1, // one user= line, non-numeric capture ignored
	}
	for _, r := range set.rules {
		snap := r.series.snapshot()
		var v float64
		if len(snap) > 0 {
			v = snap[0].value
		}
		if w, ok := want[r.series.name]; ok && v != w {
			t.Errorf("%s = %v, want %v (a matching line must be recorded even when the capture is not a number)",
				r.series.name, v, w)
		}
	}
}
