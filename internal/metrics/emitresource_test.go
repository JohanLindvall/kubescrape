package metrics

// The resource an emit_metric observation is keyed on is the one the caller
// holds AT THE CALL, and a transform script mutates its resource in place.
// These pin that rule so it cannot drift into "the resource as the batch
// arrived" — see EmitDirect's doc for why the call-time reading is the one
// every other read in a script agrees with.

import (
	"testing"
	"time"
)

// A script that edits the resource between two emits puts the two observations
// in two series. Deterministic (the script is hermetic and its statements are
// ordered), documented, and the same answer whichever verb reads the map.
func TestEmitDirectKeysOnTheResourceAtCallTime(t *testing.T) {
	setTimeForTest(time.Unix(1_800_600_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{Name: "emit_total", Type: CounterType, Value: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	// The pdata map a script mutates in place.
	r := res(map[string]string{"service.name": "checkout"})
	if err := set.EmitDirect("emit_total", 1, nil, r); err != nil {
		t.Fatal(err)
	}
	r.PutStr("tenant", "blue") // the script edits the resource, mid-run
	if err := set.EmitDirect("emit_total", 1, nil, r); err != nil {
		t.Fatal(err)
	}

	db := set.rules[0].series.db
	if len(db) != 2 {
		t.Fatalf("live samples = %d, want 2: an emit is keyed on the resource AS OF THAT CALL, "+
			"so an edit between two emits separates them", len(db))
	}
	// And the earlier sample keeps the identity it was admitted under: the
	// rendering happens at admit, so mutating the caller's map afterwards
	// cannot rewrite a series that already exists.
	var withTenant, without int
	for _, samp := range db {
		if samp.value != 1 {
			t.Fatalf("sample value = %v, want 1 per emit", samp.value)
		}
		lbls, err := parseLabels(samp.resource)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := lbls.get("tenant"); ok {
			withTenant++
		} else {
			without++
		}
	}
	if withTenant != 1 || without != 1 {
		t.Fatalf("resources: %d with the edit, %d without; want one of each — a retroactive "+
			"rewrite of the first sample's identity would make the observation depend on what "+
			"the script did AFTER it", withTenant, without)
	}
}

// The other half of the rule: emits made under the same resource state share a
// series however many times the map is read.
func TestEmitDirectUnmutatedResourceSharesOneSeries(t *testing.T) {
	setTimeForTest(time.Unix(1_800_600_100, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{Name: "emit_total", Type: CounterType, Value: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	r := res(map[string]string{"service.name": "checkout"})
	for range 3 {
		if err := set.EmitDirect("emit_total", 1, nil, r); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(set.rules[0].series.db); n != 1 {
		t.Fatalf("live samples = %d, want 1", n)
	}
}

// -logs-metrics-name-prefix prefixes every EXPORTED series name, and a script
// names the metric the way the logMetrics config does. EmitDirect compared the
// script's name against the PREFIXED series name, so the moment an operator
// set a prefix, every script following the documented contract failed every
// batch with "no logMetrics rule declares this metric" — a failed export, and
// on the tailer's in-place seam a rewind that re-read and re-failed the same
// files on every sweep.
func TestEmitDirectAcceptsTheDeclaredNameUnderANamePrefix(t *testing.T) {
	setTimeForTest(time.Unix(1_800_600_200, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{
		{Name: "errors_total", Type: CounterType, Value: "1"},
		// A declared name equal to ANOTHER rule's prefixed series name: the
		// declared match must win, deterministically.
		{Name: "app_x_total", Type: CounterType, Value: "1"},
		{Name: "x_total", Type: CounterType, Value: "1"},
	}, WithNamePrefix("app_"))
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]*series{}
	for _, r := range set.rules {
		byName[r.series.name] = r.series
	}

	if err := set.EmitDirect("errors_total", 1, nil, noRes()); err != nil {
		t.Fatalf("the declared name under a prefix: %v", err)
	}
	// The prefixed spelling still works, for a script written around the old
	// behaviour.
	if err := set.EmitDirect("app_errors_total", 1, nil, noRes()); err != nil {
		t.Fatalf("the prefixed name: %v", err)
	}
	if got := byName["app_errors_total"]; got == nil || got.count != 1 {
		t.Fatalf("both emits must land in the one app_errors_total series")
	}
	for samp := range byName["app_errors_total"].all() {
		if samp.value != 2 {
			t.Fatalf("app_errors_total = %v, want 2", samp.value)
		}
	}

	if err := set.EmitDirect("app_x_total", 1, nil, noRes()); err != nil {
		t.Fatal(err)
	}
	if byName["app_app_x_total"].count != 1 || byName["app_x_total"].count != 0 {
		t.Fatalf("emit_metric(\"app_x_total\") reached the rule whose PREFIXED name it is (app_x_total=%d, app_app_x_total=%d); "+
			"the rule that DECLARES the name must win", byName["app_x_total"].count, byName["app_app_x_total"].count)
	}

	if err := set.EmitDirect("nope_total", 1, nil, noRes()); err == nil {
		t.Fatal("an undeclared name must still be a script error")
	}
}
