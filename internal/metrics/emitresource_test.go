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
	for i := 0; i < 3; i++ {
		if err := set.EmitDirect("emit_total", 1, nil, r); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(set.rules[0].series.db); n != 1 {
		t.Fatalf("live samples = %d, want 1", n)
	}
}
