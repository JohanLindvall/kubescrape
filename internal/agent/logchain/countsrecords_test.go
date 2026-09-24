package logchain

import (
	"reflect"
	"testing"
)

// CountsRecords is the one answer to "does this configuration move a
// per-record counter", and a producer that gates its observation proof on it
// (the events reader) must never be told no for a config that counts: the
// hand-written copy it replaces left enrichment out, and re-counted every
// buffered event on every redelivering restart of a default configuration.
//
// So every Config field is judged here, by reflection: each one set alone must
// make CountsRecords true, except the fields named as counting nothing. A new
// field fails this test until someone decides which side it is on — which is
// the moment to extend Input.Observed's list too.
func TestCountsRecordsNamesEveryCountingField(t *testing.T) {
	countsNothing := map[string]bool{
		// Lifting attributes off the line moves no counter.
		"LogAttrs": true,
	}
	if (Config{}).CountsRecords() {
		t.Fatal("the zero Config counts records")
	}
	typ := reflect.TypeFor[Config]()
	for i := range typ.NumField() {
		f := typ.Field(i)
		var c Config
		v := reflect.ValueOf(&c).Elem().Field(i)
		switch f.Type.Kind() {
		case reflect.Pointer:
			v.Set(reflect.New(f.Type.Elem()))
		case reflect.Bool:
			v.SetBool(true)
		default:
			t.Fatalf("Config.%s is a %s; teach this test to set it, then decide whether it counts records",
				f.Name, f.Type)
		}
		if got, want := c.CountsRecords(), !countsNothing[f.Name]; got != want {
			t.Errorf("Config{%s} CountsRecords = %v, want %v: a field that moves a per-record counter "+
				"must be named by CountsRecords (and gated by Input.Observed); one that moves none belongs "+
				"in countsNothing", f.Name, got, want)
		}
	}
}
