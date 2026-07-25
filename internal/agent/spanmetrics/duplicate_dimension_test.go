package spanmetrics

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func attrKey(a pcommon.Map) string { return fmt.Sprint(a.AsRaw()) }

// A configured dimension repeating a built-in must be ignored. putDims writes
// names in order and a later write wins, while an extra dimension resolves from
// span/resource ATTRIBUTES — and span.name/span.kind/status.code are span
// FIELDS, not attributes, so they resolve to "". Appending the duplicate
// therefore blanked the real built-in label; and because the series key still
// distinguished the series, two different spans rendered byte-identical
// attribute sets in one export (conflicting duplicate points downstream).
func TestDuplicateDimensionDoesNotBlankBuiltin(t *testing.T) {
	exp := &capExporter{}
	g := New(Config{Dimensions: []string{"span.name"}})

	g.Consume(traces("svc",
		spanSpec{name: "checkout", dur: 0.01},
		spanSpec{name: "payment", dur: 0.02},
	))
	if err := g.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}

	m, ok := exp.find("traces.span.metrics.calls")
	if !ok {
		t.Fatal("calls metric missing")
	}
	dps := m.Sum().DataPoints()

	seen := map[string]bool{}
	names := map[string]bool{}
	for i := 0; i < dps.Len(); i++ {
		a := dps.At(i).Attributes()
		v, present := a.Get("span.name")
		if !present || v.Str() == "" {
			t.Errorf("span.name label = %q (present=%v); a duplicate configured dimension blanked the built-in",
				v.Str(), present)
		}
		names[v.Str()] = true

		key := attrKey(a)
		if seen[key] {
			t.Errorf("two data points share the identical attribute set %v", a.AsRaw())
		}
		seen[key] = true
	}
	for _, want := range []string{"checkout", "payment"} {
		if !names[want] {
			t.Errorf("span.name %q missing from the exported series", want)
		}
	}
}
