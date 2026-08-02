package cumagg_test

import (
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
	"github.com/JohanLindvall/kubescrape/internal/agent/spanmetrics"
)

// The two aggregators' staleAfter fields mean the same thing, and for a while
// they did not answer the same way: serviceGraph.staleAfter refused a negative
// duration, traceMetrics.staleAfter CLAMPED it to zero — which is that field's
// "disable eviction" spelling. So `traceMetrics: {staleAfter: "-15m"}` passed
// -check-config, passed New, and silently turned the cardinality cap into the
// one-way latch that eviction exists to prevent, on the one configuration where
// an operator plainly meant to set a window.
//
// Both surfaces now parse through cumagg.ParseStaleAfter. This test is the
// cross-package assertion that they still agree: it is deliberately outside
// both packages, because agreement between them is the property, and a test
// inside either one can only see its own half.
func TestNegativeStaleAfterRejectedByBothConfigSurfaces(t *testing.T) {
	for _, value := range []string{"-15m", "-1s", "-1ns"} {
		t.Run(value, func(t *testing.T) {
			sgErr := (&servicegraph.Config{StaleAfter: value}).Validate()
			smErr := spanmetrics.Config{StaleAfter: value}.Validate()
			if sgErr == nil {
				t.Errorf("serviceGraph.staleAfter accepted %q", value)
			}
			if smErr == nil {
				t.Errorf("traceMetrics.staleAfter accepted %q (the clamp is back: a negative "+
					"value disables eviction and the cardinality cap becomes a one-way latch)", value)
			}
			if sgErr == nil || smErr == nil {
				return
			}
			// Same shape of message, each naming its OWN config path: the rule
			// is shared, the field name an operator has to fix is not.
			for _, c := range []struct {
				name, field string
				err         error
			}{
				{"serviceGraph", "serviceGraph.staleAfter", sgErr},
				{"traceMetrics", "traceMetrics.staleAfter", smErr},
			} {
				for _, want := range []string{c.field, value, "negative", `"0"`} {
					if !strings.Contains(c.err.Error(), want) {
						t.Errorf("%s error %q does not name %q", c.name, c.err, want)
					}
				}
			}
		})
	}
}

// The other half of the same drift: a valid value, and the "0 disables it"
// escape hatch, must also read identically on both surfaces.
func TestStaleAfterSpellingsAgree(t *testing.T) {
	for _, value := range []string{"", "0", "0s", "5m", "15m"} {
		if err := (&servicegraph.Config{StaleAfter: value}).Validate(); err != nil {
			t.Errorf("serviceGraph.staleAfter %q rejected: %v", value, err)
		}
		if err := (spanmetrics.Config{StaleAfter: value}).Validate(); err != nil {
			t.Errorf("traceMetrics.staleAfter %q rejected: %v", value, err)
		}
	}
	for _, value := range []string{"fifteen minutes", "15", "m15"} {
		if err := (&servicegraph.Config{StaleAfter: value}).Validate(); err == nil {
			t.Errorf("serviceGraph.staleAfter accepted the unparseable %q", value)
		}
		if err := (spanmetrics.Config{StaleAfter: value}).Validate(); err == nil {
			t.Errorf("traceMetrics.staleAfter accepted the unparseable %q", value)
		}
	}
}
