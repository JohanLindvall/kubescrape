package servicemonitors

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// monitorWithRelabelings builds a ServiceMonitor whose single endpoint carries
// the given metricRelabelings.
func monitorWithRelabelings(t *testing.T, rules []any) *Monitor {
	t.Helper()
	m, err := Parse(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "sm", "namespace": "tenant"},
		"spec": map[string]any{
			"selector": map[string]any{},
			"endpoints": []any{map[string]any{
				"port": "http", "metricRelabelings": rules,
			}},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func chainBytes(rules []RelabelRule) int {
	n := 0
	for _, r := range rules {
		n += len(r.Regex)
		for _, l := range r.SourceLabels {
			n += len(l)
		}
	}
	return n
}

// A ServiceMonitor is a tenant-authored object: anyone with edit rights in one
// namespace can create one, and by default (-monitor-namespaces unset) it is
// honoured cluster-wide. Its metricRelabelings chain is copied into every
// target it resolves to — which the metadata service marshals into ONE
// node-targets document per request — and walked per SAMPLE by every agent
// scraping such a target. So the chain a single endpoint may impose is bounded
// here, at the parse door, exactly as the tailer bounds a pod annotation's log
// rules.
func TestOneEndpointsRelabelChainIsBounded(t *testing.T) {
	rules := make([]any, 0, 20000)
	for i := range 20000 {
		rules = append(rules, map[string]any{
			"action":       "drop",
			"sourceLabels": []any{"__name__"},
			"regex":        strconv.Itoa(i) + strings.Repeat("x", 40),
		})
	}
	m := monitorWithRelabelings(t, rules)
	got := m.Endpoints[0].MetricRelabelings
	t.Logf("20000 rules -> %d kept, %d bytes", len(got), chainBytes(got))
	if len(got) > maxRelabelRules {
		t.Errorf("kept %d rules, over the per-endpoint ceiling of %d", len(got), maxRelabelRules)
	}
	if n := chainBytes(got); n > maxRelabelChainBytes {
		t.Errorf("kept %d chain bytes, over the per-endpoint ceiling of %d", n, maxRelabelChainBytes)
	}
	// A prefix is kept, not nothing: the rules the operator wrote first are
	// still applied.
	if len(got) == 0 {
		t.Fatal("the whole chain was refused; the bound keeps the prefix")
	}
	if got[0].Regex != "0"+strings.Repeat("x", 40) {
		t.Errorf("the kept prefix is not the head of the chain: first regex = %q", got[0].Regex)
	}
	// Fail CLOSED and DIAGNOSABLE: a refusal nobody can see gets configured
	// away, and here it is invisible in the data — the series the operator
	// asked to drop simply arrive.
	if ig := m.Endpoints[0].Ignored; !slices.Contains(ig, relabelCappedIgnored) {
		t.Errorf("the refusal is not reported: Ignored = %v", ig)
	}
}

// The byte half is the one a rule count cannot express — the lesson
// scrape.MaxPortsPerPod records as "a bound on ENTRIES is not a bound on
// BYTES". Few rules, enormous regexes.
func TestFewHugeRelabelRulesAreBoundedToo(t *testing.T) {
	var rules []any
	for i := range 8 {
		rules = append(rules, map[string]any{
			"action":       "keep",
			"sourceLabels": []any{"__name__"},
			// Individually under the per-rule budget, so only the chain budget
			// can stop them.
			"regex": strconv.Itoa(i) + strings.Repeat("y", maxRelabelRuleBytes-16),
		})
	}
	m := monitorWithRelabelings(t, rules)
	got := m.Endpoints[0].MetricRelabelings
	if n := chainBytes(got); n > maxRelabelChainBytes {
		t.Errorf("8 near-maximal rules kept %d bytes, over the ceiling of %d", n, maxRelabelChainBytes)
	}
	if !slices.Contains(m.Endpoints[0].Ignored, relabelCappedIgnored) {
		t.Errorf("the refusal is not reported: Ignored = %v", m.Endpoints[0].Ignored)
	}
}

// A rule bigger than the WHOLE chain budget refuses the ENDPOINT, and this is
// the direction the ceiling deliberately fails in.
//
// Skipping the rule and applying its neighbours is the fail-OPEN: the one shape
// that is legitimately this large is a `keep` allowlist, and a chain shipped
// without it exports every series the allowlist excluded — invisible in the
// data, since the series simply arrive. So the endpoint yields nothing at all,
// which is the outcome that cannot be mistaken for the CR being honoured, and
// it is the same trade (and the same Refused field) the endpoint's string
// ceilings make.
func TestAnOversizedRelabelRuleRefusesTheEndpoint(t *testing.T) {
	m := monitorWithRelabelings(t, []any{
		map[string]any{"action": "drop", "sourceLabels": []any{"__name__"}, "regex": "first"},
		map[string]any{"action": "keep", "sourceLabels": []any{"__name__"},
			"regex": strings.Repeat("z", maxRelabelRuleBytes+1)},
		map[string]any{"action": "drop", "sourceLabels": []any{"__name__"}, "regex": "last"},
	})
	ep := m.Endpoints[0]
	if ep.Refused == "" || !strings.Contains(ep.Refused, "metricRelabelings") {
		t.Fatalf("Refused = %q: the endpoint was not refused for its oversized rule", ep.Refused)
	}
	// Nothing of the endpoint survives: a refused endpoint must resolve to no
	// target through the port door either, or a caller that never learned to
	// read Refused scrapes the default path with a chain the CR does not
	// describe.
	if ep.Port != "" || ep.TargetPort != nil {
		t.Errorf("a refused endpoint still names a port: %q/%v", ep.Port, ep.TargetPort)
	}
	if ep.MetricRelabelings != nil {
		t.Errorf("a refused endpoint still carries %d rules; they filter nothing and are retained for the life of the CR",
			len(ep.MetricRelabelings))
	}
	// Reported exactly once — the report is what moves
	// kubescrape_monitor_fields_ignored_total, and the walk and the refusal
	// must not both write it.
	n := 0
	for _, ig := range ep.Ignored {
		if ig == relabelOversizeIgnored {
			n++
		}
	}
	if n != 1 {
		t.Errorf("the oversized rule is reported %d times in %v, want once", n, ep.Ignored)
	}
}

// The other half of the same decision: the per-rule ceiling is the WHOLE chain
// budget, so the shape that is legitimately large — a metric allowlist as one
// keep rule with a long alternation — comes through untouched. It used to be
// half the chain budget, which refused ~170 metric names and then failed open.
func TestALargeSingleKeepAllowlistIsApplied(t *testing.T) {
	// ~170 metric names of ~30 characters, i.e. over the old per-rule ceiling
	// and inside the chain budget.
	names := make([]string, 0, 170)
	for i := range 170 {
		names = append(names, "kube_pod_status_phase_long_"+strconv.Itoa(i))
	}
	regex := strings.Join(names, "|")
	if len(regex) <= 4<<10 || len(regex) >= maxRelabelChainBytes {
		t.Fatalf("precondition: the allowlist is %d bytes; it must be over the old 4 KiB rule ceiling and inside the chain budget",
			len(regex))
	}
	m := monitorWithRelabelings(t, []any{
		map[string]any{"action": "keep", "sourceLabels": []any{"__name__"}, "regex": regex},
	})
	ep := m.Endpoints[0]
	if ep.Refused != "" {
		t.Fatalf("a legitimate %d-byte allowlist refused the endpoint: %q", len(regex), ep.Refused)
	}
	if len(ep.MetricRelabelings) != 1 || ep.MetricRelabelings[0].Regex != regex {
		t.Fatalf("the allowlist was not applied: %d rule(s)", len(ep.MetricRelabelings))
	}
	for _, ig := range ep.Ignored {
		if strings.HasPrefix(ig, "metricRelabelings") {
			t.Errorf("a legitimate allowlist reported %q", ig)
		}
	}
}

// The bounds are far above anything legitimate: an ordinary chain must come
// through untouched and report nothing, or an operator learns to ignore the
// report.
func TestAnOrdinaryRelabelChainIsUntouched(t *testing.T) {
	var rules []any
	for i := range 12 {
		rules = append(rules, map[string]any{
			"action":       "drop",
			"sourceLabels": []any{"__name__"},
			"regex":        "container_(network_tcp_usage_total|tasks_state|cpu_load_average_10s)_" + strconv.Itoa(i),
		})
	}
	// Plus the one legitimately large shape: a metric allowlist as a single
	// keep rule with a long alternation.
	rules = append(rules, map[string]any{
		"action":       "keep",
		"sourceLabels": []any{"__name__"},
		"regex":        strings.Repeat("kube_pod_status_phase|", 100) + "up",
	})
	m := monitorWithRelabelings(t, rules)
	if got := len(m.Endpoints[0].MetricRelabelings); got != len(rules) {
		t.Errorf("an ordinary chain of %d rules was cut to %d", len(rules), got)
	}
	for _, ig := range m.Endpoints[0].Ignored {
		if strings.HasPrefix(ig, "metricRelabelings") {
			t.Errorf("an ordinary chain reported %q", ig)
		}
	}
}
