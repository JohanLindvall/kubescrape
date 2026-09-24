package servicemonitors

import (
	"runtime"
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

// …and it refuses it WHEREVER the rule sits, which is the whole reason the
// per-rule ceiling is spelled as the whole chain budget: a rule over it fits no
// chain in any order, so its POSITION must not decide the verdict.
//
// It did. Both aggregate ceilings stopped the walk, so an oversized rule placed
// past one of them was never measured: `oversize` stayed false, the endpoint
// kept its Port, and every target it resolved to was served with the prefix
// applied and the allowlist silently absent — the fail-OPEN this ceiling exists
// to prevent, reported through relabelCappedIgnored, i.e. as the fail-open the
// AGGREGATE ceilings are documented to be, so neither
// kubescrape_monitor_fields_ignored_total nor /v1/explain could tell the two
// apart. Both doors are pinned because they were separately reachable.
func TestAnOversizedRelabelRulePastTheCountCapStillRefusesTheEndpoint(t *testing.T) {
	var rules []any
	for i := range maxRelabelRules {
		rules = append(rules, map[string]any{
			"action": "drop", "sourceLabels": []any{"__name__"},
			"regex": "small" + strconv.Itoa(i),
		})
	}
	// The rule the count ceiling would have stopped the walk before reaching.
	rules = append(rules, map[string]any{
		"action": "keep", "sourceLabels": []any{"__name__"},
		"regex": strings.Repeat("z", maxRelabelRuleBytes+1),
	})
	assertOversizeRefusal(t, monitorWithRelabelings(t, rules))
}

func TestAnOversizedRelabelRulePastTheChainByteCapStillRefusesTheEndpoint(t *testing.T) {
	var rules []any
	// Nine ~1 KiB rules: the chain budget binds partway through them, well
	// inside the 64-rule count ceiling, so this is the OTHER door.
	for i := range 9 {
		rules = append(rules, map[string]any{
			"action": "drop", "sourceLabels": []any{"__name__"},
			"regex": strconv.Itoa(i) + strings.Repeat("y", 1<<10),
		})
	}
	if len(rules) >= maxRelabelRules {
		t.Fatalf("precondition: %d filler rules must stay under the count ceiling of %d", len(rules), maxRelabelRules)
	}
	rules = append(rules, map[string]any{
		"action": "keep", "sourceLabels": []any{"__name__"},
		"regex": strings.Repeat("z", maxRelabelRuleBytes+1),
	})
	assertOversizeRefusal(t, monitorWithRelabelings(t, rules))
}

// assertOversizeRefusal is TestAnOversizedRelabelRuleRefusesTheEndpoint's
// verdict, applied to a monitor whose oversized rule sits past an aggregate
// ceiling: the endpoint yields nothing and says why.
func assertOversizeRefusal(t *testing.T, m *Monitor) {
	t.Helper()
	ep := m.Endpoints[0]
	if ep.Refused == "" || !strings.Contains(ep.Refused, relabelRefusedField) {
		t.Fatalf("Refused = %q, rules kept = %d, Ignored = %v: the oversized rule escaped the per-rule ceiling, "+
			"so the endpoint is served with its allowlist silently missing",
			ep.Refused, len(ep.MetricRelabelings), ep.Ignored)
	}
	if ep.Port != "" || ep.TargetPort != nil {
		t.Errorf("a refused endpoint still names a port: %q/%v", ep.Port, ep.TargetPort)
	}
	if ep.MetricRelabelings != nil {
		t.Errorf("a refused endpoint still carries %d rules", len(ep.MetricRelabelings))
	}
	if !slices.Contains(ep.Ignored, relabelOversizeIgnored) {
		t.Errorf("the refusal is not reported as an oversize: Ignored = %v", ep.Ignored)
	}
}

// The walk continuing past an aggregate ceiling must not make the REPORT grow
// with the unread tail: a capped chain is one entry, not one per rule the
// ceiling refused.
func TestACappedChainReportsOnceHoweverLongTheTailIs(t *testing.T) {
	var rules []any
	for i := range maxRelabelRules + 500 {
		rules = append(rules, map[string]any{
			"action": "drop", "sourceLabels": []any{"__name__"},
			"regex": "small" + strconv.Itoa(i),
		})
	}
	// Unsupported actions in the tail, which the walk now reaches and must stay
	// silent about: each would otherwise embed a DISTINCT tenant-chosen value.
	for i := range 20 {
		rules = append(rules, map[string]any{"action": "hashmod" + strconv.Itoa(i), "regex": "x"})
	}
	ep := monitorWithRelabelings(t, rules).Endpoints[0]
	if len(ep.MetricRelabelings) != maxRelabelRules {
		t.Errorf("kept %d rules, want the %d-rule prefix", len(ep.MetricRelabelings), maxRelabelRules)
	}
	if got := len(ep.Ignored); got != 1 || ep.Ignored[0] != relabelCappedIgnored {
		t.Errorf("Ignored = %v (%d entries), want exactly one %q", ep.Ignored, got, relabelCappedIgnored)
	}
}

// fillEchoBudget returns maxRelabelIgnored rules that each report a DISTINCT
// tenant-echo entry (a custom separator over two sourceLabels — the arm that is
// free-form under every CRD version), so the report's echo ceiling is exactly
// full before whatever follows them.
func fillEchoBudget() []any {
	var rules []any
	for i := range maxRelabelIgnored {
		rules = append(rules, map[string]any{
			"action": "drop", "sourceLabels": []any{"__name__", "job"},
			"separator": "sep" + strconv.Itoa(i), "regex": "x",
		})
	}
	return rules
}

// The walk's VERDICTS must survive a report whose echo ceiling is already full.
// They went through the same capped closure as the tenant echoes, so eight
// ordinary unsupported rules ahead of an oversized `keep` allowlist left the
// endpoint REFUSED — no targets — while its report, and so the one warning line
// an operator reads, named only the eight ordinary rules.
//
// Reverse-patch check: routing the oversize entry back through report() fails
// the first case.
func TestRefusalVerdictSurvivesAFullReport(t *testing.T) {
	rules := append(fillEchoBudget(), map[string]any{
		"action": "keep", "sourceLabels": []any{"__name__"},
		"regex": strings.Repeat("z", maxRelabelRuleBytes+1),
	})
	assertOversizeRefusal(t, monitorWithRelabelings(t, rules))
}

// The same for the aggregate chain cap, the documented fail-OPEN, whose report
// entry is the ONLY signal it has: the endpoint still yields targets, so
// nothing on /v1/explain says the tail of the chain is missing.
func TestCappedVerdictSurvivesAFullReport(t *testing.T) {
	rules := fillEchoBudget()
	for i := range maxRelabelRules + 6 {
		rules = append(rules, map[string]any{
			"action": "drop", "sourceLabels": []any{"__name__"}, "regex": "small" + strconv.Itoa(i),
		})
	}
	ep := monitorWithRelabelings(t, rules).Endpoints[0]
	if len(ep.MetricRelabelings) != maxRelabelRules {
		t.Fatalf("kept %d rules, want the %d-rule prefix", len(ep.MetricRelabelings), maxRelabelRules)
	}
	if !slices.Contains(ep.Ignored, relabelCappedIgnored) {
		t.Errorf("the chain was capped and the report does not say so: %v", ep.Ignored)
	}
}

// A repeated echo is one fact, not eight: eight identical `labeldrop` rules —
// an ordinary honest shape — must not spend the whole budget and push a
// DIFFERENT unsupported action out of the report.
func TestIdenticalUnsupportedRulesAreReportedOnce(t *testing.T) {
	var rules []any
	for range 2 * maxRelabelIgnored {
		rules = append(rules, map[string]any{"action": "labeldrop", "regex": "tmp_.*"})
	}
	rules = append(rules, map[string]any{"action": "hashmod", "sourceLabels": []any{"pod"}, "regex": "x"})
	ep := monitorWithRelabelings(t, rules).Endpoints[0]
	want := []string{"metricRelabelings.action=labeldrop", "metricRelabelings.action=hashmod"}
	if !slices.Equal(ep.Ignored, want) {
		t.Errorf("Ignored = %v, want %v", ep.Ignored, want)
	}
}

// A keep/drop regex that does not compile refuses the ENDPOINT at the parse
// door. The agent fails the whole scrape on it, and MergeMonitorEndpoint
// concatenates the chains of every monitor resolving to one URL — so admitting
// it let one tenant's typo fail another monitor's merged target on every agent
// while the counter, the warning and /v1/explain all read clean.
//
// Reverse-patch check: removing the compile in relabelChain admits the rule
// and this fails on the Refused assertion.
func TestUncompilableRelabelRegexRefusesTheEndpoint(t *testing.T) {
	ep := monitorWithRelabelings(t, []any{
		map[string]any{"action": "drop", "sourceLabels": []any{"__name__"}, "regex": "ok_.*"},
		map[string]any{"action": "drop", "sourceLabels": []any{"__name__"}, "regex": "("},
	}).Endpoints[0]
	if !strings.Contains(ep.Refused, relabelRefusedField) {
		t.Fatalf("Refused = %q: an uncompilable regex was admitted into the served chain (%d rules)",
			ep.Refused, len(ep.MetricRelabelings))
	}
	if ep.Port != "" || ep.MetricRelabelings != nil {
		t.Errorf("a refused endpoint still names a port or carries rules: %q, %d rules", ep.Port, len(ep.MetricRelabelings))
	}
	if !slices.Contains(ep.Ignored, relabelInvalidIgnored) {
		t.Errorf("the refusal is not reported as an invalid regex: %v", ep.Ignored)
	}
	// /v1/explain names the reason from RefusalReasons: Refused alone says
	// "metricRelabelings.regex", which an oversize rule refuses under too.
	if got := ep.RefusalReasons(); !slices.Equal(got, []string{relabelInvalidIgnored}) {
		t.Errorf("RefusalReasons = %v, want the invalid-regex verdict alone", got)
	}
}

// The compile is the AGENT's (kubemeta.CompileRelabelRegex): a regex that is
// only well-formed inside the anchoring wrap is well-formed, because that is
// the form the agent compiles, and a door that checked the bare regex would
// refuse a rule the agent applies without complaint.
func TestRelabelRegexIsValidatedInTheAgentsAnchoredForm(t *testing.T) {
	ep := monitorWithRelabelings(t, []any{
		map[string]any{"action": "drop", "sourceLabels": []any{"__name__"}, "regex": "a)|(b"},
		map[string]any{"action": "keep", "sourceLabels": []any{"__name__"}}, // "" = Prometheus' (.*)
	}).Endpoints[0]
	if ep.Refused != "" || len(ep.MetricRelabelings) != 2 {
		t.Errorf("Refused = %q, %d rules: a regex the agent compiles was refused", ep.Refused, len(ep.MetricRelabelings))
	}
}

// Only ADMITTED rules are compiled. A rule past the aggregate ceilings is never
// applied, so it cannot poison a scrape, and compiling every rule the walk
// visits would make the parse cost the CR's size rather than the chain budget.
func TestUncompilableRegexPastTheChainCapDoesNotRefuse(t *testing.T) {
	var rules []any
	for i := range maxRelabelRules {
		rules = append(rules, map[string]any{
			"action": "drop", "sourceLabels": []any{"__name__"}, "regex": "small" + strconv.Itoa(i),
		})
	}
	rules = append(rules, map[string]any{"action": "drop", "sourceLabels": []any{"__name__"}, "regex": "("})
	ep := monitorWithRelabelings(t, rules).Endpoints[0]
	if ep.Refused != "" || len(ep.MetricRelabelings) != maxRelabelRules {
		t.Errorf("Refused = %q, %d rules: the capped prefix must be served", ep.Refused, len(ep.MetricRelabelings))
	}
}

// A keep/drop that READS a label an earlier, unapplied mutating rule writes is
// not the rule the CR wrote: Prometheus runs the replace first, kubescrape does
// not run it at all, so the keep tests the user's regex against a label that
// was never rewritten — and drops every series while the target reports up=1.
// The only report used to be "action=replace has no effect". The endpoint is
// REFUSED instead, the oversize ceiling's trade.
//
// Reverse-patch check: dropping the writes.touches check admits the keep and
// this fails on the Refused assertion.
func TestKeepAfterUnsupportedReplaceRefusesTheEndpoint(t *testing.T) {
	ep := monitorWithRelabelings(t, []any{
		map[string]any{"action": "replace", "sourceLabels": []any{"__name__"},
			"regex": "(.*)_total", "targetLabel": "base", "replacement": "$1"},
		map[string]any{"action": "keep", "sourceLabels": []any{"base"}, "regex": "http_requests"},
	}).Endpoints[0]
	if ep.Refused != relabelDependentField {
		t.Fatalf("Refused = %q, rules = %+v: a keep reading an unapplied replace's target was applied alone",
			ep.Refused, ep.MetricRelabelings)
	}
	if ep.Port != "" || ep.MetricRelabelings != nil {
		t.Errorf("a refused endpoint still names a port or carries rules: %q, %d rules", ep.Port, len(ep.MetricRelabelings))
	}
	if !slices.Contains(ep.Ignored, relabelDependentIgnored) || !slices.Contains(ep.Ignored, "metricRelabelings.action=replace") {
		t.Errorf("the refusal and its cause are not both reported: %v", ep.Ignored)
	}
	if got := ep.RefusalReasons(); !slices.Equal(got, []string{relabelDependentIgnored}) {
		t.Errorf("RefusalReasons = %v, want the dependency verdict alone", got)
	}
}

// The dependency tracking must be PRECISE on the ordinary chains, or it refuses
// endpoints that do exactly what their CR says. Each case pairs an unapplied
// mutating rule with a filter it does or does not touch.
func TestRelabelDependencyIsTrackedPerLabel(t *testing.T) {
	cases := []struct {
		name      string
		mutate    map[string]any
		filterSrc []any
		dependent bool
	}{
		{"replace into another label, drop on __name__", map[string]any{"action": "replace", "targetLabel": "team"},
			[]any{"__name__"}, false},
		{"replace into __name__, drop on __name__", map[string]any{"action": "Replace", "targetLabel": "__name__"},
			[]any{"__name__"}, true},
		{"hashmod target read by the filter", map[string]any{"action": "hashmod", "targetLabel": "shard", "modulus": int64(4)},
			[]any{"shard"}, true},
		{"lowercase target among two sources", map[string]any{"action": "lowercase", "targetLabel": "env"},
			[]any{"__name__", "env"}, true},
		{"templated replace target", map[string]any{"action": "replace", "targetLabel": "${1}"},
			[]any{"__name__"}, true},
		{"labeldrop of an unrelated label", map[string]any{"action": "labeldrop", "regex": "tmp_.*"},
			[]any{"__name__"}, false},
		{"labeldrop of the filtered label", map[string]any{"action": "labeldrop", "regex": "tmp_.*"},
			[]any{"tmp_x"}, true},
		{"labelkeep that keeps the filtered label", map[string]any{"action": "LabelKeep", "regex": "__name__|job"},
			[]any{"job"}, false},
		{"labelkeep that removes the filtered label", map[string]any{"action": "labelkeep", "regex": "__name__|job"},
			[]any{"pod"}, true},
		{"labeldrop whose regex does not compile", map[string]any{"action": "labeldrop", "regex": "("},
			[]any{"__name__"}, true},
		{"labelmap writes names from the data", map[string]any{"action": "labelmap", "regex": "__meta_(.+)"},
			[]any{"__name__"}, true},
		{"a filter action kubescrape skips writes nothing", map[string]any{"action": "keepequal", "targetLabel": "job"},
			[]any{"job"}, false},
		{"a filter with no sourceLabels reads nothing", map[string]any{"action": "labelmap"}, nil, false},
		// No action at all is Prometheus' default, replace.
		{"action-less rule's target read by the filter", map[string]any{"sourceLabels": []any{"__name__"}, "targetLabel": "shard"},
			[]any{"shard"}, true},
		{"action-less rule into another label", map[string]any{"targetLabel": "team"},
			[]any{"__name__"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			filter := map[string]any{"action": "drop", "regex": "x"}
			if c.filterSrc != nil {
				filter["sourceLabels"] = c.filterSrc
			}
			ep := monitorWithRelabelings(t, []any{c.mutate, filter}).Endpoints[0]
			if got := ep.Refused == relabelDependentField; got != c.dependent {
				t.Errorf("refused as dependent = %v, want %v (Refused %q, %d rules, Ignored %v)",
					got, c.dependent, ep.Refused, len(ep.MetricRelabelings), ep.Ignored)
			}
			if !c.dependent && len(ep.MetricRelabelings) != 1 {
				t.Errorf("an independent filter was not applied: %d rules", len(ep.MetricRelabelings))
			}
		})
	}
}

// Order is the whole of the dependency: a mutating rule AFTER the filter cannot
// have changed what the filter read.
func TestMutatingRuleAfterTheFilterIsNotADependency(t *testing.T) {
	ep := monitorWithRelabelings(t, []any{
		map[string]any{"action": "keep", "sourceLabels": []any{"service"}, "regex": "api"},
		map[string]any{"action": "replace", "targetLabel": "service", "replacement": "renamed"},
	}).Endpoints[0]
	if ep.Refused != "" || len(ep.MetricRelabelings) != 1 {
		t.Errorf("Refused = %q, %d rules: a filter BEFORE the mutation depends on nothing", ep.Refused, len(ep.MetricRelabelings))
	}
}

// A rule with no `action` is Prometheus' default replace, and the report says
// so rather than echoing an empty value: "action=" reads as a malformed rule,
// "action=replace(default)" names what the rule does and why it was skipped.
//
// Reverse-patch check: noting "" instead of "replace" admits the keep, and
// this fails on the Refused assertion.
func TestActionlessRuleIsTheDefaultReplace(t *testing.T) {
	ep := monitorWithRelabelings(t, []any{
		map[string]any{"sourceLabels": []any{"__name__"}, "targetLabel": "svc"},
		map[string]any{"action": "keep", "sourceLabels": []any{"svc"}, "regex": "api"},
	}).Endpoints[0]
	if ep.Refused != relabelDependentField {
		t.Fatalf("Refused = %q, %d rules: a keep reading an action-less replace's target was applied alone",
			ep.Refused, len(ep.MetricRelabelings))
	}
	if !slices.Contains(ep.Ignored, relabelDefaultActionIgnored) || slices.Contains(ep.Ignored, "metricRelabelings.action=") {
		t.Errorf("Ignored = %v, want the default replace named", ep.Ignored)
	}
}

// costlyRegex is `a{1000}` repeated to just under the 8 KiB per-rule byte
// ceiling — with room for a `__name__` source label, so a keep carrying it is
// ADMITTED by every byte bound — and ~1.17M compiled instructions, measured at
// ~350 MB allocated and ~52 MB retained per compile.
var costlyRegex = strings.Repeat("a{1000}", 1168)

// parseAlloc is how many bytes one Parse of rules allocates.
func parseAlloc(t *testing.T, rules []any) (*Monitor, uint64) {
	t.Helper()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	m := monitorWithRelabelings(t, rules)
	runtime.ReadMemStats(&after)
	return m, after.TotalAlloc - before.TotalAlloc
}

// An UNAPPLIED labeldrop's regex is compiled to evaluate which labels it
// removes, and the compiled programs are held for the whole walk. Bounded by
// bytes alone, 64 such rules held ~3.3 GB at once on every parse of the CR —
// the initial LIST included — in a singleton the chart ships uncapped. Its
// cost is measured first; over the budget the predicate is not compiled and
// the walk fails CLOSED, refusing the dependent filter.
//
// Reverse-patch check: dropping the cost measure in relabelWrites.note compiles
// the predicate — the allocation assertion fails by two orders of magnitude.
func TestCostlyLabeldropPredicateIsRefusedWithoutBeingCompiled(t *testing.T) {
	rules := []any{}
	for range 3 {
		rules = append(rules, map[string]any{"action": "labeldrop", "regex": costlyRegex})
	}
	rules = append(rules, map[string]any{"action": "keep", "sourceLabels": []any{"__name__"}, "regex": "up"})
	m, alloc := parseAlloc(t, rules)
	ep := m.Endpoints[0]
	if ep.Refused != relabelDependentField {
		t.Errorf("Refused = %q, %d rules: a filter behind an unevaluable labeldrop must fail closed",
			ep.Refused, len(ep.MetricRelabelings))
	}
	if alloc > 16<<20 {
		t.Errorf("one Parse allocated %d MB: the labeldrop predicates were compiled", alloc>>20)
	}
}

// The predicates' cost is bounded in TOTAL, like an applied chain's: several
// individually-admissible predicates cannot add up to what one refused one
// would have held.
func TestLabeldropPredicatesShareOneCostBudget(t *testing.T) {
	// Each ~10k: under the per-regex bound, over it in pairs.
	pred := "(?:abcdefghij){1000}"
	rules := []any{
		map[string]any{"action": "labeldrop", "regex": pred},
		map[string]any{"action": "labeldrop", "regex": "x" + pred},
		map[string]any{"action": "drop", "sourceLabels": []any{"unrelated"}, "regex": "x"},
	}
	var w relabelWrites
	for _, r := range rules[:2] {
		w.note("labeldrop", "", r.(map[string]any)["regex"].(string))
	}
	if !w.all || len(w.preds) != 1 {
		t.Errorf("all = %v with %d predicates held: the second must overflow the shared budget", w.all, len(w.preds))
	}
	if ep := monitorWithRelabelings(t, rules).Endpoints[0]; ep.Refused != relabelDependentField {
		t.Errorf("Refused = %q: a filter behind predicates over the shared budget must fail closed", ep.Refused)
	}
}

// An ADMITTED keep/drop is validated at the parse door, and validation used to
// be a full compile on the informer's goroutine: the same regex cost ~0.4 s and
// ~350 MB per endpoint per Parse — 128 endpoints to a monitor, so ~50 s for one
// event. It is measured instead, refused as oversize (the remedy is the byte
// ceiling's: shrink it), and never compiled — and since a refused endpoint is
// never served, no agent compiles it either.
//
// Reverse-patch check: validating with kubemeta.CompileRelabelRegex's compile
// alone admits both endpoints, and every assertion fails.
func TestAdmittedRegexOverTheCostBudgetRefusesTheEndpointWithoutCompiling(t *testing.T) {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	ep := map[string]any{"port": "http", "metricRelabelings": []any{
		map[string]any{"action": "keep", "sourceLabels": []any{"__name__"}, "regex": costlyRegex},
	}}
	m, err := Parse(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "sm", "namespace": "tenant"},
		"spec":     map[string]any{"selector": map[string]any{}, "endpoints": []any{ep, ep}},
	}})
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	if n := RelabelRuleBytes(costlyRegex, []string{"__name__"}); n > maxRelabelRuleBytes {
		t.Fatalf("the rule is %d bytes: the byte ceiling, not the cost bound, would refuse it", n)
	}
	for i, e := range m.Endpoints {
		if e.Refused != relabelRefusedField || e.Port != "" || e.MetricRelabelings != nil {
			t.Errorf("endpoint %d: Refused = %q, port %q, %d rules: an admitted regex over the cost budget was served",
				i, e.Refused, e.Port, len(e.MetricRelabelings))
		}
		if got := e.RefusalReasons(); !slices.Equal(got, []string{relabelOversizeIgnored}) {
			t.Errorf("endpoint %d: RefusalReasons = %v, want the oversize verdict", i, got)
		}
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 16<<20 {
		t.Errorf("one Parse allocated %d MB: the admitted regexes were compiled", alloc>>20)
	}
}

// Rules that each fit the cost budget but overflow it together keep the
// chain's PREFIX, exactly as the byte budget does — the aggregate ceiling fails
// open for the tail, the per-rule one refuses.
func TestChainCostOverflowKeepsThePrefix(t *testing.T) {
	ep := monitorWithRelabelings(t, []any{
		map[string]any{"action": "drop", "sourceLabels": []any{"__name__"}, "regex": "(?:abcdefghij){1000}"},
		map[string]any{"action": "drop", "sourceLabels": []any{"__name__"}, "regex": "(?:klmnopqrst){1000}"},
		map[string]any{"action": "drop", "sourceLabels": []any{"__name__"}, "regex": "small"},
	}).Endpoints[0]
	if ep.Refused != "" || len(ep.MetricRelabelings) != 1 {
		t.Fatalf("Refused = %q, %d rules: want the one-rule prefix served", ep.Refused, len(ep.MetricRelabelings))
	}
	if !slices.Contains(ep.Ignored, relabelCappedIgnored) {
		t.Errorf("Ignored = %v: the capped tail is not reported", ep.Ignored)
	}
}
