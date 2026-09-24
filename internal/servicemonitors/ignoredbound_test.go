package servicemonitors

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// THE ATTACK: relabelChain walks the endpoint's metricRelabelings ONCE and
// produces TWO outputs, and the ceilings bounded only one. The `action` and
// `separator` arms report a TENANT-CHOSEN string verbatim and `continue` above
// the applied-rule ceiling — which counts APPLIED rules, so a chain that
// applies none can never reach it however it is tuned. `separator` is the arm
// that works against every prometheus-operator version, being free-form text
// where newer CRDs enum-validate `action`.
//
// Measured before the bound: a ~1.1 MiB CR fragment of 20,000 keep rules with
// distinct separators yielded 0 applied rules and 20,000 Ignored entries —
// 1,180,000 bytes retained on the endpoint in the index for as long as the CR
// exists, and a Warn line of the same order re-emitted on every edit of the CR
// (warnIgnored is gated on UpsertChanged's news, not on a first sighting).
//
// Reverse-patch check: replacing report(...) with a plain append in the two
// arms restores 20,000 entries and this fails on the first assertion.
func TestUnsupportedRelabelRulesCannotGrowTheReport(t *testing.T) {
	const n = 20000
	rules := make([]any, 0, n)
	for i := range n {
		rules = append(rules, map[string]any{
			"action": "keep", "regex": ".*",
			// TWO sourceLabels, so the separator is actually written and the
			// rule is skipped (with one or none it is applied as written).
			"sourceLabels": []any{"__name__", "job"},
			// Distinct per rule, so nothing dedupes them away.
			"separator": strconv.Itoa(i) + strings.Repeat("s", 30),
		})
	}
	ep := monitorWithRelabelings(t, rules).Endpoints[0]
	if len(ep.MetricRelabelings) != 0 {
		t.Fatalf("a custom separator must suppress the rule; %d applied", len(ep.MetricRelabelings))
	}
	if len(ep.Ignored) > maxRelabelIgnored+1 {
		t.Fatalf("the report is unbounded: %d entries (ceiling %d + the summary)", len(ep.Ignored), maxRelabelIgnored)
	}
	if !slices.Contains(ep.Ignored, relabelReportCappedIgnored) {
		t.Errorf("the remainder is not reported at all: %v", ep.Ignored)
	}
	bytes := 0
	for _, s := range ep.Ignored {
		bytes += len(s)
		if len(s) > len("metricRelabelings.separator=")+maxIgnoredValueBytes+len("...") {
			t.Errorf("a report entry echoes the tenant's value unclipped (%d bytes)", len(s))
		}
	}
	if bytes > 1<<10 {
		t.Errorf("the report retains %d bytes on the endpoint", bytes)
	}
}

// The same for the `action` arm, and the clip is asserted to keep the
// DIAGNOSIS: "action=Replace" is the whole answer to "why is my rule ignored?",
// so the value is clipped rather than dropped.
func TestUnsupportedActionIsReportedClippedNotVerbatim(t *testing.T) {
	long := strings.Repeat("R", 4096)
	ep := monitorWithRelabelings(t, []any{
		map[string]any{"action": "Replace", "regex": ".*"},
		map[string]any{"action": long, "regex": ".*"},
	}).Endpoints[0]
	if !slices.Contains(ep.Ignored, "metricRelabelings.action=Replace") {
		t.Errorf("a short action value must still be reported in full: %v", ep.Ignored)
	}
	for _, s := range ep.Ignored {
		if strings.Contains(s, long) {
			t.Errorf("the 4 KiB action value is echoed verbatim into the report and the log line")
		}
	}
	if !slices.Contains(ep.Ignored, "metricRelabelings.action="+strings.Repeat("R", maxIgnoredValueBytes)+"...") {
		t.Errorf("the clipped entry is missing or spelled differently: %v", ep.Ignored)
	}
}

// A clip must not cut a rune in half: the entry is written into a log record
// and a JSON document, where half a rune is a mojibake byte.
func TestClipValueCutsOnARuneBoundary(t *testing.T) {
	for _, s := range []string{strings.Repeat("é", 200), strings.Repeat("𝄞", 200), strings.Repeat("あ", 200)} {
		got := clipValue(s)
		if !strings.HasSuffix(got, "...") {
			t.Fatalf("not clipped: %q", got)
		}
		if body := strings.TrimSuffix(got, "..."); !strings.HasPrefix(s, body) || len(body) > maxIgnoredValueBytes {
			t.Errorf("clip is not a valid prefix within the ceiling: %d bytes", len(body))
		} else if strings.ContainsRune(body, '�') {
			t.Errorf("clip cut a rune in half: %q", body)
		}
	}
}

// One level up: the per-endpoint report is bounded, but a monitor's ENDPOINT
// LIST multiplies it — maxEndpointsPerMonitor bounds the list at 128, far above
// what a person reads — and IgnoredFields' whole output is joined into ONE log
// record by warnIgnored. Every endpoint here contributes DISTINCT entries, so
// nothing dedupes: unbounded, this is the same growth one level higher.
func TestIgnoredFieldsAcrossEndpointsIsBounded(t *testing.T) {
	eps := make([]Endpoint, 0, 2000)
	for i := range 2000 {
		eps = append(eps, Endpoint{Ignored: []string{
			"metricRelabelings.action=" + strconv.Itoa(i),
			"metricRelabelings.separator=" + strconv.Itoa(i),
		}})
	}
	got := IgnoredFields(eps)
	if len(got) > maxIgnoredFields+1 {
		t.Fatalf("IgnoredFields returned %d entries; the joined warning line grows with the endpoint list", len(got))
	}
	if !slices.Contains(got, ignoredFieldsCapped) {
		t.Errorf("the remainder is not reported: %v", got)
	}
	if n := len(strings.Join(got, ",")); n > 8<<10 {
		t.Errorf("the joined warning line is %d bytes", n)
	}
	// Cutting happens AFTER the sort, so two identical CRs report identically
	// whatever order their endpoints were walked in.
	if !slices.IsSorted(got[:len(got)-1]) {
		t.Errorf("the kept prefix is not sorted, so it depends on walk order: %v", got)
	}
}

// A short report is untouched — the bound must not turn an ordinary
// partially-applied CR into an unreadable one.
func TestOrdinaryIgnoredReportIsUntouched(t *testing.T) {
	got := IgnoredFields([]Endpoint{{Ignored: []string{"oauth2", "params", "oauth2"}}})
	if !slices.Equal(got, []string{"oauth2", "params"}) {
		t.Errorf("got %v", got)
	}
}

// A bound on BYTES that charges only characters is walked past by a list of
// EMPTY strings. Measured through the real parser and json.Marshal: ONE keep
// rule with 500,000 empty sourceLabels — a ~1.5 MiB CR, inside etcd's object
// limit — was charged 2 bytes (its regex), admitted by both ceilings, and
// marshalled to 1,500,049 bytes in EVERY target the monitor resolves to.
//
// Reverse-patch check: dropping relabelLabelBytes from RelabelRuleBytes
// re-admits the rule and this fails. RelabelRuleBytes is also the merge door's
// accounting (internal/scrape's appendRelabelChain), so this pins both.
func TestEmptySourceLabelsAreChargedByTheChainCeiling(t *testing.T) {
	labels := make([]any, 500000)
	for i := range labels {
		labels[i] = ""
	}
	ep := monitorWithRelabelings(t, []any{
		map[string]any{"action": "keep", "regex": ".*", "sourceLabels": labels},
	}).Endpoints[0]
	if len(ep.MetricRelabelings) != 0 {
		b, _ := json.Marshal(ep.MetricRelabelings)
		t.Fatalf("a rule with 500k empty sourceLabels was admitted: %d bytes per target", len(b))
	}
	if !slices.Contains(ep.Ignored, relabelOversizeIgnored) {
		t.Errorf("the refusal is not reported: %v", ep.Ignored)
	}
}

// separatorFlood is n endpoints each reporting eight DISTINCT separator echoes
// (two sourceLabels, so the separator is written and the rule skipped) — the
// arm that is free-form text under every CRD version.
func separatorFlood(n int) []any {
	return echoFlood(n, func(v string) map[string]any {
		return map[string]any{
			"action": "drop", "sourceLabels": []any{"__name__", "job"}, "separator": v, "regex": "x",
		}
	})
}

// actionFlood is the same with DISTINCT unsupported `action` values, which an
// older, non-enum-validating CRD admits. `action=` sorts ahead of every
// `metricRelabelings.regex(...)` verdict, where `separator=` sorts after them.
func actionFlood(n int) []any {
	return echoFlood(n, func(v string) map[string]any { return map[string]any{"action": v, "regex": "x"} })
}

func echoFlood(n int, rule func(value string) map[string]any) []any {
	var eps []any
	for i := range n {
		var rules []any
		for j := range 8 {
			rules = append(rules, rule("v"+strconv.Itoa(i)+"_"+strconv.Itoa(j)))
		}
		eps = append(eps, map[string]any{"port": "http", "metricRelabelings": rules})
	}
	return eps
}

// The ceiling bounds the TENANT-VALUED echoes and nothing else. It used to cut
// the sorted list as one, and separator echoes sort under `m` — so a flood of
// them pushed out every refusal that sorts later: `path(oversize)` on an
// endpoint that yields NO targets, and a PodMonitor's own
// `podMetricsEndpoints(capped)` list cap, leaving the warning line naming only
// relabel clauses.
//
// Reverse-patch check: restoring the plain sort-then-cut fails all three cases.
func TestIgnoredFieldsCeilingNeverDropsARefusal(t *testing.T) {
	t.Run("ServiceMonitor path oversize", func(t *testing.T) {
		eps := append([]any{map[string]any{"port": "http", "path": "/" + strings.Repeat("p", maxEndpointPathBytes)}},
			separatorFlood(10)...)
		m, err := Parse(&unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"name": "sm", "namespace": "tenant"},
			"spec":     map[string]any{"selector": map[string]any{}, "endpoints": eps},
		}})
		if err != nil {
			t.Fatal(err)
		}
		assertReportKeeps(t, IgnoredFields(m.Endpoints), "path"+oversizeSuffix)
	})
	t.Run("ServiceMonitor relabel oversize", func(t *testing.T) {
		eps := append(actionFlood(9), map[string]any{"port": "http", "metricRelabelings": []any{
			map[string]any{"action": "keep", "sourceLabels": []any{"__name__"},
				"regex": strings.Repeat("z", maxRelabelRuleBytes+1)},
		}})
		m, err := Parse(&unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"name": "sm", "namespace": "tenant"},
			"spec":     map[string]any{"selector": map[string]any{}, "endpoints": eps},
		}})
		if err != nil {
			t.Fatal(err)
		}
		assertReportKeeps(t, IgnoredFields(m.Endpoints), relabelOversizeIgnored)
	})
	t.Run("PodMonitor endpoint-list cap", func(t *testing.T) {
		m, err := ParsePodMonitor(&unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"name": "pm", "namespace": "tenant"},
			"spec": map[string]any{"selector": map[string]any{},
				"podMetricsEndpoints": separatorFlood(maxEndpointsPerMonitor + 1)},
		}})
		if err != nil {
			t.Fatal(err)
		}
		assertReportKeeps(t, IgnoredFields(m.Endpoints), "podMetricsEndpoints"+cappedSuffix)
	})
}

// assertReportKeeps checks that a flooded report kept want, stayed bounded and
// sorted, and said that it cut something.
func assertReportKeeps(t *testing.T, got []string, want string) {
	t.Helper()
	if !slices.Contains(got, want) {
		t.Errorf("%q was cut from the report by tenant echoes: %d entries, ending %v", want, len(got), got[len(got)-3:])
	}
	if !slices.Contains(got, ignoredFieldsCapped) {
		t.Errorf("precondition: the flood did not reach the ceiling (%d entries)", len(got))
	}
	echoes := 0
	for _, f := range got {
		if isValueEcho(f) {
			echoes++
		}
	}
	if echoes > maxIgnoredFields {
		t.Errorf("%d tenant echoes kept, over the ceiling of %d", echoes, maxIgnoredFields)
	}
	if !slices.IsSorted(got[:len(got)-1]) {
		t.Errorf("the kept report is not sorted, so it depends on walk order")
	}
}
