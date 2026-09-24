package servicemonitors

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// fuzzSeeds are FuzzParse's and FuzzParsePodMonitor's shared corpus: the
// structural oddities, plus the shapes the parse-door invariants are about —
// every secret-bearing field, a relabel chain, an oversized keep rule behind a
// FULL chain (the position the walk once stopped measuring at) and behind a
// wall of unsupported rules, a keep that depends on an unapplied replace, and
// an over-long endpoint list. epKey is the kind's endpoint-list key, so the
// same shapes reach both parsers.
func fuzzSeeds(epKey string) [][]byte {
	var seeds [][]byte
	for _, s := range []string{
		`{"metadata":{"namespace":"monitoring","name":"m1"},"spec":{"selector":{"matchLabels":{"app":"web"}},"namespaceSelector":{"matchNames":["a","b"]},"EPS":[{"port":"http","path":"/metrics","scheme":"https"}]}}`,
		`{"metadata":{"namespace":"ns","name":"n"},"spec":{"selector":{"matchExpressions":[{"key":"k","operator":"In","values":["v"]}]},"namespaceSelector":{"any":true},"EPS":[{"targetPort":8080},{"targetPort":"named"}]}}`,
		`{"spec":{}}`,
		`{"spec":{"selector":{"matchExpressions":[{"key":"k","operator":"BadOp"}]}}}`,
		`{"spec":{"EPS":[{"port":123}]}}`, // wrong type for port
		`{"spec":{"selector":"not-an-object"}}`,
		`{"spec":"not-an-object"}`,
		`{}`,
		`{"spec":{"EPS":"not-a-list"}}`,
		`{"spec":{"namespaceSelector":{"any":"notabool"}}}`,
		`{"spec":{"selector":{"matchLabels":{"k":123}}}}`,
		`{"spec":{"EPS":[{"targetPort":{"nested":"object"}}]}}`,
		`{"metadata":{"namespace":"t","name":"s"},"spec":{"selector":{},"EPS":[{"port":"m","bearerTokenSecret":{"name":"a/b","key":"k"}}]}}`,
	} {
		seeds = append(seeds, []byte(strings.ReplaceAll(s, `"EPS"`, strconv.Quote(epKey))))
	}
	rule := func(action, regex string, src ...string) map[string]any {
		r := map[string]any{"action": action, "regex": regex}
		if len(src) > 0 {
			l := make([]any, len(src))
			for i, v := range src {
				l[i] = v
			}
			r["sourceLabels"] = l
		}
		return r
	}
	oversized := rule("keep", strings.Repeat("a|", maxRelabelRuleBytes/2+1), "__name__")
	var full, walled []any
	for i := range maxRelabelRules {
		full = append(full, rule("drop", "m"+strconv.Itoa(i), "__name__"))
	}
	full = append(full, oversized)
	for i := range 8 {
		walled = append(walled, rule("replace", "x", "l"+strconv.Itoa(i)))
	}
	walled = append(walled, oversized)
	var many []any
	for i := range maxEndpointsPerMonitor + 2 {
		many = append(many, map[string]any{"port": "p" + strconv.Itoa(i)})
	}
	withRules := func(rules ...any) map[string]any {
		ep := everySecretField()
		ep["metricRelabelings"] = rules
		return ep
	}
	for _, eps := range [][]any{
		{everySecretField()},
		{withRules(rule("keep", "up|http_.*", "__name__"), rule("Drop", "debug", "level"))},
		{withRules(full...)},
		{withRules(walled...)},
		{withRules(map[string]any{"action": "replace", "targetLabel": "svc", "regex": "(.*)"}, rule("keep", "api", "svc"))},
		{withRules(rule("keep", "a)|(b", "__name__"))},
		many,
	} {
		b, err := json.Marshal(map[string]any{
			"metadata": map[string]any{"namespace": "tenant", "name": "seed"},
			"spec":     map[string]any{"selector": map[string]any{}, epKey: eps},
		})
		if err != nil {
			panic(err)
		}
		seeds = append(seeds, b)
	}
	return seeds
}

// checkParsed is the parse door's contract, asserted over whatever a fuzzer
// hands it. These are the position- and combination-dependent invariants this
// package has repeatedly got wrong — an oversize verdict that depended on
// where in the chain the rule sat, a verdict pushed out of a full report — so
// they are checked on arbitrary input rather than on the fixtures each fix
// came with:
//
//   - the endpoint list, each relabel chain and its bytes are within their
//     ceilings, and every applied rule is a lower-case keep/drop whose regex
//     compiles in the agent's form;
//   - every bounded string is within its ceiling — a secret ref measured NET
//     of the "<namespace>/" the parser prefixes AFTER the bound is enforced;
//   - a refused endpoint resolves to nothing, retains none of its bounded
//     strings, and names the reason for every field it was refused for;
//   - every secret ref is "<the monitor's namespace>/name/key" with exactly
//     one slash after the namespace (the /v1/scrape-auth allowlist's shape);
//   - the reports stay bounded.
func checkParsed(t *testing.T, ns string, eps []Endpoint) {
	t.Helper()
	if len(eps) > maxEndpointsPerMonitor {
		t.Fatalf("%d endpoints kept, over the %d ceiling", len(eps), maxEndpointsPerMonitor)
	}
	echoes := 0
	for _, f := range IgnoredFields(eps) {
		if isValueEcho(f) {
			echoes++
		}
	}
	if echoes > maxIgnoredFields {
		t.Fatalf("the monitor's report carries %d tenant echoes, over maxIgnoredFields", echoes)
	}
	for i := range eps {
		ep := &eps[i]
		if len(ep.Ignored) > 96 {
			t.Fatalf("endpoint %d reports %d entries; the per-endpoint report must stay a constant", i, len(ep.Ignored))
		}
		if len(ep.MetricRelabelings) > maxRelabelRules {
			t.Fatalf("endpoint %d applies %d relabel rules, over the %d ceiling", i, len(ep.MetricRelabelings), maxRelabelRules)
		}
		chain := 0
		for _, r := range ep.MetricRelabelings {
			chain += RelabelRuleBytes(r.Regex, r.SourceLabels)
			if r.Action != "keep" && r.Action != "drop" {
				t.Fatalf("endpoint %d applies a %q rule; only lower-case keep/drop are applied", i, r.Action)
			}
			if _, err := kubemeta.CompileRelabelRegex(r.Regex); err != nil {
				t.Fatalf("endpoint %d applies a rule the agent cannot compile: %v", i, err)
			}
		}
		if chain > maxRelabelChainBytes {
			t.Fatalf("endpoint %d's chain is %d bytes, over the %d ceiling", i, chain, maxRelabelChainBytes)
		}
		secret := map[*string]bool{}
		for _, p := range ep.secretRefs() {
			secret[p] = true
			if *p == "" {
				continue
			}
			rest, ok := strings.CutPrefix(*p, ns+"/")
			if !ok || strings.Count(rest, "/") != 1 {
				t.Fatalf("endpoint %d secret ref %q is not %q + name/key", i, *p, ns+"/")
			}
		}
		for _, f := range ep.boundedFields() {
			limit := f.max
			if secret[f.value] && *f.value != "" {
				limit += len(ns) + 1
			}
			if len(*f.value) > limit {
				t.Fatalf("endpoint %d %s is %d bytes, over its ceiling of %d", i, f.name, len(*f.value), limit)
			}
		}
		if ep.Refused == "" {
			continue
		}
		if ep.Port != "" || ep.TargetPort != nil || ep.MetricRelabelings != nil {
			t.Fatalf("refused endpoint %d (%s) still resolves: port=%q targetPort=%v rules=%d",
				i, ep.Refused, ep.Port, ep.TargetPort, len(ep.MetricRelabelings))
		}
		for _, f := range ep.boundedFields() {
			if *f.value != "" {
				t.Fatalf("refused endpoint %d retains %s", i, f.name)
			}
		}
		reasons := ep.RefusalReasons()
		for field := range strings.SplitSeq(ep.Refused, ",") {
			if !slices.ContainsFunc(reasons, func(r string) bool { return strings.HasPrefix(r, field+"(") }) {
				t.Fatalf("endpoint %d is refused for %q, but no report entry names it: %v", i, field, ep.Ignored)
			}
		}
	}
}

// fuzzJSONObject decodes a fuzz input the way an informer would hand it over,
// skipping what no informer can deliver.
func fuzzJSONObject(t *testing.T, data []byte) *unstructured.Unstructured {
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		t.Skip() // only structurally valid JSON reaches an informer
	}
	return &unstructured.Unstructured{Object: obj}
}

// FuzzParse feeds arbitrary JSON (decoded into the unstructured object shape a
// dynamic informer would deliver) to Parse: it never panics, and a monitor it
// returns has a selector, answers ServiceNamespaces, and meets checkParsed.
func FuzzParse(f *testing.F) {
	for _, s := range fuzzSeeds(endpointsKey("ServiceMonitor")) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		u := fuzzJSONObject(t, data)
		m, err := Parse(u)
		if err != nil {
			return
		}
		if m.Selector == nil {
			t.Fatalf("Parse returned a monitor with a nil selector for %q", data)
		}
		_ = m.ServiceNamespaces() // must not panic
		checkParsed(t, u.GetNamespace(), m.Endpoints)
	})
}

// FuzzParsePodMonitor is FuzzParse for the other kind. The two share the
// parse skeleton, which is exactly why the kind-specific half — the spec type,
// its endpoint-list key — needs its own fuzzer: a door the shared oracle is
// never pointed at is a door nothing checks.
func FuzzParsePodMonitor(f *testing.F) {
	for _, s := range fuzzSeeds(endpointsKey("PodMonitor")) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		u := fuzzJSONObject(t, data)
		m, err := ParsePodMonitor(u)
		if err != nil {
			return
		}
		if m.Selector == nil {
			t.Fatalf("ParsePodMonitor returned a monitor with a nil selector for %q", data)
		}
		_ = m.PodNamespaces() // must not panic
		checkParsed(t, u.GetNamespace(), m.Endpoints)
	})
}

// FuzzIndexUpsert exercises the whole Index lifecycle (Upsert parses, Delete,
// All) against fuzzed objects so the concurrency-free store paths never panic
// on malformed input.
func FuzzIndexUpsert(f *testing.F) {
	f.Add([]byte(`{"metadata":{"namespace":"n","name":"a"},"spec":{"selector":{}}}`))
	f.Add([]byte(`{"spec":{"selector":{"matchExpressions":[{"key":"k","operator":"Bad"}]}}}`))
	f.Add([]byte(`{}`))
	ix := NewIndex()
	f.Fuzz(func(t *testing.T, data []byte) {
		var obj map[string]any
		if err := json.Unmarshal(data, &obj); err != nil {
			t.Skip()
		}
		u := &unstructured.Unstructured{Object: obj}
		if err := ix.Upsert(u); err != nil {
			return
		}
		_ = ix.All()
		ix.Delete(u.GetNamespace(), u.GetName())
	})
}
