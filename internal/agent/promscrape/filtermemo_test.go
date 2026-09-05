package promscrape

import (
	"context"
	"strings"
	"testing"
	"time"
)

// retainedMemoBytes is what the session's memos actually hold alive, counted
// off the maps themselves rather than off the session's own accounting — a
// broken charge would agree with itself.
func retainedMemoBytes(s *filterSession) int {
	n := 0
	for k := range s.masks {
		n += len(k)
	}
	for k := range s.lblMatch {
		n += len(k.value)
	}
	return n
}

// The per-scrape memos are keyed by text the TARGET chooses, so their entry
// caps bound the wrong quantity: 100k series names of 16 KiB gzip to a ~2 MiB
// response and would retain 1.6 GB in the agent, the same amplification the
// parser's TYPE table had. This is the memo half of that bound.
//
// The second half of the test is the one that matters more: exhausting the memo
// must not change a single VERDICT. A memo is an optimization, and a bound that
// silently altered filtering would be a worse bug than the memory it saves.
func TestFilterSessionMemoIsBoundedByBytes(t *testing.T) {
	// Two rules so BOTH memos fill: the first exercises the name-mask memo, the
	// second (no `metrics`, so it is in every mask) makes every call resolve a
	// label value through the (matcher, value) memo.
	filter, err := newMetricFilter([]FilterRule{
		{Action: "keep", Metrics: "spared_.*"},
		{Action: "drop", Labels: map[string]string{"zone": "eu.*"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := filter.session()

	const (
		names   = 4000
		nameLen = 1000
	)
	for i := 0; i < names; i++ {
		name := strings.Repeat("n", nameLen-8) + pad8Scrape(i)
		value := strings.Repeat("v", nameLen-8) + pad8Scrape(i)
		if !s.Keep(name, []Label{{Name: "zone", Value: value}}) {
			t.Fatalf("series %d was dropped by a filter that only drops zone=eu.*", i)
		}
	}
	if retained := retainedMemoBytes(s); retained > maxMemoBytes {
		t.Fatalf("filter session memos retain %d bytes of target-chosen key text, want <= %d "+
			"(%d names + %d label values of %d bytes; the entry caps bound the COUNT, which is not a memory bound)",
			retained, maxMemoBytes, names, names, nameLen)
	}
	if len(s.masks) < 10 {
		t.Fatalf("only %d names memoized: the byte bound must stop the memo GROWING, not disable it", len(s.masks))
	}

	// Verdicts, after the budget is spent: identical to the unmemoized filter's.
	long := strings.Repeat("x", nameLen)
	for _, tc := range []struct {
		name   string
		labels []Label
		want   bool
	}{
		{"anything_" + long, []Label{{Name: "zone", Value: "eu-west"}}, false},
		{"anything_" + long, []Label{{Name: "zone", Value: "us-east"}}, true},
		{"spared_" + long, []Label{{Name: "zone", Value: "eu-west"}}, true}, // the name rule wins first
	} {
		if got := s.Keep(tc.name, tc.labels); got != tc.want {
			t.Fatalf("with the memo full, Keep(%.10s..., %v) = %v, want %v", tc.name, tc.labels, got, tc.want)
		}
		if got := filter.Keep(tc.name, tc.labels); got != tc.want {
			t.Fatalf("unmemoized Keep(%.10s..., %v) = %v, want %v", tc.name, tc.labels, got, tc.want)
		}
	}
}

// pad8Scrape renders a fixed-width suffix so every generated key is exactly the
// intended length.
func pad8Scrape(i int) string {
	const digits = "0123456789"
	var b [8]byte
	for j := 7; j >= 0; j-- {
		b[j] = digits[i%10]
		i /= 10
	}
	return string(b[:])
}

// retainedSplitMemoBytes is what the split batcher's memos hold alive, counted
// off the maps rather than off the batcher's own accounting — a broken charge
// would agree with itself.
func retainedSplitMemoBytes(b *splitBatcher) int {
	n := 0
	for k := range b.ruleMemo {
		n += len(k)
	}
	for k := range b.dropMemo {
		n += len(k.name)
	}
	return n
}

// The split batcher's two per-scrape memos are the filter session's siblings and
// were missing its byte bound: both are keyed by text the TARGET chooses, both
// deliberately survive reset() (the mappings are pure), so they accumulate for
// the whole scrape while every chunk around them flushes normally and every
// scrape counter reads healthy. A splitter-matched target serving 100k series
// names of 16 KiB — a body that gzips to a couple of MiB, the transport
// gunzipping it transparently — would retain ~1.6 GB against an agent whose
// chart limit is 512Mi.
//
// The entry cap does not help: it bounds how many names are remembered, and a
// name is bounded only by the parser's line bound. Nor does the parser's intern
// table absorb it — a name over maxInternedNameLen is never interned, so each
// long one is a fresh allocation the memo then pins.
//
// The second half is the one that matters more: exhausting the memo must not
// change a single VERDICT. A memo is an optimization, and a bound that silently
// altered routing or label dropping would be worse than the memory it saves.
func TestSplitBatcherMemosAreBoundedByBytes(t *testing.T) {
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodLabels: map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}},
		Rules: []SplitRule{{
			Metrics:    `kube_pod_.+`,
			GroupBy:    map[string]string{"namespace": "k8s.namespace.name", "pod": "k8s.pod.name"},
			DropLabels: `drop_.*`,
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	target := testTarget("http://kube-state-metrics.invalid/metrics")
	target.Pod.Labels = map[string]string{"app.kubernetes.io/name": "kube-state-metrics"}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{}, Exporter: &captureExporter{}, StartTime: time.Now(),
		Splitters: sp, Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
	})
	b := newSplitBatcher(context.Background(), s, target, sp[0], time.Now())

	labels := []Label{{Name: "namespace", Value: "ns1"}, {Name: "pod", Value: "pod1"}}
	_, rule, _ := b.route("kube_pod_info", labels)
	if rule == nil {
		t.Fatal("the splitter did not match its own rule's family")
	}

	// Names matching NO rule are memoized too (a nil rule is a verdict), which
	// is what makes every distinct series name of a matched target retained.
	const (
		names   = 4000
		nameLen = 1000
	)
	for i := 0; i < names; i++ {
		b.route(strings.Repeat("m", nameLen-8)+pad8Scrape(i), labels)
		b.dropped(rule, strings.Repeat("l", nameLen-8)+pad8Scrape(i))
	}
	if retained := retainedSplitMemoBytes(b); retained > maxMemoBytes {
		t.Fatalf("split batcher memos retain %d bytes of target-chosen key text, want <= %d "+
			"(%d series names + %d label names of %d bytes; the entry caps bound the COUNT, which is not a memory bound)",
			retained, maxMemoBytes, names, names, nameLen)
	}
	if len(b.ruleMemo) < 10 || len(b.dropMemo) < 10 {
		t.Fatalf("memo entries after the budget: rule=%d drop=%d — the bound must stop the memos GROWING, not disable them",
			len(b.ruleMemo), len(b.dropMemo))
	}

	// Verdicts, after the budget is spent: identical to the unmemoized ones.
	long := strings.Repeat("x", nameLen)
	if _, got, _ := b.route("kube_pod_"+long, labels); got != sp[0].ruleFor("kube_pod_"+long) || got == nil {
		t.Fatalf("with the memo full, route() no longer matches the rule ruleFor does")
	}
	if _, got, _ := b.route("unmatched_"+long, labels); got != nil {
		t.Fatalf("with the memo full, an unmatched family routed to %v, want the target's own resource", got)
	}
	for _, tc := range []struct {
		label string
		want  bool
	}{{"drop_" + long, true}, {"keep_" + long, false}} {
		if got := b.dropped(rule, tc.label); got != tc.want {
			t.Fatalf("with the memo full, dropped(%.10s...) = %v, want %v", tc.label, got, tc.want)
		}
	}
}
