package attrs

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestNilFilterKeepsAll(t *testing.T) {
	f, err := NewFilterFromLists(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if f != nil {
		t.Fatal("empty filter should be nil")
	}
	if !f.Keep("anything") {
		t.Fatal("nil filter must keep everything")
	}
}

func TestFilterSemantics(t *testing.T) {
	cases := []struct {
		enable, disable []string
		key             string
		want            bool
	}{
		// Enable only: everything else is dropped.
		{[]string{`k8s\.pod\..*`}, nil, "k8s.pod.name", true},
		{[]string{`k8s\.pod\..*`}, nil, "k8s.deployment.name", false},
		// Anchored: a prefix match is not enough.
		{[]string{`k8s\.pod`}, nil, "k8s.pod.name", false},
		// Disable only: everything else is kept.
		{nil, []string{`k8s\.pod\.label\..*`}, "k8s.pod.label.app", false},
		{nil, []string{`k8s\.pod\.label\..*`}, "k8s.pod.name", true},
		// Both: must match enable AND not disable.
		{[]string{`k8s\..*`}, []string{`k8s\.pod\.label\..*`}, "k8s.pod.name", true},
		{[]string{`k8s\..*`}, []string{`k8s\.pod\.label\..*`}, "k8s.pod.label.app", false},
		{[]string{`k8s\..*`}, []string{`k8s\.pod\.label\..*`}, "service.name", false},
		// Multiple patterns: any may match.
		{[]string{`k8s\.pod\.name`, `k8s\.namespace\.name`}, nil, "k8s.namespace.name", true},
		{[]string{`k8s\.pod\.name`, `k8s\.namespace\.name`}, nil, "k8s.pod.uid", false},
		// A pattern may contain a comma — the whole reason the list form
		// replaced the comma-separated one, which split this into fragments
		// RE2 compiled as literal braces and silently dropped everything.
		{[]string{`k8s\.pod\.label\..{1,3}`}, nil, "k8s.pod.label.app", true},
		{[]string{`k8s\.pod\.label\..{1,3}`}, nil, "k8s.pod.label.toolong", false},
	}
	for _, c := range cases {
		f, err := NewFilterFromLists(c.enable, c.disable)
		if err != nil {
			t.Fatalf("NewFilterFromLists(%q, %q): %v", c.enable, c.disable, err)
		}
		if got := f.Keep(c.key); got != c.want {
			t.Errorf("enable=%q disable=%q key=%q: keep=%v, want %v", c.enable, c.disable, c.key, got, c.want)
		}
	}
}

func TestFilterInvalidRegex(t *testing.T) {
	if _, err := NewFilterFromLists([]string{"("}, nil); err == nil {
		t.Fatal("invalid enable pattern must error")
	}
	if _, err := NewFilterFromLists(nil, []string{"["}); err == nil {
		t.Fatal("invalid disable pattern must error")
	}
}

func TestFilterApply(t *testing.T) {
	f, err := NewFilterFromLists(nil, []string{`k8s\.pod\.label\..*`, `url\.full`})
	if err != nil {
		t.Fatal(err)
	}
	res := pcommon.NewResource()
	res.Attributes().PutStr("k8s.pod.name", "p")
	res.Attributes().PutStr("k8s.pod.label.app", "x")
	res.Attributes().PutStr("url.full", "http://x")
	f.Apply(res)
	if res.Attributes().Len() != 1 {
		t.Fatalf("attrs = %v", res.Attributes().AsRaw())
	}
	if _, ok := res.Attributes().Get("k8s.pod.name"); !ok {
		t.Fatal("kept attribute missing")
	}
}
