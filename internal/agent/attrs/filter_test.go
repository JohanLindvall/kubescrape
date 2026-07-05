package attrs

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestNilFilterKeepsAll(t *testing.T) {
	f, err := NewFilter("", "")
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
		enable, disable string
		key             string
		want            bool
	}{
		// Enable only: everything else is dropped.
		{`k8s\.pod\..*`, "", "k8s.pod.name", true},
		{`k8s\.pod\..*`, "", "k8s.deployment.name", false},
		// Anchored: a prefix match is not enough.
		{`k8s\.pod`, "", "k8s.pod.name", false},
		// Disable only: everything else is kept.
		{"", `k8s\.pod\.label\..*`, "k8s.pod.label.app", false},
		{"", `k8s\.pod\.label\..*`, "k8s.pod.name", true},
		// Both: must match enable AND not disable.
		{`k8s\..*`, `k8s\.pod\.label\..*`, "k8s.pod.name", true},
		{`k8s\..*`, `k8s\.pod\.label\..*`, "k8s.pod.label.app", false},
		{`k8s\..*`, `k8s\.pod\.label\..*`, "service.name", false},
		// Comma-separated lists.
		{`k8s\.pod\.name,k8s\.namespace\.name`, "", "k8s.namespace.name", true},
		{`k8s\.pod\.name,k8s\.namespace\.name`, "", "k8s.pod.uid", false},
	}
	for _, c := range cases {
		f, err := NewFilter(c.enable, c.disable)
		if err != nil {
			t.Fatalf("NewFilter(%q, %q): %v", c.enable, c.disable, err)
		}
		if got := f.Keep(c.key); got != c.want {
			t.Errorf("enable=%q disable=%q key=%q: keep=%v, want %v", c.enable, c.disable, c.key, got, c.want)
		}
	}
}

func TestFilterInvalidRegex(t *testing.T) {
	if _, err := NewFilter("(", ""); err == nil {
		t.Fatal("invalid enable pattern must error")
	}
	if _, err := NewFilter("", "["); err == nil {
		t.Fatal("invalid disable pattern must error")
	}
}

func TestFilterApply(t *testing.T) {
	f, err := NewFilter("", `k8s\.pod\.label\..*,url\.full`)
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
