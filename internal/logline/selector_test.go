package logline

import "testing"

func TestParseSelectors(t *testing.T) {
	lookup := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}
	set, err := ParseSelectors(
		[]string{"level=error", "env!=dev"},
		[]string{"msg=timeout"},
	)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		vals map[string]string
		want bool
	}{
		{"all match", map[string]string{"level": "error", "env": "prod", "msg": "read timeout"}, true},
		{"exact miss", map[string]string{"level": "info", "env": "prod", "msg": "read timeout"}, false},
		{"negation excludes", map[string]string{"level": "error", "env": "dev", "msg": "timeout"}, false},
		{"regex miss", map[string]string{"level": "error", "env": "prod", "msg": "ok"}, false},
	}
	for _, c := range cases {
		var ctx MatchContext
		if got := set.Match(lookup(c.vals), &ctx); got != c.want {
			t.Errorf("%s: match = %v, want %v", c.name, got, c.want)
		}
	}

	if _, err := ParseSelectors([]string{"bogus"}, nil); err == nil {
		t.Error("selector without operator: want error")
	}
}

func TestEmptySelectorsMatchAll(t *testing.T) {
	set, err := ParseSelectors(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var ctx MatchContext
	if !set.Match(func(string) string { return "" }, &ctx) {
		t.Error("empty selector set should match everything")
	}
}
