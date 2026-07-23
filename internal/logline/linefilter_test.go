package logline

import "testing"

func TestLineFilter(t *testing.T) {
	f, err := NewLineFilter([]LineRule{
		{Action: "keep", MatchRegexp: []string{"__line__=KEEPME"}},
		{Action: "drop", Match: []string{"__severity__=debug"}},
		{Action: "drop", MatchRegexp: []string{"level=(debug|trace)"}}, // via line fields
	})
	if err != nil {
		t.Fatal(err)
	}
	sev := func(s string) func(string) string {
		return func(k string) string {
			if k == "__severity__" {
				return s
			}
			return ""
		}
	}
	if f.Keep(sev("debug"), "something") {
		t.Error("debug severity must drop")
	}
	if !f.Keep(sev("debug"), "KEEPME anyway") {
		t.Error("earlier keep rule must win over the drop")
	}
	if !f.Keep(sev("info"), "fine") {
		t.Error("no match must keep")
	}
	if f.Keep(nil, `{"level":"trace","msg":"x"}`) {
		t.Error("line-field selector must drop")
	}
	if !f.Keep(nil, `{"level":"warn","msg":"x"}`) {
		t.Error("non-matching line-field must keep")
	}

	// Nil filter keeps everything.
	var nilf *LineFilter
	if !nilf.Keep(nil, "x") {
		t.Error("nil filter must keep")
	}
}

func TestLineFilterSample(t *testing.T) {
	f, err := NewLineFilter([]LineRule{
		{Action: "keep", MatchRegexp: []string{"__line__=noisy"}, Sample: 0.25},
	})
	if err != nil {
		t.Fatal(err)
	}
	kept := 0
	for i := 0; i < 100; i++ {
		if f.Keep(nil, "noisy line") {
			kept++
		}
	}
	if kept != 25 {
		t.Errorf("kept = %d, want 25 (deterministic 1-in-4)", kept)
	}
	// Non-matching lines are untouched by the sampling rule.
	if !f.Keep(nil, "quiet line") {
		t.Error("non-matching line must keep")
	}
}

func TestLineFilterValidation(t *testing.T) {
	if _, err := NewLineFilter([]LineRule{{Action: "nope", Match: []string{"a=b"}}}); err == nil {
		t.Error("bad action must error")
	}
	if _, err := NewLineFilter([]LineRule{{Action: "drop"}}); err == nil {
		t.Error("empty match must error")
	}
	if _, err := NewLineFilter([]LineRule{{Action: "drop", Match: []string{"a=b"}, Sample: 0.5}}); err == nil {
		t.Error("sample on drop must error")
	}
	if _, err := NewLineFilter([]LineRule{{Action: "keep", Match: []string{"a=b"}, Sample: 1.5}}); err == nil {
		t.Error("sample > 1 must error")
	}
	if f, err := NewLineFilter(nil); err != nil || f != nil {
		t.Errorf("empty rules = %v, %v; want nil, nil", f, err)
	}
}
