package logline

import "testing"

func TestLineFilter(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

// The synthetic severity resolves ONLY through the caller's attribute lookup.
// It is not a line field, so a record with no severity — the plain-source and
// ingest case, where logchain.RecordSeverity returns "" — must not have a
// tenant's own `__severity__` in its body answer an operator's rule.
func TestSeverityIsNeverResolvedFromTheLine(t *testing.T) {
	t.Parallel()
	f, err := NewLineFilter([]LineRule{
		{Action: "drop", Match: []string{SeverityKey + "=debug"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// No severity on the record at all: the line must not supply one, in
	// either format the field scan understands.
	for _, line := range []string{
		`{"` + SeverityKey + `":"debug","msg":"hi"}`,
		SeverityKey + `=debug msg=hi`,
	} {
		if !f.Keep(nil, line) {
			t.Errorf("line %q must keep: the body cannot forge a severity", line)
		}
	}
	// A real severity still drops.
	sev := func(k string) string {
		if k == SeverityKey {
			return "debug"
		}
		return ""
	}
	if f.Keep(sev, `{"msg":"hi"}`) {
		t.Error("a record whose severity IS debug must drop")
	}
	// ...and a real non-matching severity is not overridden by the body.
	info := func(k string) string {
		if k == SeverityKey {
			return "info"
		}
		return ""
	}
	if !f.Keep(info, `{"`+SeverityKey+`":"debug","msg":"hi"}`) {
		t.Error("the record's own severity must win over the body")
	}
	// A rule set selecting only on the severity reads no line field, so the
	// line is never parsed as JSON/logfmt at all.
	if !f.keys.Empty() {
		t.Errorf("KeyIndex = %v, want empty: %s is not a line field", f.keys.keys, SeverityKey)
	}
}
