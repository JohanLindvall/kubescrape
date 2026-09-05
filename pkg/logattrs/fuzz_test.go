package logattrs

import "testing"

// FuzzExtract drives the extractor over arbitrary lines. The line is the one
// input every producer hands it verbatim — a container log line, a journal
// entry, a Kubernetes event body, an Azure diagnostic record, a pushed OTLP log
// body — so a panic here is a crash reachable by anything that can write a
// log line, on the single sweep goroutine that serves every file on the node.
// The rules cover both decoders (a nested JSON path and top-level keys, which
// the logfmt reader also serves) and every target.
func FuzzExtract(f *testing.F) {
	e, err := New(&Config{Rules: []Rule{
		{Key: "user.id", Attribute: "enduser.id", Target: TargetResource},
		{Key: "region", Target: TargetScope},
		{Key: "level"},
		{Key: "status"},
		{Key: "msg"},
	}})
	if err != nil {
		f.Fatal(err)
	}
	for _, seed := range []string{
		`{"user":{"id":"u-42"},"region":"eu","level":"warn","status":503}`,
		`level=info msg="hello world" status=200 region=eu`,
		`plain text with no structure`,
		`{"level":`,
		`{"status":1e400,"level":"\ud800","msg":"\u"}`,
		`{"user":{"id":{"deep":[1,2,{"x":null}]}},"status":-0}`,
		`level="unterminated`,
		`msg=a\ b status=1 status=2 level=`,
		"",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, line string) {
		r := e.Extract(line)
		if n := len(r.Resource) + len(r.Scope) + len(r.Log); n > 5 {
			t.Fatalf("%d attributes from 5 rules", n)
		}
	})
}
