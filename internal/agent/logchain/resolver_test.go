package logchain

import (
	"math"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/pkg/logattrs"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// attrString must render EXACTLY as pcommon.Value.AsString: the resolver's
// cross-pipeline parity rests on it, and the float arm diverged once — a bare
// FormatFloat('f') against pcommon's ES6 rendering meant the same lifted field
// minted label "0.0000005" through the lifted-resource path and "5e-7" through
// the record-attribute path, two series for one value.
func TestAttrStringMatchesPcommon(t *testing.T) {
	for _, f := range []float64{
		0, 1, -1, 2.5, math.Copysign(0, -1), 5e-7, 9.99e-7, 1e-6, 1e21,
		1e21 - (1 << 16), -1e21, 1.5e300, 5e-324, 123456789.123456,
		math.Inf(1), math.Inf(-1), math.NaN(),
	} {
		if got, want := attrString(f), pcommon.NewValueDouble(f).AsString(); got != want {
			t.Errorf("attrString(%v) = %q, pcommon renders %q", f, got, want)
		}
	}
	if got, want := attrString(int64(42)), pcommon.NewValueInt(42).AsString(); got != want {
		t.Errorf("int64: attrString %q vs pcommon %q", got, want)
	}
	if got, want := attrString(true), pcommon.NewValueBool(true).AsString(); got != want {
		t.Errorf("bool: attrString %q vs pcommon %q", got, want)
	}
	if got, want := attrString("x"), pcommon.NewValueStr("x").AsString(); got != want {
		t.Errorf("string: attrString %q vs pcommon %q", got, want)
	}
}

// maps builds a record/resource attribute pair from literal string maps.
func maps(rec, res map[string]string) (pcommon.Map, pcommon.Map) {
	r, s := pcommon.NewMap(), pcommon.NewMap()
	for k, v := range rec {
		r.PutStr(k, v)
	}
	for k, v := range res {
		s.PutStr(k, v)
	}
	return r, s
}

// SeverityKey is available to RULES only. It is synthesized from a field that
// carries the CURRENT record's severity, so folding the check down into
// Resolver.label — where it would be shared by labels, values and rules — makes
// every log-derived metric configured with `__severity__` mint a label out of
// whichever record the resolver happens to be pointed at, with no key of that
// name existing anywhere in production and the whole suite still green.
func TestSeverityKeyIsRuleOnly(t *testing.T) {
	rec, res := maps(map[string]string{"http.status": "500"}, map[string]string{"k8s.pod.name": "web-abc"})
	r := New()
	r.Set(rec, res, "error")

	if got := r.RuleFn()(SeverityKey); got != "error" {
		t.Errorf("RuleFn(%s) = %q, want %q: keep/drop rules select on the enriched severity",
			SeverityKey, got, "error")
	}
	if got := r.LabelFn()(SeverityKey); got != "" {
		t.Errorf("LabelFn(%s) = %q, want empty: no such attribute exists in production, so a label built from it is the previous record's severity leaking into metric cardinality",
			SeverityKey, got)
	}
	if v, ok, present := r.ValueFn()(SeverityKey); ok || present {
		t.Errorf("ValueFn(%s) = %v (ok=%v present=%v), want no value and no attribute: severity is not a number, and reporting it PRESENT would stop a metric reading the line's own field",
			SeverityKey, v, ok, present)
	}
	// The synthetic key must not shadow ordinary lookups either.
	if got := r.RuleFn()("http.status"); got != "500" {
		t.Errorf("RuleFn(http.status) = %q, want 500", got)
	}
	if got := r.RuleFn()("k8s.pod.name"); got != "web-abc" {
		t.Errorf("RuleFn falls through to the resource: got %q, want web-abc", got)
	}
}

// A real attribute literally named __severity__ (a log line that happens to
// carry the key) still resolves for labels: the ban is on SYNTHESIZING one,
// not on a key the record genuinely has.
func TestSeverityKeyFromARealAttributeStillResolves(t *testing.T) {
	rec, res := maps(map[string]string{SeverityKey: "from-the-line"}, nil)
	r := New()
	r.Set(rec, res, "error")
	if got := r.LabelFn()(SeverityKey); got != "from-the-line" {
		t.Errorf("LabelFn = %q, want the record's own attribute value", got)
	}
	// ...and the synthetic value wins for rules, which is what `__severity__`
	// means there.
	if got := r.RuleFn()(SeverityKey); got != "error" {
		t.Errorf("RuleFn = %q, want the enriched severity %q", got, "error")
	}
}

// Record attributes shadow resource attributes: the line's own field is more
// specific than the pod/unit/ARM-resource identity it was collected under.
func TestRecordAttributesWinOverResource(t *testing.T) {
	rec, res := maps(
		map[string]string{"service.name": "from-the-line", "only.record": "r"},
		map[string]string{"service.name": "from-the-pod", "only.resource": "s"},
	)
	r := New()
	r.Set(rec, res, "")

	for _, tc := range []struct{ key, want string }{
		{"service.name", "from-the-line"},
		{"only.record", "r"},
		{"only.resource", "s"},
		{"absent", ""},
	} {
		if got := r.LabelFn()(tc.key); got != tc.want {
			t.Errorf("LabelFn(%s) = %q, want %q", tc.key, got, tc.want)
		}
		if got := r.RuleFn()(tc.key); got != tc.want {
			t.Errorf("RuleFn(%s) = %q, want %q", tc.key, got, tc.want)
		}
	}
}

// Set re-points the resolver at the next record. One Resolver is kept per
// flush/convert and reused for every record in it, so anything remembered from
// the previous record is a cross-record attribute leak: record N's severity or
// pod name landing on record N+1's metric labels and rule decisions.
func TestSetRepointsWithoutStaleState(t *testing.T) {
	r := New()
	label, rule := r.LabelFn(), r.RuleFn() // bound ONCE, as the callers do

	rec1, res1 := maps(map[string]string{"tenant": "a"}, map[string]string{"k8s.pod.name": "pod-a"})
	r.Set(rec1, res1, "error")
	if got := label("tenant"); got != "a" {
		t.Fatalf("first record: tenant = %q", got)
	}

	// The next record has neither key.
	rec2, res2 := maps(nil, map[string]string{"k8s.pod.name": "pod-b"})
	r.Set(rec2, res2, "info")
	if got := label("tenant"); got != "" {
		t.Errorf("tenant = %q after Set; the previous record's attribute leaked into this one's labels", got)
	}
	if got := label("k8s.pod.name"); got != "pod-b" {
		t.Errorf("k8s.pod.name = %q, want pod-b; the closures must read the CURRENT record", got)
	}
	if got := rule(SeverityKey); got != "info" {
		t.Errorf("rule severity = %q, want info; the previous record's severity survived Set", got)
	}

	// Closures captured before the first Set must see the same state — they
	// are bound at construction precisely so no closure is allocated per
	// record.
	if label("k8s.pod.name") != r.LabelFn()("k8s.pod.name") {
		t.Error("a closure taken before Set disagrees with one taken after: the resolver is not re-pointed in place")
	}
}

// Metric VALUES are numeric. A log line's field arrives as a JSON number (Int
// or Double after logattrs' typing) or as a plain string when it came from
// logfmt or a regex capture, and all three have to work — while a non-numeric
// field must be REJECTED rather than silently observed as 0, which would drag
// every histogram/summary it feeds toward zero.
func TestValueFnParsesNumbersAndRejectsTheRest(t *testing.T) {
	rec := pcommon.NewMap()
	rec.PutInt("bytes", 4096)
	rec.PutDouble("duration", 1.25)
	rec.PutStr("count_str", "42")
	rec.PutStr("float_str", "0.5")
	rec.PutStr("negative", "-3")
	rec.PutStr("msg", "handled request")
	rec.PutStr("empty", "")
	rec.PutBool("ok", true)
	res := pcommon.NewMap()
	res.PutStr("k8s.pod.name", "web-abc")

	r := New()
	r.Set(rec, res, "")
	value := r.ValueFn()

	for _, tc := range []struct {
		key  string
		want float64
	}{
		{"bytes", 4096}, {"duration", 1.25}, {"count_str", 42},
		{"float_str", 0.5}, {"negative", -3},
	} {
		got, ok, present := value(tc.key)
		if !ok || got != tc.want || !present {
			t.Errorf("ValueFn(%s) = %v (ok=%v present=%v), want %v", tc.key, got, ok, present, tc.want)
		}
	}
	for _, key := range []string{"msg", "empty", "k8s.pod.name", "absent"} {
		if got, ok, _ := value(key); ok {
			t.Errorf("ValueFn(%s) = %v (ok=true); a non-numeric field must not be observed as a number", key, got)
		}
	}
	// A bool is not a number: pcommon renders it "true", which ParseFloat
	// rejects.
	if got, ok, _ := value("ok"); ok {
		t.Errorf("ValueFn(ok) = %v (ok=true), want rejected", got)
	}

	// present is the LABEL tier's answer for the same key, from the same walk:
	// it is what decides whether the log-metrics engine reads the line instead,
	// so it must agree with LabelFn on every rank, EMPTY renderings included.
	label := r.LabelFn()
	for _, key := range []string{
		"bytes", "duration", "count_str", "msg", "ok", "empty", "k8s.pod.name", "absent",
	} {
		_, _, present := value(key)
		if want := label(key) != ""; present != want {
			t.Errorf("ValueFn(%s) present=%v, LabelFn resolves %q: the two halves of a metric would name different attributes",
				key, present, label(key))
		}
	}
}

// LowerSeverity is the ONE normalisation for SeverityKey selectors. There were
// five copies and only the tailer's handled WARNING, so `drop __severity__=warn`
// matched a container log and not the journal entry beside it. The constant
// fast paths exist to keep the hot path allocation-free, so they must agree
// with the general fallback rather than diverging from it.
func TestLowerSeverity(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", ""},
		// Fast paths, both spellings.
		{"TRACE", "trace"}, {"trace", "trace"},
		{"DEBUG", "debug"}, {"debug", "debug"},
		{"INFO", "info"}, {"info", "info"},
		{"WARN", "warn"}, {"warn", "warn"},
		// WARNING is NOT WARN: the copy that lacked this case lowered it via a
		// path that never ran, so the same line selected differently depending
		// on which pipeline carried it.
		{"WARNING", "warning"}, {"warning", "warning"},
		{"ERROR", "error"}, {"error", "error"},
		{"FATAL", "fatal"}, {"fatal", "fatal"},
		// General fallback: mixed case, unknown words, non-letters untouched.
		{"Warning", "warning"}, {"WaRn", "warn"}, {"Error", "error"},
		{"CRITICAL", "critical"}, {"Notice", "notice"},
		{"EMERG-1", "emerg-1"}, {"Level 5", "level 5"},
		{"lowercase-unknown", "lowercase-unknown"},
	} {
		if got := LowerSeverity(tc.in); got != tc.want {
			t.Errorf("LowerSeverity(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	// WARN and WARNING must stay DISTINCT: collapsing them would silently
	// widen every `__severity__=warn` drop rule to cover WARNING too.
	if LowerSeverity("WARNING") == LowerSeverity("WARN") {
		t.Error("WARNING and WARN normalised to the same value; a rule selecting one would select both")
	}

	// Every constant fast path must produce exactly what the general fallback
	// would, or the two disagree for the inputs that take the fast path.
	for _, in := range []string{
		"TRACE", "DEBUG", "INFO", "NOTICE", "WARN", "WARNING",
		"ERROR", "ERR", "CRIT", "ALERT", "EMERG", "FATAL",
	} {
		want := asciiLowerReference(in)
		if got := LowerSeverity(in); got != want {
			t.Errorf("fast path for %q returned %q; the general lowering gives %q", in, got, want)
		}
	}
}

// journald stamps the syslog severity texts lowercase (journald.go); every one
// must take an allocation-free path — the journal is the pipeline whose
// allocation-free severity lowering this function replaced, and five of its
// eight texts used to miss the constant list and allocate per entry. Any
// already-lower input is free now (the fallback returns it unchanged), which
// this pins alongside the constants.
func TestLowerSeverityIsAllocationFreeForLowercase(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	inputs := []string{
		"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug",
		"lowercase-unknown", "level 5",
	}
	if avg := testing.AllocsPerRun(100, func() {
		for _, in := range inputs {
			_ = LowerSeverity(in)
		}
	}); avg != 0 {
		t.Errorf("lowercase severities allocate: %v allocs/run", avg)
	}
}

func asciiLowerReference(s string) string {
	out := []byte(s)
	for i := range out {
		if 'A' <= out[i] && out[i] <= 'Z' {
			out[i] += 'a' - 'A'
		}
	}
	return string(out)
}

// The closures are bound at construction so evaluating a record allocates
// nothing: the tailer's per-line budget (BenchmarkIngestLine, 0 allocs/op)
// depends on it.
func TestResolverIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	rec, res := maps(map[string]string{"tenant": "a"}, map[string]string{"k8s.pod.name": "web-abc"})
	r := New()
	label, rule := r.LabelFn(), r.RuleFn()
	if allocs := testing.AllocsPerRun(200, func() {
		r.Set(rec, res, "info")
		_ = label("tenant")
		_ = rule(SeverityKey)
		_ = rule("k8s.pod.name")
	}); allocs != 0 {
		t.Fatalf("resolving a record allocates %v times; the closures are bound once precisely to avoid that", allocs)
	}
}

// A logAttributes rule that lifts a line field onto the RESOURCE must be
// visible to rules and metric labels. Every producer but the tailer builds
// its exported resource before running the chain, so it sees them; the tailer
// resolves against the file's base resource with the lifted attributes still
// pending. Without them ranking between record and resource, one config
// selected differently depending on which pipeline carried the line.
func TestLiftedResourceAttributesRankBetweenRecordAndResource(t *testing.T) {
	rec := pcommon.NewMap()
	rec.PutStr("only.record", "r")
	rec.PutStr("both", "from-record")
	res := pcommon.NewMap()
	res.PutStr("only.resource", "R")
	res.PutStr("both", "from-resource")
	res.PutStr("tenant", "base")

	r := New()
	r.Set(rec, res, "info")
	r.SetLifted([]logattrs.Attr{
		{Key: "tenant", Val: "lifted"},
		{Key: "both", Val: "from-lifted"},
		{Key: "count", Val: int64(7)},
		{Key: "ratio", Val: 1.5},
		{Key: "on", Val: true},
	})

	label := r.LabelFn()
	for _, tc := range []struct{ key, want string }{
		{"only.record", "r"},
		{"only.resource", "R"},
		{"both", "from-record"}, // the record still wins
		{"tenant", "lifted"},    // lifted beats the base resource
		{"count", "7"},          // typed values render like pcommon
		{"ratio", "1.5"},
		{"on", "true"},
		{"absent", ""},
	} {
		if got := label(tc.key); got != tc.want {
			t.Errorf("label(%q) = %q; want %q", tc.key, got, tc.want)
		}
	}

	if v, ok, present := r.ValueFn()("count"); !ok || v != 7 || !present {
		t.Errorf("value(count) = %v, %v, present=%v; want 7, true, true", v, ok, present)
	}

	// Set clears them: a resolver re-pointed at the next record must not carry
	// the previous line's lifted attributes.
	r.Set(rec, res, "info")
	if got := label("tenant"); got != "base" {
		t.Errorf("after Set, label(tenant) = %q; the previous line's lifted attributes leaked", got)
	}
}

func TestRecordSeverityNumberBands(t *testing.T) {
	lr := plog.NewLogRecord()
	for _, tc := range []struct {
		n    plog.SeverityNumber
		want string
	}{
		{plog.SeverityNumberUnspecified, ""},
		{plog.SeverityNumberTrace, "trace"},
		{plog.SeverityNumberDebug4, "debug"},
		{plog.SeverityNumberInfo, "info"},
		{plog.SeverityNumberWarn3, "warn"},
		{plog.SeverityNumberError2, "error"},
		{plog.SeverityNumberFatal4, "fatal"},
		{plog.SeverityNumber(99), ""},
	} {
		lr.SetSeverityNumber(tc.n)
		lr.SetSeverityText("")
		if got := RecordSeverity(lr); got != tc.want {
			t.Errorf("number %d: %q, want %q", tc.n, got, tc.want)
		}
	}
	// Text always wins over the number.
	lr.SetSeverityNumber(plog.SeverityNumberError)
	lr.SetSeverityText("WARNING")
	if got := RecordSeverity(lr); got != "warning" {
		t.Errorf("text override: %q, want warning", got)
	}
}

// __severity__ resolves to the PRODUCER's own severity word, lowercased, and a
// rule literal that selects records today must keep selecting them.
//
// There is a real defect underneath this, and it is deliberately NOT fixed
// here: the two vocabularies that reach the key disagree on the same level.
// journald stamps the syslog word (priority 4 is "warning", 3 "err", 2 "crit")
// while logenrich.Apply overwrites SeverityText with enrich's word ("warn",
// "error", "fatal") whenever it parses a level out of the BODY, so `drop
// __severity__=warn` applies to whichever priority-4 entries happen to be
// parseable. Operators spell around it with a regex — `__severity__=~^(warn|
// warning)$` — and the exported severity_number is unaffected.
//
// Canonicalising the key onto enrich's six level words looks like the fix and
// is a far worse trade, which is what this test exists to keep out. It
// un-matches every other spelling with no startup error, no -check-config
// complaint and no counter, and it does so in the direction that costs data:
// the NEGATED form INVERTS. `{action: drop, match: ["__severity__!=err"]}` —
// ship only the priority-3 journal entries — stops matching "err" on any record
// at all and drops the node's entire journal. Fixing the split honestly means
// canonicalising BOTH sides of the comparison, which the exact-selector DSL
// does not do (and a regex selector could not be canonicalised at all), and no
// startup guard can cover the pod-annotation rules of kubescrape.io/logs, which
// compile from cluster data and are deliberately IGNORED when malformed rather
// than refused.
func TestRecordSeverityKeepsProducerSpellingsSelectable(t *testing.T) {
	lr := plog.NewLogRecord()
	// journald's severity() texts, with the numbers it stamps beside them: six
	// of the eight are spellings no canonical vocabulary contains.
	for _, tc := range []struct {
		text string
		n    plog.SeverityNumber
	}{
		{"emerg", plog.SeverityNumberFatal3},
		{"alert", plog.SeverityNumberFatal2},
		{"crit", plog.SeverityNumberFatal},
		{"err", plog.SeverityNumberError},
		{"warning", plog.SeverityNumberWarn},
		{"notice", plog.SeverityNumberInfo2},
		{"info", plog.SeverityNumberInfo},
		{"debug", plog.SeverityNumberDebug},
	} {
		lr.SetSeverityText(tc.text)
		lr.SetSeverityNumber(tc.n)
		if got := RecordSeverity(lr); got != tc.text {
			t.Errorf("__severity__ for the journald text %q = %q: every rule naming %q silently stops "+
				"selecting, and its negated form inverts into matching everything", tc.text, got, tc.text)
		}
	}

	// The inversion itself, through the real rule engine rather than by
	// inspection of the resolved string.
	f, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", Match: []string{"__severity__!=err"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := New()
	keep := func(text string, n plog.SeverityNumber, body string) bool {
		rec := plog.NewLogRecord()
		rec.SetSeverityText(text)
		rec.SetSeverityNumber(n)
		r.Set(pcommon.NewMap(), pcommon.NewMap(), RecordSeverity(rec))
		return f.Keep(r.RuleFn(), body)
	}
	if !keep("err", plog.SeverityNumberError, "sshd: authentication failure") {
		t.Error("`drop __severity__!=err` dropped the priority-3 entry it exists to keep: the selector " +
			"inverted, so it now drops every entry on the node and nothing reports it")
	}
	// The other half of the rule must still work, or the assertion above would
	// pass for a filter that keeps everything.
	if keep("info", plog.SeverityNumberInfo, "started unit") {
		t.Error("`drop __severity__!=err` kept a priority-6 entry: the rule is not being applied at all")
	}
}
