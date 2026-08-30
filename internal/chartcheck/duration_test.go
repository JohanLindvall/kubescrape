package chartcheck

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

// durationValues is every chart value the templates render into a
// flag.Duration, with the two exceptions to "a Go duration string and nothing
// else" spelled out per field. helm renders a value VERBATIM into the flag, so
// a value the binary's duration parse rejects is `invalid value for flag` +
// exit 2 in the container — a CrashLoopBackOff discovered after the rollout
// started, not a refused `helm upgrade`.
//
// The table is hand-written and TestDurationValuesAreSchemaGuarded derives the
// same set from the templates and the GENERATED docs/FLAGS.md, so a duration
// value added to the chart cannot skip it. That derivation is the point:
// agent.logsIdleClose carried HALF the guard its twins
// service/agent.selfMetricsInterval had (the integer bound, no string
// pattern), so `--set-string agent.logsIdleClose=abc` rendered
// `-logs-idle-close=abc` and the agent exited 2 on flag parse — a
// per-field audit found the twins one at a time and missed one.
var durationValues = []struct {
	path    string // dotted path into values.yaml
	flag    string // the flag it renders into
	zeroNum bool   // values.yaml documents "0 disables", so the bare NUMBER 0 is legal
	empty   bool   // the flag is `with`-guarded, so "" renders nothing at all
}{
	{path: "service.selfMetricsInterval", flag: "self-metrics-interval", zeroNum: true},
	{path: "service.waitTimeout", flag: "wait-timeout"},
	{path: "service.cacheTTL", flag: "cache-ttl"},
	{path: "service.otlp.timeout", flag: "otlp-timeout"},
	{path: "agent.selfMetricsInterval", flag: "self-metrics-interval", zeroNum: true},
	{path: "agent.otlp.timeout", flag: "otlp-timeout"},
	{path: "agent.otlp.retryBackoff", flag: "otlp-retry-backoff"},
	{path: "agent.logsIdleClose", flag: "logs-idle-close", zeroNum: true},
	{path: "agent.logsMetrics.interval", flag: "logs-metrics-interval", empty: true},
	{path: "agent.logsPollInterval", flag: "logs-poll-interval"},
	{path: "agent.scrapeInterval", flag: "scrape-interval"},
	{path: "agent.journald.flushInterval", flag: "journald-flush-interval"},
	{path: "agent.cgroupStats.interval", flag: "cgroup-stats-interval"},
	{path: "agent.cgroupStats.discoverInterval", flag: "cgroup-stats-discover-interval"},
	{path: "events.flushInterval", flag: "events-flush-interval"},
	{path: "events.positionInterval", flag: "events-position-interval"},
	{path: "serviceGraph.spanMetricsInterval", flag: "ingest-span-metrics-interval"},
}

// durationRefs are the schema definitions carrying the Go-duration grammar.
// They are the ONLY place it may live: seventeen hand-copied patterns cannot
// be fixed once, which is exactly how the twin above stayed open.
var durationRefs = map[string]bool{
	"#/definitions/duration":        true,
	"#/definitions/durationOrEmpty": true,
	"#/definitions/durationOrZero":  true,
}

// featureFlags turn on every workload whose duration flags are rendered behind
// an `if`, so one `helm template` covers all of durationValues.
var featureFlags = []string{
	"--set", "agent.journald.enabled=true",
	"--set", "agent.cgroupStats.enabled=true",
	"--set", "events.enabled=true",
	"--set", "serviceGraph.enabled=true",
	"--set", "serviceGraph.spanMetrics=true",
	// Not a feature: the trace tier refuses to RENDER without a token secret
	// (servicegraph.yaml), because the binary refuses to start without one.
	// Turning the tier on is what pulls this in.
	"--set", "serviceGraph.tokenSecret.name=kubescrape-service-graph",
}

// TestDurationValuesAreSchemaGuarded is the DRIFT guard: it derives, from the
// chart templates plus the generated flag docs, which values reach a
// flag.Duration, and fails unless each one $refs the shared duration grammar
// AND appears in durationValues. Adding `foo: 30s` to the chart with a bare
// `"type": "string"` schema entry fails here — the audit does not have to be
// repeated by hand, and the helm-driven test below cannot go stale.
//
// It needs no helm (it reads three files), so it runs on every machine.
func TestDurationValuesAreSchemaGuarded(t *testing.T) {
	durFlags := durationFlagsFromDocs(t)
	rendered := chartValuePaths(t)

	// Derive: a rendered flag that the docs report with a Go-duration default
	// is a flag.Duration, and the value feeding it must be schema-guarded.
	derived := map[string]string{} // value path -> flag
	for flag, paths := range rendered {
		if !durFlags[flag] {
			continue
		}
		for _, p := range paths {
			if strings.HasPrefix(p, unresolved) {
				t.Errorf("-%s takes a duration from an expression this scan cannot read (%s); teach chartValuePaths about it rather than leaving the value unguarded", flag, strings.TrimPrefix(p, unresolved))
				continue
			}
			derived[p] = flag
		}
	}
	if len(derived) == 0 {
		t.Fatal("derived no duration values at all; the template scan or the docs/FLAGS.md parse is broken, not the chart")
	}

	table := map[string]string{}
	for _, v := range durationValues {
		table[v.path] = v.flag
	}
	for path, flag := range derived {
		if got, ok := table[path]; !ok {
			t.Errorf("%s renders into -%s, a flag.Duration, but is missing from durationValues — add it there and give it a duration $ref in values.schema.json", path, flag)
		} else if got != flag {
			t.Errorf("durationValues says %s renders -%s; the templates render -%s", path, got, flag)
		}
	}
	for path, flag := range table {
		if _, ok := derived[path]; !ok {
			t.Errorf("durationValues lists %s (-%s) but no template renders it into a duration flag; drop the row, or docs/FLAGS.md is stale (regenerate with go test ./cmd/... -update-flags-doc)", path, flag)
		}
	}

	schema := readSchema(t)
	for path := range derived {
		node := schemaNode(t, schema, path)
		if node == nil {
			continue
		}
		ref, _ := node["$ref"].(string)
		if !durationRefs[ref] {
			t.Errorf("values.schema.json %s: $ref is %q, want one of the duration definitions — a bare type/pattern here is a copy that the next fix will miss", path, ref)
		}
	}

	// ...and the grammar stays in one place: a property that spells the unit
	// alternation out again is a copy by definition.
	forEachSchemaProperty(schema, func(path string, node map[string]any) {
		if pat, _ := node["pattern"].(string); strings.Contains(pat, "ns|us") {
			t.Errorf("values.schema.json %s inlines the duration grammar; $ref #/definitions/duration instead", path)
		}
	})
	for _, name := range []string{"duration", "durationOrEmpty", "durationOrZero"} {
		defs, _ := schema["definitions"].(map[string]any)
		if _, ok := defs[name]; !ok {
			t.Errorf("values.schema.json has no #/definitions/%s", name)
		}
	}
}

// TestDurationValuesRejectWhatTheFlagCannotParse drives the pinned helm over
// every value in durationValues: what time.ParseDuration accepts must render,
// and what it rejects must be refused by name at template time, whichever way
// the operator spells it.
//
// The refusal cannot be left to a numeric bound: helm's strvals keeps any
// token it cannot read as an int64 as a STRING, so `--set x=0.5`, `=abc` and
// `=1e1` never reach the number branch at all, and `--set-string` bypasses it
// by design. One helm run per candidate value covers the whole table, and the
// per-path assertion is that the error NAMES the path — a hole shows up as a
// missing name rather than a passing render.
func TestDurationValuesRejectWhatTheFlagCannotParse(t *testing.T) {
	helm := helmBin(t)
	template := func(args ...string) ([]byte, error) {
		full := append([]string{"template", "kubescrape", "../../charts/kubescrape",
			"--namespace", "monitoring"}, args...)
		full = append(full, featureFlags...)
		out, err := exec.Command(helm, full...).CombinedOutput()
		return out, err
	}

	// Accepted: every path takes a DISTINCT duration in one render, so each
	// one's flag can be checked individually — two paths share -otlp-timeout,
	// and a shared value would let one of them render nothing unnoticed.
	var args []string
	set := map[string]string{}
	for i, v := range durationValues {
		set[v.path] = fmt.Sprintf("%dh30m", i+1)
		args = append(args, "--set-string", v.path+"="+set[v.path])
	}
	out, err := template(args...)
	if err != nil {
		t.Fatalf("plain durations were refused: %v\n%s", err, out)
	}
	for _, v := range durationValues {
		if want := "- -" + v.flag + "=" + set[v.path] + "\n"; !bytes.Contains(out, []byte(want)) {
			t.Errorf("%s=%s rendered no %q", v.path, set[v.path], strings.TrimSpace(want))
		}
	}

	// The pattern must not be tighter than the flag: sub-second, fractional,
	// multi-unit and bare-zero forms all parse.
	for _, ok := range []string{"500ms", "1.5h", "1h30m", "90s", "0s", "0", "1500us"} {
		var a []string
		for _, v := range durationValues {
			a = append(a, "--set-string", v.path+"="+ok)
		}
		if out, err := template(a...); err != nil {
			t.Errorf("--set-string <every duration>=%s was refused; it is a valid Go duration: %s", ok, out)
		}
	}

	// ...and nothing time.ParseDuration rejects gets through, by either route
	// into the value. "-1m" parses, but every duration here is a period, a
	// timeout or a budget where a negative silently means off or immediate.
	for _, bad := range []struct{ setFlag, value string }{
		{"--set-string", "abc"}, // not a duration at all
		{"--set-string", "60"},  // unitless: the shape an operator reaches for first
		{"--set-string", "0.5"},
		{"--set-string", "-1m"}, // parses, but silently means off or immediate
		{"--set-string", "1d"},  // Go has no day unit
		{"--set-string", "5 s"}, // a space is not a separator
		{"--set", "60"},         // the number branch
		{"--set", "9223372036"}, // a plain count of seconds is not a duration
		{"--set", "0.5"},        // strvals cannot read these as int64, so they
		{"--set", "abc"},        // arrive as STRINGS and the numeric bound
		{"--set", "1e1"},        // never sees them
	} {
		var a []string
		var paths []string
		for _, v := range durationValues {
			a = append(a, bad.setFlag, v.path+"="+bad.value)
			paths = append(paths, v.path)
		}
		out, err := template(a...)
		if err == nil {
			t.Errorf("%s <every duration>=%q rendered fine; the binary's duration parse would reject it at startup:\n%s", bad.setFlag, bad.value, out)
			continue
		}
		for _, p := range paths {
			if !bytes.Contains(out, []byte(jsonPointer(p))) {
				t.Errorf("%s %s=%q was not refused (the error names every OTHER path): %s", bad.setFlag, p, bad.value, out)
			}
		}
	}

	// An empty value renders `-flag=`, which is `invalid duration ""` — legal
	// only where the template `with`-guards the flag and renders nothing.
	var emptyArgs, emptyPaths []string
	for _, v := range durationValues {
		if v.empty {
			continue
		}
		emptyArgs = append(emptyArgs, "--set-string", v.path+"=")
		emptyPaths = append(emptyPaths, v.path)
	}
	out, err = template(emptyArgs...)
	if err == nil {
		t.Errorf("an empty value rendered fine for every path; `-flag=` is `invalid duration \"\"`:\n%s", out)
	} else {
		for _, p := range emptyPaths {
			if !bytes.Contains(out, []byte(jsonPointer(p))) {
				t.Errorf("an empty %s was not refused: %s", p, out)
			}
		}
	}
	for _, v := range durationValues {
		if !v.empty {
			continue
		}
		out, err := template("--set-string", v.path+"=")
		if err != nil {
			t.Errorf("%s is `with`-guarded, so \"\" must stay legal: %v\n%s", v.path, err, out)
		} else if bytes.Contains(out, []byte("- -"+v.flag+"=")) {
			t.Errorf("%s=\"\" rendered a -%s flag anyway", v.path, v.flag)
		}
	}
}

// TestDocumentedZeroRendersTheSameFlagEitherWay covers the values.yaml
// promise "0 disables". 0 is what an operator copies, and YAML reads a bare 0
// as a NUMBER — a string-only schema refused the whole install before
// rendering anything ("got number, want string"). Both forms must stay legal
// AND render the same pods, or `--set x=0` and a values file carrying "0"
// would disagree; and a bare non-zero number must NOT be legal, since only
// the special-cased 0 parses without a unit.
func TestDocumentedZeroRendersTheSameFlagEitherWay(t *testing.T) {
	helm := helmBin(t)
	template := func(args ...string) ([]byte, error) {
		full := append([]string{"template", "kubescrape", "../../charts/kubescrape",
			"--namespace", "monitoring"}, args...)
		full = append(full, featureFlags...)
		return exec.Command(helm, full...).CombinedOutput()
	}
	for _, v := range durationValues {
		t.Run(v.path, func(t *testing.T) {
			if !v.zeroNum {
				// Not documented as zeroable: the number branch stays closed,
				// so the refusal is a helm error and never a rendered pod.
				out, err := template("--set", v.path+"=0")
				if err == nil {
					t.Errorf("--set %s=0 rendered fine, but the schema types it as a duration string; either the row needs zeroNum or the schema drifted:\n%s", v.path, out)
				}
				return
			}
			num, err := template("--set", v.path+"=0")
			if err != nil {
				t.Fatalf("--set %s=0 was refused, and values.yaml documents 0: %v\n%s", v.path, err, num)
			}
			str, err := template("--set-string", v.path+"=0")
			if err != nil {
				t.Fatalf("--set-string %s=0 was refused: %v\n%s", v.path, err, str)
			}
			if !bytes.Equal(num, str) {
				t.Errorf("--set %s=0 and --set-string %s=0 render differently:\n%s",
					v.path, v.path, unifiedDiff(t, str, num))
			}
			if !bytes.Contains(num, []byte("- -"+v.flag+"=0\n")) {
				t.Errorf("--set %s=0 rendered no -%s=0 flag", v.path, v.flag)
			}
			// A bare non-zero number is not a duration in any spelling.
			if out, err := template("--set", v.path+"=30"); err == nil {
				t.Errorf("--set %s=30 rendered fine; -%s=30 is `invalid duration`:\n%s", v.path, v.flag, out)
			}
		})
	}
}

// byteSizeStringValues are the byte-size values NOT typed integer, with what
// guards them instead. There is exactly one, and it is the reason the rest are
// integers: -logs-metrics-max-bytes is rendered through the
// kubescrape.logsMetricsMaxBytes helper, whose fail() is what still refuses
// "3MiB" under --skip-schema-validation and in subchart use, where a parent's
// schema does not apply (TestLogsMetricsMaxBytesRejectsHumanSizes drives both
// layers).
var byteSizeStringValues = map[string]string{
	"agent.logsMetrics.maxBytes": "^[0-9]*$",
}

// TestByteSizeValuesAreIntegerTyped is the other half of the audit. A byte
// size has no generated type signal the way a duration does (an int flag's
// default is just a number), so the flag NAME is the honest discriminator:
// every `-*-bytes` flag the chart renders must come from an integer-typed
// value. Integer is what makes helm refuse "1GiB" and "40Mi" from --set and
// --set-string alike — a string-typed byte size would reach the template,
// where helm's int64 parses any non-number to 0 and several of these flags
// define 0 as "no bound", the opposite of what was asked for.
func TestByteSizeValuesAreIntegerTyped(t *testing.T) {
	schema := readSchema(t)
	for _, path := range byteSizeValuePaths(t) {
		node := schemaNode(t, schema, path)
		if node == nil {
			continue
		}
		if pat, ok := byteSizeStringValues[path]; ok {
			if node["type"] != "string" || node["pattern"] != pat {
				t.Errorf("values.schema.json %s: the documented string exception must stay type string + pattern %s, got type %v pattern %v", path, pat, node["type"], node["pattern"])
			}
			continue
		}
		if node["type"] != "integer" {
			t.Errorf("values.schema.json %s: type is %v, want integer — a byte size typed string lets \"3MiB\" through to int64, which renders 0", path, node["type"])
		}
	}
}

// TestByteSizeValuesRejectHumanFormats drives the pinned helm: a human-format
// size must be refused BY NAME at template time, whichever way it is spelled,
// and a plain integer must render as digits. The digits matter: helm parses a
// values FILE through JSON, so every number arrives as a float64 and
// 2147483648 formats as 2.147483648e+09 — which is why the templates cast with
// int64, and this is what fails if a new byte flag is rendered bare.
func TestByteSizeValuesRejectHumanFormats(t *testing.T) {
	helm := helmBin(t)
	paths := byteSizeValuePaths(t)
	template := func(args ...string) ([]byte, error) {
		full := append([]string{"template", "kubescrape", "../../charts/kubescrape",
			"--namespace", "monitoring"}, args...)
		full = append(full, featureFlags...)
		return exec.Command(helm, full...).CombinedOutput()
	}
	for _, bad := range []string{"1GiB", "40Mi", "3MB"} {
		for _, setFlag := range []string{"--set", "--set-string"} {
			var a []string
			for _, p := range paths {
				a = append(a, setFlag, p+"="+bad)
			}
			out, err := template(a...)
			if err == nil {
				t.Errorf("%s <every byte size>=%s rendered fine; helm's int64 turns it into 0:\n%s", setFlag, bad, out)
				continue
			}
			for _, p := range paths {
				if !bytes.Contains(out, []byte(jsonPointer(p))) {
					t.Errorf("%s %s=%s was not refused: %s", setFlag, p, bad, out)
				}
			}
		}
	}

	// A values FILE, which is the float64 route into the templates.
	file := filepath.Join(t.TempDir(), "bytes.yaml")
	values := "agent:\n  bufferDir: /var/lib/kubescrape-agent/buffer\n  bufferMaxBytes: 2147483648\n" +
		"  logsFingerprintBytes: 1048576\n  logsMetrics:\n    maxBytes: \"3145728\"\n" +
		"  otlp:\n    maxSendBytes: 4194304\n  ingest:\n    enabled: true\n    grpcMaxRecvBytes: 41943040\n" +
		"  journald:\n    maxBatchBytes: 2097152\n"
	if err := os.WriteFile(file, []byte(values), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := template("-f", file)
	if err != nil {
		t.Fatalf("byte sizes from a values file were refused: %v\n%s", err, out)
	}
	for _, want := range []string{
		"- -buffer-max-bytes=2147483648\n",
		"- -logs-fingerprint-bytes=1048576\n",
		"- -logs-metrics-max-bytes=3145728\n",
		"- -otlp-max-send-bytes=4194304\n",
		"- -ingest-grpc-max-recv-bytes=41943040\n",
		"- -journald-max-batch-bytes=2097152\n",
	} {
		if !bytes.Contains(out, []byte(want)) {
			t.Errorf("no %q in the render; a bare number would be 2.147483648e+09 here", strings.TrimSpace(want))
		}
	}
	// ...and every byte flag the chart renders is covered by that file, so a
	// new one cannot slip past the float64 check.
	for _, p := range paths {
		if !strings.Contains(values, lastKey(p)+":") {
			t.Errorf("%s is a byte size the values-file check above does not set; add it", p)
		}
	}
}

// byteSizeValuePaths are the chart values feeding a `-*-bytes` flag, derived
// from the templates so a new one joins the guards automatically.
func byteSizeValuePaths(t *testing.T) []string {
	t.Helper()
	var out []string
	for flag, paths := range chartValuePaths(t) {
		if !strings.HasSuffix(flag, "-bytes") {
			continue
		}
		for _, p := range paths {
			if strings.HasPrefix(p, unresolved) {
				t.Errorf("-%s takes a byte size from an expression this scan cannot read (%s); teach chartValuePaths about it rather than leaving the value unguarded", flag, strings.TrimPrefix(p, unresolved))
				continue
			}
			if !slices.Contains(out, p) {
				out = append(out, p)
			}
		}
	}
	if len(out) == 0 {
		t.Fatal("derived no byte-size values at all; the template scan is broken, not the chart")
	}
	sort.Strings(out)
	return out
}

func lastKey(path string) string {
	i := strings.LastIndex(path, ".")
	return path[i+1:]
}

// ---- derivation helpers -------------------------------------------------

// jsonPointer renders a dotted values path the way helm's schema validator
// names it in an error ("agent.logsIdleClose" -> "/agent/logsIdleClose").
func jsonPointer(path string) string {
	return "/" + strings.ReplaceAll(path, ".", "/")
}

var docRow = regexp.MustCompile("^\\|\\s*`-([a-zA-Z0-9-]+)`\\s*\\|\\s*(?:`([^`]*)`|—)\\s*\\|")

// durationFlagsFromDocs reads docs/FLAGS.md — GENERATED from each binary's
// registered flag set, so its default column cannot drift from the code — and
// returns the flags whose default is a Go duration. time.Duration.String()
// always emits a unit ("0s", never "0"), which is what separates a
// flag.Duration from a flag.Int whose default happens to be 0.
func durationFlagsFromDocs(t *testing.T) map[string]bool {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "docs", "FLAGS.md"))
	if err != nil {
		t.Fatalf("reading the generated flag docs: %v", err)
	}
	out := map[string]bool{}
	for _, line := range strings.Split(string(b), "\n") {
		m := docRow.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		def := m[2]
		if def == "" {
			continue
		}
		if c := def[len(def)-1]; c < 'a' || c > 'z' {
			continue
		}
		if _, err := time.ParseDuration(def); err != nil {
			continue
		}
		out[m[1]] = true
	}
	if len(out) == 0 {
		t.Fatal("docs/FLAGS.md yielded no duration flags; the table format changed and this parse needs updating")
	}
	return out
}

// unresolved prefixes a "path" the template scan could not read back to a
// values key; the guards fail on one rather than pass over it.
const unresolved = "?"

var (
	tplAction       = regexp.MustCompile(`\{\{-?\s*(.*?)\s*-?\}\}`)
	tplAssign       = regexp.MustCompile(`^\$(\w+)\s*:=\s*(\S+)$`)
	tplArg          = regexp.MustCompile(`^\s*- -([a-z0-9-]+)=(.*)$`)
	tplCommentStart = regexp.MustCompile(`\{\{-?\s*/\*`)
	tplCommentEnd   = regexp.MustCompile(`\*/\s*-?\}\}`)
)

// chartValuePaths maps each flag the templates render to the values paths that
// feed it. It resolves the three spellings this chart uses — `.Values.a.b`, a
// `{{- $x := .Values.a }}` alias, and the `{{ . }}` of an enclosing
// `{{- with .Values.a.b }}` — and reports the block depth it tracked, so a
// template shape it cannot follow fails the caller instead of silently
// yielding a smaller set (a guard that quietly degrades to "pass" is worse
// than no guard: the green check is read as coverage).
func chartValuePaths(t *testing.T) map[string][]string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "charts", "kubescrape", "templates", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no chart templates found (err=%v)", err)
	}
	out := map[string][]string{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		aliases := map[string]string{}
		var withStack []string // "" for if/range, the resolved path for with
		inComment := false
		for n, line := range strings.Split(string(b), "\n") {
			if inComment {
				if tplCommentEnd.MatchString(line) {
					inComment = false
				}
				continue
			}
			// A TEMPLATE comment only ({{- /* … */}}), never a YAML `#` line
			// that happens to contain "/*" — "prometheus.io/*" swallowed a
			// hundred lines of blocks and left the scan unbalanced.
			if tplCommentStart.MatchString(line) && !tplCommentEnd.MatchString(line) {
				inComment = true
				continue
			}
			if m := tplArg.FindStringSubmatch(line); m != nil {
				flag, expr := m[1], strings.TrimSpace(m[2])
				if inner := tplAction.FindStringSubmatch(expr); inner != nil && inner[0] == expr {
					// The VALUE is the action's last argument: `.Values.a.b`,
					// `int64 $.Values.a.b`, `$sg.b`, or the `.` of the
					// enclosing `with` (`int64 .`, `include "…" .`).
					fields := strings.Fields(inner[1])
					last := strings.TrimRight(fields[len(fields)-1], ")")
					path, ok := "", false
					if last == "." {
						for i := len(withStack) - 1; i >= 0; i-- {
							if withStack[i] != "" {
								path, ok = withStack[i], true
								break
							}
						}
					} else {
						path, ok = resolveValuePath(last, aliases)
					}
					if !ok {
						// Recorded, not dropped: a guard below decides whether
						// this flag was one it had to see. Silently skipping
						// what the scan cannot read is how a green check comes
						// to mean nothing.
						path = unresolved + filepath.Base(f) + ":" + fmt.Sprint(n+1)
					}
					if !slices.Contains(out[flag], path) {
						out[flag] = append(out[flag], path)
					}
				}
			}
			for _, m := range tplAction.FindAllStringSubmatch(line, -1) {
				body := m[1]
				switch {
				case body == "end":
					if len(withStack) == 0 {
						t.Fatalf("%s:%d: unbalanced {{ end }}; this scan no longer understands the templates", f, n+1)
					}
					withStack = withStack[:len(withStack)-1]
				case strings.HasPrefix(body, "with "):
					p, _ := resolveValuePath(strings.TrimPrefix(body, "with "), aliases)
					withStack = append(withStack, p)
				case strings.HasPrefix(body, "if ") || strings.HasPrefix(body, "range ") || strings.HasPrefix(body, "define "):
					withStack = append(withStack, "")
				case tplAssign.MatchString(body):
					a := tplAssign.FindStringSubmatch(body)
					if p, ok := resolveValuePath(a[2], aliases); ok {
						aliases["$"+a[1]] = p
					}
				}
			}
		}
		if len(withStack) != 0 {
			t.Fatalf("%s: %d template blocks left open; this scan no longer understands the templates", f, len(withStack))
		}
	}
	return out
}

// resolveValuePath turns a template expression into a dotted values path.
func resolveValuePath(expr string, aliases map[string]string) (string, bool) {
	expr = strings.TrimSpace(expr)
	switch {
	case strings.HasPrefix(expr, ".Values."):
		return strings.TrimPrefix(expr, ".Values."), true
	case strings.HasPrefix(expr, "$.Values."):
		return strings.TrimPrefix(expr, "$.Values."), true
	case strings.HasPrefix(expr, "$"):
		name, rest, _ := strings.Cut(expr, ".")
		base, ok := aliases[name]
		if !ok {
			return "", false
		}
		if rest == "" {
			return base, true
		}
		return base + "." + rest, true
	}
	return "", false
}

func readSchema(t *testing.T) map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "charts", "kubescrape", "values.schema.json"))
	if err != nil {
		t.Fatalf("reading values.schema.json: %v", err)
	}
	var s map[string]any
	if err := json.Unmarshal(b, &s); err != nil {
		t.Fatalf("values.schema.json is not valid JSON: %v", err)
	}
	return s
}

// schemaNode walks `properties` down a dotted path.
func schemaNode(t *testing.T, schema map[string]any, path string) map[string]any {
	t.Helper()
	node := schema
	for _, part := range strings.Split(path, ".") {
		props, _ := node["properties"].(map[string]any)
		next, ok := props[part].(map[string]any)
		if !ok {
			t.Errorf("values.schema.json describes no %q (additionalProperties:false would refuse it)", path)
			return nil
		}
		node = next
	}
	return node
}

// forEachSchemaProperty visits every node under `properties`, deepest last, in
// a stable order.
func forEachSchemaProperty(schema map[string]any, fn func(path string, node map[string]any)) {
	var walk func(prefix string, node map[string]any)
	walk = func(prefix string, node map[string]any) {
		props, _ := node["properties"].(map[string]any)
		names := make([]string, 0, len(props))
		for k := range props {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			child, ok := props[k].(map[string]any)
			if !ok {
				continue
			}
			path := k
			if prefix != "" {
				path = prefix + "." + k
			}
			fn(path, child)
			walk(path, child)
		}
	}
	walk("", schema)
}
