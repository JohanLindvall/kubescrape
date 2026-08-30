package attrs

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// debugLog installs a Debug-level default logger and returns the buffer it
// writes to. Build and Keep both check slog.Default().Enabled before rendering
// anything, so a test that leaves the default logger at Info observes silence
// and proves nothing.
func debugLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logged bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &logged
}

// A template that renders against the synthetic validation Context but fails
// against a real one is CONFIG that never refuses: construction accepts it
// (the error is value-dependent, and legitimately so — a node-level resource
// has no .Pod), and Build then omits the attribute from every resource with
// nothing anywhere saying why. On a first live run that is indistinguishable
// from a template nobody wired up.
func TestFailedTemplateRenderIsExplained(t *testing.T) {
	logged := debugLog(t)
	// Legal at construction: indexing an empty slice is value-dependent, so
	// validateTemplate deliberately lets it through (it works for every owned
	// pod). It fails for a pod with no owners.
	b, err := NewBuilder(&Config{Attributes: map[string]string{
		"owner": `{{ (index .Pod.Owners 0).Name }}`,
	}}, nil)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	res := pcommon.NewResource()
	b.Build(res, Context{Pod: &kubemeta.Pod{Name: "p", Namespace: "ns"}})

	if _, ok := res.Attributes().Get("owner"); ok {
		t.Fatal("the attribute rendered; this test no longer exercises the failure path")
	}
	line := logged.String()
	if !strings.Contains(line, "key=owner") {
		t.Errorf("the render failure did not name the attribute key:\n%s", line)
	}
	if !strings.Contains(line, "error=") {
		t.Errorf("the render failure did not carry the error:\n%s", line)
	}
}

// Build runs per RESOURCE — 12k of them per scrape cycle on a KSM split path —
// so a failure that is a property of the TEMPLATE must be reported once per
// template per window, never once per resource. Without the throttle this line
// is a flood proportional to the cluster.
func TestFailedTemplateRenderIsReportedOncePerTemplate(t *testing.T) {
	logged := debugLog(t)
	b, err := NewBuilder(&Config{Attributes: map[string]string{
		"owner": `{{ (index .Pod.Owners 0).Name }}`,
	}}, nil)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	ctx := Context{Pod: &kubemeta.Pod{Name: "p", Namespace: "ns"}}
	for range 50 {
		b.Build(pcommon.NewResource(), ctx)
	}
	if got := strings.Count(logged.String(), "resourceAttributes template"); got != 1 {
		t.Fatalf("50 resources logged the same template failure %d times, want 1", got)
	}
}

// An EMPTY render is how `{{ with .Node }}…{{ end }}` spells "not applicable
// here"; reporting it would bury the real failures under the intended ones.
func TestEmptyTemplateRenderIsSilent(t *testing.T) {
	logged := debugLog(t)
	b, err := NewBuilder(&Config{Attributes: map[string]string{
		"zone": `{{ with .Node }}{{ index .Labels "topology.kubernetes.io/zone" }}{{ end }}`,
	}}, nil)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	b.Build(pcommon.NewResource(), Context{Pod: &kubemeta.Pod{Name: "p"}})
	if strings.Contains(logged.String(), "resourceAttributes template") {
		t.Fatalf("an empty (not-applicable) render was reported:\n%s", logged.String())
	}
}

// enable/disable are anchored whole-key regexes, and the commonest mistake
// (`k8s.pod` for `k8s\.pod\..*`) matches nothing — so an operator who meant to
// keep a namespace of attributes strips it instead, and every downstream join
// keyed on those attributes stops working with no error anywhere. The refusal
// must name the key AND which half refused it: the two are different mistakes
// and a line that conflates them sends the operator to the wrong list.
func TestFilterRefusalsNameTheKeyAndTheHalfThatRefused(t *testing.T) {
	logged := debugLog(t)
	f, err := NewFilterFromLists([]string{`k8s\.pod\..*`}, []string{`k8s\.pod\.label\.internal\..*`})
	if err != nil {
		t.Fatalf("NewFilterFromLists: %v", err)
	}
	f.Keep("service.name")               // no enable pattern matches
	f.Keep("k8s.pod.label.internal.foo") // a disable pattern matches
	f.Keep("k8s.pod.name")               // kept: must not be reported

	out := logged.String()
	if !strings.Contains(out, `key=service.name`) || !strings.Contains(out, `reason="no enable pattern matched"`) {
		t.Errorf("the enable-list refusal was not reported as one:\n%s", out)
	}
	if !strings.Contains(out, `key=k8s.pod.label.internal.foo`) || !strings.Contains(out, `reason="a disable pattern matched"`) {
		t.Errorf("the disable-list refusal was not reported as one:\n%s", out)
	}
	if strings.Contains(out, "k8s.pod.name") {
		t.Errorf("a KEPT key was reported as removed:\n%s", out)
	}
}

// The verdict is memoized, and the report rides the memo MISS. That is what
// keeps it off the hot path: Apply runs on every attribute of every resource
// built (300k anchored evaluations per scrape on a split path), and a line per
// removed attribute per resource would be the flood, not the diagnosis.
func TestFilterRefusalIsReportedOncePerKey(t *testing.T) {
	logged := debugLog(t)
	f, err := NewFilterFromLists(nil, []string{`drop\..*`})
	if err != nil {
		t.Fatalf("NewFilterFromLists: %v", err)
	}
	for range 100 {
		f.Keep("drop.me")
	}
	if got := strings.Count(logged.String(), "key=drop.me"); got != 1 {
		t.Fatalf("100 verdicts on one key logged %d times, want 1", got)
	}
}

// Every dynamic attribute owns its throttle, so one template failing cannot
// silence another's explanation — the shape logdedupe.Table exists for, spelled
// per template because the key set is the config's and closed.
func TestEachTemplateHasItsOwnThrottle(t *testing.T) {
	b, err := NewBuilder(&Config{Attributes: map[string]string{
		"a": `{{ (index .Pod.Owners 0).Name }}`,
		"b": `{{ (index .Pod.Owners 1).Name }}`,
	}}, nil)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	seen := map[*logdedupe.Throttle]bool{}
	for i := range b.dynamic {
		if b.dynamic[i].warn == nil {
			t.Fatalf("template %q has no throttle", b.dynamic[i].key)
		}
		if seen[b.dynamic[i].warn] {
			t.Fatalf("template %q shares a throttle with another", b.dynamic[i].key)
		}
		seen[b.dynamic[i].warn] = true
	}
}
