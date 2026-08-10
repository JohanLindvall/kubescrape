package events

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// point identifies one series: a metric name plus its label pairs.
type point struct {
	name  string
	label [2]string
}

// publishProbeEnv marks the CHILD process that runs the probe below.
const publishProbeEnv = "KUBESCRAPE_EVENTS_PUBLISH_PROBE"

// A counter that has never fired is ABSENT, not zero, and absent is
// indistinguishable from "this binary does not run the events reader at all" —
// so every alert written against these ("did the gap get discarded?", "is the
// leader still exporting?") silently matches nothing on a healthy leader, which
// is exactly when the operator wants to see the zero. Run publishes the whole
// family at zero, so absent means "not the leader" and 0 means "leading,
// nothing has happened".
//
// The probe runs in a CHILD PROCESS. obs.Registry is a process global with no
// reset, and most of these series are written by other tests in this package —
// expire() writes the relist and gap labels, ingest() the type labels, persist()
// the position ones — so an in-process assertion is satisfied by whatever ran
// first and passes with publishMetrics gutted. Re-execing the test binary with
// -test.run naming only this test gives a registry nothing else has touched,
// which is what turns "absent before, present after" into a statement about
// publishMetrics rather than about test order.
func TestEventCountersArePublishedAtZero(t *testing.T) {
	if os.Getenv(publishProbeEnv) == "1" {
		probePublishedSeries(t)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestEventCountersArePublishedAtZero$", "-test.v")
	cmd.Env = append(os.Environ(), publishProbeEnv+"=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the publication probe failed: %v\n%s", err, out)
	}
}

// probePublishedSeries is the child half: in a registry nothing else has
// written to, every series below must be ABSENT before publishMetrics and
// present after it.
func probePublishedSeries(t *testing.T) {
	want := []point{
		{name: "kubescrape_events_exported_total"},
		{name: "kubescrape_events_dropped_batches_total"},
		{name: "kubescrape_events_dropped_records_total"},
		{name: "kubescrape_events_overflow_dropped_total"},
		{name: "kubescrape_event_watch_restarts_total"},
	}
	for _, tl := range eventTypeLabels {
		want = append(want, point{name: "kubescrape_events_observed_total", label: [2]string{"type", tl}})
	}
	for _, stage := range []string{stageWatch, stageReplay} {
		want = append(want, point{name: "kubescrape_event_relists_total", label: [2]string{"stage", stage}})
	}
	// Not stageReplay: a replay-stage expiry always has a relist to fall back
	// on, so that gap can never be discarded.
	want = append(want, point{name: "kubescrape_event_gap_discarded_total", label: [2]string{"stage", stageWatch}})
	for _, op := range []string{opLoad, opSave} {
		want = append(want, point{name: "kubescrape_event_position_errors_total", label: [2]string{"operation", op}})
	}

	// A registration creates the metric FAMILY; only a write creates a series
	// under it, which is the absent-vs-zero hole. Nothing has written yet.
	before := dumpedSeries()
	for _, w := range want {
		if before[w] {
			t.Fatalf("%s%v already has a series before publishMetrics ran: something other than the publication "+
				"created it, and this probe can no longer tell whether the publication works", w.name, w.label)
		}
	}

	publishMetrics()

	have := dumpedSeries()
	for _, w := range want {
		if !have[w] {
			t.Errorf("%s%v has no series after publishMetrics: absent reads as 'this replica is not running the events reader'",
				w.name, w.label)
		}
	}
}

// dumpedSeries is every (metric, label pair) the registry currently holds a
// data point for.
func dumpedSeries() map[point]bool {
	have := map[point]bool{}
	for _, s := range obs.Registry.Dump() {
		for _, p := range s.Points {
			if len(p.Labels) == 0 {
				have[point{name: s.Name}] = true
				continue
			}
			for _, l := range p.Labels {
				have[point{name: s.Name, label: l}] = true
			}
		}
	}
	return have
}

// The published set must cover every counter the package OWNS, or the next one
// added silently reintroduces the absent-vs-zero hole. The obs.Event* prefix is
// the ownership rule: those are the "Kubernetes events" block in obs.go.
// Deliberately not the shared counters this package also bumps
// (obs.LogExportFailures, whose zero belongs to whichever producer is running).
func TestEveryEventCounterIsPublished(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	used, published := map[string]bool{}, map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				return true
			}
			into := used
			if fn.Name.Name == "publishMetrics" {
				into = published
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if x, ok := sel.X.(*ast.Ident); ok && x.Name == "obs" && strings.HasPrefix(sel.Sel.Name, "Event") {
					into[sel.Sel.Name] = true
				}
				return true
			})
			return false
		})
	}
	if len(used) == 0 || len(published) == 0 {
		t.Fatalf("scan found used=%d published=%d obs.Event* counters; the scan itself is broken", len(used), len(published))
	}
	for name := range used {
		if !published[name] {
			t.Errorf("obs.%s is bumped by this package but publishMetrics gives it no zero series", name)
		}
	}
}

// publishMetrics can only give a series to a label value it knows, and
// eventTypeLabel derives its value from cluster data. A value missing from the
// list is a series that appears only once the cluster happens to produce it.
func TestEventTypeLabelsAreComplete(t *testing.T) {
	seen := map[string]bool{}
	for _, in := range []string{"Normal", "normal", "Warning", "WARNING", "", "Something", "NodeNotReady"} {
		got := eventTypeLabel(in)
		if !contains(eventTypeLabels, got) {
			t.Errorf("eventTypeLabel(%q) = %q, which is not in eventTypeLabels", in, got)
		}
		seen[got] = true
	}
	for _, want := range eventTypeLabels {
		if !seen[want] {
			t.Errorf("eventTypeLabels lists %q but no input produces it; a stale value publishes a series nothing writes", want)
		}
	}
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}
