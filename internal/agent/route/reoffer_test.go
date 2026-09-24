package route

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
)

// nopInner is the buffered chain's collector; the drain never runs in these
// tests, so it is never reached.
type nopInner struct{}

func (nopInner) ExportLogs(context.Context, plog.Logs) error          { return nil }
func (nopInner) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

// spoolingRouter is the residual's worst shape: the default chain is a disk
// buffer (which "succeeds" by spooling, collector or no collector) and one
// tenant route is down.
func spoolingRouter(t *testing.T, routeErr error) (*Router, *otlpexport.Buffered) {
	t.Helper()
	buf, err := otlpexport.OpenBuffer(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = buf.Close() })
	quiet := slog.New(slog.DiscardHandler)
	def := otlpexport.NewBuffered(nopInner{}, buf, nil, nil, time.Millisecond, quiet)
	down := &capDest{err: routeErr}
	return New(def, []Destination{{Name: "team-a", Namespaces: []string{"team-a-*"}, Exporter: down}}), def
}

// offerUntilGivenUp drives a producer's retry loop the way the tailer's
// exportWithRetry does, cycles times over, and returns the last error.
func offerUntilGivenUp(ctx context.Context, r *Router, cycles int) error {
	var err error
	for range cycles {
		err = otlpexport.Retry(ctx, 3, time.Millisecond, func() error {
			return r.ExportLogs(ctx, nsLogs("default", "team-a-web"))
		})
	}
	return err
}

// A producer that re-offers its batch until it lands (Reoffer) must not spool a
// copy of the default share per attempt while a tenant route is down: every
// copy replays to the default backend on recovery and meanwhile spends the
// -buffer-max-bytes every producer on the node shares. Unmarked, the router
// keeps sending every share every attempt — the single-shot producers depend on
// it, since they never re-send.
func TestReofferedPayloadDoesNotSpoolTheDefaultSharePerAttempt(t *testing.T) {
	transient := errors.New("connection refused")

	r, buffered := spoolingRouter(t, transient)
	err := offerUntilGivenUp(Reoffer(context.Background()), r, 5)
	if err == nil || otlpexport.IsPermanent(err) {
		t.Fatalf("export = %v, want the route's transient failure (the producer must retry, not drop)", err)
	}
	if got := buffered.Stats()["logs"].Backlog; got != 0 {
		t.Errorf("a re-offered payload spooled %d bytes of default share across 15 failed attempts, want 0: "+
			"the retry would deliver it, so every copy is a duplicate", got)
	}

	// CONTROL: unmarked, the default share goes out on every attempt.
	r, buffered = spoolingRouter(t, transient)
	if err := offerUntilGivenUp(context.Background(), r, 5); err == nil {
		t.Fatal("the route is down; the export must fail")
	}
	if buffered.Stats()["logs"].Backlog == 0 {
		t.Error("an unmarked payload's default share was withheld; a single-shot producer would lose it")
	}
}

// A PERMANENTLY rejected route share withholds nothing: no retry can deliver
// it, the producer will drop the batch on that verdict, and the default share
// must already be on its way. Once the routes succeed, the default share goes
// out too.
func TestReofferedPayloadStillDeliversTheDefaultShareWhenNoRetryCanHelp(t *testing.T) {
	r, buffered := spoolingRouter(t, &otlpexport.HTTPStatusError{Code: 400})
	err := r.ExportLogs(Reoffer(context.Background()), nsLogs("default", "team-a-web"))
	if !otlpexport.IsPermanent(err) {
		t.Fatalf("export = %v, want the route's permanent rejection", err)
	}
	if buffered.Stats()["logs"].Backlog == 0 {
		t.Error("the default share was withheld behind a route rejection no retry can clear; the producer " +
			"drops the batch next, so it would never be delivered")
	}

	def, ok := &capDest{}, &capDest{}
	r = New(def, []Destination{{Name: "team-a", Namespaces: []string{"team-a-*"}, Exporter: ok}})
	if err := r.ExportLogs(Reoffer(context.Background()), nsLogs("default", "team-a-web")); err != nil {
		t.Fatal(err)
	}
	if len(def.logs) != 1 || len(ok.logs) != 1 {
		t.Errorf("default got %d payloads and the route %d, want 1 each", len(def.logs), len(ok.logs))
	}
}

// The marker survives the detaching wrappers every shutdown flush uses.
func TestReofferSurvivesDetachedContexts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(Reoffer(context.Background())), time.Second)
	defer cancel()
	if !Reoffered(ctx) {
		t.Fatal("the Reoffer marker did not survive WithoutCancel + WithTimeout")
	}
	if Reoffered(context.Background()) {
		t.Fatal("an unmarked context reads as Reoffered")
	}
}

// The roster in reoffer.go is presented as exhaustive ("Who marks" / "Who must
// NOT mark", each checked against its failure path). This pins WHERE the marks
// are, per file: a count that moves is a new promise that a producer re-offers
// the records of a failed export until they land — precisely the moment to
// re-read the roster, since a wrong promise withholds a default share nobody
// will send again. Verify the new marker's failure path, add it to reoffer.go's
// list, then update the map.
var reofferMarkers = map[string]struct {
	n      int
	roster string
}{
	"internal/agent/tailer/flush.go":        {1, "agent/tailer"},
	"internal/agent/events/flush.go":        {1, "agent/events"},
	"internal/agent/journald/journald.go":   {1, "agent/journald"},
	"internal/agent/azurediag/azurediag.go": {1, "agent/azurediag"},
}

func TestReofferRosterNamesEveryProducerThatMarks(t *testing.T) {
	root := moduleRoot(t)
	got := map[string]int{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "bin", "hack":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if n := strings.Count(string(b), "route.Reoffer("); n > 0 {
			rel, _ := filepath.Rel(root, path)
			got[filepath.ToSlash(rel)] = n
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for file, n := range got {
		want, ok := reofferMarkers[file]
		if !ok {
			t.Errorf("%s marks route.Reoffer (%d site(s)) and is not in the roster map: verify that a failed "+
				"export's records come back until delivered, add it to reoffer.go's \"Who marks\" list, then here",
				file, n)
			continue
		}
		if n != want.n {
			t.Errorf("%s has %d route.Reoffer marks, the roster map expects %d: re-read reoffer.go's list",
				file, n, want.n)
		}
	}
	b, err := os.ReadFile(filepath.Join(root, "internal", "agent", "route", "reoffer.go"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	start, end := strings.Index(doc, "Who marks"), strings.Index(doc, "Who must NOT mark")
	if start < 0 || end < start {
		t.Fatal("reoffer.go no longer carries the \"Who marks\" / \"Who must NOT mark\" pair")
	}
	var names []string
	for file, m := range reofferMarkers {
		if _, ok := got[file]; !ok {
			t.Errorf("%s no longer marks route.Reoffer; drop it from the roster map and reoffer.go's list", file)
		}
		if !strings.Contains(doc[start:end], m.roster) {
			t.Errorf("reoffer.go's \"Who marks\" list does not name %q, which marks route.Reoffer", m.roster)
		}
		names = append(names, m.roster)
	}
	sort.Strings(names)
	t.Logf("roster covers %v", names)
}

// moduleRoot walks up from the test's directory to the module root.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test's working directory")
		}
		dir = parent
	}
}
