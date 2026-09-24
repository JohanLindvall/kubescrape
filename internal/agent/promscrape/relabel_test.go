package promscrape

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func TestRelabelKeepDrop(t *testing.T) {
	var c relabelCache
	f, _, err := c.session([]kubemeta.RelabelRule{
		{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "go_.*"},
		{Action: "keep", SourceLabels: []string{"job", "instance"}, Regex: "api;.*"},
	})
	if err != nil {
		t.Fatal(err)
	}
	lbls := []Label{{Name: "job", Value: "api"}, {Name: "instance", Value: "i1"}}
	if f.Keep("go_goroutines", lbls) {
		t.Fatal("drop rule ignored")
	}
	if !f.Keep("http_requests_total", lbls) {
		t.Fatal("kept sample dropped")
	}
	if f.Keep("http_requests_total", []Label{{Name: "job", Value: "web"}}) {
		t.Fatal("keep rule ignored (join 'web;' must not match 'api;.*')")
	}
	// Anchoring: partial matches must not count.
	f2, _, _ := c.session([]kubemeta.RelabelRule{{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "go"}})
	if !f2.Keep("go_goroutines", nil) {
		t.Fatal("unanchored partial match dropped a sample")
	}
	// Bad regex fails the session (never silently exports what was dropped).
	if _, _, err := c.session([]kubemeta.RelabelRule{{Action: "drop", Regex: "("}}); err == nil {
		t.Fatal("bad regex must error")
	}
	// nil for rule-less targets.
	if f, _, _ := c.session(nil); f != nil {
		t.Fatal("no rules must yield nil session")
	}
}

// The per-rule last-seen memo must never change a verdict: a session reused
// across a whole scrape answers every sample exactly as a FRESH session (no memo
// state at all) answers it — through runs of one join, alternations, a join
// longer than the memo keeps, and a rule whose join repeats while another's
// changes.
func TestRelabelMemoAgreesWithTheRegex(t *testing.T) {
	rules := []kubemeta.RelabelRule{
		{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "go_.*"},
		{Action: "keep", SourceLabels: []string{"job", "pod"}, Regex: "api;(p1|x+)"},
	}
	var c relabelCache
	reused, _, err := c.session(rules)
	if err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("x", maxRelabelMemoBytes+10) // matches x+, never memoised
	type sample struct {
		name string
		pod  string
	}
	seq := []sample{
		{"go_gc", "p1"}, {"go_gc", "p1"}, // dropped by rule 0, twice
		{"http_total", "p1"}, {"http_total", "p1"}, // kept
		{"http_total", "p2"}, {"http_total", "p2"}, // rule 1's join changes: dropped
		{"http_total", "p1"},                       // and back
		{"http_total", long}, {"http_total", long}, // long join: kept, re-evaluated
		{"http_total", long + "y"}, // long and NOT matching: dropped
		{"go_gc", long}, {"http_total", "p1"},
	}
	for i, smp := range seq {
		labels := []Label{{Name: "job", Value: "api"}, {Name: "pod", Value: smp.pod}}
		fresh, _, err := c.session(rules)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := reused.Keep(smp.name, labels), fresh.Keep(smp.name, labels); got != want {
			t.Fatalf("sample %d (%s, pod of %d bytes): the memoised session says keep=%v, a fresh one %v",
				i, smp.name, len(smp.pod), got, want)
		}
	}
	for i := range reused.last {
		if l := reused.last[i]; len(l.val) > maxRelabelMemoBytes {
			t.Errorf("rule %d's memo holds a %d-byte join, over the %d-byte bound", i, len(l.val), maxRelabelMemoBytes)
		}
	}
}

// The compiled-chain cache is bounded: a controller minting monitors with
// distinct (templated/hashed) regexes must not leak a compiled chain per
// fingerprint for the process' life. Evict-one at the cap keeps it hot.
// The compiled-chain cache is process-global and keyed by relabelFingerprint,
// so a collision is not a cache miss but a WRONG ANSWER: one endpoint gets
// another's keep/drop chain and exports series its own rule asked to drop.
// Every pair below aliases under a naive join of the rule text; the random
// half then throws delimiter-heavy chains at it and requires that two keys
// agree only when the chains do.
func TestRelabelFingerprintIsInjective(t *testing.T) {
	type chain = []kubemeta.RelabelRule
	pairs := []struct {
		name string
		a, b chain
	}{
		{"split vs joined sources",
			chain{{Action: "keep", SourceLabels: []string{"a", "b"}, Regex: "x"}},
			chain{{Action: "keep", SourceLabels: []string{"a;b"}, Regex: "x"}}},
		{"source/regex boundary shift",
			chain{{Action: "keep", SourceLabels: []string{"a"}, Regex: "bc"}},
			chain{{Action: "keep", SourceLabels: []string{"ab"}, Regex: "c"}}},
		{"one rule vs two with the same concatenation",
			chain{{Action: "keep", SourceLabels: []string{"a"}, Regex: "x"}, {Action: "drop", SourceLabels: []string{"b"}, Regex: "y"}},
			chain{{Action: "keep", SourceLabels: []string{"a"}, Regex: "xdropby"}}},
		{"regex forging the next rule's encoding",
			chain{{Action: "keep", SourceLabels: []string{"a"}, Regex: "x"}, {Action: "drop", SourceLabels: []string{"b"}, Regex: "y"}},
			chain{{Action: "keep", SourceLabels: []string{"a"}, Regex: "1:x4:drop1;1:b1:y"}}},
		{"no source vs one empty source",
			chain{{Action: "keep", Regex: ""}},
			chain{{Action: "keep", SourceLabels: []string{""}, Regex: ""}}},
		{"empty regex vs a trailing empty rule",
			chain{{Action: "keep", SourceLabels: []string{"a"}}},
			chain{{Action: "keep", SourceLabels: []string{"a"}}, {Action: "", SourceLabels: nil, Regex: ""}}},
		{"source/action boundary shift",
			chain{{Action: "keep", SourceLabels: []string{"a"}, Regex: ""}, {Action: "drop"}},
			chain{{Action: "keep", SourceLabels: []string{"a", "drop"}, Regex: ""}}},
	}
	for _, p := range pairs {
		if relabelFingerprint(p.a) == relabelFingerprint(p.b) {
			t.Errorf("%s: %+v and %+v share the fingerprint %q", p.name, p.a, p.b, relabelFingerprint(p.a))
		}
	}

	// Random chains over an alphabet made of the encoding's own delimiters.
	alphabet := []string{"", "a", "1", "2", ":", ";", "1:", "0:", "keep", "drop", "1;"}
	rnd := rand.New(rand.NewPCG(1, 2))
	pick := func() string { return alphabet[rnd.IntN(len(alphabet))] }
	seen := map[string]chain{}
	for range 50_000 {
		c := make(chain, rnd.IntN(3))
		for i := range c {
			c[i].Action = pick()
			for range rnd.IntN(3) {
				c[i].SourceLabels = append(c[i].SourceLabels, pick())
			}
			c[i].Regex = pick() + pick()
		}
		key := relabelFingerprint(c)
		prev, ok := seen[key]
		if !ok {
			seen[key] = c
			continue
		}
		if !slices.EqualFunc(prev, c, func(x, y kubemeta.RelabelRule) bool {
			return x.Action == y.Action && x.Regex == y.Regex && slices.Equal(x.SourceLabels, y.SourceLabels)
		}) {
			t.Fatalf("%+v and %+v share the fingerprint %q", prev, c, key)
		}
	}
}

func TestRelabelCacheBounded(t *testing.T) {
	var c relabelCache
	for i := range maxRelabelChains * 3 {
		f, _, err := c.session([]kubemeta.RelabelRule{
			{Action: "drop", SourceLabels: []string{"__name__"}, Regex: fmt.Sprintf("metric_%d_.*", i)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if f == nil {
			t.Fatal("nil session for a rule-bearing target")
		}
		// An evicted chain must not invalidate a live session (it holds its own
		// compiled slice): its drop rule still matches its own metric name.
		if f.Keep(fmt.Sprintf("metric_%d_x", i), nil) {
			t.Fatalf("session %d did not apply its own compiled chain (matching name not dropped)", i)
		}
	}
	c.mu.Lock()
	n := len(c.m)
	c.mu.Unlock()
	if n > maxRelabelChains {
		t.Errorf("cache holds %d chains, want <= %d", n, maxRelabelChains)
	}
	if n == 0 {
		t.Error("cache emptied itself: every scrape would recompile its chain")
	}
}

type fakeAuth struct{ calls int }

func (f *fakeAuth) ScrapeAuth(_ context.Context, ref string) (string, error) {
	f.calls++
	return "tok-" + ref, nil
}

// A target carrying AuthSecret scrapes with the resolved bearer token; the
// token is cached across scrapes.
func TestScrapeTargetBearerAuth(t *testing.T) {
	var gotAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		_, _ = w.Write([]byte("m 1\n"))
	}))
	t.Cleanup(srv.Close)
	auth := &fakeAuth{}
	exp := &captureExporter{}
	tgt := testTarget(srv.URL)
	tgt.AuthSecret = "ns/tok/token"
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{tgt}, Auth: auth,
		Exporter: exp, StartTime: time.Now(),
	})
	s.cycle(context.Background())
	// Every target is scheduled now, so clear the schedule to stand in for an
	// interval elapsing; the point of the test is that two SCRAPES resolve the
	// token once.
	s.setSchedule(nil, nil)
	s.cycle(context.Background())
	if got, _ := gotAuth.Load().(string); got != "Bearer tok-ns/tok/token" {
		t.Fatalf("Authorization = %q", got)
	}
	if auth.calls != 1 {
		t.Fatalf("auth calls = %d, want 1 (cached)", auth.calls)
	}
	if exp.points() != 2 {
		t.Fatalf("points = %d", exp.points())
	}
}

// A target with metricRelabelings drops the matching series end-to-end.
func TestScrapeTargetMetricRelabelings(t *testing.T) {
	srv := serveBody(t, "go_goroutines 5\nhttp_requests_total 7\n")
	exp := &captureExporter{}
	tgt := testTarget(srv.URL)
	tgt.MetricRelabelings = []kubemeta.RelabelRule{
		{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "go_.*"},
	}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets:  staticTargets{tgt},
		Exporter: exp, StartTime: time.Now(),
	})
	s.cycle(context.Background())
	if exp.points() != 1 {
		t.Fatalf("points = %d, want the go_ series dropped", exp.points())
	}
}

// A chain that will not compile fails the scrape whatever the target answers,
// so the target must not be fetched at all. Compiled after the request, a
// broken monitor cost a full HTTP scrape of every matched target every cycle —
// and a fresh handshake whenever the body outran drainClose's bound — only for
// the response to be thrown away.
func TestUncompilableRelabelChainFailsBeforeTheTargetIsFetched(t *testing.T) {
	srv, hits := countingTarget(t)
	tgt := testTarget(srv.URL)
	tgt.Source, tgt.Monitor = "servicemonitor", "ns/broken"
	tgt.MetricRelabelings = []kubemeta.RelabelRule{{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "("}}
	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{tgt}, Exporter: exp, StartTime: time.Now(),
	})
	_, err := s.scrapeTarget(context.Background(), tgt, 5*time.Second)
	if got := failureReason(err); got != reasonRelabel {
		t.Fatalf("reason = %q (%v), want %q", got, err, reasonRelabel)
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("the target was fetched %d time(s) for a scrape its relabel chain had already failed", n)
	}
	if exp.points() != 0 {
		t.Errorf("points = %d, want 0", exp.points())
	}
}
