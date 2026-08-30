package scrape

import (
	"slices"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// mergeHeld builds the target the URL's first monitor produced, as the server
// holds it (stampEndpoint already ran).
func mergeHeld(monitor string, ep servicemonitors.Endpoint) kubemeta.ScrapeTarget {
	t := kubemeta.ScrapeTarget{Monitor: monitor, Source: "servicemonitor"}
	stampEndpoint(&t, ep)
	return t
}

var dropRule = kubemeta.RelabelRule{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "a_.*"}
var keepRule = kubemeta.RelabelRule{Action: "keep", SourceLabels: []string{"job"}, Regex: "api"}

// A bare endpoint has nothing to merge and nothing to report: the target is
// already everything both monitors declared.
func TestMergeBareEndpointIsANoOp(t *testing.T) {
	held := mergeHeld("ns/a", servicemonitors.Endpoint{
		BearerSecret:      "ns/tok/token",
		MetricRelabelings: []kubemeta.RelabelRule{dropRule},
	})
	want := held
	if rep := MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{Port: "metrics"}); rep.AuthConflict {
		t.Error("a bare endpoint reported an auth conflict")
	}
	if held.Monitors != nil {
		t.Errorf("a bare endpoint was listed as a contributor: %v", held.Monitors)
	}
	if held.AuthSecret != want.AuthSecret || len(held.MetricRelabelings) != len(want.MetricRelabelings) {
		t.Errorf("a bare endpoint changed the held target: %+v", held)
	}
}

// The loser's relabel chain runs AFTER the winner's — the union of the two
// keep/drop chains applied to the one scrape.
func TestMergeConcatenatesRelabelingsAfterWinners(t *testing.T) {
	held := mergeHeld("ns/a", servicemonitors.Endpoint{
		MetricRelabelings: []kubemeta.RelabelRule{dropRule},
	})
	if rep := MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{
		MetricRelabelings: []kubemeta.RelabelRule{keepRule},
	}); rep.AuthConflict {
		t.Error("relabelings reported as an auth conflict")
	}
	if len(held.MetricRelabelings) != 2 ||
		held.MetricRelabelings[0].Regex != dropRule.Regex ||
		held.MetricRelabelings[1].Regex != keepRule.Regex {
		t.Errorf("chain = %+v, want the winner's rule then the loser's", held.MetricRelabelings)
	}
	if want := []string{"ns/a", "ns/b"}; !slices.Equal(held.Monitors, want) {
		t.Errorf("monitors = %v, want %v", held.Monitors, want)
	}
}

// A chain identical to the holder's whole current chain is the same
// declaration arriving twice (one monitor through two Services); appending it
// would serve every rule doubled.
func TestMergeSkipsIdenticalRelabelChain(t *testing.T) {
	held := mergeHeld("ns/a", servicemonitors.Endpoint{
		MetricRelabelings: []kubemeta.RelabelRule{dropRule, keepRule},
	})
	_ = MergeMonitorEndpoint(&held, "ns/a", &servicemonitors.Endpoint{
		MetricRelabelings: []kubemeta.RelabelRule{dropRule, keepRule},
	})
	if len(held.MetricRelabelings) != 2 {
		t.Errorf("identical chain was appended: %+v", held.MetricRelabelings)
	}
	if held.Monitors != nil {
		t.Errorf("an identical declaration was listed as a contributor: %v", held.Monitors)
	}
}

// Cadence: an explicit interval beats an empty one, two explicit intervals
// resolve to the finer, and the timeout always travels with the interval that
// won — including an EMPTY timeout replacing a set one.
func TestMergeCadence(t *testing.T) {
	for _, tc := range []struct {
		name           string
		heldIv, heldTo string
		epIv, epTo     string
		wantIv, wantTo string
		wantContrib    bool
	}{
		{"explicit-beats-empty", "", "", "30s", "25s", "30s", "25s", true},
		{"empty-loses", "30s", "25s", "", "", "30s", "25s", false},
		{"finer-wins", "30s", "25s", "10s", "5s", "10s", "5s", true},
		{"coarser-loses", "10s", "5s", "30s", "25s", "10s", "5s", false},
		{"timeout-travels-empty", "30s", "25s", "10s", "", "10s", "", true},
		{"equal-keeps-holder", "30s", "25s", "30s", "5s", "30s", "25s", false},
		{"prom-syntax-compares", "1m", "", "45s", "40s", "45s", "40s", true},
		{"unparseable-loser-keeps-holder", "30s", "25s", "nonsense", "1s", "30s", "25s", false},
		{"unparseable-holder-keeps-holder", "nonsense", "", "30s", "25s", "nonsense", "", false},
		{"lone-timeout-adopted", "", "", "", "9s", "", "9s", true},
		{"lone-timeout-both-keeps-holder", "", "7s", "", "9s", "", "7s", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			held := mergeHeld("ns/a", servicemonitors.Endpoint{Interval: tc.heldIv, ScrapeTimeout: tc.heldTo})
			_ = MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{Interval: tc.epIv, ScrapeTimeout: tc.epTo})
			if held.Interval != tc.wantIv || held.ScrapeTimeout != tc.wantTo {
				t.Errorf("cadence = %q/%q, want %q/%q", held.Interval, held.ScrapeTimeout, tc.wantIv, tc.wantTo)
			}
			if contributed := len(held.Monitors) > 0; contributed != tc.wantContrib {
				t.Errorf("contributed = %v (monitors %v), want %v", contributed, held.Monitors, tc.wantContrib)
			}
		})
	}
}

// Auth/TLS is one group, adopted whole when exactly one side carries any of
// it: the endpoint's CA, cert, serverName and skip-verify describe ONE TLS
// client, and mixing two monitors' halves would build one neither CR asked for.
func TestMergeAdoptsOneSidedAuthWhole(t *testing.T) {
	held := mergeHeld("ns/a", servicemonitors.Endpoint{
		MetricRelabelings: []kubemeta.RelabelRule{dropRule},
	})
	if rep := MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{
		BearerSecret:  "ns/tok/token",
		TLSCA:         "ns/ca/ca.crt",
		TLSServerName: "svc.internal",
	}); rep.AuthConflict {
		t.Error("one-sided auth reported as a conflict")
	}
	if held.AuthSecret != "ns/tok/token" || held.TLSCA != "ns/ca/ca.crt" || held.TLSServerName != "svc.internal" {
		t.Errorf("auth group not adopted whole: %+v", held)
	}
	if want := []string{"ns/a", "ns/b"}; !slices.Equal(held.Monitors, want) {
		t.Errorf("monitors = %v, want %v", held.Monitors, want)
	}
}

// Identical auth on both sides is what each CR asked for; differing auth keeps
// the holder's and reports the conflict — the one merge outcome that loses a
// declaration.
func TestMergeAuthIdenticalKeepsConflictingReports(t *testing.T) {
	held := mergeHeld("ns/a", servicemonitors.Endpoint{BearerSecret: "ns/tok/token"})
	if rep := MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{BearerSecret: "ns/tok/token"}); rep.AuthConflict {
		t.Error("identical auth reported as a conflict")
	}
	if held.Monitors != nil {
		t.Errorf("identical auth listed a contributor: %v", held.Monitors)
	}

	if rep := MergeMonitorEndpoint(&held, "ns/c", &servicemonitors.Endpoint{BearerSecret: "ns/other/token"}); !rep.AuthConflict {
		t.Error("differing auth not reported as a conflict")
	}
	if held.AuthSecret != "ns/tok/token" {
		t.Errorf("the holder's auth was replaced: %q", held.AuthSecret)
	}
	// InsecureSkipVerify alone is auth material too: it selects the trust
	// decision, and a monitor that verifies must conflict with one that does
	// not.
	held2 := mergeHeld("ns/a", servicemonitors.Endpoint{InsecureSkipVerify: true})
	if rep := MergeMonitorEndpoint(&held2, "ns/b", &servicemonitors.Endpoint{BearerSecret: "ns/tok/token"}); !rep.AuthConflict {
		t.Error("skip-verify vs bearer not reported as a conflict")
	}
	if !held2.InsecureSkipVerify || held2.AuthSecret != "" {
		t.Errorf("the holder's auth group was not kept whole: %+v", held2)
	}
}

// A loser can contribute relabelings AND conflict on auth in one merge; the
// chain still concatenates and the monitor is still listed — only its auth is
// refused.
func TestMergeConflictStillMergesTheRest(t *testing.T) {
	held := mergeHeld("ns/a", servicemonitors.Endpoint{BearerSecret: "ns/tok/token"})
	if rep := MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{
		BearerSecret:      "ns/other/token",
		MetricRelabelings: []kubemeta.RelabelRule{dropRule},
		Interval:          "10s",
	}); !rep.AuthConflict {
		t.Error("differing auth not reported")
	}
	if held.AuthSecret != "ns/tok/token" {
		t.Errorf("holder's auth replaced: %q", held.AuthSecret)
	}
	if len(held.MetricRelabelings) != 1 || held.Interval != "10s" {
		t.Errorf("the conflicting monitor's mergeable config was dropped with its auth: %+v", held)
	}
	if want := []string{"ns/a", "ns/b"}; !slices.Equal(held.Monitors, want) {
		t.Errorf("monitors = %v, want %v", held.Monitors, want)
	}
}

// The merge must not write into the indexed monitor's own rule slice:
// stampEndpoint copies, and appending must stay on the copy.
//
// The fixture has SPARE CAPACITY and the assertions look past len, because the
// obvious shape of this test cannot fail: a one-element slice literal has
// cap == len, so the merge's append always reallocates and an aliasing
// stampEndpoint leaves the endpoint's array untouched — the test passed on the
// broken implementation it was written to catch. The pointer check is the
// direct one (aliasing is a shared backing array, nothing else); the
// through-write check is what an index-1 write into a shared array looks like.
func TestMergeDoesNotAliasTheEndpointsRules(t *testing.T) {
	rules := append(make([]kubemeta.RelabelRule, 0, 4), dropRule)
	epA := servicemonitors.Endpoint{MetricRelabelings: rules}
	held := mergeHeld("ns/a", epA)
	if len(held.MetricRelabelings) == 0 {
		t.Fatal("stampEndpoint dropped the endpoint's rules")
	}
	if &held.MetricRelabelings[0] == &epA.MetricRelabelings[0] {
		t.Fatal("stampEndpoint aliased the indexed endpoint's rule slice instead of copying it")
	}
	_ = MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{
		MetricRelabelings: []kubemeta.RelabelRule{keepRule},
	})
	if len(epA.MetricRelabelings) != 1 || epA.MetricRelabelings[0].Regex != dropRule.Regex {
		t.Errorf("the indexed endpoint's rules were mutated: %+v", epA.MetricRelabelings)
	}
	if full := rules[:cap(rules)]; full[1].Action != "" || full[1].Regex != "" {
		t.Errorf("the merge wrote into the indexed endpoint's backing array: %+v", full[1])
	}
}

// A monitor contributing through a SECOND of its own endpoints is still one
// monitor: the `monitors` list is documented as absent whenever Monitor alone
// describes the target, so a consumer reading a non-empty list as "more than
// one monitor resolved to this URL" reads it wrong. The server walks every
// endpoint of one CR against one pod, so two endpoints colliding on one URL —
// here the second carrying an interval the first lacks — reach the merge with
// monitor == t.Monitor.
func TestMergeSameMonitorSecondEndpointIsNotAContributor(t *testing.T) {
	for _, tc := range []struct {
		name string
		ep   servicemonitors.Endpoint
	}{
		{"cadence", servicemonitors.Endpoint{Interval: "15s"}},
		{"relabelings", servicemonitors.Endpoint{MetricRelabelings: []kubemeta.RelabelRule{keepRule}}},
		{"auth", servicemonitors.Endpoint{BearerSecret: "ns/tok/token"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			held := mergeHeld("ns/pm", servicemonitors.Endpoint{
				MetricRelabelings: []kubemeta.RelabelRule{dropRule},
			})
			_ = MergeMonitorEndpoint(&held, "ns/pm", &tc.ep)
			if held.Monitors != nil {
				t.Errorf("a single monitor's second endpoint produced monitors = %v, want absent", held.Monitors)
			}
		})
	}

	// A genuinely second monitor arriving AFTER the holder's own extra endpoint
	// still lists both — the holder first.
	held := mergeHeld("ns/pm", servicemonitors.Endpoint{})
	_ = MergeMonitorEndpoint(&held, "ns/pm", &servicemonitors.Endpoint{Interval: "15s"})
	_ = MergeMonitorEndpoint(&held, "ns/other", &servicemonitors.Endpoint{
		MetricRelabelings: []kubemeta.RelabelRule{keepRule},
	})
	if want := []string{"ns/pm", "ns/other"}; !slices.Equal(held.Monitors, want) {
		t.Errorf("monitors = %v, want %v", held.Monitors, want)
	}
}

// The multi-Service fold — the same monitor endpoint offered once per matched
// Service, which is where the union broke — is pinned in internal/server
// (monitorunion_test.go), against the REAL nodeTargets loop. It was pinned
// here first, against a hand-copied miniature of that loop, and a miniature
// proves nothing about the caller: the exactness that keeps the fold a union
// is the caller's offer dedup (see MergeMonitorEndpoint's caller contract), so
// a re-implementation here would have passed while the server served the
// chain twice.

func TestPromDurationComparison(t *testing.T) {
	for _, tc := range []struct {
		in   string
		ok   bool
		want string // compared against via promDuration(want)
	}{
		{"30s", true, "30s"},
		{"1m", true, "60s"},
		{"1d", true, "24h"},
		{"1w", true, "7d"},
		{"1d12h", true, "36h"},
		{"500ms", true, "500ms"},
		{"1h30m", true, "90m"},
		{"", false, ""},
		{"0", false, ""},
		{"nonsense", false, ""},
		{"290y290y", false, ""}, // overflow must read as incomparable, not tiny
	} {
		got, ok := promDuration(tc.in)
		if ok != tc.ok {
			t.Errorf("promDuration(%q) ok = %v, want %v", tc.in, ok, tc.ok)
			continue
		}
		if !ok {
			continue
		}
		want, wok := promDuration(tc.want)
		if !wok || got != want {
			t.Errorf("promDuration(%q) = %v, want %v (%q)", tc.in, got, want, tc.want)
		}
	}
}
