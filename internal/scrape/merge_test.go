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
	if _, conflict := MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{Port: "metrics"}); conflict {
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
	if _, conflict := MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{
		MetricRelabelings: []kubemeta.RelabelRule{keepRule},
	}); conflict {
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
	_, _ = MergeMonitorEndpoint(&held, "ns/a", &servicemonitors.Endpoint{
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
			_, _ = MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{Interval: tc.epIv, ScrapeTimeout: tc.epTo})
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
	if _, conflict := MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{
		BearerSecret:  "ns/tok/token",
		TLSCA:         "ns/ca/ca.crt",
		TLSServerName: "svc.internal",
	}); conflict {
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
	if _, conflict := MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{BearerSecret: "ns/tok/token"}); conflict {
		t.Error("identical auth reported as a conflict")
	}
	if held.Monitors != nil {
		t.Errorf("identical auth listed a contributor: %v", held.Monitors)
	}

	if _, conflict := MergeMonitorEndpoint(&held, "ns/c", &servicemonitors.Endpoint{BearerSecret: "ns/other/token"}); !conflict {
		t.Error("differing auth not reported as a conflict")
	}
	if held.AuthSecret != "ns/tok/token" {
		t.Errorf("the holder's auth was replaced: %q", held.AuthSecret)
	}
	// InsecureSkipVerify alone is auth material too: it selects the trust
	// decision, and a monitor that verifies must conflict with one that does
	// not.
	held2 := mergeHeld("ns/a", servicemonitors.Endpoint{InsecureSkipVerify: true})
	if _, conflict := MergeMonitorEndpoint(&held2, "ns/b", &servicemonitors.Endpoint{BearerSecret: "ns/tok/token"}); !conflict {
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
	if _, conflict := MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{
		BearerSecret:      "ns/other/token",
		MetricRelabelings: []kubemeta.RelabelRule{dropRule},
		Interval:          "10s",
	}); !conflict {
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
func TestMergeDoesNotAliasTheEndpointsRules(t *testing.T) {
	epA := servicemonitors.Endpoint{MetricRelabelings: []kubemeta.RelabelRule{dropRule}}
	held := mergeHeld("ns/a", epA)
	_, _ = MergeMonitorEndpoint(&held, "ns/b", &servicemonitors.Endpoint{
		MetricRelabelings: []kubemeta.RelabelRule{keepRule},
	})
	if len(epA.MetricRelabelings) != 1 || epA.MetricRelabelings[0].Regex != dropRule.Regex {
		t.Errorf("the indexed endpoint's rules were mutated: %+v", epA.MetricRelabelings)
	}
}

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
