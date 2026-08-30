package scrape

// mergeCadence short-circuits when both sides spell the interval identically,
// because promDuration is a regexp and re-deriving "these two equal strings are
// equal durations" was 48.6% of every object a colliding-monitor derivation
// allocated. A short-circuit on a merge is only safe if it is EQUIVALENT, so
// this drives the equal case through every spelling that reaches it — including
// the ones promDuration cannot parse, where the long path also keeps the
// holder's.

import (
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func TestMergeCadenceEqualIntervalsKeepTheHolder(t *testing.T) {
	for _, tc := range []struct {
		name              string
		interval          string
		heldTO, askedTO   string
		wantAdopted       bool
		wantIvl, wantTOut string
	}{
		{"plain", "30s", "", "", false, "30s", ""},
		{"operator syntax", "1d12h", "", "", false, "1d12h", ""},
		{"holder has a timeout", "30s", "10s", "20s", false, "30s", "10s"},
		{"only the endpoint has a timeout", "30s", "", "20s", false, "30s", ""},
		// Unparseable and equal: promDuration fails on BOTH sides, so the long
		// path returns false too. The short-circuit must not turn an
		// unparseable pair into an adoption.
		{"unparseable", "banana", "", "5s", false, "banana", ""},
		{"empty unit", "30", "", "", false, "30", ""},
		{"zero", "0s", "", "", false, "0s", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			held := kubemeta.ScrapeTarget{Interval: tc.interval, ScrapeTimeout: tc.heldTO}
			ep := servicemonitors.Endpoint{Interval: tc.interval, ScrapeTimeout: tc.askedTO}
			if got := mergeCadence(&held, &ep); got != tc.wantAdopted {
				t.Errorf("mergeCadence adopted = %v, want %v", got, tc.wantAdopted)
			}
			if held.Interval != tc.wantIvl || held.ScrapeTimeout != tc.wantTOut {
				t.Errorf("after the merge interval=%q timeout=%q, want %q/%q",
					held.Interval, held.ScrapeTimeout, tc.wantIvl, tc.wantTOut)
			}
		})
	}
}

// …and a DIFFERENT spelling of the same duration still goes the long way, which
// is what makes the short-circuit a fast path rather than a semantic change:
// "1m" and "60s" are equal durations spelled differently, and equal durations
// keep the holder's — through the parse, not through the string compare.
func TestMergeCadenceDifferentSpellingsStillCompare(t *testing.T) {
	held := kubemeta.ScrapeTarget{Interval: "1m", ScrapeTimeout: "10s"}
	ep := servicemonitors.Endpoint{Interval: "60s", ScrapeTimeout: "20s"}
	if mergeCadence(&held, &ep) {
		t.Error("60s is not finer than 1m; the holder must keep")
	}
	if held.Interval != "1m" || held.ScrapeTimeout != "10s" {
		t.Errorf("holder became %q/%q", held.Interval, held.ScrapeTimeout)
	}
	// And a genuinely finer one still wins, with its own timeout.
	finer := servicemonitors.Endpoint{Interval: "15s", ScrapeTimeout: "5s"}
	if !mergeCadence(&held, &finer) {
		t.Fatal("15s is finer than 1m and must be adopted")
	}
	if held.Interval != "15s" || held.ScrapeTimeout != "5s" {
		t.Errorf("adopted %q/%q, want 15s/5s", held.Interval, held.ScrapeTimeout)
	}
}
