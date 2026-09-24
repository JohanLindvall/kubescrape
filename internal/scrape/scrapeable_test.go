package scrape

import (
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// Scrapeable (the derivation's yes/no) and ScrapeableReasons (/v1/explain's
// notScrapeableWhy) must be two readings of ONE check: a pod explain calls
// scrapeable while nodeTargets serves it nothing — or the reverse — inverts the
// question explain exists to answer. They were two independent if-chains held
// equal by nothing; now both read unscrapeableReasons, and this pins it over
// every combination of the four conditions, including that each failed
// condition yields exactly one reason naming it.
func TestScrapeableAndItsReasonsAreOneCheck(t *testing.T) {
	now := time.Now()
	conds := []struct {
		name  string
		apply func(*kubemeta.Pod)
		// word is a fragment of the one reason this condition must produce.
		word string
	}{
		{"no IP", func(p *kubemeta.Pod) { p.PodIP = "" }, "no IP"},
		{"deleted", func(p *kubemeta.Pod) { p.DeletedAt = &now }, "deleted"},
		{"terminating", func(p *kubemeta.Pod) { p.DeletionTimestamp = &now }, "terminating"},
		{"finished", func(p *kubemeta.Pod) { p.Phase = "Succeeded" }, "finished"},
	}
	for mask := range 1 << len(conds) {
		pod := basePod()
		var want []string
		for i, c := range conds {
			if mask&(1<<i) != 0 {
				c.apply(&pod)
				want = append(want, c.word)
			}
		}
		reasons := ScrapeableReasons(pod)
		if got := Scrapeable(pod); got != (len(reasons) == 0) {
			t.Fatalf("mask %04b: Scrapeable = %v but ScrapeableReasons = %q", mask, got, reasons)
		}
		if got := Scrapeable(pod); got != (mask == 0) {
			t.Fatalf("mask %04b: Scrapeable = %v, want %v", mask, got, mask == 0)
		}
		if len(reasons) != len(want) {
			t.Fatalf("mask %04b: %d reasons %q, want one per failed condition (%q)", mask, len(reasons), reasons, want)
		}
		for i, w := range want {
			if !strings.Contains(reasons[i], w) {
				t.Errorf("mask %04b: reason %d = %q, want it to name %q", mask, i, reasons[i], w)
			}
		}
	}
	// Failed is the other finished phase; it must read the same way.
	pod := basePod()
	pod.Phase = "Failed"
	if Scrapeable(pod) || len(ScrapeableReasons(pod)) != 1 {
		t.Errorf("a Failed pod: Scrapeable = %v, reasons %q", Scrapeable(pod), ScrapeableReasons(pod))
	}
}

// Scrapeable runs once per pod per target derivation; reading the shared set
// must stay allocation-free.
func TestScrapeableIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector changes escape analysis; allocation budgets are pinned without it")
	}
	pod := basePod()
	if n := testing.AllocsPerRun(100, func() { _ = Scrapeable(pod) }); n != 0 {
		t.Errorf("Scrapeable allocates %v per call, want 0", n)
	}
}
