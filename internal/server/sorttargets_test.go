package server

// sortTargets replaced a sort.Slice over the served list with an index
// permutation, for the reason its doc comment gives: the element is 616 bytes
// and embeds the pod document. A permutation applied wrongly does not fail
// loudly — it duplicates one target and drops another, which reads downstream
// as a pod that stopped being scraped — so the order is pinned against the
// sort it replaced, and the permutation is pinned as a permutation.

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// sortTargetsKey renders the four fields the order is defined on.
func sortTargetsKey(t *kubemeta.ScrapeTarget) string {
	return t.URL + "\x00" + t.Monitor + "\x00" + t.Source + "\x00" + t.Pod.UID
}

// randomTargets builds a list with heavy key collisions on every prefix of the
// order, so the tiebreaks are exercised rather than skipped by a distinct URL.
func randomTargets(r *rand.Rand, n int) []kubemeta.ScrapeTarget {
	out := make([]kubemeta.ScrapeTarget, n)
	for i := range out {
		out[i] = kubemeta.ScrapeTarget{
			URL:     fmt.Sprintf("http://10.0.0.%d:%d/metrics", r.IntN(4), 9090+r.IntN(3)),
			Monitor: []string{"", "", "monitoring/a", "monitoring/b"}[r.IntN(4)],
			Source:  []string{"pod", "service", "servicemonitor", "podmonitor"}[r.IntN(4)],
			Pod:     kubemeta.Pod{UID: fmt.Sprintf("uid-%d", r.IntN(n+1))},
		}
	}
	return out
}

// The order must be exactly the one sort.Slice produced. The comparator is a
// TOTAL order (the pod UID is the final tiebreak), so an unstable sort's answer
// is unique and the two are directly comparable.
func TestSortTargetsMatchesTheSortItReplaced(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, n := range []int{0, 1, 2, 3, 7, 110, 513} {
		got := randomTargets(r, n)
		want := make([]kubemeta.ScrapeTarget, n)
		copy(want, got)
		sort.Slice(want, func(i, j int) bool {
			return sortTargetsKey(&want[i]) < sortTargetsKey(&want[j])
		})

		sortTargets(got)

		for i := range got {
			if a, b := sortTargetsKey(&got[i]), sortTargetsKey(&want[i]); a != b {
				t.Fatalf("n=%d: element %d = %q, want %q", n, i, a, b)
			}
		}
	}
}

// A permutation must MOVE elements, never duplicate or lose them: the failure
// mode of a wrong cycle walk is a served list that still has the right length
// and the right ETag shape while one pod's target has silently become a copy
// of another's.
func TestSortTargetsIsAPermutation(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for _, n := range []int{1, 2, 5, 64, 200} {
		in := randomTargets(r, n)
		// Stamp a unique marker on each element, in a field the order does not
		// read, so a duplicate is visible even among equal sort keys.
		for i := range in {
			in[i].Address = fmt.Sprintf("marker-%d", i)
		}
		before := map[string]int{}
		for i := range in {
			before[in[i].Address]++
		}

		sortTargets(in)

		after := map[string]int{}
		for i := range in {
			after[in[i].Address]++
		}
		if len(after) != n {
			t.Fatalf("n=%d: %d distinct elements survived the sort, want %d", n, len(after), n)
		}
		for k, c := range before {
			if after[k] != c {
				t.Fatalf("n=%d: element %q appears %d times after the sort, want %d", n, k, after[k], c)
			}
		}
	}
}

// permuteTargets is the half that has no comparator to hide behind: given an
// explicit permutation it must apply exactly that one.
func TestPermuteTargetsAppliesTheIndex(t *testing.T) {
	for _, idx := range [][]int32{
		{0, 1, 2, 3},       // identity
		{3, 2, 1, 0},       // full reversal (two 2-cycles)
		{1, 2, 3, 0},       // one 4-cycle
		{0, 2, 1, 3},       // one 2-cycle among fixed points
		{2, 0, 1, 4, 3, 5}, // a 3-cycle, a 2-cycle and a fixed point
	} {
		n := len(idx)
		s := make([]kubemeta.ScrapeTarget, n)
		for i := range s {
			s[i].Address = fmt.Sprintf("%d", i)
		}
		want := make([]string, n)
		for i, from := range idx {
			want[i] = fmt.Sprintf("%d", from)
		}
		permuteTargets(s, append([]int32(nil), idx...))
		for i := range s {
			if s[i].Address != want[i] {
				t.Fatalf("idx=%v: s[%d] = %q, want %q", idx, i, s[i].Address, want[i])
			}
		}
	}
}
