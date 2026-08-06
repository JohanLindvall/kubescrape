package transform

// The package doc's cost model is load-bearing prose: it is what an operator
// budgets a transforms file against, and it omitted the per-batch deep copy —
// which measures as ~91% of a no-op script's cost — for as long as the copy
// existed. Two reviewers independently reported the same gap. Pin it the way
// this repo pins its other drift-prone prose (logscrub's default set, the
// metrics catalogue): the claim has to name the copy, or this fails.

import (
	"os"
	"strings"
	"testing"
)

func TestPackageDocStatesTheBatchCopy(t *testing.T) {
	src, err := os.ReadFile("transform.go")
	if err != nil {
		t.Fatal(err)
	}
	doc, _, ok := strings.Cut(string(src), "\npackage transform")
	if !ok {
		t.Fatal("no package clause in transform.go")
	}
	_, cost, ok := strings.Cut(doc, "// Cost model:")
	if !ok {
		t.Fatal("the package doc no longer states a cost model")
	}
	for _, want := range []string{"copy", "CopyTo"} {
		if !strings.Contains(cost, want) {
			t.Errorf("the package doc's cost model does not mention %q; a transforms file's dominant cost is the deep copy of every batch, not the script", want)
		}
	}
}
