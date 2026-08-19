package transform

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The roster in handoff.go is presented as EXHAUSTIVE — it is one half of a
// "Who hands off" / "Who must NOT hand off" pair, it carries the assurance
// "each verified against its failure path, not assumed", and transform.go
// points readers at it as authoritative — but nothing enforced that. It drifted
// once already: agent/cgroupstats started marking Handoff at two call sites in
// cmd/kubescrape-agent and the roster kept naming four producers, so a reviewer
// auditing the in-place contract would have concluded cgroupstats exports
// through the copying path. It does not.
//
// This is the tripwire. It pins WHERE the marks are, per file, and which roster
// entries each file's marks belong to. A count that moves is not a failure of
// this test — it is a new (or removed) promise about a payload nobody is
// allowed to look at afterwards, which is precisely the moment to re-read the
// roster. Verify the new marker's FAILURE path, write it into handoff.go's
// list, then update the map below.
//
// Per FILE rather than per package because the drift that happened is invisible
// at package granularity: cgroupstats' marks live in main.go, which the roster
// already covered for internal/metrics.
var handoffMarkers = map[string]struct {
	count  int
	roster []string
}{
	"internal/agent/promscrape/scraper.go":  {1, []string{"agent/promscrape"}},
	"internal/agent/promscrape/cadvisor.go": {1, []string{"agent/promscrape"}},
	"internal/agent/promscrape/summary.go":  {1, []string{"agent/promscrape"}},
	"internal/agent/cumagg/cumagg.go":       {1, []string{"agent/cumagg"}},
	"internal/agent/otlpingest/server.go":   {4, []string{"agent/otlpingest"}},
	// Six marks, two producers: obs.Registry and the logMetrics set
	// (internal/metrics), plus the cgroup sampler's export loop and its final
	// export. Both are marked at the call site because that is what knows the
	// retry policy — which is exactly why neither is visible from its own
	// package's source.
	"cmd/kubescrape-agent/main.go": {6, []string{"internal/metrics", "agent/cgroupstats"}},
}

func TestHandoffRosterNamesEveryProducerThatMarks(t *testing.T) {
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
		if n := strings.Count(string(b), "transform.Handoff("); n > 0 {
			rel, _ := filepath.Rel(root, path)
			got[filepath.ToSlash(rel)] = n
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for file, n := range got {
		want, ok := handoffMarkers[file]
		if !ok {
			t.Errorf("%s marks transform.Handoff (%d site(s)) and is not in the roster map: "+
				"verify its FAILURE path (does a retry rebuild from source, and is the payload "+
				"never read back?), add it to handoff.go's \"Who hands off\" list, then add it here", file, n)
			continue
		}
		if n != want.count {
			t.Errorf("%s has %d Handoff marks, the roster map expects %d: a marker was added or "+
				"removed — re-read handoff.go's list and update both", file, n, want.count)
		}
	}
	for file := range handoffMarkers {
		if _, ok := got[file]; !ok {
			t.Errorf("%s no longer marks transform.Handoff; drop it from the roster map (and from "+
				"handoff.go's list if nothing else in its package marks)", file)
		}
	}

	// The roster itself must NAME every producer those marks belong to.
	list := handsOffSection(t, root)
	seen := map[string]bool{}
	for _, m := range handoffMarkers {
		for _, name := range m.roster {
			if seen[name] {
				continue
			}
			seen[name] = true
			if !strings.Contains(list, name) {
				t.Errorf("handoff.go's \"Who hands off\" list does not name %q, which marks Handoff; "+
					"a roster that is short an entry is worse than no roster, because it is read as "+
					"proof the missing producer was checked", name)
			}
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	t.Logf("roster covers %v across %d marking files", names, len(got))
}

// handsOffSection returns the text of handoff.go's "Who hands off" list.
func handsOffSection(t *testing.T, root string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "internal", "agent", "transform", "handoff.go"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	start := strings.Index(doc, "Who hands off")
	end := strings.Index(doc, "Who must NOT hand off")
	if start < 0 || end < 0 || end < start {
		t.Fatal("handoff.go no longer carries the \"Who hands off\" / \"Who must NOT hand off\" pair; " +
			"the roster is the contract's only record that each marker was checked")
	}
	return doc[start:end]
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
