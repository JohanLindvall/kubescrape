//go:build linux

package cgroupstats

import (
	"path/filepath"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// LINUX-TAGGED, because it asserts a property only readdir_linux.go has. The
// !linux fallback IS os.ReadDir — deliberately, so this package's parsing tests
// still run on a developer's machine — and measured against that body this
// budget is exceeded eightfold (2,532 allocations against 430), so an untagged
// copy would fail the one build it exists to keep working.
//
// The whole point of the raw listing: a cgroup directory holds ~45 control
// files and one or two children, and the walk wants only the children.
//
// os.ReadDir materialised a DirEntry and a name string for EVERY entry, so a
// pass over a 200-container node allocated 21,950 objects and 1.44 MiB — 98% of
// it for files discarded on the next line, and the largest allocation source in
// a pipeline whose per-second sweep allocates nothing at all. This is the
// enforcement of the fix; the budget scales with DIRECTORIES so a fixture with
// fatter cgroups cannot quietly re-admit the per-file cost.
func TestDiscoveryDoesNotAllocatePerControlFile(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector changes escape analysis and adds bookkeeping allocations")
	}
	const containers = 20
	// A cgroup directory's real file count, which is what makes the ratio
	// between "per directory" and "per entry" decidable.
	const filesPerDir = 45
	root := newRoot(t)
	var dirs []string
	for i := range containers {
		d := systemdContainerDir(root, i, hexID(i))
		makeContainer(t, d, uint64(i)*1000, 1<<20, 1<<18)
		dirs = append(dirs, d, filepath.Dir(d))
	}
	dirs = append(dirs, root, filepath.Join(root, "kubepods.slice"),
		filepath.Join(root, "kubepods.slice", "kubepods-burstable.slice"))
	for _, d := range dirs {
		for i := range filesPerDir {
			writeFile(t, filepath.Join(d, "ctl."+string(rune('a'+i%26))+string(rune('a'+i/26))), "0\n")
		}
	}

	var found []containerDir
	got := testing.AllocsPerRun(20, func() {
		f, _, err := discoverContainers(root)
		if err != nil {
			t.Fatal(err)
		}
		found = f
	})
	if len(found) != containers {
		t.Fatalf("discovered %d containers, want %d", len(found), containers)
	}
	// Every directory costs a slice and a name per SUBDIRECTORY, plus the path
	// joins and the result entry; nothing costs anything per control file. Ten
	// per directory is generous for that and still an order of magnitude below
	// the ~2.3 per ENTRY that listing the files costs.
	budget := float64(10 * len(dirs))
	if got > budget {
		t.Errorf("a discovery pass over %d directories holding %d control files each allocates %.0f objects, over the %.0f budget: "+
			"the listing is materialising entries it discards (os.ReadDir builds a DirEntry and a name for every control file; readdir_linux.go filters on the getdents64 entry type instead)",
			len(dirs), filesPerDir, got, budget)
	}
	t.Logf("%d directories, %d files each: %.0f allocations per pass", len(dirs), filesPerDir, got)
}
