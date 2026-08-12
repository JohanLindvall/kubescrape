package cgroupstats

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// subdirs replaces os.ReadDir on the discovery path, so the ONE thing that
// matters is that it answers the same question. A raw getdents64 loop that
// disagreed by a single entry would silently stop discovering some shape of
// container cgroup — which is the failure mode this whole package exists to
// make impossible.
//
// The comparison runs over a directory holding everything a walk can meet:
// subdirectories, plain files, a symlink to a directory (os.ReadDir's
// DirEntry.IsDir is FALSE for one, since it reports the entry's own type), a
// dangling symlink, a name with an unusual byte, and enough entries to spill
// past one getdents64 buffer. That directory is the required case and its
// assertion is exact equality.
//
// The live kernel directories are then compared too, but WEAKLY, because they
// change while they are being read: two independent listings of /proc taken
// back to back differed in 189 of 200 comparisons on a box spawning short-lived
// processes, and 0 of 200 on an idle one — a test binary racing other test
// binaries in CI is the busy case. See liveDisagreements.
func TestSubdirsMatchesReadDir(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"kubepods.slice", "system.slice", "a", "b-c.scope", "x y", "über.slice"} {
		if err := os.Mkdir(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"cgroup.controllers", "cpu.stat", "memory.current", "memory.stat", "pids.max"} {
		writeFile(t, filepath.Join(dir, f), "0\n")
	}
	if err := os.Symlink(filepath.Join(dir, "a"), filepath.Join(dir, "link-to-dir")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "nothing"), filepath.Join(dir, "dangling")); err != nil {
		t.Fatal(err)
	}
	// Past one 4 KiB buffer: ~200 entries of ~30 bytes each is two getdents64
	// calls, which is the boundary a hand-rolled parser gets wrong.
	for i := range 200 {
		if err := os.Mkdir(filepath.Join(dir, "deep-"+hexID(i)[:20]), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	buf := make([]byte, dirBufBytes)

	// The fabricated directory is QUIESCENT — nothing but this test writes to it
	// — so the two listings are two views of one set and every difference is a
	// parser bug. This is the required case and its assertion is exact.
	want, err := readDirSubdirs(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := subdirs(dir, buf)
	if err != nil {
		t.Fatalf("subdirs(%s): %v", dir, err)
	}
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(want, got) {
		t.Errorf("subdirs(%s) disagrees with os.ReadDir:\n got %v\nwant %v", dir, got, want)
	}

	// The live directories are worth listing too — they are the real kernel
	// filesystems this parser runs against, with entry counts, name lengths and
	// d_type values no fixture reproduces — but they MOVE: /proc's entries are
	// process ids, so a process starting or exiting between the two listings
	// changes the set, and comparing two independent snapshots for equality then
	// fails for a reason that has nothing to do with the parser. See
	// liveDisagreements for what is asserted instead.
	for _, d := range []string{DefaultRoot, "/proc", "/sys/fs/cgroup/system.slice"} {
		var persistent []string
		for attempt := range liveAttempts {
			bad, readable := liveDisagreements(d, buf)
			if !readable {
				// Not present on this machine, or it went away mid-test; the
				// fabricated dir above is the required case.
				persistent = nil
				break
			}
			if len(bad) == 0 {
				persistent = nil
				break
			}
			if attempt == 0 {
				persistent = bad
			} else {
				persistent = slices.DeleteFunc(persistent, func(s string) bool { return !slices.Contains(bad, s) })
			}
			if len(persistent) == 0 {
				break
			}
		}
		if len(persistent) > 0 {
			t.Errorf("subdirs(%s) disagrees with os.ReadDir about %v on all %d attempts, for entries that were present (or absent) in both os.ReadDir listings taken either side of it: "+
				"a difference that survives that is the parser's, not the directory changing under it", d, persistent, liveAttempts)
		}
	}
}

// liveAttempts is how many times a disagreement over a live directory is
// re-measured before it counts. A parser bug is deterministic — dropping the
// last entry of a getdents64 buffer, or misreading d_type — so it reproduces on
// every attempt over the same long-lived entries; a scheduler does not create
// and destroy one name this many times in a row.
const liveAttempts = 5

// liveDisagreements lists how subdirs and os.ReadDir disagree about a directory
// whose contents may be changing, ignoring every entry that moved while they
// ran. It reports false when the directory cannot be listed at all.
//
// The directory is listed os.ReadDir / subdirs / os.ReadDir. A name in BOTH
// os.ReadDir listings was there for the whole window, so subdirs owes it; a name
// in NEITHER was never there, so subdirs inventing it is a bug. A name in only
// one of them appeared or vanished inside the window and is unconstrained —
// which is exactly the churn that used to fail the test.
//
// What that gives up: a parser bug affecting only an entry that happened to
// churn goes unseen on this pass. That is worth running anyway, because such a
// bug is not specific to that entry — it reproduces on the fabricated
// directory's exact comparison above, which holds every entry shape a walk can
// meet including a two-buffer spill.
func liveDisagreements(d string, buf []byte) ([]string, bool) {
	before, err := readDirSubdirs(d)
	if err != nil {
		return nil, false
	}
	got, err := subdirs(d, buf)
	if err != nil {
		return []string{"error " + err.Error()}, true
	}
	after, err := readDirSubdirs(d)
	if err != nil {
		return nil, false
	}
	have := make(map[string]bool, len(got))
	for _, n := range got {
		have[n] = true
	}
	const inBefore, inAfter = 1, 2
	seen := make(map[string]int, len(before)+len(after))
	for _, n := range before {
		seen[n] |= inBefore
	}
	for _, n := range after {
		seen[n] |= inAfter
	}
	var bad []string
	for n, when := range seen {
		if when == inBefore|inAfter && !have[n] {
			bad = append(bad, "missing "+n)
		}
	}
	for _, n := range got {
		if seen[n] == 0 {
			bad = append(bad, "invented "+n)
		}
	}
	slices.Sort(bad)
	return bad, true
}

// readDirSubdirs is the os.ReadDir half of the comparison: the names subdirs is
// expected to return, filtered by the same DirEntry.IsDir the doc comment on
// subdirs points at.
func readDirSubdirs(d string) ([]string, error) {
	ents, err := os.ReadDir(d)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// An unreadable directory has to be an error, not an empty listing: discovery
// reads an empty listing as "these containers are gone" only when the pass
// reports itself COMPLETE, and a silent empty here would retire every live
// container on the node.
func TestSubdirsReportsAnUnreadableDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the permission bits")
	}
	dir := t.TempDir()
	sub := filepath.Join(dir, "locked")
	if err := os.Mkdir(sub, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sub, 0o755) })
	if _, err := subdirs(sub, make([]byte, dirBufBytes)); err == nil {
		t.Error("an unreadable directory listed as empty; discovery would read that as every container having gone away")
	}
	if _, err := subdirs(filepath.Join(dir, "absent"), make([]byte, dirBufBytes)); err == nil {
		t.Error("a missing directory listed as empty")
	}
	// A plain FILE is not a directory listing either, and O_DIRECTORY is what
	// turns that into an error at open rather than a surprise mid-parse.
	writeFile(t, filepath.Join(dir, "afile"), "x")
	if _, err := subdirs(filepath.Join(dir, "afile"), make([]byte, dirBufBytes)); err == nil {
		t.Error("a regular file listed as a directory")
	}
}
