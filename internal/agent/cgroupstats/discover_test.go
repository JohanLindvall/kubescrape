package cgroupstats

import (
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func discoveredIDs(t *testing.T, root string) []string {
	t.Helper()
	found, _, err := discoverContainers(root)
	if err != nil {
		t.Fatalf("discoverContainers: %v", err)
	}
	var ids []string
	for _, c := range found {
		ids = append(ids, c.id)
	}
	sort.Strings(ids)
	return ids
}

// The kind layout is the one verified on a live node, and the one a hardcoded
// /sys/fs/cgroup/kubepods.slice would miss entirely.
func TestDiscoverKindNestedLayout(t *testing.T) {
	root := newRoot(t)
	a, b := hexID(1), hexID(2)
	makeContainer(t, kindContainerDir(root, 1, a), 1000, 1<<20, 1<<18)
	makeContainer(t, kindContainerDir(root, 1, b), 2000, 2<<20, 1<<18)

	found, _, err := discoverContainers(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 2 {
		t.Fatalf("found %d containers, want 2: %+v", len(found), found)
	}
	for _, c := range found {
		if c.podUID != podUID(1) {
			t.Errorf("container %s: podUID = %q, want %q", c.id[:8], c.podUID, podUID(1))
		}
	}
}

// Every other layout a Kubernetes node produces, discovered by the same walk.
func TestDiscoverEveryLayout(t *testing.T) {
	cases := []struct {
		name string
		dir  func(root string, pod int, cid string) string
	}{
		{"systemd burstable", systemdContainerDir},
		{"systemd guaranteed (no QoS segment)", guaranteedContainerDir},
		{"cgroupfs driver", cgroupfsContainerDir},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := newRoot(t)
			cid := hexID(7)
			makeContainer(t, tc.dir(root, 3, cid), 1000, 1<<20, 1<<18)

			found, _, err := discoverContainers(root)
			if err != nil {
				t.Fatal(err)
			}
			if len(found) != 1 {
				t.Fatalf("found %d containers, want 1: %+v", len(found), found)
			}
			if found[0].id != cid {
				t.Errorf("id = %q, want %q", found[0].id, cid)
			}
			if found[0].podUID != podUID(3) {
				t.Errorf("podUID = %q, want %q", found[0].podUID, podUID(3))
			}
		})
	}
}

// Two layouts side by side, which is not a real node but proves the walk does
// not stop at the first kubepods root it finds.
func TestDiscoverBothRootsAtOnce(t *testing.T) {
	root := newRoot(t)
	makeContainer(t, kindContainerDir(root, 1, hexID(1)), 1000, 1<<20, 1<<18)
	makeContainer(t, systemdContainerDir(root, 2, hexID(2)), 1000, 1<<20, 1<<18)

	ids := discoveredIDs(t, root)
	if len(ids) != 2 {
		t.Fatalf("found %v, want both containers", ids)
	}
}

// The walk must not wander into the rest of the machine's cgroups. system.slice
// on a real node holds a hundred-odd units, several of which have children of
// their own, and reading all of that every discovery cycle is pure waste.
func TestDiscoverSkipsUnrelatedSlices(t *testing.T) {
	root := newRoot(t)
	makeContainer(t, systemdContainerDir(root, 1, hexID(1)), 1000, 1<<20, 1<<18)
	for i := range 40 {
		unit := filepath.Join(root, "system.slice", "unit-"+string(rune('a'+i%26))+".service", "deep", "deeper")
		if err := os.MkdirAll(unit, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// A container-shaped directory parked outside any pod slice must not be
	// picked up either: cgroupid.Parse's shape verdict requires a pod segment.
	if err := os.MkdirAll(filepath.Join(root, "system.slice", "cri-containerd-"+hexID(9)+".scope"), 0o755); err != nil {
		t.Fatal(err)
	}
	ids := discoveredIDs(t, root)
	if len(ids) != 1 || ids[0] != hexID(1) {
		t.Fatalf("found %v, want only the pod's container", ids)
	}
}

// CRI-O parks crio-conmon-<id>.scope as a sibling of the container's own scope,
// with the SAME container id in its name. Both satisfy cgroupid.Parse's shape
// verdict, so without the dedupe one container would produce two entries — two
// OTLP resources with identical identity in one payload.
func TestDiscoverDedupesTheConmonSiblingScope(t *testing.T) {
	root := newRoot(t)
	cid := hexID(4)
	pod := filepath.Join(root, "kubepods.slice", "kubepods-burstable.slice",
		"kubepods-burstable-pod"+systemdUID(5)+".slice")
	makeContainer(t, filepath.Join(pod, "crio-"+cid+".scope"), 1000, 1<<20, 1<<18)
	makeContainer(t, filepath.Join(pod, "crio-conmon-"+cid+".scope"), 5, 1<<10, 0)

	found, _, err := discoverContainers(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 {
		t.Fatalf("found %d entries, want 1 (the conmon sibling must fold into the container): %+v", len(found), found)
	}
	if base := filepath.Base(found[0].path); base != "crio-"+cid+".scope" {
		t.Errorf("kept %q, want the container's own scope (the shorter basename)", base)
	}
}

// The dedupe must not depend on readdir order.
func TestDiscoverConmonDedupeIsOrderIndependent(t *testing.T) {
	root := newRoot(t)
	cid := hexID(4)
	pod := filepath.Join(root, "kubepods.slice", "kubepods-burstable.slice",
		"kubepods-burstable-pod"+systemdUID(5)+".slice")
	// "crio-conmon-..." sorts BEFORE "crio-<hex>..." whenever the id starts
	// with a digit, so this ordering is the one a real readdir produces.
	makeContainer(t, filepath.Join(pod, "crio-conmon-"+cid+".scope"), 5, 1<<10, 0)
	makeContainer(t, filepath.Join(pod, "crio-"+cid+".scope"), 1000, 1<<20, 1<<18)

	found, _, err := discoverContainers(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || filepath.Base(found[0].path) != "crio-"+cid+".scope" {
		t.Fatalf("got %+v, want only the container's own scope", found)
	}
}

// Pointing the flag straight at the kubepods slice must work: it is the obvious
// thing to try when autodetection is in doubt, and the search phase has to
// notice that the root IS the root.
func TestDiscoverWhenRootIsTheKubepodsSlice(t *testing.T) {
	base := newRoot(t)
	makeContainer(t, systemdContainerDir(base, 1, hexID(1)), 1000, 1<<20, 1<<18)
	root := filepath.Join(base, "kubepods.slice")

	ids := discoveredIDs(t, root)
	if len(ids) != 1 {
		t.Fatalf("found %v, want the container", ids)
	}
}

func TestDiscoverEmptyRootFindsNothing(t *testing.T) {
	root := newRoot(t)
	if ids := discoveredIDs(t, root); len(ids) != 0 {
		t.Fatalf("found %v in an empty hierarchy", ids)
	}
}

func TestDiscoverUnreadableRootIsAnError(t *testing.T) {
	if _, _, err := discoverContainers(filepath.Join(t.TempDir(), "not-mounted")); err == nil {
		t.Fatal("expected an error for a root that does not exist")
	}
}

// The other side of that rule, in the ROOT SEARCH specifically: a directory
// BELOW the root that cannot be listed is an incomplete pass, never an error —
// so whatever the search did find is still returned and still sampled.
//
// It is pinned because kubepodsRoots used to say otherwise in its shape: one
// recursive func returning error, with a `depth == 0` arm that only the
// outermost call could reach, and error plumbing threaded through the loop that
// could not fire. A future edit that made a subtree failure escape would have
// looked like tightening the existing code, and it would have made an
// unreadable neighbour directory retire every container on the node
// (Sampler.walk drops the whole pass on an error).
func TestDiscoverUnreadableDirectoryBesideTheRootIsIncompleteNotAnError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read a 0000 directory")
	}
	root := newRoot(t)
	makeContainer(t, systemdContainerDir(root, 1, hexID(1)), 1000, 1<<20, 1<<18)
	// A sibling of kubepods.slice that the depth-0 pass descends into (every
	// child of the root is walked, since a custom --cgroup-root can be named
	// anything) and cannot list.
	locked := filepath.Join(root, "kubelet.slice")
	if err := os.Mkdir(locked, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })

	found, complete, err := discoverContainers(root)
	if err != nil {
		t.Fatalf("an unreadable directory beside the root failed the whole pass: %v", err)
	}
	if complete {
		t.Error("the pass reported itself COMPLETE with a directory it could not read; discovery would then retire every container the failed subtree might have held")
	}
	if len(found) != 1 {
		t.Errorf("found %d containers, want the 1 under the readable root", len(found))
	}
}

// CRI-O creates the conmon scope BEFORE the container's own, so a discovery
// pass can legitimately see only the supervisor. Latching it for the
// container's life reported the supervisor's few MiB under the workload's
// identity (measured: 4 MiB where the container held 512), with nothing to say
// so — a tracked container therefore follows a BETTER path when one appears.
func TestTrackedContainerIsRepointedAtItsOwnScope(t *testing.T) {
	h := newHarness(t)
	cid := hexID(4)
	pod := filepath.Join(h.root, "kubepods.slice", "kubepods-burstable.slice",
		"kubepods-burstable-pod"+systemdUID(5)+".slice")
	conmon := filepath.Join(pod, "crio-conmon-"+cid+".scope")
	own := filepath.Join(pod, "crio-"+cid+".scope")

	// Pass one: only the supervisor exists.
	makeContainer(t, conmon, 1000, 4<<20, 0)
	h.discover()
	if got := h.tracked[cid]; got == nil || got.dir != conmon {
		t.Fatalf("the first pass tracked %v, want the only path there was", got)
	}
	h.advance(time.Second)

	// Pass two: the container's own scope is there now.
	makeContainer(t, own, 1000, 512<<20, 0)
	h.discover()
	c := h.tracked[cid]
	if c == nil || c.dir != own {
		t.Fatalf("still sampling %v; the supervisor's cgroup is reported under the container's identity for its whole life", c.dir)
	}

	// And the supervisor's readings are gone with it: folding two unrelated
	// cgroups into one distribution would be worse than either alone, and the
	// CPU baseline would derive one enormous rate across the switch.
	if c.mem.n != 0 || c.cpu.n != 0 || c.havePrev {
		t.Errorf("the supervisor's window survived the re-point (mem n=%d cpu n=%d havePrev=%v)", c.mem.n, c.cpu.n, c.havePrev)
	}
	h.advance(time.Second)
	setUsage(t, own, 2_000_000)
	h.advance(time.Second)
	setUsage(t, own, 3_000_000)
	h.advance(time.Second)
	g := h.exportOnce(t)[cid]
	if g[nameMemMax] != float64(512<<20) {
		t.Errorf("%s = %v, want the container's own %v", nameMemMax, g[nameMemMax], float64(512<<20))
	}
}

// The reverse ordering happens at the END of a container's life: its own scope
// is removed while conmon lingers. Following the listing DOWN to the supervisor
// there would reproduce the same defect at the other end, so a re-point only
// ever goes to a better path.
func TestTrackedContainerIsNotRepointedAtASupervisorScope(t *testing.T) {
	h := newHarness(t)
	cid := hexID(4)
	pod := filepath.Join(h.root, "kubepods.slice", "kubepods-burstable.slice",
		"kubepods-burstable-pod"+systemdUID(5)+".slice")
	own := filepath.Join(pod, "crio-"+cid+".scope")
	makeContainer(t, own, 1000, 512<<20, 0)
	makeContainer(t, filepath.Join(pod, "crio-conmon-"+cid+".scope"), 5, 4<<20, 0)
	h.discover()
	if h.tracked[cid].dir != own {
		t.Fatalf("tracked %q, want the container's own scope", h.tracked[cid].dir)
	}

	if err := os.RemoveAll(own); err != nil {
		t.Fatal(err)
	}
	h.discover()
	if got := h.tracked[cid]; got != nil && got.dir != own {
		t.Errorf("re-pointed to %q after the container's own scope went away; the supervisor would be measured as the workload", got.dir)
	}
}

// A directory that cannot be listed is NOT evidence that the containers under
// it are gone. Pruning on it retired live containers, discarding their window
// and their CPU baseline, and counted nothing — the pipeline just quietly lost
// data. Same rule as the tailer's checkpoint pruning: prune on a SUCCEEDED
// listing only.
func TestIncompleteListingRetiresNothingAndIsCounted(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read a 0000 directory")
	}
	h := newHarness(t)
	pod := filepath.Join(h.root, "kubepods.slice", "kubepods-burstable.slice",
		"kubepods-burstable-pod"+systemdUID(1)+".slice")
	dir := filepath.Join(pod, "cri-containerd-"+hexID(1)+".scope")
	makeContainer(t, dir, 0, 100<<20, 0)
	h.discover()
	if h.Containers() != 1 {
		t.Fatalf("Containers() = %d, want 1", h.Containers())
	}
	h.advance(time.Second)
	setUsage(t, dir, 1_000_000)
	h.advance(time.Second)
	setUsage(t, dir, 2_000_000)
	h.advance(time.Second)

	// The pod slice becomes unlistable. The container is still there.
	if err := os.Chmod(pod, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(pod, 0o755) })

	// Counted as `subtree`, not `root`: the two are different failures with
	// different remedies (a directory inside a working hierarchy, versus a
	// hierarchy that is not mounted and a pipeline that is silent), and the
	// counter that conflated them diagnosed only the second.
	beforeSub := obs.CgroupDiscoveryErrors.WithLabelValues("subtree").Value()
	beforeRoot := obs.CgroupDiscoveryErrors.WithLabelValues("root").Value()
	h.discover()
	if got := obs.CgroupDiscoveryErrors.WithLabelValues("subtree").Value(); got <= beforeSub {
		t.Errorf("a partially failed listing moved no discovery-error counter (%v -> %v)", beforeSub, got)
	}
	if got := obs.CgroupDiscoveryErrors.WithLabelValues("root").Value(); got != beforeRoot {
		t.Errorf("an unreadable pod slice was counted as an unreadable ROOT (%v -> %v); the root counter is what says the mount is missing and NOTHING is being exported", beforeRoot, got)
	}
	if got := h.Containers(); got != 1 {
		t.Fatalf("Containers() = %d after an unreadable directory; a listing that failed is not proof a container went away, and retiring it throws away its window and its CPU baseline", got)
	}
	if g := h.exportOnce(t)[hexID(1)]; g == nil || math.Abs(g[nameCPUMax]-1) > 1e-9 {
		t.Errorf("the window did not survive the failed listing: %v", g)
	}
}

func TestIsKubepodsSlice(t *testing.T) {
	for _, name := range []string{"kubepods", "kubepods.slice", "kubelet-kubepods.slice", "foo-kubepods.slice"} {
		if !isKubepodsSlice(name) {
			t.Errorf("%q should be a kubepods root", name)
		}
	}
	for _, name := range []string{"kubepods-burstable.slice", "system.slice", "kubelet.slice", "kubepodsx", "podkubepods-x"} {
		if isKubepodsSlice(name) {
			t.Errorf("%q should NOT be a kubepods root", name)
		}
	}
}
