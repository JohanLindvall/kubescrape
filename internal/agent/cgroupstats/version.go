package cgroupstats

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Filesystem magics, from linux/magic.h.
const (
	cgroup2Magic = 0x63677270
	cgroup1Magic = 0x0027e0eb
)

// controllersFile exists in every cgroup v2 DIRECTORY and nowhere in a v1
// hierarchy.
const controllersFile = "cgroup.controllers"

// ErrUnsupportedNode reports that the node's own cgroup layout — not anything
// the operator typed — is what this sampler cannot read: a cgroup v1
// hierarchy, or a default root that is not a cgroup mount at all.
//
// It exists because the two failures need OPPOSITE handling and only the caller
// knows which it is looking at. A wrong -cgroup-stats-root is an operator error
// that is identical on every node and must be loud and fatal. A v1 node is a
// property of the NODE: on a mixed fleet, enabling one flag would otherwise
// CrashLoop the DaemonSet on every v1 node and take that node's LOG pipeline
// down with it — for a metric — and -check-config cannot warn about it, because
// the node's cgroup version is not knowable at check time. So the caller
// disables THIS pipeline, logs an error naming the version and the flag, and
// leaves every other pipeline running.
var ErrUnsupportedNode = errors.New("cgroupstats: this node does not expose a cgroup v2 hierarchy")

// checkCgroup2 reports whether root is a cgroup v2 hierarchy.
//
// v2 is required rather than merely preferred, because the files at these names
// either do not exist under v1 or do not mean the same thing. v1 spells
// cumulative CPU time cpuacct.usage in NANOseconds (this package divides by
// 1e6, so every rate would be reported 1000x low), splits memory across a
// separate hierarchy, and has no memory.current at all. A sampler that merely
// found nothing would be survivable; one that reported plausible wrong numbers
// is worse than no sampler, and a burst metric that is silently three orders of
// magnitude out is exactly the kind of thing that gets believed.
//
// Two probes, in order, because neither alone covers both the real node and the
// test tree:
//
//   - statfs on the root. On a v2 node that is cgroup2fs and settles it. On a
//     v1 node /sys/fs/cgroup is a TMPFS holding one directory per controller,
//     so its magic says nothing — which is why a miss falls through rather than
//     concluding.
//   - the presence of cgroup.controllers. It is NOT the root's own marker: the
//     kernel writes one into EVERY v2 directory, which is precisely why the
//     probe also passes for a container's own cgroup view (the mount a pod gets
//     without the host hostPath) and for a fabricated tree in t.TempDir(). What
//     it distinguishes is v2 from v1 and from nothing-mounted, which is all this
//     function claims; whether the hierarchy actually HOLDS pod cgroups is
//     discovery's question, answered by the empty-root warning.
func checkCgroup2(root string) error {
	if magic, err := fsMagic(root); err == nil {
		switch magic {
		case cgroup2Magic:
			return nil
		case cgroup1Magic:
			return fmt.Errorf("%s is a cgroup v1 hierarchy; the cgroup sampler reads cgroup v2 only "+
				"(v1 spells cumulative CPU time cpuacct.usage in nanoseconds and has no memory.current, so the files this reads are absent or mean something else). "+
				"Boot the node with systemd.unified_cgroup_hierarchy=1, or leave -cgroup-stats off on v1 nodes", root)
		}
	}
	if _, err := os.Stat(filepath.Join(root, controllersFile)); err == nil {
		return nil
	}
	return fmt.Errorf("%s is not a cgroup v2 hierarchy: it holds no %s file (a cgroup v1 node has one directory per controller here instead, and an unmounted path has nothing). "+
		"The cgroup sampler reads cgroup v2 only; mount the host's %s read-only into this container, or point -cgroup-stats-root at where it is mounted",
		root, controllersFile, DefaultRoot)
}
