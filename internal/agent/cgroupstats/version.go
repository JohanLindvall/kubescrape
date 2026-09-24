package cgroupstats

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
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
// hierarchy (at ANY root, explicit or default — see v1Error), or, at the
// default root only, a v2 one without the memory controller or a path that is
// not a cgroup mount at all.
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

// v1Error is checkCgroup2's verdict that root IS a cgroup v1 hierarchy: the
// root is itself a v1 controller mount, or it is the tmpfs a v1 node (or a
// systemd hybrid one) mounts its controllers under, recognised by its `memory`
// child being a v1 mount.
//
// It is a TYPE, and New classifies it as ErrUnsupportedNode even at an
// EXPLICIT -cgroup-stats-root, because it answers the question the explicit
// root's fatality exists for. A root that is fatal is one the operator got
// wrong — a typo, a path nothing is mounted at — identical on every node and
// fixed by editing one value. A root that holds a genuine v1 hierarchy proves
// the opposite: the operator pointed at the node's cgroup mount, and what this
// sampler cannot read is the node's cgroup VERSION, which differs node by node
// on a mixed fleet. The chart renders an explicit root by default (the host
// hierarchy at /host/sys/fs/cgroup, so it does not shadow this container's own
// /sys/fs/cgroup and hide its memory limit from internal/cli), so treating a
// v1 hierarchy there as fatal would CrashLoop the DaemonSet — logs included —
// on every v1 node of the fleet for the sake of one metric.
type v1Error struct {
	root string
	// controller is the v1 controller mount that proved it, when the root
	// itself is the tmpfs above them ("" when the root is one).
	controller string
}

func (e *v1Error) Error() string {
	what := e.root + " is a cgroup v1 hierarchy"
	if e.controller != "" {
		what = e.root + " holds a cgroup v1 hierarchy (" + e.controller + " is a cgroup v1 controller mount)"
	}
	return what + "; the cgroup sampler reads cgroup v2 only " +
		"(v1 spells cumulative CPU time cpuacct.usage in nanoseconds and has no memory.current, so the files this reads are absent or mean something else). " +
		"Boot the node with systemd.unified_cgroup_hierarchy=1, or leave -cgroup-stats off on v1 nodes"
}

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
//   - statfs on the root. On a v2 node that is cgroup2fs and settles the
//     VERSION (the controller check below still applies). On a v1 node
//     /sys/fs/cgroup is a TMPFS holding one directory per controller,
//     so its magic says nothing — which is why a miss falls through rather than
//     concluding.
//   - the presence of cgroup.controllers. It is NOT the root's own marker: the
//     kernel writes one into EVERY v2 directory, which is precisely why the
//     probe also passes for a container's own cgroup view (the mount a pod gets
//     without the host hostPath) and for a fabricated tree in t.TempDir(). What
//     it distinguishes is v2 from v1 and from nothing-mounted, which is all this
//     function claims; whether the hierarchy actually HOLDS pod cgroups is
//     discovery's question, answered by the empty-root warning.
//
// And then the CONTENTS of cgroup.controllers, whichever probe matched: the
// memory controller must be available here. Being v2 is necessary and not
// sufficient — memory.current and memory.stat exist only on cgroups where the
// memory controller is enabled, while cpu.stat is a core file present
// everywhere. The shape that passes the first two probes and fails this one is
// a systemd HYBRID node's unified mount (/sys/fs/cgroup/unified): cgroup2 magic,
// an EMPTY cgroup.controllers, and kubepods scopes carrying only the core files.
// Accepting it started a sampler that resolved every container, opened none of
// them and exported nothing, with a warning that blamed the metadata service.
func checkCgroup2(root string) error {
	magic, merr := fsMagic(root)
	if merr == nil && magic == cgroup1Magic {
		return &v1Error{root: root}
	}
	controllers, err := os.ReadFile(filepath.Join(root, controllersFile))
	if err != nil {
		if merr == nil && magic == cgroup2Magic {
			return fmt.Errorf("%s is a cgroup v2 mount but its %s could not be read, so whether the memory controller is available cannot be told: %w",
				root, controllersFile, err)
		}
		// A v1 node's /sys/fs/cgroup is a tmpfs of controller mounts, so the
		// root's own magic said nothing; the memory controller's does. Only a
		// REAL v1 mount counts — a directory merely named `memory` (a test tree,
		// or an operator's typo landing on some unrelated path) is not evidence
		// of a node's cgroup version and still reads as "not a hierarchy".
		if mem := filepath.Join(root, "memory"); isCgroupV1Mount(mem) {
			return &v1Error{root: root, controller: mem}
		}
		return fmt.Errorf("%s is not a cgroup v2 hierarchy: it holds no %s file (a cgroup v1 node has one directory per controller here instead, and an unmounted path has nothing). "+
			"The cgroup sampler reads cgroup v2 only; mount the host's %s read-only into this container, or point -cgroup-stats-root at where it is mounted",
			root, controllersFile, DefaultRoot)
	}
	if !slices.Contains(strings.Fields(string(controllers)), "memory") {
		return fmt.Errorf("%s is a cgroup v2 hierarchy without the memory controller (its %s lists %q), so no container cgroup below it has the memory.current and memory.stat this sampler reads. "+
			"On a systemd hybrid node this is the unified mount (/sys/fs/cgroup/unified), whose controllers all live in the v1 hierarchy: boot the node with systemd.unified_cgroup_hierarchy=1, or leave -cgroup-stats off there",
			root, controllersFile, strings.TrimSpace(string(controllers)))
	}
	return nil
}

// isCgroupV1Mount reports whether path is a cgroup v1 controller mount.
func isCgroupV1Mount(path string) bool {
	magic, err := fsMagic(path)
	return err == nil && magic == cgroup1Magic
}
