package cli

// GOMEMLIMIT: turning an OOMKill into GC pressure.
//
// Go's GC sizes the next collection at GOGC percent above the LIVE heap and
// knows nothing about the cgroup it runs in. A workload whose live heap is
// small but whose transient garbage is large therefore has a heap GOAL that
// tracks the biggest thing it happens to be holding: the agent's measured
// scrape cycle takes heap_alloc from 9.0 MB to 57.9 MB (6.4x) every 30s, and
// the goal follows. Nothing in that loop knows the DaemonSet ships
// `limits.memory: 512Mi`, so a cycle that needs more than the limit is not
// collected harder — it is OOMKilled, which on this workload loses the tailer's
// unflushed batch and every buffered span, and does it on whichever node the
// fat target happens to land on.
//
// A soft memory limit is the runtime's answer: the heap goal becomes
// min(GOGC goal, limit goal), so it can only ever make the GC run EARLIER. Two
// consequences, both measured (see memlimit_test.go and the campaign report):
//
//   - When the limit does not bind it costs exactly nothing. A workload whose
//     peak sat far below the limit measured 19.70 GC cycles per GB allocated
//     with the limit set and 19.70 without it (10 interleaved rounds).
//   - When it does bind it costs GC and buys survival. A burst sized at the
//     boundary of a 384 MiB cgroup was OOMKilled 3 times in 16 runs with no
//     limit and 0 times in 16 with one, at 10.7 -> 17.2 cycles per GB.
//
// WHY THE CODE READS THE CGROUP RATHER THAN THE CHART SETTING THE ENV VAR.
// Three deployment paths ship in this repo — the chart, deploy/*.yaml, and
// whatever an operator writes — and only one of them can be taught helm
// arithmetic. The downward API can hand a container `limits.memory` verbatim
// but cannot take a fraction of it, and GOMEMLIMIT must be a fraction (see
// memLimitShare). Reading the cgroup covers all three paths, follows a limit
// an operator edits or a VPA resizes without a re-render, and is the same
// source tailbuffer already sizes maxSpans against — so the two cannot
// disagree about how much memory this pod has.
//
// WHAT IS DELIBERATELY NOT DONE: there is no fallback to the host's MemTotal.
// An UNCAPPED workload has nothing to be insured against, and a soft limit at a
// fraction of the NODE's RAM would have a Burstable pod collect against a
// ceiling it does not own. The metadata service ships with no memory limit ON
// PURPOSE (its footprint scales with the cluster, and a number picked in a
// values file is a number picked without knowing the cluster), so it gets
// nothing from this and that is the right answer, not an oversight.
//
// An ANCESTOR cgroup's limit is not read either, and that is the same rule
// rather than a second one. This code walked from the leaf up to the mount
// taking the minimum, which reads a node-scale number under another name: with
// --enforce-node-allocatable=pods (the kubelet default) kubepods.slice carries
// a memory limit of the node's whole allocatable memory, and --cgroups-per-qos
// puts one on the QoS slice below it — so an uncapped container inherited
// ~0.9 x the NODE's RAM as its heap goal and the paragraph above described a
// behaviour the code did not have. A pod-level limit is no better a number to
// derive from: the kubelet only sets one when EVERY container in the pod is
// capped (in which case this container's own cgroup already carries the
// tighter answer), and it is shared with the siblings, so a fraction of it is
// not a ceiling this process owns either. Reading only the container's own
// cgroup is also what keeps this in step with tailbuffer's maxSpans sizing,
// which reads the container's memory.max and nothing above it.
//
// GOGC is left alone. GOMEMLIMIT can only lower the heap goal, so it is pure
// tail insurance and never trades memory for CPU; raising GOGC would, and on a
// workload whose live set scales with the cluster that is a trade nobody here
// can make on the operator's behalf.

import (
	"bufio"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
)

// memLimitShare is the fraction of the cgroup's memory limit handed to the Go
// runtime as its soft limit.
//
// The remainder is for everything GOMEMLIMIT cannot see. Go's accounting covers
// the heap, stacks, and runtime structures; it does not cover the binary's own
// mapped text and data, the C heap of the agent's one cgo dependency
// (libsystemd, under the `journald` tag), or anything the kernel charges to the
// cgroup on this process's behalf. 10% is the ratio automemlimit and the wider
// Go-in-Kubernetes practice settled on, and the measurement here agrees from
// the other direction: on the live agent the quantity GOMEMLIMIT bounds read
// 89.7 MB while the process's RSS read 66.9 MB, i.e. Go's own accounting was
// already 1.34x the footprint the cgroup actually charges. At 0.9 of a 384 MiB
// cgroup the worst peak RSS observed across a sweep was 295 MiB.
const memLimitShare = 0.9

// cgroupRoot is the mount point the limit is read under; a var so the test can
// point it at a fixture tree.
var cgroupRoot = "/sys/fs/cgroup"

// procCgroup is this process's cgroup membership file; a var for the same
// reason.
var procCgroup = "/proc/self/cgroup"

// SetMemoryLimit gives the Go runtime a soft memory limit derived from this
// container's cgroup memory limit. It is a no-op when the workload is not
// capped, or when the operator has already set GOMEMLIMIT — the env var is the
// override, and it is checked through the RUNTIME's current value rather than
// through os.Getenv so that anything the runtime accepted wins, whatever
// spelling it arrived in.
//
// Called from both mains right after the startup line, so the line below sits
// next to the build identity an operator is already reading.
func SetMemoryLimit(log *slog.Logger) {
	if cur := debug.SetMemoryLimit(-1); cur != math.MaxInt64 {
		log.Info("Go soft memory limit already set; leaving it alone",
			"limitBytes", cur, "note", "GOMEMLIMIT is the override")
		return
	}
	limit, path, ok := cgroupMemoryLimit()
	if !ok {
		// Not a warning: an uncapped workload is a legitimate, documented shape
		// here (the metadata service ships that way), and a fleet-wide warning
		// about a deliberate choice is noise on every start.
		log.Debug("no cgroup memory limit found; leaving the Go heap goal to GOGC alone",
			"note", "set a container memory limit, or GOMEMLIMIT, to bound the heap goal")
		return
	}
	soft := int64(float64(limit) * memLimitShare)
	if soft <= 0 {
		return
	}
	debug.SetMemoryLimit(soft)
	log.Info("Go soft memory limit set from the cgroup memory limit",
		"limitBytes", soft, "cgroupLimitBytes", limit, "share", memLimitShare, "path", path,
		"note", "a heap excursion now costs GC instead of an OOMKill; set GOMEMLIMIT to override")
}

// cgroupMemoryLimit reports THIS CONTAINER'S OWN memory limit, and the file it
// came from. It never reads an ancestor's — see the file comment for why a
// pod-slice or kubepods.slice limit is the wrong number to hand the Go runtime.
//
// The container's own cgroup is named by /proc/self/cgroup, which covers both
// layouts: inside a cgroup namespace the path is "/" and the file is the mount
// point's own, and with cgroupns=host — still a supported kubelet
// configuration, and what any systemd scope on a plain host looks like — it is
// the full path several levels down, whose file is still the container's.
//
// The mount ROOT is a fallback for one layout that would otherwise read
// nothing: cgroup v1 in Kubernetes and Docker bind-mounts the container's own
// cgroup directory at /sys/fs/cgroup/memory while /proc/self/cgroup keeps
// naming the host path, which does not exist inside the container. That
// fallback cannot smuggle a node-scale number back in: a genuine host root has
// no memory.max at all (cgroup v2 does not give the root cgroup controller
// files) and spells v1's limit as the unlimited sentinel, both of which
// readCgroupBytes reports as no limit.
func cgroupMemoryLimit() (int64, string, bool) {
	v2, v1 := cgroupPaths()
	for _, p := range limitFiles(cgroupRoot, v2, "memory.max") {
		if v, ok := readCgroupBytes(p); ok {
			return v, p, true
		}
	}
	// cgroup v1 puts the controller in its own subtree.
	if v1 != "" {
		for _, p := range limitFiles(filepath.Join(cgroupRoot, "memory"), v1, "memory.limit_in_bytes") {
			if v, ok := readCgroupBytes(p); ok {
				return v, p, true
			}
		}
	}
	return 0, "", false
}

// limitFiles lists the files that may hold this container's own limit, most
// specific first: the cgroup /proc/self/cgroup names, then the mount root
// itself. A rel that would leave base is dropped entirely, so a malformed
// /proc/self/cgroup can never read a file outside the cgroup mount.
func limitFiles(base, rel, name string) []string {
	base = filepath.Clean(base)
	out := make([]string, 0, 2)
	if dir := filepath.Join(base, rel); dir != base && strings.HasPrefix(dir, base+string(filepath.Separator)) {
		out = append(out, filepath.Join(dir, name))
	}
	return append(out, filepath.Join(base, name))
}

// cgroupPaths reads /proc/self/cgroup and returns this process's path in the
// v2 hierarchy and in the v1 memory controller. Either may be empty; "/" is
// the normal answer inside a cgroup namespace and is what makes the walk above
// degenerate to reading the mount point itself.
func cgroupPaths() (v2, v1 string) {
	f, err := os.Open(procCgroup)
	if err != nil {
		return "/", ""
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		// hierarchy-ID:controller-list:path
		parts := strings.SplitN(sc.Text(), ":", 3)
		if len(parts) != 3 {
			continue
		}
		switch {
		case parts[0] == "0" && parts[1] == "":
			v2 = parts[2]
		case slices.Contains(strings.Split(parts[1], ","), "memory"):
			v1 = parts[2]
		}
	}
	if v2 == "" {
		v2 = "/"
	}
	return v2, v1
}

// readCgroupBytes reads one cgroup limit file. "max" and the sentinel v1 uses
// for unlimited (a number near the int64 maximum) report not-ok: neither is a
// limit anything can be planned against.
func readCgroupBytes(path string) (int64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(b))
	if s == "max" {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 || v >= 1<<62 {
		return 0, false
	}
	return v, true
}
