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
// consequences, both measured by the campaign's cgroup harness (not by a test
// in this package — cgroup_test.go pins which limit is READ and memlimit_test.go
// when it is applied, not what it does to the GC):
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
// memLimitShare). Reading the cgroup covers all three paths, follows whatever
// limit the container was STARTED with however it got there (a template edit,
// or a VPA in Recreate mode, whose admission webhook rewrites the resources at
// pod creation without a re-render), and is the same source tailbuffer already
// sizes maxSpans against — so the two cannot disagree about how much memory
// this pod has.
//
// It is read ONCE, at container start (so is tailbuffer's), and that is the
// limit of the claim: an IN-PLACE resize (InPlacePodVerticalScaling, VPA's
// InPlaceOrRecreate mode) rewrites memory.max under the running process and
// leaves the soft limit at 0.9x the OLD value until the container restarts —
// after a shrink it can sit ABOVE the new cgroup limit, which is the OOMKill
// insurance gone. A container `resizePolicy` of RestartContainer for memory
// makes an in-place memory resize a restart, which makes the startup read
// correct again; nothing here re-reads.
//
// WHAT IS DELIBERATELY NOT DONE: there is no fallback to the host's MemTotal.
// An UNCAPPED workload has nothing to be insured against, and a soft limit at a
// fraction of the NODE's RAM would have a Burstable pod collect against a
// ceiling it does not own. The metadata service ships with no memory limit ON
// PURPOSE (its footprint scales with the cluster, and a number picked in a
// values file is a number picked without knowing the cluster), so it gets
// nothing from this and that is the right answer, not an oversight.
//
// Which cgroup's limit is read — this container's OWN, never an ancestor's —
// is the reader's rule, and cgroup.go's file comment carries it.
//
// GOGC is left alone. GOMEMLIMIT can only lower the heap goal, so it is pure
// tail insurance and never trades memory for CPU; raising GOGC would, and on a
// workload whose live set scales with the cluster that is a trade nobody here
// can make on the operator's behalf.

import (
	"log/slog"
	"math"
	"os"
	"runtime/debug"
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

// SetMemoryLimit gives the Go runtime a soft memory limit derived from this
// container's cgroup memory limit. It is a no-op when the workload is not
// capped, or when the operator has already set GOMEMLIMIT — the env var is the
// override.
//
// The override is checked on the ENVIRONMENT first, and that is not
// redundant with the runtime check after it: the runtime reads GOMEMLIMIT=off
// as math.MaxInt64 — the very value it reports when the variable is unset — so
// a runtime-only check could not tell "no limit, deliberately" from "nobody
// said anything" and replaced the operator's explicit opt-out with 0.9 x the
// cgroup limit. Any non-empty value still in the environment is one the runtime
// ACCEPTED, because a malformed GOMEMLIMIT is fatal at process start; an empty
// one is what the runtime itself treats as unset. The runtime's current value
// is still consulted for a limit set in code (debug.SetMemoryLimit), which no
// environment check can see.
//
// Called from both mains right after the startup line, so the line below sits
// next to the build identity an operator is already reading.
func SetMemoryLimit(log *slog.Logger) {
	if v := os.Getenv("GOMEMLIMIT"); v != "" {
		log.Info("GOMEMLIMIT is set in the environment; leaving the Go soft memory limit alone",
			"limitBytes", debug.SetMemoryLimit(-1), "note", "GOMEMLIMIT is the override; GOMEMLIMIT=off disables the soft limit")
		return
	}
	if cur := debug.SetMemoryLimit(-1); cur != math.MaxInt64 {
		log.Info("Go soft memory limit already set; leaving it alone",
			"limitBytes", cur, "note", "GOMEMLIMIT is the override")
		return
	}
	limit, path, ok := CgroupMemoryLimit()
	if !ok {
		if mountRoot, shadowed := cgroupMountShadowed(); shadowed {
			// NOT the deliberate uncapped shape below, and worth a warning: the
			// cgroup filesystem at cgroupRoot is an ANCESTOR hierarchy mounted
			// over this container's own (a hostPath of the node's
			// /sys/fs/cgroup at /sys/fs/cgroup — which is exactly what the
			// cgroup-stats pipeline needs mounted somewhere). Inside a cgroup
			// namespace /proc/self/cgroup then says "/", which names that
			// ancestor's root rather than this container, and the container's
			// own directory is somewhere beneath it under a name nothing here
			// can learn. A capped pod therefore reads as uncapped and runs
			// without the insurance, and the Debug line was all it said.
			log.Warn("the cgroup filesystem is an ancestor hierarchy mounted over this container's own; its memory limit cannot be read, so no Go soft memory limit is set",
				"path", cgroupRoot, "note", "mount the node's cgroup hierarchy somewhere else (e.g. /host/sys/fs/cgroup, with -cgroup-stats-root pointing at it), or set GOMEMLIMIT; mountinfo root "+mountRoot)
			return
		}
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
