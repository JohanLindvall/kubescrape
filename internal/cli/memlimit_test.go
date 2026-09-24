package cli

import (
	"bytes"
	"log/slog"
	"math"
	"runtime/debug"
	"strings"
	"testing"
)

// soleLimit runs SetMemoryLimit from a clean slate — the runtime unlimited and
// GOMEMLIMIT absent, unless the test says otherwise before calling — and
// returns the limit it left behind plus what it logged. The process-wide soft
// limit is restored afterwards, which is also why none of these tests may run
// in parallel.
func soleLimit(t *testing.T, before int64) (int64, string) {
	t.Helper()
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })
	debug.SetMemoryLimit(before)
	var buf bytes.Buffer
	SetMemoryLimit(slog.New(NewLogfmtHandler(&buf, slog.LevelDebug)))
	return debug.SetMemoryLimit(-1), buf.String()
}

// The ordinary case: 90% of this container's own limit.
func TestSetMemoryLimitAppliesTheShareOfTheCgroupLimit(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	fakeCgroup(t, "0::/\n", map[string]string{"memory.max": "536870912\n"})
	got, logged := soleLimit(t, math.MaxInt64)
	limit := int64(536870912)
	if want := int64(float64(limit) * memLimitShare); got != want {
		t.Fatalf("soft limit = %d, want %d (%v of the cgroup limit)\n%s", got, want, memLimitShare, logged)
	}
}

// GOMEMLIMIT=off is the operator saying "no soft limit", and it must win like
// any other value. The runtime reads "off" as math.MaxInt64 — the same value it
// reports when the variable is unset — so a check of the runtime's value alone
// took the explicit opt-out for silence and installed 0.9 x the cgroup limit
// anyway, then logged "set GOMEMLIMIT to override".
func TestGOMEMLIMITOffIsNotOverridden(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "off")
	fakeCgroup(t, "0::/\n", map[string]string{"memory.max": "536870912\n"})
	got, logged := soleLimit(t, math.MaxInt64)
	if got != math.MaxInt64 {
		t.Fatalf("GOMEMLIMIT=off was overridden with a soft limit of %d\n%s", got, logged)
	}
	if !strings.Contains(logged, "leaving the Go soft memory limit alone") {
		t.Errorf("want the line saying GOMEMLIMIT was honoured, got:\n%s", logged)
	}
}

// A limit set in CODE is invisible to any environment check, which is what the
// runtime-value check is still there for.
func TestProgrammaticLimitIsLeftAlone(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	fakeCgroup(t, "0::/\n", map[string]string{"memory.max": "536870912\n"})
	if got, logged := soleLimit(t, 1<<30); got != 1<<30 {
		t.Fatalf("a programmatic limit of %d was replaced with %d\n%s", 1<<30, got, logged)
	}
}

// Uncapped is a deliberate shape (the metadata service ships that way): no
// limit, and nothing above Debug.
func TestUncappedSetsNoLimitQuietly(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	fakeCgroup(t, "0::/\n", map[string]string{"memory.max": "max\n"})
	fakeMountinfo(t, "30 25 0:26 / %s rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup2 rw\n")
	got, logged := soleLimit(t, math.MaxInt64)
	if got != math.MaxInt64 {
		t.Fatalf("an uncapped workload got a soft limit of %d\n%s", got, logged)
	}
	if strings.Contains(logged, "level=WARN") {
		t.Errorf("an uncapped workload warned; it is a documented shape:\n%s", logged)
	}
}

// The node's cgroup hierarchy bind-mounted OVER the container's own (the
// cgroup-stats pipeline's hostPath at /sys/fs/cgroup): inside a cgroup
// namespace /proc/self/cgroup says "/", which now names the NODE's root, whose
// memory.max does not exist — so a capped agent read as uncapped and ran
// without the soft limit, reporting it only at Debug. Measured in a container
// with --memory 256m: limit found with the default mounts, nothing found with
// the host's /sys/fs/cgroup mounted there, and mountinfo showing root /../..
// for it. The limit is still unreadable (the container's own directory has a
// name nothing here can learn), but it is no longer silent.
func TestShadowedCgroupMountWarns(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	fakeCgroup(t, "0::/\n", map[string]string{})
	fakeMountinfo(t, "30 25 0:26 /../.. %s ro,nosuid,nodev,noexec,relatime - cgroup2 cgroup2 rw\n")
	got, logged := soleLimit(t, math.MaxInt64)
	if got != math.MaxInt64 {
		t.Fatalf("soft limit = %d from a hierarchy that is not this container's\n%s", got, logged)
	}
	if !strings.Contains(logged, "level=WARN") || !strings.Contains(logged, "ancestor hierarchy") {
		t.Errorf("want a WARN naming the shadowed mount, got:\n%s", logged)
	}
}

// The share is what stands between Go's accounting and everything the cgroup
// charges that Go cannot see. Pinned so a change to it is a deliberate one.
func TestShareLeavesHeadroom(t *testing.T) {
	if memLimitShare <= 0 || memLimitShare >= 1 {
		t.Fatalf("memLimitShare = %v; must leave headroom for non-Go memory", memLimitShare)
	}
	limit := int64(512 << 20)
	soft := int64(float64(limit) * memLimitShare)
	if soft >= limit {
		t.Fatalf("soft limit %d is not below the cgroup limit %d", soft, limit)
	}
}
