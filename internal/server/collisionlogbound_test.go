package server

import (
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// The collision warning names the targets that collide, and a collision GROUP
// is every target on the node sharing one (job, instance) — which for a
// hostNetwork workload is one member per replica per annotated port. The
// throttle bounds how OFTEN the line is written and cannot bound how LARGE it
// is: the same lesson as the merged-chain, contributor and explain ceilings,
// and the same shape as the ~750 KB warn line the ignored-fields report could
// produce.
//
// Two members name the configuration (a collision needs two) and the count
// carries the rest, so nothing an operator acts on is lost.
//
// Reverse-patch check: restoring the unbounded `for _, ct := range c.Targets`
// makes this fail at ~19 KB.
func TestCollisionWarningIsBoundedByMembers(t *testing.T) {
	const replicas = 200
	h := &recordingHandler{}
	st := store.New(time.Minute)
	for i := range replicas {
		// One workload, one node address, one annotated port: every replica
		// lands on the same (job, instance).
		st.UpsertPod(replicaPod("web-abc-"+strconv.Itoa(i), "10.0.0.5", "9100", true))
	}
	s := New(Config{
		Store: st, Services: services.NewIndex(), Log: slog.New(h),
		Resolver: deploymentResolver{}, MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	})

	targets, _ := s.nodeTargets("node1")
	if len(targets) != replicas {
		t.Fatalf("served %d targets, want one per replica", len(targets))
	}
	lines := h.matching("export the same series identity")
	if len(lines) != 1 {
		t.Fatalf("logged %d collision warnings, want exactly 1", len(lines))
	}
	line := lines[0]
	t.Logf("%d colliding replicas -> %d byte log record", replicas, len(line))
	// The static note in the record is ~600 bytes; anything under 2 KiB is the
	// member list being bounded rather than accumulated.
	if len(line) > 2<<10 {
		t.Errorf("the collision warning is %d bytes for %d colliding targets; the member list must be bounded",
			len(line), replicas)
	}
	// Bounded, but never silently: the count is what says the pair shown is
	// two of many, and it is the difference between "these two collide" and
	// "your whole workload does".
	if want := "and " + strconv.Itoa(replicas-2) + " more targets"; !strings.Contains(line, want) {
		t.Errorf("the warning does not say how many members it left out (want %q): %s", want, line)
	}
	// And it still names a colliding pair, which is what the operator fixes.
	if !strings.Contains(line, "on default/web-abc-0") || !strings.Contains(line, "on default/web-abc-1") {
		t.Errorf("the warning no longer names a colliding pair: %s", line)
	}
}
