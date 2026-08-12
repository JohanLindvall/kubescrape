package cgroupstats

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// The mean and the sample count are what make the other four readable, and
// their VALUES have to be the window's own — see the argument above the metric
// name block.
//
// The fixture is arithmetic that can be checked by hand: three CPU intervals at
// 1, 2 and 3 cores, so mean = 2 exactly and samples = 3, beside max = 3 and
// min = 1; and four working-set readings whose mean is likewise exact.
func TestMeanAndSampleCountDescribeTheWindow(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 0, 0)
	h.discover()

	// The first sweep is the CPU baseline and the first memory reading.
	setMem(t, dir, 10<<20, 0)
	h.advance(time.Second)
	var usec uint64
	for i, cores := range []uint64{1, 2, 3} {
		usec += cores * 1_000_000
		setUsage(t, dir, usec)
		setMem(t, dir, uint64(20+10*i)<<20, 0)
		h.advance(time.Second)
	}

	got := h.exportOnce(t)[hexID(1)]
	for _, tc := range []struct {
		name string
		want float64
	}{
		{nameCPUMean, 2},
		{nameCPUSamples, 3},
		{nameCPUMax, 3},
		{nameCPUMin, 1},
		{nameMemSamples, 4},
		// 10, 20, 30, 40 MiB.
		{nameMemMean, 25 << 20},
	} {
		if v, ok := got[tc.name]; !ok {
			t.Errorf("%s was not exported", tc.name)
		} else if math.Abs(v-tc.want) > 1e-6*math.Max(1, math.Abs(tc.want)) {
			t.Errorf("%s = %g, want %g", tc.name, v, tc.want)
		}
	}
}

// A HELD window re-states the previous distribution, and the sample count is
// the ONE field that must not be re-stated: it is what tells an operator
// staring at one container's flat max that they are looking at a re-statement
// rather than a measurement. (obs.CgroupHeldWindows counts the same event
// fleet-wide, which cannot answer the question about a single container.)
func TestAHeldWindowReportsItsOwnSampleCountNotTheHeldOne(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 0, 0)
	h.discover()

	setMem(t, dir, 10<<20, 0)
	h.advance(time.Second)
	var usec uint64
	for range 3 {
		usec += 2_000_000
		setUsage(t, dir, usec)
		setMem(t, dir, 30<<20, 0)
		h.advance(time.Second)
	}
	first := h.exportOnce(t)[hexID(1)]
	if first[nameCPUSamples] != 3 {
		t.Fatalf("the measured window reports %g CPU samples, want 3", first[nameCPUSamples])
	}

	// A window with a single sweep in it: not a distribution, so the previous
	// one is re-stated.
	usec += 2_000_000
	setUsage(t, dir, usec)
	h.advance(time.Second)
	held := h.exportOnce(t)[hexID(1)]

	if held[nameCPUMax] != first[nameCPUMax] {
		t.Fatalf("the sparse window did not hold the previous distribution: max %g vs %g", held[nameCPUMax], first[nameCPUMax])
	}
	if got := held[nameCPUSamples]; got >= 2 {
		t.Errorf("a held window reports %g CPU samples: the count is the only marker distinguishing a re-statement from a measurement, so it must describe THIS window (0 or 1), not the one being re-stated", got)
	}
	if got := held[nameMemSamples]; got >= 2 {
		t.Errorf("a held window reports %g working-set samples, want fewer than 2", got)
	}
}

// A RETIRED container's final window is rendered by measured() rather than
// finish() — a gone container gets no hold — so the two fields that describe
// the window instead of its extremes are computed in a second place, and that
// second place is the one no living container ever takes. The mean must be the
// window's mean and not one of its ends, and the count must be what THIS window
// counted: an OOM-killed container's last window is the case the package exists
// for, and both of those numbers are what the burst in _max is read against.
//
// The fixture is TestMeanAndSampleCountDescribeTheWindow's, so it is checkable
// by hand and every expected value is distinct from the extremes beside it: CPU
// intervals of 1, 2 and 3 cores (mean 2, max 3, three samples) and working sets
// of 10, 20, 30 and 40 MiB (mean 25, max 40, four samples).
func TestAGoneContainersFinalWindowCarriesTheWindowsOwnMeanAndCount(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 0, 0)
	h.discover()

	setMem(t, dir, 10<<20, 0)
	h.advance(time.Second) // the CPU baseline and the first working-set reading
	var usec uint64
	for i, cores := range []uint64{1, 2, 3} {
		usec += cores * 1_000_000
		setUsage(t, dir, usec)
		setMem(t, dir, uint64(20+10*i)<<20, 0)
		h.advance(time.Second)
	}

	if err := os.RemoveAll(filepath.Dir(dir)); err != nil {
		t.Fatal(err)
	}
	h.discover() // the cgroup left the hierarchy: the next export is its last

	got := h.exportOnce(t)[hexID(1)]
	if got == nil {
		t.Fatal("the vanished container's final window was not exported")
	}
	for _, tc := range []struct {
		name string
		want float64
		why  string
	}{
		{nameCPUMean, 2, "the mean of the 1, 2 and 3 core intervals, not the 3 in _max"},
		{nameMemMean, 25 << 20, "the mean of the 10, 20, 30 and 40 MiB readings, not the 40 in _max"},
		{nameCPUSamples, 3, "the three CPU intervals this window measured"},
		{nameMemSamples, 4, "the four working-set readings this window measured"},
	} {
		v, ok := got[tc.name]
		if !ok {
			t.Errorf("%s was not exported on the final window", tc.name)
			continue
		}
		if math.Abs(v-tc.want) > 1e-6*math.Max(1, math.Abs(tc.want)) {
			t.Errorf("the retirement render put %g in %s, want %g: %s", v, tc.name, tc.want, tc.why)
		}
	}
}

// A container that lived and died inside roughly one sampling period is the
// OBSERVABLE half of the short-lived blind spot: everything measured about it
// is discarded (a single reading is not a distribution, and a gone container
// gets no hold — its last datapoint has to be real), so it has to be counted.
//
// It is a LOWER BOUND on the loss, and a narrow one: measured at the 15s
// default against a 5s lifetime, 20.0% of containers are described, 13.3% land
// here and 66.7% are never discovered at all. See DefaultDiscoverInterval for
// the whole table and for the closed form behind it.
func TestAContainerTooShortLivedToMeasureIsCounted(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 1000, 1<<20, 0)
	h.discover()

	before := obs.CgroupWindowsDropped.WithLabelValues("too_short").Value()
	// One sweep only: a CPU rate needs two readings and a working-set
	// distribution needs two samples, so nothing can be described.
	h.advance(time.Second)
	if err := os.RemoveAll(filepath.Dir(dir)); err != nil {
		t.Fatal(err)
	}
	h.discover() // the cgroup is gone: marked for its final flush

	if got := h.exportOnce(t); len(got) != 0 {
		t.Fatalf("a container with one sample exported %v; a gone container's last datapoint must be measured or absent", got)
	}
	if got := obs.CgroupWindowsDropped.WithLabelValues("too_short").Value(); got != before+1 {
		t.Errorf("kubescrape_cgroup_windows_dropped_total{reason=\"too_short\"} = %g, want %g: a container that vanished before it could be described is the only evidence a node produces that -cgroup-stats-discover-interval is losing short-lived containers",
			got, before+1)
	}

	// A container that DID produce a distribution is not counted there — the
	// counter has to mean something.
	dir2 := systemdContainerDir(h.root, 2, hexID(2))
	makeContainer(t, dir2, 0, 1<<20, 0)
	h.discover()
	var usec uint64
	for range 3 {
		usec += 1_000_000
		setUsage(t, dir2, usec)
		h.advance(time.Second)
	}
	before = obs.CgroupWindowsDropped.WithLabelValues("too_short").Value()
	if err := os.RemoveAll(filepath.Dir(dir2)); err != nil {
		t.Fatal(err)
	}
	h.discover()
	if got := h.exportOnce(t); len(got) == 0 {
		t.Fatal("a measured container's final window was not exported")
	}
	if got := obs.CgroupWindowsDropped.WithLabelValues("too_short").Value(); got != before {
		t.Errorf("a container that exported a real distribution was counted as too short (%g -> %g)", before, got)
	}

	// And the case that decides whether the counter is alertable at all: an
	// ordinary container that has been exporting for its whole life and dies
	// shortly after an export. Its FINAL window holds one sample and is lost —
	// but "too short to measure" is not what happened, and at a 1s period
	// inside a 30s window a few percent of every normal shutdown lands here.
	dir3 := systemdContainerDir(h.root, 3, hexID(3))
	makeContainer(t, dir3, 0, 1<<20, 0)
	h.discover()
	usec = 0
	for range 3 {
		usec += 1_000_000
		setUsage(t, dir3, usec)
		h.advance(time.Second)
	}
	if got := h.exportOnce(t); len(got) == 0 {
		t.Fatal("the long-lived container's first window was not exported")
	}
	before = obs.CgroupWindowsDropped.WithLabelValues("too_short").Value()
	usec += 1_000_000 // one lone sample into the next window, then it dies
	setUsage(t, dir3, usec)
	h.advance(time.Second)
	if err := os.RemoveAll(filepath.Dir(dir3)); err != nil {
		t.Fatal(err)
	}
	h.discover()
	h.exportOnce(t)
	if got := obs.CgroupWindowsDropped.WithLabelValues("too_short").Value(); got != before {
		t.Errorf("a container that had been exporting all along was counted as too short because its FINAL partial window held one sample (%g -> %g): "+
			"the counter is meant to say a container was never describable, and one that fires on ordinary shutdowns cannot be alerted on", before, got)
	}
}

// THE PROCESS exiting is not the CONTAINER being short-lived.
//
// stop() marks every tracked container gone so FinalExport carries one last
// window, which puts the whole node's set through the too_short branch. A
// SIGTERM taken before the second sweep — a CrashLooping agent, a rollout that
// rolls straight back, any node whose agent has been up for less than one
// sampling period — would then publish "this node loses short-lived containers"
// once per container on the node, for containers that are all still running.
// A loss counter that fires hardest when nothing was lost cannot be alerted on,
// which is the whole reason the too_short verdict exists rather than counting
// every empty final window.
//
// The container that GENUINELY vanished just before the signal keeps its
// verdict: stop() only marks the ones that were not already gone.
func TestShutdownDoesNotCountEveryContainerAsTooShortLived(t *testing.T) {
	h := newHarness(t)
	const live = 4
	for i := 1; i <= live; i++ {
		makeContainer(t, systemdContainerDir(h.root, i, hexID(i)), 0, 1<<20, 0)
	}
	// And one that really does die inside a sampling period, so the test proves
	// the exemption is narrow rather than proving the counter is dead.
	dyingDir := systemdContainerDir(h.root, live+1, hexID(live+1))
	makeContainer(t, dyingDir, 0, 1<<20, 0)
	h.discover()

	// ONE sweep: no container has two readings of anything yet, so every one of
	// them is a too_short candidate the moment it is marked gone.
	h.advance(time.Second)
	if err := os.RemoveAll(filepath.Dir(dyingDir)); err != nil {
		t.Fatal(err)
	}
	h.discover() // the dead one is marked gone the ordinary way

	before := obs.CgroupWindowsDropped.WithLabelValues("too_short").Value()
	h.stop() // SIGTERM: Run marks every remaining container gone
	if got := h.exportOnce(t); len(got) != 0 {
		t.Fatalf("the final flush exported %v; nothing had two samples", got)
	}
	if got := obs.CgroupWindowsDropped.WithLabelValues("too_short").Value(); got != before+1 {
		t.Errorf("kubescrape_cgroup_windows_dropped_total{reason=\"too_short\"} moved by %g over a shutdown of %d live containers plus one that really vanished, want 1: "+
			"stop() marks the whole node's set gone, so counting them makes an agent restart look like a node full of containers too short to measure",
			got-before, live)
	}
}
