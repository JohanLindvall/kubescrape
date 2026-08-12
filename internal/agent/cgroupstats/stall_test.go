package cgroupstats

import (
	"bytes"
	"context"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// --- the minimum-elapsed guard -----------------------------------------

// A stall must not manufacture a burst.
//
// sampleLoop drives the sweep off a time.Ticker, and a ticker whose receiver
// was blocked delivers the buffered tick IMMEDIATELY and then the next one on
// its ORIGINAL schedule — so the two readings straddling a stall can be
// milliseconds apart. usage_usec advances in the scheduler's accounting quanta,
// so a quotient over milliseconds is dominated by where those quanta landed.
// Measured against a REAL cgroup hard-capped at 0.5 cores running a busy loop:
// undisturbed 1-second readings hold a p95 of 0.5015 and a max of 0.5017 cores,
// while the catch-up pair 5 ms after a 2-second stall reads 0.0000 to 0.9672
// (median 0.86, sixteen of thirty draws above the undisturbed max, peak +93%).
// Nothing downstream can tell that from the real burst _max exists to preserve.
//
// The fixture is that shape exactly: a container at a steady 0.5 cores, a 2s
// stall (which is what a resolveBudget-bound discovery pass used to cost when
// it shared this goroutine), then the catch-up pair 5ms apart across which the
// counter happens to advance by a whole 10ms quantum — an instantaneous 2.0
// cores.
func TestAStalledSweepDoesNotInflateTheMax(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 20<<20)
	h.discover()

	var usec uint64
	// Ten undisturbed seconds at 0.5 cores.
	h.advance(time.Second) // the baseline; no rate yet
	for range 10 {
		usec += 500_000 // 0.5 core-seconds per second
		setUsage(t, dir, usec)
		h.advance(time.Second)
	}
	// The stall: the sweep did not run for two seconds, and the container went
	// on using 0.5 cores throughout.
	usec += 1_000_000
	setUsage(t, dir, usec)
	h.advance(2 * time.Second)
	// The catch-up tick, 5ms later, across which the accounting caught up by a
	// 10ms quantum.
	usec += 10_000
	setUsage(t, dir, usec)
	h.advance(5 * time.Millisecond)
	// And back to the normal cadence.
	for range 3 {
		usec += 500_000
		setUsage(t, dir, usec)
		h.advance(time.Second)
	}

	got := h.exportOnce(t)[hexID(1)]
	max := got[nameCPUMax]
	if max > 0.75 {
		t.Errorf("_max = %.4f cores for a container that never exceeded 0.5: the pair of readings straddling a stall is milliseconds apart, and a rate over milliseconds is the accounting quantum rather than the load. "+
			"sampleOne must refuse an interval below half the sampling period", max)
	}
	if max < 0.49 {
		t.Errorf("_max = %.4f cores; the real 0.5 must survive the guard", max)
	}
}

// The guard DEFERS an interval, it does not discard one — which is why nothing
// counts it, and the distinction is the whole reason the baseline assignment
// stopped being unconditional. cpu.stat is cumulative, so KEEPING the baseline
// charges the skipped sliver's CPU time to the next interval; rebasing on it
// would throw that time away, silently, on a pipeline whose entire job is not
// to lose the busy parts.
//
// The fixture separates all three behaviours by arithmetic:
//
//	t+1.0s  +1.0 core-second over 1.0s            → 1.0 cores
//	t+1.4s  +2.0 core-seconds over 0.4s           → the sliver (below the 0.5s guard)
//	t+2.4s  +1.0 core-second
//	t+3.4s  +1.0 core-second
//
//	no guard          rates 1.0, 5.0, 1.0, 1.0    max 5.00, n 4
//	guard + rebase    rates 1.0,      1.0, 1.0    max 1.00, n 3  ← 2 core-seconds gone
//	guard + defer     rates 1.0, 2.143,     1.0   max 2.14, n 3  ← every microsecond kept
//
// The middle row is the one worth staring at: dropping AND rebasing loses two
// whole core-seconds of a container's work with nothing to show for it. The
// deferred reading smears the sliver's burst across 1.4s instead of reporting
// it at 5 cores over 400ms, which is the acknowledged price of refusing to
// divide by a stall-shortened interval.
//
// That price is not free and it is not counted, so the last assertion here is
// the argument for not counting it: what the skip costs is a POINT of the
// distribution, and the window REPORTS its own point count on the wire as
// container_cpu_usage_samples. A fleet-wide counter would say the same thing
// about no container in particular; this says it about the one whose stddev
// and max were computed over n-1.
func TestAShortIntervalIsDeferredNotDiscarded(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 20<<20)
	h.discover()

	var usec uint64
	h.advance(time.Second) // baseline

	usec += 1_000_000
	setUsage(t, dir, usec)
	h.advance(time.Second)

	usec += 2_000_000 // two core-seconds inside a 400ms sliver
	setUsage(t, dir, usec)
	h.advance(400 * time.Millisecond)

	usec += 1_000_000
	setUsage(t, dir, usec)
	h.advance(time.Second)

	usec += 1_000_000
	setUsage(t, dir, usec)
	h.advance(time.Second)

	h.mu.Lock()
	c := h.tracked[hexID(1)]
	n, max, mean := c.cpu.n, c.cpu.max, c.cpu.mean
	h.mu.Unlock()

	if n != 3 {
		t.Fatalf("the window holds %d CPU rate samples, want 3: the sliver must contribute no sample of its own", n)
	}
	if max > 3 {
		t.Errorf("_max = %.4f cores: the sliver's rate was derived over its own 400ms divisor, which is the stall artefact the guard exists to refuse", max)
	}
	if max < 1.5 {
		t.Errorf("_max = %.4f cores: the sliver's two core-seconds were DISCARDED rather than deferred — keeping the baseline is what charges them to the next interval, and dropping them loses real work with nothing counting it", max)
	}
	// 4 core-seconds of the 5 consumed fall inside the three measured intervals
	// (the first second is the baseline), over 3.4s of measured wall time: the
	// mean of the three rates is (1.0 + 3.0/1.4 + 1.0)/3.
	if want := (1 + 3.0/1.4 + 1) / 3; math.Abs(mean-want) > 0.01 {
		t.Errorf("mean = %.4f cores, want %.4f: every microsecond cpu.stat reported has to appear in exactly one interval", mean, want)
	}

	// And the skip is VISIBLE, which is why it needs no counter of its own:
	// four sweeps derived a rate over three intervals, and the exported count
	// says three. An operator reading a suspicious stddev against
	// window/interval sees immediately that this window took fewer readings
	// than its cadence promises.
	g := h.exportOnce(t)[hexID(1)]
	if g == nil {
		t.Fatal("nothing was exported for the container")
	}
	if got := g[nameCPUSamples]; got != 3 {
		t.Errorf("container_cpu_usage_samples = %g after one deferred interval, want 3: "+
			"the deferral costs the distribution a point, and this gauge is the whole of how that is reported — it must be the WINDOW's own count", got)
	}
	if got := g[nameCPUMean]; math.Abs(got-(1+3.0/1.4+1)/3) > 0.01 {
		t.Errorf("container_cpu_usage_mean = %g; the deferred sliver's CPU time must still reach the exported mean", got)
	}
}

// The guard is a FRACTION of the configured period, not a constant: a sampler
// running at the 100ms floor must still take its samples.
func TestTheGuardScalesWithTheSamplingPeriod(t *testing.T) {
	s, err := New(Config{Root: newRoot(t), Interval: MinInterval, Resolver: &fakeResolver{},
		Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer s.closeAll()
	if s.minElapsed != MinInterval/2 {
		t.Errorf("minElapsed = %s at a %s period; a constant floor would refuse every interval a fast sampler takes",
			s.minElapsed, MinInterval)
	}
}

// --- discovery is not on the sampling goroutine -------------------------

// blockingResolver holds the FIRST lookup of a given id until it is released,
// which is what a metadata service that has gone slow looks like from here.
type blockingResolver struct {
	fakeResolver
	mu      sync.Mutex
	blockID string
	release chan struct{}
	entered chan struct{}
	once    bool
}

func (b *blockingResolver) FillContainerResource(ctx context.Context, res pcommon.Resource, id, uid string) (bool, bool) {
	b.mu.Lock()
	block := id == b.blockID && !b.once
	if block {
		b.once = true
	}
	b.mu.Unlock()
	if block {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	}
	return b.fakeResolver.FillContainerResource(ctx, res, id, uid)
}

// A discovery pass can block for as long as the metadata service takes
// (resolveBudget bounds a pass at two seconds, and one lookup at five), and
// while it does, the node must go on being measured.
//
// This is the in-tree trigger for the stall above: discovery used to run on the
// sampler's own goroutine, so every slow pass was a gap in the sampling of
// EVERY container on the node, followed by a catch-up tick whose rate is the
// accounting quantum. Moving it to its own goroutine removes the trigger rather
// than papering over it.
func TestASlowDiscoveryPassDoesNotStopTheSampling(t *testing.T) {
	root := newRoot(t)
	makeContainer(t, systemdContainerDir(root, 1, hexID(1)), 1000, 1<<20, 1<<18)
	res := &blockingResolver{blockID: hexID(2), release: make(chan struct{}), entered: make(chan struct{})}
	s, err := New(Config{
		Root: root, Interval: MinInterval, DiscoverInterval: MinDiscoverInterval,
		Resolver: res, Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.closeAll()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx, &fakeExporter{}, time.Hour) // no export inside the test
	}()
	defer func() {
		cancel()
		<-done
	}()

	// Wait until sampling is genuinely running before arming the block: Run's
	// FIRST discovery pass is synchronous (deliberately — see Run), so a
	// container created before it would block the startup rather than a
	// periodic pass, and the test would prove nothing.
	waitFor(t, "the sampler to take its first readings", func() bool {
		return cpuSamples(s, hexID(1)) >= 2
	})

	// The second container is the one whose resolution blocks.
	makeContainer(t, systemdContainerDir(root, 2, hexID(2)), 1000, 1<<20, 1<<18)
	select {
	case <-res.entered:
	case <-time.After(20 * time.Second):
		t.Fatal("the discovery pass never reached the blocking lookup")
	}

	// Discovery is now stuck. The first container must keep being sampled: at
	// the 100ms period that is ~8 sweeps in 800ms, and the bar is deliberately
	// a small fraction of that so a loaded CI machine cannot fail it. With
	// discovery on the sampling goroutine it is 0.
	before := cpuSamples(s, hexID(1))
	time.Sleep(800 * time.Millisecond)
	got := cpuSamples(s, hexID(1)) - before
	close(res.release)
	if got < 3 {
		t.Errorf("the sampler took %d readings while a discovery pass was blocked in a metadata lookup, want at least 3: "+
			"a pass is bounded by resolveBudget (2s), and sharing the goroutine turns every slow pass into a node-wide gap plus a catch-up tick", got)
	}
}

// waitFor polls cond until it holds, failing the test on a generous deadline.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	waitForWithin(t, 20*time.Second, what, cond)
}

// waitForWithin is waitFor with the deadline as the assertion: a test whose
// property is a CADENCE has to bound the wait below the cadence it is proving
// is not in use.
func waitForWithin(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", limit, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// cpuSamples is the number of CPU RATE samples the container's current window
// holds, or 0 while it is not tracked yet (these tests poll a running sampler,
// so "not there yet" is a normal answer rather than a failure).
func cpuSamples(s *Sampler, id string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.tracked[id]; ok {
		return c.cpu.n
	}
	return 0
}

// --- the discovery interval is an operator's choice ---------------------

// The flag has to reach the ticker, which is the only thing that makes the
// short-lived-container trade available at all. A container created after
// startup must be picked up on the CONFIGURED cadence.
//
// Two things make it able to FAIL, and this test had neither:
//
//   - the DEADLINE. Waiting the ordinary generous twenty seconds proves
//     nothing, because a discoverLoop ticking on the 15s CONSTANT satisfies it
//     too. The bound below is five times the configured 1s cadence and three
//     times below the default, so the only way to miss it is to tick on
//     something other than what was configured.
//   - a container the STARTUP pass cannot have taken. Run's first discovery
//     pass is synchronous, and a container created while Run is still getting
//     going is picked up by that pass whatever the ticker is doing — which is
//     how the twenty-second version still passed with s.discoverEvery replaced
//     by the constant. So the first container is created BEFORE Run and waited
//     for, which is what proves the startup pass is over, and only then is the
//     second one created.
func TestTheConfiguredDiscoveryIntervalDrivesTheLoop(t *testing.T) {
	root := newRoot(t)
	makeContainer(t, systemdContainerDir(root, 1, hexID(1)), 1000, 1<<20, 1<<18)
	s, err := New(Config{
		Root: root, Interval: MinInterval, DiscoverInterval: MinDiscoverInterval,
		Resolver: &fakeResolver{}, Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.closeAll()
	if s.DiscoverInterval() != MinDiscoverInterval {
		t.Fatalf("DiscoverInterval() = %s, want the configured %s", s.DiscoverInterval(), MinDiscoverInterval)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx, &fakeExporter{}, time.Hour)
	}()
	defer func() {
		cancel()
		<-done
	}()

	waitFor(t, "the synchronous startup pass to take the container that existed before Run",
		func() bool { return s.Containers() > 0 })

	makeContainer(t, systemdContainerDir(root, 2, hexID(2)), 1000, 1<<20, 1<<18)
	waitForWithin(t, 5*time.Second,
		"a container created AFTER the startup pass to be discovered on the CONFIGURED 1s cadence — only the periodic loop can pick this one up, and one ticking on DefaultDiscoverInterval (15s) instead of s.discoverEvery cannot make this deadline",
		func() bool { return s.Containers() > 1 })
}

// A discovery cadence below the floor arrives here only from a Config built in
// code (the flag is refused in checkFlagValues); the library guarantee is
// MinInterval's exactly — normalise, and say so.
func TestSubFloorDiscoveryIntervalIsClampedAndNamed(t *testing.T) {
	buf := &bytes.Buffer{}
	s, err := New(Config{Root: newRoot(t), DiscoverInterval: time.Millisecond, Resolver: &fakeResolver{},
		Logger: slog.New(slog.NewTextHandler(buf, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer s.closeAll()
	if s.DiscoverInterval() != MinDiscoverInterval {
		t.Errorf("discovery interval = %s, want the %s floor", s.DiscoverInterval(), MinDiscoverInterval)
	}
	if !strings.Contains(buf.String(), "floor") {
		t.Errorf("the clamp was silent:\n%s", buf.String())
	}
	s2, err := New(Config{Root: newRoot(t), Resolver: &fakeResolver{},
		Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.closeAll()
	if s2.DiscoverInterval() != DefaultDiscoverInterval {
		t.Errorf("unset discovery interval = %s, want %s", s2.DiscoverInterval(), DefaultDiscoverInterval)
	}
}
