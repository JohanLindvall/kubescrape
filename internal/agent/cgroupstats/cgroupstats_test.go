package cgroupstats

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// --- harness -----------------------------------------------------------

type fakeExporter struct {
	sent []pmetric.Metrics
	err  error
}

func (f *fakeExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, md)
	return nil
}

type harness struct {
	*Sampler
	root string
	res  *fakeResolver
	log  *bytes.Buffer
	t    time.Time
}

// newHarness builds a sampler over a fresh fake hierarchy with a fixed clock.
func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{root: newRoot(t), res: &fakeResolver{}, log: &bytes.Buffer{}}
	h.t = time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	s, err := New(Config{
		Root:     h.root,
		Interval: time.Second,
		Resolver: h.res,
		Logger:   slog.New(slog.NewTextHandler(h.log, &slog.HandlerOptions{Level: slog.LevelDebug})),
		now:      func() time.Time { return h.t },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.Sampler = s
	t.Cleanup(s.closeAll)
	return h
}

// discover runs one full discovery pass (walk, reconcile, resolve) at the
// harness clock. It shadows Sampler.discover so the tests read as the sampler
// goroutine's loop does.
func (h *harness) discover() { h.Sampler.discover(context.Background(), h.t) }

// advance moves the fixed clock and takes one sample sweep, as the ticker
// would.
func (h *harness) advance(d time.Duration) {
	h.t = h.t.Add(d)
	h.sample()
}

// gauges flattens one exported payload into name -> value, per container id.
func gauges(t *testing.T, md pmetric.Metrics) map[string]map[string]float64 {
	t.Helper()
	out := map[string]map[string]float64{}
	rms := md.ResourceMetrics()
	for i := range rms.Len() {
		rm := rms.At(i)
		id := ""
		if v, ok := rm.Resource().Attributes().Get("container.id"); ok {
			id = v.Str()
		}
		byName := map[string]float64{}
		sms := rm.ScopeMetrics()
		for j := range sms.Len() {
			ms := sms.At(j).Metrics()
			for k := range ms.Len() {
				m := ms.At(k)
				byName[m.Name()] = m.Gauge().DataPoints().At(0).DoubleValue()
			}
		}
		out[id] = byName
	}
	return out
}

func (h *harness) exportOnce(t *testing.T) map[string]map[string]float64 {
	t.Helper()
	exp := &fakeExporter{}
	if err := h.export(context.Background(), exp); err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(exp.sent) == 0 {
		return map[string]map[string]float64{}
	}
	return gauges(t, exp.sent[0])
}

// setUsage rewrites a container's cpu.stat in place (the held fd sees it).
func setUsage(t *testing.T, dir string, usec uint64) {
	t.Helper()
	writeFile(t, filepath.Join(dir, fileCPUStat), cpuStat(usec))
}

func setMem(t *testing.T, dir string, current, inactive uint64) {
	t.Helper()
	writeFile(t, filepath.Join(dir, fileMemCurrent), fmtUint(current))
	writeFile(t, filepath.Join(dir, fileMemStat), memStat(inactive))
}

func fmtUint(v uint64) string {
	var b []byte
	if v == 0 {
		return "0\n"
	}
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b) + "\n"
}

// --- version / startup -------------------------------------------------

// cgroup v1 is refused, and the message has to say v2 — an operator reading
// "no containers found" would go looking for a mount problem that is not
// there.
func TestCgroupV1IsRefused(t *testing.T) {
	// A v1 node's /sys/fs/cgroup is a tmpfs with one directory per controller
	// and NO cgroup.controllers file, which is what this reproduces (statfs
	// over a tmpdir reports the tmpdir's own filesystem, so the file probe is
	// the branch under test — see checkCgroup2).
	root := t.TempDir()
	for _, c := range []string{"cpu", "cpuacct", "memory", "blkio"} {
		if err := os.MkdirAll(filepath.Join(root, c), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	_, err := New(Config{Root: root, Resolver: &fakeResolver{}})
	if err == nil {
		t.Fatal("expected a refusal on a cgroup v1 hierarchy")
	}
	if !strings.Contains(err.Error(), "v2") {
		t.Errorf("error %q does not name cgroup v2", err)
	}
}

// A node this sampler cannot read is a property of the NODE, and enabling one
// flag on a mixed fleet must not CrashLoop the DaemonSet on every cgroup v1
// node — which would take that node's LOG pipeline down for the sake of a
// metric. An UNCONFIGURED root therefore reports ErrUnsupportedNode, which the
// agent turns into "this pipeline off, everything else running".
func TestUnsupportedNodeIsDistinguishableFromAnOperatorError(t *testing.T) {
	// The same v1 verdict, twice, differing only in whether the operator chose
	// the root. (The probe is injected so this decides the classification on any
	// machine, rather than only on one whose real /sys/fs/cgroup happens to be
	// the shape the test needs.)
	v1 := func(string) error { return errors.New("that is a cgroup v1 hierarchy") }
	quiet := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	// The DEFAULT root: a property of the node. Enabling one flag on a mixed
	// fleet must not CrashLoop this DaemonSet — that takes the node's LOGS down
	// for the sake of a metric — so the agent disables this pipeline alone.
	_, err := New(Config{Resolver: &fakeResolver{}, Logger: quiet, check: v1})
	if !errors.Is(err, ErrUnsupportedNode) {
		t.Fatalf("err = %v, want ErrUnsupportedNode so the agent degrades instead of CrashLooping the node's other pipelines", err)
	}
	if !strings.Contains(err.Error(), "v1") {
		t.Errorf("the error does not name the version, which is the one thing the operator needs from it: %v", err)
	}

	// An EXPLICIT root that is not a cgroup v2 hierarchy is an operator error,
	// identical on every node and fixable by editing one value: still fatal.
	_, err = New(Config{Root: "/somewhere/else", Resolver: &fakeResolver{}, Logger: quiet, check: v1})
	if err == nil {
		t.Fatal("an explicit root that is not a cgroup v2 hierarchy must be refused")
	}
	if errors.Is(err, ErrUnsupportedNode) {
		t.Errorf("an explicitly configured -cgroup-stats-root reported ErrUnsupportedNode, so the agent would DISABLE the pipeline instead of refusing a value the operator can fix: %v", err)
	}
}

func TestUnmountedRootIsRefused(t *testing.T) {
	_, err := New(Config{Root: filepath.Join(t.TempDir(), "nope"), Resolver: &fakeResolver{}})
	if err == nil {
		t.Fatal("expected a refusal for a root that is not mounted")
	}
}

func TestResolverIsRequired(t *testing.T) {
	if _, err := New(Config{Root: newRoot(t)}); err == nil {
		t.Fatal("expected a refusal without a Resolver")
	}
}

// A sampling period below the floor arrives here only from a Config built in
// code (the flag is refused in checkFlagValues), and the library guarantee is
// the same as for a non-positive one: normalise, and say so.
func TestSubFloorIntervalIsClampedAndNamed(t *testing.T) {
	buf := &bytes.Buffer{}
	s, err := New(Config{Root: newRoot(t), Interval: time.Millisecond, Resolver: &fakeResolver{},
		Logger: slog.New(slog.NewTextHandler(buf, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer s.closeAll()
	if s.interval != MinInterval {
		t.Errorf("interval = %s, want the %s floor: three cgroup reads per container per period runs on the sweep goroutine", s.interval, MinInterval)
	}
	if !strings.Contains(buf.String(), "floor") {
		t.Errorf("the clamp was silent:\n%s", buf.String())
	}
	if s2, err := New(Config{Root: newRoot(t), Resolver: &fakeResolver{},
		Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}); err != nil {
		t.Fatal(err)
	} else if s2.interval != DefaultInterval {
		t.Errorf("unset interval = %s, want %s", s2.interval, DefaultInterval)
	}
}

// The journald lesson: a v2 root with nothing in it must produce a LOUD line,
// not a clean start followed by silence. It is a warning rather than a refusal
// because a freshly joined node genuinely has no pods yet — see New.
func TestEmptyRootWarnsLoudlyAndNamesTheMount(t *testing.T) {
	h := newHarness(t)
	out := h.log.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("no warning was logged for an empty root:\n%s", out)
	}
	if !strings.Contains(out, "no container cgroups") {
		t.Errorf("the warning does not say what is wrong:\n%s", out)
	}
	if !strings.Contains(out, DefaultRoot) {
		t.Errorf("the warning does not name the mount to add:\n%s", out)
	}
}

// The other way to export nothing, which needs a DIFFERENT answer: the cgroups
// are there and none of them resolves. "Mount /sys/fs/cgroup" would send the
// operator to the wrong place, so the line names the metadata service instead.
func TestUnresolvableCgroupsWarnAboutMetadataNotTheMount(t *testing.T) {
	h := newHarness(t)
	makeContainer(t, systemdContainerDir(h.root, 1, hexID(1)), 1000, 1<<20, 1<<18)
	h.res.reject(hexID(1))
	h.log.Reset()
	h.discover()

	out := h.log.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("a sampler that resolved nothing said nothing:\n%s", out)
	}
	if !strings.Contains(out, "resolved none") {
		t.Errorf("the warning does not name the actual condition:\n%s", out)
	}
	if strings.Contains(out, "no container cgroups") {
		t.Errorf("the warning blames the mount for a metadata failure:\n%s", out)
	}
}

func TestNonEmptyRootDoesNotWarn(t *testing.T) {
	root := newRoot(t)
	makeContainer(t, systemdContainerDir(root, 1, hexID(1)), 1000, 1<<20, 1<<18)
	buf := &bytes.Buffer{}
	s, err := New(Config{Root: root, Resolver: &fakeResolver{},
		Logger: slog.New(slog.NewTextHandler(buf, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer s.closeAll()
	if strings.Contains(buf.String(), "level=WARN") {
		t.Fatalf("warned about a root that has containers:\n%s", buf.String())
	}
	// New deliberately resolves nothing (a whole node's metadata lookups must
	// not run in front of every other pipeline's start), so what it can report
	// is what it FOUND.
	if got := s.Discovered(); got != 1 {
		t.Fatalf("Discovered() = %d, want 1", got)
	}
	if got := s.Containers(); got != 0 {
		t.Fatalf("Containers() = %d before any resolution, want 0", got)
	}
	s.discover(context.Background(), time.Now())
	if got := s.Containers(); got != 1 {
		t.Fatalf("Containers() = %d after a discovery pass, want 1", got)
	}
}

// --- sampling ----------------------------------------------------------

// The whole feature, as an assertion. The fixture is the motivating story from
// the package doc: a 30-second window in which a container idles at 0.02 cores
// for 28 seconds and then spikes to 4 cores for 2. A cadvisor scrape spanning
// that window reports the average, ~0.29 cores; _max reports 4.
func TestBurstSurvivesAsMaxWhileTheAverageErasesIt(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 20<<20)
	h.discover()

	h.advance(time.Second) // the baseline reading; no rate can exist yet
	var usec uint64
	rates := make([]float64, 0, 30)
	for i := range 30 {
		r := 0.02
		if i >= 28 {
			r = 4
		}
		rates = append(rates, r)
		usec += uint64(r * 1e6)
		setUsage(t, dir, usec)
		h.advance(time.Second)
	}

	g := h.exportOnce(t)[hexID(1)]
	if g == nil {
		t.Fatal("nothing exported for the container")
	}
	if math.Abs(g[nameCPUMax]-4) > 1e-6 {
		t.Errorf("%s = %v, want 4", nameCPUMax, g[nameCPUMax])
	}
	if math.Abs(g[nameCPUMin]-0.02) > 1e-6 {
		t.Errorf("%s = %v, want 0.02", nameCPUMin, g[nameCPUMin])
	}
	if math.Abs(g[nameCPUStddev]-populationStddev(rates)) > 1e-6 {
		t.Errorf("%s = %v, want %v", nameCPUStddev, g[nameCPUStddev], populationStddev(rates))
	}

	// What the cadvisor scrape over the same window would have said. If this
	// ratio ever stops being large the fixture has stopped demonstrating the
	// feature, and the test is worth nothing.
	var sum float64
	for _, r := range rates {
		sum += r
	}
	avg := sum / float64(len(rates))
	if g[nameCPUMax] < 10*avg {
		t.Errorf("the burst (%v cores) is only %.1fx the window average (%v); the fixture no longer demonstrates the feature",
			g[nameCPUMax], g[nameCPUMax]/avg, avg)
	}
	t.Logf("cadvisor would report %.3f cores for this window; max=%v stddev=%v", avg, g[nameCPUMax], g[nameCPUStddev])
}

func populationStddev(xs []float64) float64 {
	var sum float64
	for _, x := range xs {
		sum += x
	}
	mean := sum / float64(len(xs))
	var ss float64
	for _, x := range xs {
		ss += (x - mean) * (x - mean)
	}
	return math.Sqrt(ss / float64(len(xs)))
}

// Working set is cadvisor's: memory.current minus inactive_file. A different
// definition would make _max and _min incomparable with the series they sit
// beside, which is the entire reason for the naming.
func TestWorkingSetMatchesCadvisorsDefinition(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 20<<20)
	h.discover()

	h.advance(time.Second) // 80 MiB
	setMem(t, dir, 300<<20, 20<<20)
	h.advance(time.Second) // 280 MiB
	setMem(t, dir, 300<<20, 310<<20)
	h.advance(time.Second) // inactive_file above current: clamped to 0, never wrapped

	g := h.exportOnce(t)[hexID(1)]
	if g[nameMemMax] != float64(280<<20) {
		t.Errorf("%s = %v, want %v", nameMemMax, g[nameMemMax], float64(280<<20))
	}
	if g[nameMemMin] != 0 {
		t.Errorf("%s = %v, want 0 (an inactive_file above memory.current must clamp, not wrap)", nameMemMin, g[nameMemMin])
	}
}

// A restarted cgroup resets usage_usec. The unsigned subtraction would turn
// that into an enormous positive rate, which is exactly the shape of the burst
// this package exists to report — so it must be recognised and dropped.
func TestCounterResetProducesNoRate(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 5_000_000, 100<<20, 0)
	h.discover()

	h.advance(time.Second) // baseline at 5s of CPU
	setUsage(t, dir, 10)   // the cgroup was replaced
	h.advance(time.Second)
	setUsage(t, dir, 500_010) // then half a core for a second
	h.advance(time.Second)
	setUsage(t, dir, 600_010) // and a tenth of one (two rates: a real window)
	h.advance(time.Second)

	g := h.exportOnce(t)[hexID(1)]
	if math.Abs(g[nameCPUMax]-0.5) > 1e-9 {
		t.Errorf("%s = %v, want 0.5 — the reset interval must not become a rate", nameCPUMax, g[nameCPUMax])
	}
	if g[nameCPUMax] > 1000 {
		t.Fatalf("a counter reset was rendered as a %v-core burst", g[nameCPUMax])
	}
}

// A container that has never been sampled at all contributes nothing — not an
// empty resource, not a zero.
func TestUnsampledContainerIsNotExported(t *testing.T) {
	h := newHarness(t)
	makeContainer(t, systemdContainerDir(h.root, 1, hexID(1)), 1000, 100<<20, 0)
	h.discover()
	if got := h.exportOnce(t); len(got) != 0 {
		t.Fatalf("exported %v before any sample was taken", got)
	}
}

// Containers come and go mid-window. The one that appeared must be picked up
// and exported for the part of the window it existed for.
func TestContainerAppearsMidWindow(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 2, hexID(2))
	makeContainer(t, dir, 0, 50<<20, 0)
	h.discover()

	h.advance(time.Second)
	setUsage(t, dir, 250_000)
	h.advance(time.Second)
	setUsage(t, dir, 350_000)
	h.advance(time.Second)

	g := h.exportOnce(t)[hexID(2)]
	if g == nil {
		t.Fatal("the container that appeared mid-window was not exported")
	}
	if math.Abs(g[nameCPUMax]-0.25) > 1e-9 {
		t.Errorf("%s = %v, want 0.25", nameCPUMax, g[nameCPUMax])
	}
}

// A container whose cgroup disappears has its WINDOW FLUSHED, once, and is then
// retired. That window is the OOM-killed or crashed container's last seconds —
// the case the package doc names as the motivation — and dropping it threw the
// burst away at exactly the moment it mattered. The identity needs no lookup at
// that point (it was resolved once at discovery and cached), and the metadata
// service would serve one anyway: a deleted pod stays resolvable by container
// id for its -cache-ttl.
func TestVanishedContainerFlushesItsFinalWindow(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 0)
	h.discover()

	h.advance(time.Second)
	setUsage(t, dir, 4_000_000)
	h.advance(time.Second) // 4 cores
	setUsage(t, dir, 4_100_000)
	h.advance(time.Second)

	// The container is torn down. Its last window has not been exported yet.
	if err := os.RemoveAll(filepath.Dir(dir)); err != nil {
		t.Fatal(err)
	}
	h.discover()
	if got := h.Containers(); got != 0 {
		t.Fatalf("Containers() = %d after the teardown, want 0 (its descriptors must be released immediately)", got)
	}

	// A sweep over a container whose descriptors are gone must not invent
	// anything — no partial interval, no zero rate folded into the window.
	h.advance(time.Second)

	g := h.exportOnce(t)[hexID(1)]
	if g == nil {
		t.Fatal("the vanished container's window was discarded; that is the burst this package exists to keep")
	}
	if math.Abs(g[nameCPUMax]-4) > 1e-9 {
		t.Errorf("%s = %v, want 4 — the final window must carry what was measured", nameCPUMax, g[nameCPUMax])
	}
	if math.Abs(g[nameCPUMin]-0.1) > 1e-9 {
		t.Errorf("%s = %v, want 0.1; a read against a released descriptor produced a bogus rate", nameCPUMin, g[nameCPUMin])
	}

	// And it is retired: the flush happens once, not every window forever.
	if got := h.exportOnce(t); len(got) != 0 {
		t.Fatalf("the retired container is still being exported: %v", got)
	}
}

// Windows are per-export, not cumulative: the second export must describe the
// second interval only.
func TestWindowsResetAtEachExport(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 0)
	h.discover()

	h.advance(time.Second)
	setUsage(t, dir, 4_000_000)
	h.advance(time.Second) // 4 cores
	setUsage(t, dir, 8_000_000)
	h.advance(time.Second) // 4 cores again
	if g := h.exportOnce(t)[hexID(1)]; math.Abs(g[nameCPUMax]-4) > 1e-9 {
		t.Fatalf("first window max = %v, want 4", g[nameCPUMax])
	}

	setUsage(t, dir, 8_100_000)
	h.advance(time.Second) // 0.1 cores
	setUsage(t, dir, 8_200_000)
	h.advance(time.Second)
	g := h.exportOnce(t)[hexID(1)]
	if math.Abs(g[nameCPUMax]-0.1) > 1e-9 {
		t.Errorf("second window max = %v, want 0.1 — the first window's burst leaked across the reset", g[nameCPUMax])
	}
}

// A cgroup whose files stop being readable must not stop the sweep, must not
// fabricate a value, and must be diagnosable: the counters say what broke, the
// throttled warning says where.
func TestUnreadableCgroupIsSurvivedAndNamed(t *testing.T) {
	h := newHarness(t)
	broken := systemdContainerDir(h.root, 1, hexID(1))
	healthy := systemdContainerDir(h.root, 2, hexID(2))
	makeContainer(t, broken, 0, 100<<20, 0)
	makeContainer(t, healthy, 0, 100<<20, 0)
	h.discover()

	h.advance(time.Second)
	// Replace the broken container's cpu.stat with something unparseable, in
	// place, so the held descriptor keeps working and the CONTENT is what
	// fails. (A removed directory is the other case — see
	// TestVanishedContainerFlushesItsFinalWindow.)
	writeFile(t, filepath.Join(broken, fileCPUStat), "user_usec 1\n")
	setUsage(t, healthy, 1_000_000)
	h.log.Reset()
	h.advance(time.Second)
	setUsage(t, healthy, 2_000_000)
	h.advance(time.Second)

	g := h.exportOnce(t)
	if _, ok := g[hexID(1)][nameCPUMax]; ok {
		t.Errorf("a container whose cpu.stat lost usage_usec still produced a CPU gauge: %v", g[hexID(1)])
	}
	if math.Abs(g[hexID(2)][nameCPUMax]-1) > 1e-9 {
		t.Errorf("the healthy container was not sampled: %v", g[hexID(2)])
	}
	if out := h.log.String(); !strings.Contains(out, broken) || !strings.Contains(out, fileCPUStat) {
		t.Errorf("the warning does not name the failing cgroup and file:\n%s", out)
	}
}

// --- sparse windows: hold the last value -------------------------------

// A window holding one sample of a signal is not a distribution: its stddev is
// 0 by construction and its max and min are the same number, which is the
// average the cadvisor scrape already publishes. Such a window re-states the
// last distribution that WAS measured, per signal, so the series keeps its
// shape instead of alternating between real and degenerate points.
func TestSparseWindowHoldsTheLastValuePerSignal(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 0)
	h.discover()

	// A container's FIRST window with one memory reading and no CPU rate at
	// all: nothing is held yet, so nothing is emitted for either signal. A gap
	// is honest about a distribution that has not been measured; inventing one
	// is not.
	h.advance(time.Second)
	if got := h.exportOnce(t); len(got) != 0 {
		t.Fatalf("a first window of one sample emitted %v; there is no previous value to hold", got)
	}

	// A real window: two CPU rates and two working-set readings.
	setUsage(t, dir, 2_000_000)
	setMem(t, dir, 200<<20, 0)
	h.advance(time.Second)
	setUsage(t, dir, 3_000_000)
	setMem(t, dir, 300<<20, 0)
	h.advance(time.Second)
	first := h.exportOnce(t)[hexID(1)]
	if math.Abs(first[nameCPUMax]-2) > 1e-9 || first[nameMemMax] != float64(300<<20) {
		t.Fatalf("the measured window is wrong: %v", first)
	}

	// Now a window with ONE memory sample and NO new CPU rate... except that a
	// CPU rate needs two raw readings, so a single sweep yields exactly one
	// rate and one working-set value. Both signals are therefore sparse, and
	// both hold.
	setUsage(t, dir, 3_500_000)
	setMem(t, dir, 999<<20, 0)
	h.advance(time.Second)
	held := h.exportOnce(t)[hexID(1)]
	if held == nil {
		t.Fatal("a sparse window emitted nothing even though a previous distribution was measured")
	}
	for name, want := range map[string]float64{
		nameCPUStddev: first[nameCPUStddev], nameCPUMax: first[nameCPUMax], nameCPUMin: first[nameCPUMin],
		nameMemStddev: first[nameMemStddev], nameMemMax: first[nameMemMax], nameMemMin: first[nameMemMin],
	} {
		if held[name] != want {
			t.Errorf("%s = %v on a sparse window, want the held %v", name, held[name], want)
		}
	}
	if held[nameMemMax] == float64(999<<20) {
		t.Errorf("the one-sample window was published as a distribution")
	}
}

// The hold is PER SIGNAL because the two fill at different rates: a CPU rate
// costs two raw readings, so CPU is permanently one sample behind memory. A
// per-container rule would throw away a perfectly good working-set
// distribution to protect the CPU one.
func TestHoldIsPerSignalNotPerContainer(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 0)
	h.discover()

	// Two sweeps with the CPU counter frozen: two working-set readings (a real
	// distribution) but two identical rates of 0 — which is also a real
	// distribution, so this fixture has to make the two DIFFER. Freeze memory
	// instead: one memory value repeated is still two samples, so drive the
	// sparsity where it actually occurs — a container discovered mid-window.
	h.advance(time.Second) // reading 1: mem only, no rate
	setMem(t, dir, 500<<20, 0)
	h.advance(time.Second) // reading 2: mem n=2, cpu n=1 (one rate)

	g := h.exportOnce(t)[hexID(1)]
	if g == nil {
		t.Fatal("the working-set distribution was dropped along with the CPU one")
	}
	if _, ok := g[nameCPUMax]; ok {
		t.Errorf("one CPU rate was published as a distribution: %v", g)
	}
	if g[nameMemMax] != float64(500<<20) || g[nameMemMin] != float64(100<<20) {
		t.Errorf("memory max/min = %v/%v, want %v/%v", g[nameMemMax], g[nameMemMin], float64(500<<20), float64(100<<20))
	}
}

// --- identity ----------------------------------------------------------

// The identity handed to the resolver is what decides whether these series join
// the cadvisor ones. Both halves of it come from the cgroup PATH, so both must
// arrive — at discovery, which is where the sampler decides whether the cgroup
// is worth three descriptors.
func TestTheCgroupPathSuppliesBothHalvesOfTheIdentity(t *testing.T) {
	h := newHarness(t)
	dir := kindContainerDir(h.root, 6, hexID(3))
	makeContainer(t, dir, 0, 10<<20, 0)
	h.discover()

	asked := h.res.asked()
	if len(asked) != 1 {
		t.Fatalf("resolver called %d times at discovery, want 1", len(asked))
	}
	if asked[0].containerID != hexID(3) || asked[0].podUID != podUID(6) {
		t.Fatalf("resolver got (%q, %q), want (%q, %q)", asked[0].containerID, asked[0].podUID, hexID(3), podUID(6))
	}

	h.advance(time.Second)
	setUsage(t, dir, 1_000_000)
	h.advance(time.Second)
	setUsage(t, dir, 2_000_000)
	h.advance(time.Second)

	exp := &fakeExporter{}
	if err := h.export(context.Background(), exp); err != nil {
		t.Fatal(err)
	}
	res := exp.sent[0].ResourceMetrics().At(0).Resource().Attributes()
	if v, ok := res.Get("k8s.pod.uid"); !ok || v.Str() != podUID(6) {
		t.Errorf("the resolver's attributes did not reach the payload: %v", res.AsRaw())
	}
	if sc := exp.sent[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Scope().Name(); sc != scopeName {
		t.Errorf("scope = %q, want %q", sc, scopeName)
	}
}

// The resource is REBUILT from current metadata at every export, and a label
// edited in place has to show up here on the same schedule it shows up on the
// cadvisor series these gauges annotate.
//
// Caching the resource for the container's lifetime does fix identity FLAPPING,
// and it buys staleness that defeats the feature just as thoroughly: an edited
// pod or namespace label — or a `.Node` attribute template that resolved before
// node metadata landed — leaves these six gauges carrying one attribute set
// while cadvisor's carry another, under the same job and instance. Two
// attribute sets that disagree do not join, and joining is the entire premise.
func TestResourceIsRebuiltFromCurrentMetadataOnEveryExport(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 10<<20, 0)
	h.res.set("k8s.pod.label.team", "payments")
	h.discover()

	window := func() {
		h.advance(time.Second)
		setMem(t, dir, 20<<20, 0)
		h.advance(time.Second)
		setMem(t, dir, 10<<20, 0)
	}
	attrsOf := func() map[string]any {
		t.Helper()
		exp := &fakeExporter{}
		if err := h.export(context.Background(), exp); err != nil {
			t.Fatal(err)
		}
		if len(exp.sent) == 0 || exp.sent[0].ResourceMetrics().Len() == 0 {
			t.Fatal("nothing was exported")
		}
		return exp.sent[0].ResourceMetrics().At(0).Resource().Attributes().AsRaw()
	}

	window()
	if got := attrsOf()["k8s.pod.label.team"]; got != "payments" {
		t.Fatalf("k8s.pod.label.team = %v, want the resolved value", got)
	}

	// Somebody relabels the pod. cadvisor's rows pick it up on their next
	// scrape, through the same lookup and the same cache.
	h.res.set("k8s.pod.label.team", "checkout")
	window()
	if got := attrsOf()["k8s.pod.label.team"]; got != "checkout" {
		t.Errorf("k8s.pod.label.team = %v after the label was edited, want %q — a resource cached for the container's lifetime diverges PERMANENTLY from the cadvisor resource it has to join", got, "checkout")
	}

	// And an attribute that only becomes resolvable later — the shape a `.Node`
	// template takes when node metadata lands after this container was first
	// seen — appears rather than being missing forever.
	h.res.set("k8s.node.label.zone", "eu-north-1a")
	window()
	if got := attrsOf()["k8s.node.label.zone"]; got != "eu-north-1a" {
		t.Errorf("k8s.node.label.zone = %v; an attribute that resolves after the container was discovered must still reach the payload", got)
	}
}

// The property the caching experiment was RIGHT about, and the one kept: a
// container the metadata service cannot place is not exported, so a series
// never appears and disappears with a `job` label attached. Resolution failing
// is the flap risk — a label edit is not.
//
// The window it measured is destroyed by the same act (snapshot resets as it
// renders), so it is COUNTED. A loss nobody can see is the one thing worse
// than a loss.
func TestAContainerThatStopsResolvingIsNotExportedAndTheLossIsCounted(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 10<<20, 0)
	h.discover()
	if h.Containers() != 1 {
		t.Fatalf("Containers() = %d, want 1", h.Containers())
	}

	h.advance(time.Second)
	setMem(t, dir, 40<<20, 0)
	h.advance(time.Second)

	// The metadata service loses the container between discovery and export —
	// an outage, or a pod tombstone that aged out from under a cgroup nobody
	// has cleaned up yet.
	h.res.reject(hexID(1))
	beforeDrop := obs.CgroupWindowsDropped.WithLabelValues("unresolved").Value()
	beforeUnres := obs.CgroupUnresolved.WithLabelValues("export").Value()

	exp := &fakeExporter{}
	if err := h.export(context.Background(), exp); err != nil {
		t.Fatal(err)
	}
	if len(exp.sent) != 0 {
		t.Errorf("a container with no identity was exported anyway: %v",
			exp.sent[0].ResourceMetrics().At(0).Resource().Attributes().AsRaw())
	}
	if got := obs.CgroupWindowsDropped.WithLabelValues("unresolved").Value(); got != beforeDrop+1 {
		t.Errorf("windows dropped = %v, want %v: a measured window thrown away must be counted", got, beforeDrop+1)
	}
	if got := obs.CgroupUnresolved.WithLabelValues("export").Value(); got != beforeUnres+1 {
		t.Errorf("unresolved{export} = %v, want %v", got, beforeUnres+1)
	}

	// And it comes back on its own when the metadata service does.
	h.res.accept(hexID(1))
	h.advance(time.Second)
	setMem(t, dir, 50<<20, 0)
	h.advance(time.Second)
	if g := h.exportOnce(t)[hexID(1)]; g == nil {
		t.Error("the container did not resume exporting once it resolved again")
	}
}

// A failed SEND destroys the window too — the snapshot resets as it renders,
// deliberately, because a window carried forward either double-counts into the
// next one or silently stretches the interval its numbers claim to describe.
// That trade is defensible; making it invisible is not.
func TestAFailedExportCountsTheWindowsItLost(t *testing.T) {
	h := newHarness(t)
	for i := 1; i <= 3; i++ {
		dir := systemdContainerDir(h.root, i, hexID(i))
		makeContainer(t, dir, 0, uint64(i)<<20, 0)
	}
	h.discover()
	h.advance(time.Second)
	for i := 1; i <= 3; i++ {
		setMem(t, systemdContainerDir(h.root, i, hexID(i)), uint64(i+10)<<20, 0)
	}
	h.advance(time.Second)

	before := obs.CgroupWindowsDropped.WithLabelValues("export_failed").Value()
	exp := &fakeExporter{err: errors.New("collector refused it")}
	if err := h.export(context.Background(), exp); err == nil {
		t.Fatal("a refused export reported success")
	}
	if got := obs.CgroupWindowsDropped.WithLabelValues("export_failed").Value(); got != before+3 {
		t.Errorf("windows dropped = %v, want %v (one per container in the refused payload)", got, before+3)
	}
}

// A container the metadata service cannot place is NOT EXPORTED. Its resource
// would carry no service.name, hence no Prometheus job, hence a series
// attributed to nothing that joins none of the cadvisor series these gauges
// exist to explain — and one successful later lookup would silently turn it
// into a DIFFERENT series.
//
// This is also what keeps every pod's SANDBOX out of the payload: the pause
// container's id appears in no pod's containerStatuses, so the metadata service
// cannot resolve it (proved against the real store, server and metaclient in
// internal/server's TestSandboxContainerIDDoesNotResolve).
func TestUnresolvedContainerIsNotExportedAndIsCounted(t *testing.T) {
	h := newHarness(t)
	workload := systemdContainerDir(h.root, 1, hexID(1))
	sandbox := systemdContainerDir(h.root, 1, hexID(2))
	makeContainer(t, workload, 0, 100<<20, 0)
	makeContainer(t, sandbox, 0, 1<<20, 0)
	h.res.reject(hexID(2))

	before := obs.CgroupUnresolved.WithLabelValues("pending").Value()
	h.discover()
	if got := h.Containers(); got != 1 {
		t.Fatalf("Containers() = %d, want only the resolvable one (the sandbox mints a phantom second resource per pod)", got)
	}
	if got := h.Unresolved(); got != 1 {
		t.Errorf("Unresolved() = %d, want 1", got)
	}
	if got := obs.CgroupUnresolved.WithLabelValues("pending").Value(); got <= before {
		t.Errorf("a skipped container moved no counter (%v -> %v); the condition has to be visible", before, got)
	}

	h.advance(time.Second)
	setUsage(t, workload, 1_000_000)
	setUsage(t, sandbox, 1_000_000)
	h.advance(time.Second)
	setUsage(t, workload, 2_000_000)
	setUsage(t, sandbox, 2_000_000)
	h.advance(time.Second)

	g := h.exportOnce(t)
	if _, ok := g[hexID(2)]; ok {
		t.Errorf("the unresolved container was exported: %v", g[hexID(2)])
	}
	if _, ok := g[hexID(1)]; !ok {
		t.Errorf("the resolvable container was not exported: %v", g)
	}
	// Nothing is even READ for it: no descriptors are opened until an identity
	// exists.
	if _, ok := h.tracked[hexID(2)]; ok {
		t.Errorf("an unresolved container holds descriptors and is sampled for data nobody will ever export")
	}
}

// A container that does not resolve on the first pass is RETRIED, and starts
// being sampled the moment it does — which is the normal case for a container
// discovered in the second before the kubelet posts its id to the API server.
func TestUnresolvedContainerIsRetriedOnEachDiscoveryPass(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 0)
	h.res.reject(hexID(1))
	h.discover()
	if h.Containers() != 0 {
		t.Fatal("the refused container was tracked anyway")
	}

	h.t = h.t.Add(discoverEvery)
	h.discover()
	if got := len(h.res.asked()); got != 2 {
		t.Fatalf("the resolver was asked %d times over two passes, want 2 (an unresolved container must be retried)", got)
	}

	h.res.accept(hexID(1))
	h.t = h.t.Add(discoverEvery)
	h.discover()
	if got := h.Containers(); got != 1 {
		t.Fatalf("Containers() = %d after the metadata service caught up, want 1", got)
	}
	if got := h.Unresolved(); got != 0 {
		t.Errorf("Unresolved() = %d, want 0", got)
	}
}

// The retry is BOUNDED: a cgroup that never resolves — every pod's sandbox is
// exactly that — must not be offered to the metadata service every fifteen
// seconds for the life of the node. It is given up on, counted, and retried far
// more slowly (a give-up is not a deletion: the other way to never resolve is a
// metadata service that is briefly unreachable, and abandoning a real container
// for its whole lifetime over that would be the worse bug).
func TestUnresolvableContainerIsGivenUpOnAndRetriedSlowly(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 0)
	h.res.reject(hexID(1))

	before := obs.CgroupUnresolved.WithLabelValues("abandoned").Value()
	h.discover()
	for range 3 { // a few passes, all inside the grace period
		h.t = h.t.Add(discoverEvery)
		h.discover()
	}
	if got := obs.CgroupUnresolved.WithLabelValues("abandoned").Value(); got != before {
		t.Fatalf("gave up inside the grace period (%v -> %v): a container whose id has not reached the API server yet resolves within about a second, but the metadata service's negative cache is a minute deep", before, got)
	}

	// Past the grace period: given up on, and counted.
	h.t = h.t.Add(maxUnresolvedAge)
	h.discover()
	if got := obs.CgroupUnresolved.WithLabelValues("abandoned").Value(); got != before+1 {
		t.Fatalf("abandoned counter = %v, want %v", got, before+1)
	}

	// And no longer asked about on every pass.
	asked := len(h.res.asked())
	h.t = h.t.Add(discoverEvery)
	h.discover()
	if got := len(h.res.asked()); got != asked {
		t.Errorf("an abandoned cgroup was retried on the very next pass (%d -> %d); one lookup per pod per %s is the point of giving up", asked, got, abandonRetryEvery)
	}

	// But it IS retried eventually, and recovers if the metadata service does.
	h.res.accept(hexID(1))
	h.t = h.t.Add(abandonRetryEvery)
	h.discover()
	if got := h.Containers(); got != 1 {
		t.Fatalf("Containers() = %d; an abandoned cgroup must recover when the metadata service does — a five-minute outage cannot cost a real container its whole lifetime", got)
	}
}

// One slow lookup must not stall the sweep goroutine behind a whole node's
// worth of them. What does not fit in the pass's budget rolls to the next one.
func TestResolutionBudgetBoundsOnePass(t *testing.T) {
	h := newHarness(t)
	for i := 1; i <= 3; i++ {
		makeContainer(t, systemdContainerDir(h.root, i, hexID(i)), 0, 1<<20, 0)
	}
	// A budget of zero: the first lookup always runs (progress can never stall
	// entirely), the rest wait for the next pass.
	h.resolveBudget = 0
	h.res.delay = time.Millisecond
	h.discover()
	if got := len(h.res.asked()); got != 1 {
		t.Fatalf("the resolver was asked %d times in one budgeted pass, want 1", got)
	}
	if got := h.Containers(); got != 1 {
		t.Errorf("Containers() = %d, want 1", got)
	}
	h.discover()
	if got := h.Containers(); got != 2 {
		t.Errorf("Containers() = %d after a second pass, want 2 — the remainder must roll forward, not be forgotten", got)
	}
}

// The six names are the contract with every dashboard that will join them
// against cadvisor's. A rename is a wire break, so it costs a deliberate test
// edit.
func TestMetricNamesAreExactlyTheDocumentedSix(t *testing.T) {
	want := []string{
		"container_cpu_usage_stddev",
		"container_cpu_usage_max",
		"container_cpu_usage_min",
		"container_memory_working_set_bytes_stddev",
		"container_memory_working_set_bytes_max",
		"container_memory_working_set_bytes_min",
	}
	if len(metricNames) != len(want) {
		t.Fatalf("metricNames has %d entries, want %d", len(metricNames), len(want))
	}
	for i, n := range want {
		if metricNames[i] != n {
			t.Errorf("metricNames[%d] = %q, want %q", i, metricNames[i], n)
		}
	}
}

// The unit field can rewrite the NAME under the OTLP→Prometheus translation
// (it appends the unit as a suffix), so the memory gauges deliberately leave it
// empty and the CPU ones use a brace-annotated unit, which is never appended.
// See the block comment in cgroupstats.go.
func TestUnitsCannotRewriteTheNames(t *testing.T) {
	if !strings.HasPrefix(unitCores, "{") || !strings.HasSuffix(unitCores, "}") {
		t.Errorf("unitCores = %q; a non-annotated unit would be appended to container_cpu_usage_* as a name suffix", unitCores)
	}
	if unitBytes != "" {
		t.Errorf("unitBytes = %q; the memory names carry _bytes in the MIDDLE, so a stated unit risks "+
			"container_memory_working_set_bytes_max_bytes, which joins nothing", unitBytes)
	}
}

// --- sweep jitter ------------------------------------------------------

// The CPU rate is CPU time divided by the wall time it accrued over, and both
// ends of that interval have to be read at the same point of the sweep.
//
// Timing every container from the SWEEP'S START instead — one clock reading
// handed to every container, while each is actually read at its own varying
// offset inside the sweep — puts the difference between those two offsets
// straight into the divisor. Measured on a live node: a dead-flat container
// reported a standard deviation of up to 0.012 cores and a correspondingly
// inflated max, out of nothing but the jitter between "when the sweep began"
// and "when this container's cpu.stat was actually read".
//
// The assertion is the mechanism, because the consequence cannot be pinned
// order-independently: Go randomises map iteration, so which container is read
// first is not the test's to choose. What IS decidable is that each container
// was timed from its own read — n distinct instants for n containers, rather
// than one instant shared by all of them.
func TestEachContainerIsTimedFromItsOwnRead(t *testing.T) {
	h := newHarness(t)
	const n = 4
	for i := range n {
		makeContainer(t, systemdContainerDir(h.root, i+1, hexID(i+1)), uint64(i)*1000, 1<<20, 0)
	}
	h.discover()

	// A clock that advances on every reading, which is what a real one does:
	// the sweep takes time, and the containers in it are not read at once.
	base := h.t
	var handed []time.Time
	h.now = func() time.Time {
		t := base.Add(time.Duration(len(handed)+1) * 10 * time.Millisecond)
		handed = append(handed, t)
		return t
	}
	h.sample()

	stamps := map[time.Time]int{}
	h.mu.Lock()
	for _, c := range h.tracked {
		stamps[c.prevAt]++
	}
	h.mu.Unlock()
	if len(stamps) != n {
		t.Fatalf("%d containers were timed from %d distinct instants: the sweep's own start time is in every container's rate divisor, "+
			"and the offset between it and the container's actual read shows up as standard deviation on a dead-flat load (measured: 0.012 cores)",
			n, len(stamps))
	}
	for ts := range stamps {
		if stamps[ts] != 1 {
			t.Errorf("two containers share the read instant %v", ts)
		}
		if !slices.Contains(handed, ts) {
			t.Errorf("a container was timed from %v, which the clock never handed out during the sweep", ts)
		}
	}

	// And a second sweep re-times each of them individually rather than
	// stamping one shared instant again: the divisor of every rate derived
	// below is that container's own read-to-read distance.
	sweep1 := len(handed)
	h.sample()
	h.mu.Lock()
	defer h.mu.Unlock()
	stamps = map[time.Time]int{}
	for _, c := range h.tracked {
		stamps[c.prevAt]++
		if !slices.Contains(handed[sweep1:], c.prevAt) {
			t.Errorf("a container kept the stamp %v from the previous sweep", c.prevAt)
		}
	}
	if len(stamps) != n {
		t.Errorf("the second sweep timed %d containers from %d distinct instants", n, len(stamps))
	}
}

// Containers() backs a self-metrics gauge evaluated on the export goroutine. It
// must not queue behind a sweep that is reading every cgroup on the node, or
// one pipeline's filesystem reads delay the whole process's self-metrics.
func TestContainersDoesNotTakeTheSampleLock(t *testing.T) {
	h := newHarness(t)
	makeContainer(t, systemdContainerDir(h.root, 1, hexID(1)), 0, 1<<20, 0)
	h.discover()

	h.mu.Lock() // stand in for a sweep in progress
	done := make(chan int, 1)
	go func() { done <- h.Containers() + h.Unresolved() + h.Discovered() }()
	select {
	case got := <-done:
		if got != 2 { // 1 sampled + 0 pending + 1 discovered
			t.Errorf("counts = %d, want 2", got)
		}
	case <-time.After(2 * time.Second):
		h.mu.Unlock()
		t.Fatal("Containers() blocked on the sample mutex; the obs gauge would stall every self-metrics export behind a sweep")
	}
	h.mu.Unlock()
}

// --- lifecycle ---------------------------------------------------------

// Run must stop on cancellation and leave the final window for the caller to
// export on ITS budget: a self-manufactured timeout cannot be fitted inside the
// pod's termination grace, which only the agent's shutdown sequence tracks
// (metrics.Registry.FinalExport carries the same correction).
func TestRunLeavesTheFinalWindowToTheCallersBudget(t *testing.T) {
	root := newRoot(t)
	dir := systemdContainerDir(root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 0)

	res := &fakeResolver{}
	s, err := New(Config{Root: root, Interval: MinInterval, Resolver: res,
		Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))})
	if err != nil {
		t.Fatal(err)
	}
	exp := &fakeExporter{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		// An export interval far longer than the test: no periodic export can
		// happen, so whatever is sent came from the shutdown path.
		s.Run(ctx, exp, time.Hour)
	}()
	// Let a few sample ticks land, moving usage so a rate exists.
	deadline := time.Now().Add(5 * time.Second)
	for s.samplesTaken() < 4 && time.Now().Before(deadline) {
		setUsage(t, dir, uint64(s.samplesTaken()+1)*100_000)
		time.Sleep(MinInterval / 4)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if len(exp.sent) != 0 {
		t.Fatalf("Run exported %d payloads of its own; the final window belongs to the caller's shutdown budget", len(exp.sent))
	}
	if s.Containers() != 0 {
		t.Errorf("Run left %d containers being sampled (descriptors leaked)", s.Containers())
	}

	// The caller's step, with its own detached, budgeted context.
	fctx, fcancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer fcancel()
	if err := s.FinalExport(fctx, exp); err != nil {
		t.Fatalf("FinalExport: %v", err)
	}
	if len(exp.sent) != 1 {
		t.Fatalf("sent %d payloads, want exactly the final window", len(exp.sent))
	}
	if exp.sent[0].DataPointCount() == 0 {
		t.Error("the final window carried no data points")
	}
	// Exported once, not once per call: everything is retired by then.
	if err := s.FinalExport(fctx, exp); err != nil {
		t.Fatal(err)
	}
	if len(exp.sent) != 1 {
		t.Errorf("the final window was exported %d times", len(exp.sent))
	}
}

// samplesTaken counts completed readings, for tests that must wait for the
// ticker without sleeping a fixed amount.
func (s *Sampler) samplesTaken() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.tracked {
		n += int(c.cpu.n) + int(c.mem.n)
	}
	return n
}

// --- two exporters at once ----------------------------------------------

// blockingExporter parks inside ExportMetrics until it is released, which is
// how a test holds one exporter in the middle of a send while another runs.
type blockingExporter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingExporter) ExportMetrics(_ context.Context, _ pmetric.Metrics) error {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return nil
}

// discardExporter signals that it was reached and throws the payload away. It
// holds no slice, so two of them on two goroutines share nothing.
type discardExporter struct {
	entered chan struct{}
	once    sync.Once
}

func (d *discardExporter) ExportMetrics(_ context.Context, _ pmetric.Metrics) error {
	d.once.Do(func() { close(d.entered) })
	return nil
}

// Run's export loop and FinalExport are CONCURRENT in production, and this is
// the shape that makes them so: the agent's shutdown joins its producers under
// a bounded budget and proceeds to the final exports when the join times out
// (main.go) — deliberately, so one stuck producer cannot cost every other
// pipeline its last window. A collector that has stopped answering is exactly
// what makes the join time out, and it is exactly what leaves Run's export loop
// inside a send.
//
// So both bodies snapshot, render and reset the same windows at the same time.
// They shared one reusable scratch slice and neither held a lock while reading
// it: the second's snapshot overwrote the array the first was still rendering
// from, which is a data race in the plain sense and, quietly, one payload's
// containers split across two exports.
//
// Under -race this fails outright without the fix. Without -race it still
// pins the observable half: the final export must carry the window rather than
// interleaving with the loop's.
func TestFinalExportDoesNotRaceTheExportLoop(t *testing.T) {
	root := newRoot(t)
	for i := 1; i <= 4; i++ {
		makeContainer(t, systemdContainerDir(root, i, hexID(i)), 0, uint64(i)<<20, 0)
	}
	s, err := New(Config{Root: root, Interval: MinInterval, Resolver: &fakeResolver{},
		Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.closeAll)
	// Two readings apiece, so every container has a real distribution and the
	// sparse-window hold keeps them emitting while the test runs.
	s.discover(context.Background(), time.Now())
	for i := 1; i <= 4; i++ {
		setMem(t, systemdContainerDir(root, i, hexID(i)), uint64(i+10)<<20, 0)
	}
	s.sample()
	s.sample()

	blocked := &blockingExporter{entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		s.Run(ctx, blocked, time.Millisecond)
	}()
	<-blocked.entered // the export loop is inside a send it will not return from

	// SIGTERM. The sample loop stops; the export loop cannot.
	cancel()

	// The blocked export has already snapshotted and RESET every window, so the
	// one the final export will carry is the empty one behind it. Fill it here:
	// a retiring container's flush carries what it MEASURED and never a held
	// value (a dead container's last datapoint has to be real), so a fixture
	// leaving fewer than two samples in that window would send an empty payload
	// and prove nothing about which exporter sent what. Sampling from the test
	// is safe with the sample loop stopping — sample() takes the same mutex.
	for range 3 {
		s.sample()
	}

	// The shutdown sequence's producer join times out here and moves on.
	final := &discardExporter{entered: make(chan struct{})}
	finalDone := make(chan struct{})
	go func() {
		defer close(finalDone)
		fctx, fcancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer fcancel()
		if err := s.FinalExport(fctx, final); err != nil {
			t.Errorf("FinalExport: %v", err)
		}
	}()

	select {
	case <-final.entered:
		// The unfixed shape: a second payload was snapshotted and rendered out
		// of the same scratch while the first was still in flight. Under -race
		// the detector has already fired on it.
	case <-time.After(250 * time.Millisecond):
		// The fixed shape: FinalExport is waiting on the export token.
	}
	close(blocked.release)

	select {
	case <-finalDone:
	case <-time.After(10 * time.Second):
		t.Fatal("FinalExport never completed; the two exporters deadlocked")
	}
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run never returned")
	}
	select {
	case <-final.entered:
	default:
		t.Error("the final window was never sent")
	}
}

// The same body reached from two goroutines with nothing blocking, hammered:
// the rendezvous above proves one overlap, this proves the fix holds under
// repetition rather than by luck of scheduling.
func TestConcurrentExportsAreSerialised(t *testing.T) {
	root := newRoot(t)
	for i := 1; i <= 8; i++ {
		makeContainer(t, systemdContainerDir(root, i, hexID(i)), 0, uint64(i)<<20, 0)
	}
	s, err := New(Config{Root: root, Interval: MinInterval, Resolver: &fakeResolver{},
		Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.closeAll)
	s.discover(context.Background(), time.Now())
	for i := 1; i <= 8; i++ {
		setMem(t, systemdContainerDir(root, i, hexID(i)), uint64(i+10)<<20, 0)
	}
	s.sample()
	s.sample()

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				// Each goroutine its own exporter: sharing one would be the
				// test's race rather than the sampler's.
				if err := s.export(context.Background(), &discardExporter{entered: make(chan struct{})}); err != nil {
					t.Errorf("export: %v", err)
					return
				}
				s.sample()
			}
		}()
	}
	wg.Wait()
}

// --- caps ---------------------------------------------------------------

// The container cap bounds FILE DESCRIPTORS — three are held open per sampled
// container — so it must count only entries that hold some.
//
// Testing it against tracked+pending spent that budget on entries that own no
// descriptor at all, and the entries in question are not exotic: EVERY pod
// contributes one permanently unresolvable cgroup (its sandbox), by design. On
// a large node the sandboxes therefore filled the cap and the real workload
// containers — the only ones that can ever be exported — were refused.
func TestPendingCgroupsDoNotConsumeTheDescriptorCap(t *testing.T) {
	h := newHarness(t)
	h.maxTracked = 3
	// One pod slice holding three sandbox cgroups and three workload ones. The
	// sandboxes sort FIRST in the listing (lower ids), which is what put them
	// at the front of the queue for a cap they cannot use.
	for i := 1; i <= 3; i++ {
		makeContainer(t, systemdContainerDir(h.root, 1, hexID(i)), 0, 1<<20, 0)
		h.res.reject(hexID(i))
	}
	for i := 11; i <= 13; i++ {
		makeContainer(t, systemdContainerDir(h.root, 1, hexID(i)), 0, uint64(i)<<20, 0)
	}
	h.discover()

	if got := h.Containers(); got != 3 {
		t.Fatalf("Containers() = %d, want all 3 workload containers: the three sandbox cgroups hold no descriptors and must not be charged against a descriptor cap", got)
	}
	if got := h.Unresolved(); got != 3 {
		t.Errorf("Unresolved() = %d, want the 3 sandboxes", got)
	}
}

// And the descriptor cap does still bind, where the descriptors are spent.
func TestTheDescriptorCapBindsOnPromotion(t *testing.T) {
	h := newHarness(t)
	h.maxTracked = 2
	for i := 1; i <= 3; i++ {
		makeContainer(t, systemdContainerDir(h.root, 1, hexID(i)), 0, uint64(i)<<20, 0)
	}
	before := obs.CgroupContainersCapped.WithLabelValues("tracked").Value()
	h.discover()
	if got := h.Containers(); got != 2 {
		t.Fatalf("Containers() = %d, want the cap of 2", got)
	}
	if got := obs.CgroupContainersCapped.WithLabelValues("tracked").Value(); got != before+1 {
		t.Errorf("capped{tracked} = %v, want %v", got, before+1)
	}
	// Refused, not discarded: it stays pending, so it takes the slot of
	// whichever sampled container retires first.
	if got := h.Unresolved(); got != 1 {
		t.Errorf("Unresolved() = %d, want the refused container still queued", got)
	}
}

// A container REFUSED by the descriptor cap has still had a lookup spent on it,
// and the queue ordering has to know that.
//
// The resolution pass is ordered least-recently-tried first, so that a budget
// that truncates the pass rotates through the pending set instead of running a
// fresh lottery every time. An entry that resolves but is then refused by the
// cap took the cap-refusal branch, which left its lastTry at the zero value —
// so it sorted to the FRONT of every later pass, forever, and the containers
// queued behind it were never asked about at all. That is the starvation the
// ordering exists to prevent, reintroduced at exactly the moment the node is
// under pressure.
func TestACapRefusedContainerDoesNotStarveTheQueue(t *testing.T) {
	h := newHarness(t)
	h.maxTracked = 1
	// A budget of zero attempts exactly one lookup per pass (the first is
	// always attempted), which is what makes the ORDER observable.
	h.resolveBudget = 0
	for i := 1; i <= 3; i++ {
		makeContainer(t, systemdContainerDir(h.root, 1, hexID(i)), 0, uint64(i)<<20, 0)
	}

	// Pass 1 fills the single descriptor slot with the first container.
	h.discover()
	if got := h.Containers(); got != 1 {
		t.Fatalf("Containers() = %d after the first pass, want 1", got)
	}
	// Pass 2 asks about the next one, which the cap refuses.
	h.t = h.t.Add(discoverEvery)
	h.discover()
	// Pass 3 must ask about the LAST one — the only pending container that has
	// never been tried.
	h.t = h.t.Add(discoverEvery)
	h.discover()

	asked := h.res.asked()
	if len(asked) != 3 {
		t.Fatalf("the budget let %d lookups through in three passes, want 3", len(asked))
	}
	seen := map[string]bool{}
	for _, c := range asked {
		seen[c.containerID] = true
	}
	for i := 1; i <= 3; i++ {
		if !seen[hexID(i)] {
			t.Errorf("container %s was never asked about in three budgeted passes; the one the cap refused kept its zero lastTry and sorted to the front of every pass, so the queue behind it starved. Asked: %v", hexID(i), asked)
		}
	}
}

// The pending set is bounded too — separately, because it bounds MEMORY and not
// descriptors, and because a cap that cannot be reached is not a cap.
func TestThePendingSetHasItsOwnCap(t *testing.T) {
	h := newHarness(t)
	h.maxPending = 2
	for i := 1; i <= 5; i++ {
		makeContainer(t, systemdContainerDir(h.root, 1, hexID(i)), 0, 1<<20, 0)
		h.res.reject(hexID(i))
	}
	before := obs.CgroupContainersCapped.WithLabelValues("pending").Value()
	h.discover()
	if got := h.Unresolved(); got != 2 {
		t.Fatalf("Unresolved() = %d, want the cap of 2", got)
	}
	if got := obs.CgroupContainersCapped.WithLabelValues("pending").Value(); got <= before {
		t.Errorf("capped{pending} = %v, want a move from %v: a bound that moves no counter is invisible", got, before)
	}
	if !strings.Contains(h.log.String(), "unresolved-cgroup cap") {
		t.Errorf("the pending cap did not name itself in the log:\n%s", h.log.String())
	}
}

// --- the hold is bounded ------------------------------------------------

// The sparse-window hold bridges a SAMPLING gap; it must not impersonate a live
// container. Unbounded, a dead-but-still-listed cgroup — the state
// repointLocked deliberately creates on CRI-O, where a container's own scope is
// removed while its conmon scope lingers under the same id, and the state a
// stale listing creates anywhere — republishes its last measurement as fresh
// data forever, indistinguishable from a live reading.
func TestTheHoldIsBoundedAndThenTheSignalStops(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 0)
	h.discover()

	// A real distribution for BOTH signals, so both have something to hold: a
	// CPU rate costs two raw readings before it yields even one value, so
	// three sweeps are the minimum that gives CPU two rates.
	h.advance(time.Second)
	setUsage(t, dir, 1_000_000)
	setMem(t, dir, 200<<20, 0)
	h.advance(time.Second)
	setUsage(t, dir, 3_000_000)
	setMem(t, dir, 300<<20, 0)
	h.advance(time.Second)
	first := h.exportOnce(t)[hexID(1)]
	if first == nil || first[nameMemMax] != float64(300<<20) {
		t.Fatalf("the measured window is wrong: %v", first)
	}
	if _, ok := first[nameCPUMax]; !ok {
		t.Fatalf("the CPU signal has no measured distribution to hold: %v", first)
	}

	// Now nothing is sampled at all — the container is listed and unreadable.
	beforeHeld := obs.CgroupHeldWindows.WithLabelValues("held").Value()
	for i := 1; i <= maxHeldWindows; i++ {
		g := h.exportOnce(t)[hexID(1)]
		if g == nil || g[nameMemMax] != first[nameMemMax] || g[nameCPUMax] != first[nameCPUMax] {
			t.Fatalf("hold %d emitted %v, want the last measured distribution %v", i, g, first)
		}
	}
	if got := obs.CgroupHeldWindows.WithLabelValues("held").Value(); got != beforeHeld+float64(2*maxHeldWindows) {
		t.Errorf("held counter = %v, want %v (both signals hold, per window)", got, beforeHeld+float64(2*maxHeldWindows))
	}

	// Past the bound the signal stops. A gap is the honest report of a
	// container producing nothing; a flat max forever is not.
	beforeExpired := obs.CgroupHeldWindows.WithLabelValues("expired").Value()
	if g := h.exportOnce(t); len(g) != 0 {
		t.Fatalf("the hold republished past its bound (%d windows): %v", maxHeldWindows, g)
	}
	if got := obs.CgroupHeldWindows.WithLabelValues("expired").Value(); got != beforeExpired+2 {
		t.Errorf("expired counter = %v, want %v (one per signal, once, at the transition)", got, beforeExpired+2)
	}

	// And it stays stopped, without re-counting the transition on every window.
	for range 3 {
		if g := h.exportOnce(t); len(g) != 0 {
			t.Fatalf("the signal came back with no new samples: %v", g)
		}
	}
	if got := obs.CgroupHeldWindows.WithLabelValues("expired").Value(); got != beforeExpired+2 {
		t.Errorf("expired counter = %v after three more empty windows, want it counted once at the transition", got)
	}

	// A real window starts a fresh hold budget.
	h.advance(time.Second)
	setMem(t, dir, 500<<20, 0)
	h.advance(time.Second)
	if g := h.exportOnce(t)[hexID(1)]; g == nil || g[nameMemMax] != float64(500<<20) {
		t.Fatalf("the container did not resume when it started producing samples again: %v", g)
	}
	if g := h.exportOnce(t)[hexID(1)]; g == nil || g[nameMemMax] != float64(500<<20) {
		t.Errorf("the hold budget did not reset after a measured window: %v", g)
	}
}

// --- retiring a container that is listed but no longer readable ----------

// breakContainer makes every read of a container's three files fail while
// leaving the DIRECTORY in place, so discovery keeps listing it. That is the
// state repointLocked deliberately creates on CRI-O (the container's own scope
// is removed while its conmon scope lingers under the same id) and the state a
// stale listing creates anywhere.
func breakContainer(t *testing.T, dir string) {
	t.Helper()
	for _, f := range []string{fileCPUStat, fileMemCurrent, fileMemStat} {
		writeFile(t, filepath.Join(dir, f), "")
	}
}

// A container that is still listed and no longer readable must be RETIRED, not
// carried forever.
//
// Without this it kept its three open descriptors and issued three failing
// preads every sampling period — on the goroutine that also tails every log
// file on the node — for the life of the process, and went on being counted in
// kubescrape_cgroup_containers as a container currently being sampled. The hold
// bound stopped it EXPORTING; nothing stopped it costing.
func TestAListedButUnreadableContainerIsRetired(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 0)
	h.discover()

	// A real window first, so the container has a distribution to hold and the
	// retirement is not just "it never produced anything".
	h.advance(time.Second)
	setUsage(t, dir, 1_000_000)
	setMem(t, dir, 200<<20, 0)
	h.advance(time.Second)
	setUsage(t, dir, 3_000_000)
	setMem(t, dir, 300<<20, 0)
	h.advance(time.Second)
	if g := h.exportOnce(t)[hexID(1)]; g == nil {
		t.Fatal("the fixture never produced a measured window")
	}
	if got := h.Containers(); got != 1 {
		t.Fatalf("Containers() = %d, want 1", got)
	}

	breakContainer(t, dir)
	before := obs.CgroupContainersRetired.Value()

	// Each window: two sweeps in which every read fails, then an export.
	window := func() map[string]map[string]float64 {
		h.advance(time.Second)
		h.advance(time.Second)
		return h.exportOnce(t)
	}
	// While the hold lasts the signal still emits — that is the hold's job, and
	// retiring earlier would cut it short — but the container already stops
	// counting as SAMPLED, because it is not being sampled.
	for i := 1; i <= maxHeldWindows; i++ {
		if g := window()[hexID(1)]; g == nil {
			t.Fatalf("window %d emitted nothing; the hold is supposed to bridge this", i)
		}
		if got := h.Containers(); got != 0 {
			t.Errorf("kubescrape_cgroup_containers = %d after %d windows in which every read failed; the gauge must count what is being SAMPLED, not what is merely tracked", got, i)
		}
		if got := h.Discovered(); got != 1 {
			t.Errorf("Discovered() = %d, want 1: the cgroup IS still in the hierarchy, and the difference between the two counts is what says so", got)
		}
		if _, ok := h.tracked[hexID(1)]; !ok {
			t.Fatalf("the container was retired after %d windows, cutting its own hold short", i)
		}
	}

	// The window the hold expires on is the window it is retired on: the first
	// one in which it would export nothing anyway.
	if g := window(); len(g) != 0 {
		t.Errorf("a container that has answered no read for %d windows is still exporting: %v", maxDeadWindows, g)
	}
	h.mu.Lock()
	c, stillTracked := h.tracked[hexID(1)]
	h.mu.Unlock()
	if stillTracked {
		t.Fatalf("the container is still tracked after %d dead windows, holding three descriptors and issuing three failing reads per period forever (open=%v)", maxDeadWindows, c.open)
	}
	if got := obs.CgroupContainersRetired.Value(); got != before+1 {
		t.Errorf("retired counter = %v, want %v: a container dropped from the sampled set has to be visible", got, before+1)
	}
	if !strings.Contains(h.log.String(), "retired a container") {
		t.Errorf("the retirement was silent:\n%s", h.log.String())
	}

	// And the retirement STICKS. The cgroup is still in the hierarchy, so a
	// discovery pass that treated it as new would re-open its descriptors
	// fifteen seconds later and start the same three dead windows again — a
	// retirement undone before it saved anything.
	h.t = h.t.Add(discoverEvery)
	h.discover()
	if got := h.Containers(); got != 0 {
		t.Errorf("Containers() = %d one discovery pass after the retirement: the cgroup is still listed, and re-adopting it puts the descriptors and the failing reads straight back", got)
	}
	if got := h.Unresolved(); got != 1 {
		t.Errorf("Unresolved() = %d, want the retired container held on the slow clock (a give-up is not a deletion — whatever broke may heal)", got)
	}
	for range 4 { // more passes, still nothing re-adopted
		h.t = h.t.Add(discoverEvery)
		h.discover()
		h.advance(time.Second)
	}
	if got := h.Containers(); got != 0 {
		t.Errorf("Containers() = %d after five passes; the quarantine must hold for %s", got, abandonRetryEvery)
	}

	// It does get another chance, though, and takes it when the files read
	// again — the state this bound exists for is usually permanent, but a
	// retirement that could never be undone would be a deletion.
	makeContainer(t, dir, 0, 100<<20, 0)
	h.t = h.t.Add(abandonRetryEvery)
	h.discover()
	if got := h.Containers(); got != 1 {
		t.Errorf("Containers() = %d after %s and a cgroup that reads again, want 1", got, abandonRetryEvery)
	}
}

// The retirement must not fire on a container that merely went quiet, or a
// perfectly healthy container is dropped every time its cgroup files are slow
// to change. Only a window in which a sample was ATTEMPTED and NOTHING was read
// counts, and one successful read of any of the three files clears the streak.
func TestRetirementNeedsFailedReadsAndNotJustSilence(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 0)
	h.discover()
	h.advance(time.Second)
	h.advance(time.Second)

	// Windows with no sweep at all: the sampler was not running, which says
	// nothing about whether this cgroup is readable.
	for range maxDeadWindows + 2 {
		h.exportOnce(t)
	}
	if _, ok := h.tracked[hexID(1)]; !ok {
		t.Fatal("a container was retired over windows in which nothing was sampled")
	}

	// And a container whose cpu.stat is broken while memory still reads is
	// still being sampled: it produces one of the two signals, which is data.
	writeFile(t, filepath.Join(dir, fileCPUStat), "user_usec 1\n")
	for range maxDeadWindows + 2 {
		h.advance(time.Second)
		h.advance(time.Second)
		h.exportOnce(t)
	}
	if _, ok := h.tracked[hexID(1)]; !ok {
		t.Fatal("a container whose working set still reads was retired because its cpu.stat stopped parsing")
	}
	if got := h.Containers(); got != 1 {
		t.Errorf("Containers() = %d, want 1: it is being sampled, just not for CPU", got)
	}
}

// --- the last datapoint of a dead container ------------------------------

// A vanished container's final flush must carry what was MEASURED or nothing at
// all — never the previous window's numbers re-stamped at the retirement
// instant.
//
// The hold exists to bridge a sampling hiccup in a LIVING container. Applied to
// a dying one it invents a final reading, and the last datapoint of a container
// is exactly the one an operator zooms into after an OOM kill: it would show the
// working set the container had a window earlier, timestamped at the moment it
// died, with nothing to say it was a copy.
func TestAVanishedContainersFinalWindowIsNeverAHeldValue(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 0)
	h.discover()

	// A real window, exported: now there IS something to hold.
	h.advance(time.Second)
	setUsage(t, dir, 1_000_000)
	setMem(t, dir, 500<<20, 0)
	h.advance(time.Second)
	setUsage(t, dir, 3_000_000)
	setMem(t, dir, 700<<20, 0)
	h.advance(time.Second)
	first := h.exportOnce(t)[hexID(1)]
	if first == nil || first[nameMemMax] != float64(700<<20) {
		t.Fatalf("the measured window is wrong: %v", first)
	}

	// The container is torn down one sample into the next window — too few for
	// a distribution, which is the whole case.
	h.advance(time.Second)
	if err := os.RemoveAll(filepath.Dir(dir)); err != nil {
		t.Fatal(err)
	}
	h.discover()

	g := h.exportOnce(t)
	if len(g) != 0 {
		t.Errorf("the final flush of a dead container republished %v. Those are the PREVIOUS window's numbers stamped at the retirement time: a fabricated last datapoint, on the one datapoint an operator reads most carefully", g[hexID(1)])
	}
	// And it is retired either way — a gap is not a reason to keep it around.
	if _, ok := h.tracked[hexID(1)]; ok {
		t.Error("the vanished container was not retired")
	}
}

// --- payload cost -------------------------------------------------------

// Every metric of every ResourceMetrics carries its description, and this
// pipeline emits one ResourceMetrics PER CONTAINER: descriptor text is repeated
// once per container per metric per export, forever.
//
// The first cut of these six descriptions carried a ~480-byte explanatory note
// apiece. Measured by this test's own fixture — 110 containers, a realistic
// resource — that was 410,740 bytes per export of which 288,640 were
// description: 70% of the payload, four bytes of prose shipped and parsed for
// every byte of data, every scrape interval, forever. (Shipped and parsed, not
// STORED: Prometheus and Mimir keep HELP per metric family, not per series, so
// the repetition is a wire and parse cost and not a storage one.) Shortening
// them took the payload to 173,800 bytes with 51,700 of description, a 58% cut
// overall and an 82% cut in descriptor text.
//
// That is the same arithmetic promscrape's cadvisor and split batchers charge
// to their chunk estimate (metricMeta.apply / metaFieldBytes), and the reason
// the answer here is SHORTENING rather than chunking: chunking bounds a payload
// against a receiver's message limit, which otlpexport already does for
// everything this agent sends (exact proto size, split at -otlp-max-send-bytes
// via pkg/otlpsplit), and it would still ship every one of those bytes.
func TestDescriptionsAreAffordablePerContainer(t *testing.T) {
	const containers = 110 // the kubelet's default max-pods
	h := newHarness(t)
	// A REALISTIC resource, because the share depends on it: the resolver
	// stamps roughly this much (the cadvisor attribute set plus the derived
	// identity), and measuring against the three-attribute test fake would
	// overstate the description's share of a payload nobody ships.
	for k, v := range map[string]string{
		"k8s.namespace.name":   "payments-production",
		"k8s.pod.name":         "checkout-api-7d9f4b6c88-x2kqp",
		"k8s.container.name":   "checkout-api",
		"k8s.node.name":        "ip-10-42-31-207.eu-north-1.compute.internal",
		"container.image.name": "registry.example.com/payments/checkout-api:1.42.7",
		"k8s.deployment.name":  "checkout-api",
		"k8s.pod.ip":           "10.42.31.88",
		"service.namespace":    "payments-production",
		"service.instance.id":  "cadvisor-checkout-api-7d9f4b6c88-x2kqp/checkout-api",
		"k8s.cluster.name":     "prod-eu-north-1",
	} {
		h.res.set(k, v)
	}
	snap := make([]windowPair, 0, containers)
	for i := range containers {
		snap = append(snap, windowPair{
			id: hexID(i + 1), podUID: podUID(i + 1),
			cpu: signalOut{emit: true, stddev: 0.25, max: 4, min: 0.01},
			mem: signalOut{emit: true, stddev: 1 << 20, max: 512 << 20, min: 16 << 20},
		})
	}
	md := h.build(context.Background(), snap, h.t)
	if md.ResourceMetrics().Len() != containers {
		t.Fatalf("built %d resources, want %d", md.ResourceMetrics().Len(), containers)
	}
	var m pmetric.ProtoMarshaler
	total := m.MetricsSize(md)

	// The same payload with the descriptions stripped is the floor: everything
	// but the prose.
	rms := md.ResourceMetrics()
	for i := range rms.Len() {
		sms := rms.At(i).ScopeMetrics()
		for j := range sms.Len() {
			ms := sms.At(j).Metrics()
			for k := range ms.Len() {
				ms.At(k).SetDescription("")
			}
		}
	}
	bare := m.MetricsSize(md)
	desc := total - bare
	share := float64(desc) / float64(total)
	t.Logf("%d containers: %d bytes total, %d bytes of description (%.0f%%); the pre-shortening fixture measured 410740 / 288640 (70%%)",
		containers, total, desc, share*100)
	// The absolute budget is the one that means something — the share is partly
	// a property of how small six float64s are — but both are asserted, because
	// a payload that is mostly prose is the thing that went wrong.
	if per := desc / containers; per > 700 {
		t.Errorf("descriptions cost %d bytes per container per export (%d for six metrics), over the 700-byte budget", per, desc)
	}
	if share > 0.40 {
		t.Errorf("descriptions are %.0f%% of the payload (%d of %d bytes) for %d containers. "+
			"A description is repeated once per container per metric per export; the long-form argument for the pipeline belongs in the package doc, docs/METRICS.md and the flag help, none of which is charged per container",
			share*100, desc, total, containers)
	}
	// And each one has to be short enough that a longer one is a deliberate act
	// rather than an accident.
	for _, d := range []string{descCPUStddev, descCPUMax, descCPUMin, descMemStddev, descMemMax, descMemMin} {
		if len(d) > 110 {
			t.Errorf("description is %d bytes, over the 110-byte budget: %q", len(d), d)
		}
	}
}

// --- giving up, and un-giving up ----------------------------------------

// Abandonment is measured from the first FAILED ATTEMPT, not from discovery.
//
// A resolution pass is budgeted, so a metadata service that has gone slow rolls
// most of the pending set over to the next pass. Timed from discovery, a cgroup
// that waited out its whole grace period in the QUEUE was abandoned on its very
// first failure — one slow minute dropping a whole node to the ten-minute retry
// cadence, which is the opposite of what a grace period is for.
func TestAbandonmentIsTimedFromTheFirstFailedAttempt(t *testing.T) {
	h := newHarness(t)
	// A budget of zero attempts exactly one cgroup per pass (the first is
	// always attempted), which is the queueing this is about. The order is
	// least-recently-tried, so the two rotate.
	h.resolveBudget = 0
	for _, seed := range []int{1, 2} {
		makeContainer(t, systemdContainerDir(h.root, 1, hexID(seed)), 0, 1<<20, 0)
		h.res.reject(hexID(seed))
	}

	h.discover() // pass 1: hexID(1) is attempted and fails; hexID(2) is queued
	if got := len(h.res.asked()); got != 1 {
		t.Fatalf("the budget let %d lookups through, want 1", got)
	}

	// Long enough that a clock started at DISCOVERY has run out for both.
	h.t = h.t.Add(maxUnresolvedAge + time.Second)
	before := obs.CgroupUnresolved.WithLabelValues("abandoned").Value()
	h.discover() // pass 2: hexID(2)'s FIRST attempt
	asked := h.res.asked()
	if got := asked[len(asked)-1].containerID; got != hexID(2) {
		t.Fatalf("pass 2 asked about %s, want the never-tried %s (least-recently-tried first)", got, hexID(2))
	}
	if got := obs.CgroupUnresolved.WithLabelValues("abandoned").Value(); got != before {
		t.Errorf("a cgroup was abandoned on its FIRST failed lookup (%v -> %v) because it had been sitting in a budget-truncated queue since discovery; the grace period exists to survive a slow metadata service, and timing it from discovery spends it before the first attempt", before, got)
	}

	// The one that really has been failing that long IS abandoned, so this is
	// not just "nothing is ever given up on".
	h.t = h.t.Add(time.Second)
	h.discover() // pass 3: hexID(1) again, first failed at T0
	if got := obs.CgroupUnresolved.WithLabelValues("abandoned").Value(); got != before+1 {
		t.Errorf("abandoned = %v, want %v: a cgroup whose first FAILURE was a grace period ago must still be given up on", got, before+1)
	}
}

// The steady-state COST of the give-up rule, counted in lookups, on the node
// shape that actually exists: a full complement of permanently-404 sandbox
// cgroups beside a workload container that resolves and exports every window.
//
// The rule is "one metadata lookup per abandoned cgroup per abandonRetryEvery",
// and the previous design could not deliver it. It reconsidered the whole
// abandoned set on EVIDENCE — any successful lookup anywhere — and bumped that
// evidence once per resolved container per EXPORT, so on any node that exports
// anything the evidence arm was permanently satisfied and every sandbox fell
// back to the reconsiderEvery floor: measured on this fixture, 20 lookups per
// sandbox per 20 minutes instead of 2, i.e. ~110 metadata GETs a minute on a
// 110-pod node. Worse, the floor equals the resolver's negative-cache depth, so
// each of those retries landed just OUTSIDE the cache and cost a real round
// trip — the precise opposite of the reason the floor was chosen.
//
// Classifying the failure is what makes the cadence real: a sandbox's 404 is a
// statement about the sandbox, and somebody else's success is not evidence
// against it.
func TestAbandonedSandboxesCostOneLookupPerSlowRetry(t *testing.T) {
	const sandboxes = 8
	h := newHarness(t)
	// One workload container that resolves, is sampled, and is exported every
	// window — the "whether or not the node is exporting" half of the claim,
	// and what fed the old evidence arm.
	live := systemdContainerDir(h.root, 99, hexID(99))
	makeContainer(t, live, 0, 100<<20, 0)
	for i := 1; i <= sandboxes; i++ {
		makeContainer(t, systemdContainerDir(h.root, i, hexID(i)), 0, 1<<20, 0)
		h.res.reject(hexID(i))
	}

	mem := uint64(100 << 20)
	// One 15s discovery cadence, with a sweep on each and an export every other
	// one (a 30s window, the shipped default).
	tick := func(n int) {
		for i := range n {
			mem += 1 << 20
			setMem(t, live, mem, 0)
			h.advance(discoverEvery)
			h.discover()
			if i%2 == 1 {
				h.exportOnce(t)
			}
		}
	}

	// Five minutes: past maxUnresolvedAge, so every sandbox is abandoned.
	tick(20)
	if h.Containers() != 1 {
		t.Fatalf("Containers() = %d, want the one workload container", h.Containers())
	}
	if got := obs.CgroupUnresolved.WithLabelValues("abandoned").Value(); got < sandboxes {
		t.Fatalf("only %v of %d sandboxes reached the abandoned state; the fixture is not measuring the steady state", got, sandboxes)
	}

	// Now measure the steady state itself.
	base := len(h.res.asked())
	const minutes = 20
	tick(minutes * 4)
	lookups := len(h.res.asked()) - base
	// The workload container is asked once per EXPORT (the resource rebuild),
	// which is not what this bounds.
	sandboxLookups := 0
	for _, c := range h.res.asked()[base:] {
		if c.containerID != hexID(99) {
			sandboxLookups++
		}
	}
	budget := sandboxes * (minutes*int(time.Minute)/int(abandonRetryEvery) + 1)
	t.Logf("%d sandboxes over %d minutes: %d sandbox lookups (%.2f per sandbox per minute), %d lookups in total",
		sandboxes, minutes, sandboxLookups, float64(sandboxLookups)/float64(sandboxes*minutes), lookups)
	if sandboxLookups > budget {
		t.Errorf("%d sandbox lookups in %d minutes, want at most %d (one per sandbox per %s). "+
			"At the reconsiderEvery floor this would be %d — one per sandbox per minute — which is what a node that exports anything used to do, every one of them a real round trip just outside the metadata service's negative cache",
			sandboxLookups, minutes, budget, abandonRetryEvery, sandboxes*minutes)
	}
	// And it is not vacuously low: the slow retry does still happen, because a
	// give-up is not a deletion.
	if sandboxLookups == 0 {
		t.Errorf("no abandoned cgroup was retried at all in %d minutes; a real container caught by the give-up rule would never come back", minutes)
	}
}

// An unreachable metadata service is not evidence about any container, and the
// two consequences are what this pins.
//
// Nothing is abandoned while the service cannot answer — an outage otherwise
// drops a whole node's REAL containers to the ten-minute cadence, having learnt
// nothing about any of them — and when the service returns, sampling resumes on
// the next pass rather than after the long clock.
func TestAnUnreachableServiceAbandonsNothingAndRecoversPromptly(t *testing.T) {
	h := newHarness(t)
	dir := systemdContainerDir(h.root, 1, hexID(1))
	makeContainer(t, dir, 0, 100<<20, 0)
	h.res.setDown(true)

	before := obs.CgroupUnresolved.WithLabelValues("abandoned").Value()
	beforeUnreachable := obs.CgroupUnresolved.WithLabelValues("unreachable").Value()
	h.discover()
	// Half an hour of outage: many times maxUnresolvedAge.
	for range 30 * 4 {
		h.t = h.t.Add(discoverEvery)
		h.discover()
	}
	if got := obs.CgroupUnresolved.WithLabelValues("abandoned").Value(); got != before {
		t.Errorf("a container was given up on during a metadata OUTAGE (%v -> %v): the service said nothing about it, and abandoning on that puts every real container on the node on the ten-minute cadence for a condition that has nothing to do with them", before, got)
	}
	if got := obs.CgroupUnresolved.WithLabelValues("unreachable").Value(); got <= beforeUnreachable {
		t.Errorf("unreachable lookups moved no counter (%v -> %v); the failure class an operator has to be able to see is the one that is NOT the cgroup's fault", beforeUnreachable, got)
	}

	// The service comes back.
	h.res.setDown(false)
	h.t = h.t.Add(discoverEvery)
	h.discover()
	if got := h.Containers(); got != 1 {
		t.Fatalf("Containers() = %d on the first pass after the metadata service recovered, want 1", got)
	}
}

// The other half of the same rule: an already-abandoned cgroup whose retry finds
// the service unreachable goes back on the FAST clock. The give-up was justified
// by a definitive answer, and the failure that has just replaced that answer is
// not one — so the node resumes within reconsiderEvery of the service returning
// instead of serving out the rest of abandonRetryEvery.
func TestAnAbandonedCgroupDropsBackToTheFastClockWhenTheServiceBreaks(t *testing.T) {
	h := newHarness(t)
	makeContainer(t, systemdContainerDir(h.root, 1, hexID(1)), 0, 1<<20, 0)
	h.res.reject(hexID(1))
	h.discover()
	h.t = h.t.Add(maxUnresolvedAge + time.Second)
	h.discover()
	if obs.CgroupUnresolved.WithLabelValues("abandoned").Value() == 0 {
		t.Fatal("the fixture never reached the abandoned state")
	}

	// Its slow retry comes round and finds the service down.
	h.res.setDown(true)
	h.t = h.t.Add(abandonRetryEvery)
	asked := len(h.res.asked())
	h.discover()
	if len(h.res.asked()) == asked {
		t.Fatal("the abandoned cgroup was never retried; the fixture proves nothing")
	}

	// The service comes back a minute later. Under the slow clock this cgroup
	// would wait another ten.
	h.res.accept(hexID(1))
	h.res.setDown(false)
	h.t = h.t.Add(reconsiderEvery)
	h.discover()
	if got := h.Containers(); got != 1 {
		t.Errorf("Containers() = %d one %s after the service recovered; a retry that came back UNREACHABLE proves the give-up may no longer hold, so it must not re-arm the %s clock", got, reconsiderEvery, abandonRetryEvery)
	}
}

// A cgroup the service DEFINITIVELY does not know stays on the slow clock, and
// other containers resolving all around it changes nothing. That is the rule the
// evidence arm used to break: a sandbox is unresolvable because it is a sandbox,
// and somebody else's successful lookup is not an argument about it.
func TestSomebodyElsesSuccessDoesNotReconsiderADefinitiveMiss(t *testing.T) {
	h := newHarness(t)
	makeContainer(t, systemdContainerDir(h.root, 1, hexID(1)), 0, 1<<20, 0)
	h.res.reject(hexID(1))
	h.discover()
	h.t = h.t.Add(maxUnresolvedAge + time.Second)
	h.discover()

	// A stream of new containers, each resolving: on the old design, evidence
	// on every pass.
	asked := len(h.res.asked())
	for i := 2; i <= 5; i++ {
		makeContainer(t, systemdContainerDir(h.root, i, hexID(i)), 0, uint64(i)<<20, 0)
		h.t = h.t.Add(discoverEvery)
		h.discover()
	}
	extra := len(h.res.asked()) - asked - 4 // minus the four new containers
	if extra > 0 {
		t.Errorf("the abandoned cgroup was retried %d times inside one %s window because other containers resolved; nothing about them says this cgroup has an identity", extra, abandonRetryEvery)
	}
}
