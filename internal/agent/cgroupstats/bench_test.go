package cgroupstats

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// benchSampler builds a sampler over n fabricated containers with a fixed
// clock, discovered and resolved, ready for sample() to be called directly. The
// clock is returned by POINTER: the sample path times each container from its
// own previous read, so a benchmark or a budget test has to be able to move it.
func benchSampler(tb testing.TB, n int) (s *Sampler, clock *time.Time, done func()) {
	tb.Helper()
	root := newRootTB(tb)
	for i := range n {
		makeContainerTB(tb, systemdContainerDir(root, i, hexID(i)), uint64(i)*1000, 1<<20, 1<<18)
	}
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	s, err := New(Config{
		Root:     root,
		Resolver: &fakeResolver{},
		Logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		now:      func() time.Time { return now },
	})
	if err != nil {
		tb.Fatal(err)
	}
	s.discover(context.Background(), now)
	if got := s.Containers(); got != n {
		tb.Fatalf("tracked %d containers, want %d", got, n)
	}
	return s, &now, s.closeAll
}

// The testing.TB flavours of the tree helpers, so the benchmark can build one
// too (the helpers in tree_test.go take *testing.T because every other caller
// does).
func newRootTB(tb testing.TB) string {
	tb.Helper()
	root := tb.TempDir()
	if err := os.WriteFile(root+"/"+controllersFile, []byte("cpu memory pids\n"), 0o644); err != nil {
		tb.Fatal(err)
	}
	return root
}

func makeContainerTB(tb testing.TB, dir string, usageUsec, memCurrent, inactiveFile uint64) {
	tb.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tb.Fatal(err)
	}
	write := func(name, content string) {
		if err := os.WriteFile(dir+"/"+name, []byte(content), 0o644); err != nil {
			tb.Fatal(err)
		}
	}
	write(fileCPUStat, cpuStat(usageUsec))
	write(fileMemCurrent, fmtUint(memCurrent))
	write(fileMemStat, memStat(inactiveFile))
}

// TestSampleAllocationBudget is the ENFORCEMENT of the read path's zero-alloc
// claim; BenchmarkSample below only reports it, and a benchmark cannot fail a
// build.
//
// The budget is 0 because the whole point of holding descriptors open and
// preading into a reused buffer is that a 200-container node's 600 reads a
// second cost the shared node agent no garbage at all. Anything that
// reintroduces a per-read path join, an os.File, a fmt call or a strconv
// conversion shows up here.
func TestSampleAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector changes escape analysis and adds bookkeeping allocations")
	}
	const containers = 8
	s, clock, done := benchSampler(t, containers)
	defer done()

	got := testing.AllocsPerRun(200, func() {
		*clock = clock.Add(time.Second)
		s.sample()
	})
	if got > 0 {
		t.Errorf("sample() over %d containers allocates %.1f objects per sweep, want 0", containers, got)
	}
}

// The two readers, measured on their own, so a regression points at the file
// rather than at the sweep.
func TestReadIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector changes escape analysis and adds bookkeeping allocations")
	}
	dir := t.TempDir()
	makeContainerTB(t, dir, 123456, 1<<20, 1<<18)
	fds, err := openCgroupFDs(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fds.close()
	buf := make([]byte, readBufBytes)

	if got := testing.AllocsPerRun(500, func() {
		if _, err := readField(fds.cpuStat, buf, keyUsageUsec); err != nil {
			t.Fatal(err)
		}
	}); got > 0 {
		t.Errorf("readField(cpu.stat) allocates %.1f, want 0", got)
	}
	if got := testing.AllocsPerRun(500, func() {
		if _, err := readField(fds.memStat, buf, keyInactiveFile); err != nil {
			t.Fatal(err)
		}
	}); got > 0 {
		t.Errorf("readField(memory.stat) allocates %.1f, want 0", got)
	}
	if got := testing.AllocsPerRun(500, func() {
		if _, err := readValue(fds.memCurrent, buf); err != nil {
			t.Fatal(err)
		}
	}); got > 0 {
		t.Errorf("readValue(memory.current) allocates %.1f, want 0", got)
	}
}

// BenchmarkSample reports the per-sweep cost of the sample path; the budget
// above is what enforces it. b.Loop rather than a b.N loop: the old form lets
// the compiler eliminate a body whose result is unused, which is exactly the
// failure mode for a benchmark claiming zero allocations.
func BenchmarkSample(b *testing.B) {
	s, clock, done := benchSampler(b, 100)
	defer done()
	b.ReportAllocs()
	for b.Loop() {
		*clock = clock.Add(time.Second)
		s.sample()
	}
}

// BenchmarkReadField is the per-file half: three of these are one container's
// sample.
func BenchmarkReadField(b *testing.B) {
	dir := b.TempDir()
	makeContainerTB(b, dir, 123456, 1<<20, 1<<18)
	fds, err := openCgroupFDs(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer fds.close()
	buf := make([]byte, readBufBytes)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := readField(fds.memStat, buf, keyInactiveFile); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDiscover covers the periodic walk. It is not on the per-second path,
// but it runs on the same goroutine, so a pathological cost there stalls
// sampling.
func BenchmarkDiscover(b *testing.B) {
	s, clock, done := benchSampler(b, 100)
	defer done()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		s.discover(ctx, *clock)
	}
}
