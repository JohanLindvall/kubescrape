package tailbuffer

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The rest of this package's tests and benchmarks configure deliberately extreme
// bounds (MaxSpans: 1<<22 and the like) to exercise the FIFO rather than the
// memory budget, and they must not depend on how much RAM the machine running
// them has. Unknown-limit is the neutral setting: memory.go then neither derives
// nor refuses, it only reports. Every test that is ABOUT the budget supplies its
// own limit.
func TestMain(m *testing.M) {
	memLimit = func() (int64, string) { return 0, "unknown (test)" }
	os.Exit(m.Run())
}

// testLog collects warnings so a test can assert that a sizing problem was said
// out loud rather than merely handled.
func testLog(t *testing.T) (*slog.Logger, func() string) {
	t.Helper()
	var sb strings.Builder
	h := slog.NewTextHandler(&sb, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), sb.String
}

const gib = int64(1) << 30

// A DEFAULT ceiling that the container cannot afford is lowered rather than left
// to be discovered by the OOM killer.
func TestDefaultMaxSpansIsLoweredToFitTheMemoryLimit(t *testing.T) {
	log, out := testLog(t)
	s := settings{maxSpans: defaultMaxSpans, maxSpansPerTrace: 1000}
	// 256 MiB: a quarter is 64 MiB, which affords 65536 spans.
	if err := applyMemoryBudget(&s, false, 256<<20, "test", log); err != nil {
		t.Fatal(err)
	}
	if s.maxSpans != 65536 {
		t.Fatalf("maxSpans %d, want the affordable 65536 (the default %d does not fit a 256 MiB limit)", s.maxSpans, defaultMaxSpans)
	}
	if !strings.Contains(out(), "lowering the tail-sampling maxSpans default") {
		t.Fatalf("the derivation was silent; log was:\n%s", out())
	}
}

// The derived ceiling never falls below maxSpansPerTrace: New refuses that
// combination, so a derivation producing it would turn a memory limit into a
// startup failure.
func TestDerivedMaxSpansStaysAboveMaxSpansPerTrace(t *testing.T) {
	log, _ := testLog(t)
	s := settings{maxSpans: defaultMaxSpans, maxSpansPerTrace: 5000}
	if err := applyMemoryBudget(&s, false, 16<<20, "test", log); err != nil { // affords 4096
		t.Fatal(err)
	}
	if s.maxSpans < s.maxSpansPerTrace {
		t.Fatalf("derived maxSpans %d below maxSpansPerTrace %d: New would refuse its own derivation", s.maxSpans, s.maxSpansPerTrace)
	}
}

// A limit that comfortably affords the default changes nothing.
func TestDefaultMaxSpansIsKeptWhenItFits(t *testing.T) {
	log, _ := testLog(t)
	s := settings{maxSpans: defaultMaxSpans, maxSpansPerTrace: 1000}
	if err := applyMemoryBudget(&s, false, 4*gib, "test", log); err != nil {
		t.Fatal(err)
	}
	if s.maxSpans != defaultMaxSpans {
		t.Fatalf("maxSpans %d, want the default %d untouched at a 4 GiB limit", s.maxSpans, defaultMaxSpans)
	}
}

// An EXPLICIT ceiling above the budget share is honoured — the operator asked —
// but it is said out loud.
func TestExplicitMaxSpansAboveTheBudgetWarns(t *testing.T) {
	log, out := testLog(t)
	s := settings{maxSpans: 500_000, maxSpansPerTrace: 1000} // ~500 MiB
	if err := applyMemoryBudget(&s, true, gib, "test", log); err != nil {
		t.Fatal(err)
	}
	if s.maxSpans != 500_000 {
		t.Fatalf("an explicit maxSpans was changed to %d", s.maxSpans)
	}
	if !strings.Contains(out(), "above this workload's memory budget") {
		t.Fatalf("an over-budget explicit ceiling was accepted silently; log was:\n%s", out())
	}
}

// An explicit ceiling whose spans alone need the WHOLE limit is refused: that
// config cannot reach its own bound without being OOM-killed first, and the OOM
// loses every buffered span.
func TestExplicitMaxSpansThatCannotFitIsRefused(t *testing.T) {
	log, _ := testLog(t)
	s := settings{maxSpans: 2_000_000, maxSpansPerTrace: 1000} // ~2 GiB
	err := applyMemoryBudget(&s, true, gib, "cgroup v2 memory.max", log)
	if err == nil {
		t.Fatal("a maxSpans needing more than the entire memory limit was accepted")
	}
	for _, want := range []string{"2000000", "memory limit", "add shards"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal must name the arithmetic and a remedy; %q missing from: %v", want, err)
		}
	}
}

// With no readable limit nothing is derived or refused — a sizing rule whose
// inputs cannot be read must not become a startup failure — but the estimate is
// still reported so the number is in the log.
func TestUnknownLimitOnlyReports(t *testing.T) {
	log, out := testLog(t)
	s := settings{maxSpans: 10_000_000, maxSpansPerTrace: 1000}
	if err := applyMemoryBudget(&s, true, 0, "", log); err != nil {
		t.Fatal(err)
	}
	if s.maxSpans != 10_000_000 {
		t.Fatal("an unknown limit changed the ceiling")
	}
	if !strings.Contains(out(), "without a known memory ceiling") {
		t.Fatalf("the sizing was not reported; log was:\n%s", out())
	}
}

// New applies the budget: a config that could only OOM fails to start, where an
// operator sees it, rather than at 3am.
func TestNewRefusesAConfigThatImpliesAnOOM(t *testing.T) {
	old := memLimit
	memLimit = func() (int64, string) { return 512 << 20, "test limit" }
	defer func() { memLimit = old }()

	_, err := New(Config{Config: alwaysCfg(), MaxSpans: 4_000_000}, discard{}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err == nil {
		t.Fatal("New accepted a maxSpans eight times the workload's memory limit")
	}
	if !strings.Contains(err.Error(), "OOM-killed") {
		t.Fatalf("the error must say what happens: %v", err)
	}
}

// New lowers an unaffordable DEFAULT instead of refusing: the operator did not
// ask for that number, so the tier starts with a ceiling that fits.
func TestNewLowersTheDefaultCeiling(t *testing.T) {
	old := memLimit
	memLimit = func() (int64, string) { return 256 << 20, "test limit" }
	defer func() { memLimit = old }()

	b, err := New(Config{Config: alwaysCfg()}, discard{}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if b.set.maxSpans >= defaultMaxSpans {
		t.Fatalf("maxSpans %d was not lowered from the default %d under a 256 MiB limit", b.set.maxSpans, defaultMaxSpans)
	}
}

// The cgroup readers, against files rather than the machine's own.
func TestReadMemFile(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if v, ok := readMemFile(write("limit", "1073741824\n")); !ok || v != gib {
		t.Fatalf("readMemFile: got %d %v, want 1 GiB", v, ok)
	}
	if _, ok := readMemFile(write("max", "max\n")); ok {
		t.Fatal("cgroup v2 \"max\" must read as no limit")
	}
	if _, ok := readMemFile(write("v1max", "9223372036854771712\n")); ok {
		t.Fatal("cgroup v1's unlimited sentinel must read as no limit")
	}
	if _, ok := readMemFile(filepath.Join(dir, "absent")); ok {
		t.Fatal("a missing file must read as no limit")
	}

	meminfo := write("meminfo", "MemFree:  123 kB\nMemTotal:       2097152 kB\nBuffers: 1 kB\n")
	if v, ok := readMemTotal(meminfo); !ok || v != 2*gib {
		t.Fatalf("readMemTotal: got %d %v, want 2 GiB", v, ok)
	}
}
