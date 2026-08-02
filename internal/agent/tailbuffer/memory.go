package tailbuffer

// Sizing maxSpans against memory that actually exists.
//
// The loss mode of this layer is SELF-CORRELATED, and that is what this file is
// about. Buffered spans are acked but not durable, so the thing that loses them
// is a hard kill; the likeliest hard kill of a workload whose job is holding
// hundreds of thousands of spans in memory is the OOM killer; and the buffer is
// what feeds it. An operator raising maxSpans to avoid early decisions is
// therefore raising the odds of the event that loses EVERYTHING buffered, which
// is the opposite of what they were trying to do. Early decisions are the relief
// valve — a trace judged a second early degrades gracefully (tailsample treats a
// partial trace as a lower bound) — and the OOM is the failure.
//
// So the ceiling is measured against something real rather than left to a number
// in a values file:
//
//	maxSpans x 1 KiB must fit in a QUARTER of this container's memory limit.
//
// At a 1 GiB limit that is 262144 spans, which is roughly where the built-in
// default of 200000 was hand-placed. The quarter is not timidity: the same
// process runs the service-graph pairing store, the span-metrics series map, the
// scrape/export buffers and Go's own heap slack, and the estimate below is a
// point estimate of a number that varies by a factor of three with span shape.
//
// The per-span figure comes from bench_test.go: ~365 B for a minimal
// two-attribute span, ~1 KiB for a realistic one, plus ~470 B per (pushed
// payload, trace) group for the ResourceSpans wrapper. 1 KiB is the realistic
// one, and a fleet whose spans are fatter than that should lower maxSpans rather
// than discover the difference at 3am.
//
// The direction of the adjustment is deliberate. A default that cannot fit is
// lowered (and said so); an EXPLICIT setting is never raised or silently
// lowered, because the operator asked — it warns above the budget share and is
// REFUSED only when the spans alone would need the whole limit, which is not a
// judgement call but an OOM by construction.

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// spanCostBytes is the memory one buffered span is budgeted at. See the file
// comment: a measured mid-point, not a bound.
const spanCostBytes = 1024

// bufferBudgetShare is the fraction of the workload's memory limit the span
// buffer may plan to occupy.
const bufferBudgetShare = 0.25

// memLimit reports the workload's memory ceiling and where it was read. A
// package var so tests can supply one instead of the machine's.
var memLimit = detectMemoryLimit

// detectMemoryLimit reads the container's memory limit, falling back to the
// machine's RAM when the workload is not capped. Both are "something real"; the
// source is named in every message, because a warning sized against 64 GiB of
// host RAM means something different from one sized against a 1 GiB limit.
//
// Returns 0 when neither can be read, in which case nothing is derived or
// refused and only the estimate is logged — a sizing rule an operator cannot
// see the inputs of must not become a startup failure.
func detectMemoryLimit() (int64, string) {
	// cgroup v2: the container's own limit, "max" when uncapped.
	if v, ok := readMemFile("/sys/fs/cgroup/memory.max"); ok {
		return v, "cgroup v2 memory.max"
	}
	// cgroup v1.
	if v, ok := readMemFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); ok {
		return v, "cgroup v1 memory.limit_in_bytes"
	}
	if v, ok := readMemTotal("/proc/meminfo"); ok {
		return v, "host MemTotal (this workload has no memory limit)"
	}
	return 0, ""
}

// readMemFile reads one cgroup limit file. "max" and the sentinel values the
// kernel uses for "unlimited" report not-ok.
func readMemFile(path string) (int64, bool) {
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
		// cgroup v1 spells "unlimited" as a number near the int64 maximum
		// (PAGE_COUNTER_MAX scaled), which is not a limit anyone can plan
		// against.
		return 0, false
	}
	return v, true
}

// readMemTotal reads MemTotal (in kB) out of /proc/meminfo.
func readMemTotal(path string) (int64, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		rest, ok := strings.CutPrefix(line, "MemTotal:")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			return 0, false
		}
		kb, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil || kb <= 0 {
			return 0, false
		}
		return kb * 1024, true
	}
	return 0, false
}

// applyMemoryBudget checks (and where the ceiling is a DEFAULT, lowers) the span
// ceiling against limit, and reports the sizing either way. explicit says the
// operator set maxSpans themselves.
//
// It is a pure function of its arguments — the filesystem read is memLimit's —
// so the arithmetic is testable without a cgroup.
func applyMemoryBudget(s *settings, explicit bool, limit int64, source string, log *slog.Logger) error {
	est := int64(s.maxSpans) * spanCostBytes
	if limit <= 0 {
		log.Info("tail-sampling buffer sized without a known memory ceiling",
			"maxSpans", s.maxSpans, "estimatedBytes", est, "bytesPerSpan", spanCostBytes,
			"note", "could not read a cgroup memory limit or MemTotal; keep maxSpans x 1 KiB under a quarter of this pod's memory limit")
		return nil
	}
	affordable := int64(float64(limit)*bufferBudgetShare) / spanCostBytes
	switch {
	case !explicit && affordable < int64(s.maxSpans):
		// A default that does not fit. Lower it: an early decision is a graceful
		// degradation, an OOM loses every buffered span at once.
		n := int(affordable)
		if n < s.maxSpansPerTrace {
			// The bound Validate/New enforce must survive the derivation, or a
			// single trace could never be buffered whole.
			n = s.maxSpansPerTrace
		}
		log.Warn("lowering the tail-sampling maxSpans default to fit this workload's memory limit",
			"maxSpans", n, "default", s.maxSpans, "limitBytes", limit, "limitSource", source,
			"bytesPerSpan", spanCostBytes, "budgetShare", bufferBudgetShare,
			"note", "set tailSampling.maxSpans explicitly to override, raise the memory limit, or add shards (the ring divides the span rate)")
		s.maxSpans = n
	case est >= limit:
		// Not a judgement call: the spans alone need the whole limit, so this
		// config reaches its own ceiling only by being OOM-killed first — and an
		// OOM loses every buffered span, which is the failure this layer's
		// bounds exist to trade AGAINST.
		return fmt.Errorf("tailSampling.maxSpans %d needs about %d bytes of spans (at ~%d B each), which is at or above this workload's memory limit of %d bytes (%s): it would be OOM-killed before the bound ever bound, losing every buffered span. Lower maxSpans to at most %d, raise the memory limit, or add shards",
			s.maxSpans, est, spanCostBytes, limit, source, affordable)
	case int64(s.maxSpans) > affordable:
		log.Warn("tailSampling.maxSpans is above this workload's memory budget; a hard kill here is an OOM and loses every buffered span",
			"maxSpans", s.maxSpans, "estimatedBytes", est, "affordableMaxSpans", affordable,
			"limitBytes", limit, "limitSource", source, "bytesPerSpan", spanCostBytes, "budgetShare", bufferBudgetShare)
	default:
		log.Info("tail-sampling buffer sized",
			"maxSpans", s.maxSpans, "estimatedBytes", est, "affordableMaxSpans", affordable,
			"limitBytes", limit, "limitSource", source, "bytesPerSpan", spanCostBytes)
	}
	return nil
}
