package obs

// Go runtime and process self-metrics, pushed over OTLP with everything else.
//
// These were previously a Prometheus text endpoint (GET /metrics) backed by
// client_golang's Go and process collectors. That was the only Prometheus
// exposition left in a project whose stated rule is "OTLP-only — never
// reintroduce Prometheus exposition"; nothing scraped it (no manifest carries a
// prometheus.io annotation for it), and it pulled client_golang and its
// transitive dependencies into a binary that advertises a small footprint. The
// same numbers now flow through Registry like every other metric.
//
// Values come from runtime/metrics (cheap, no stop-the-world) and /proc/self
// (Linux is the only supported target). Everything is registered as a
// Gauge/CounterFunc evaluated AT EXPORT time, so the cost is one read per
// export interval, not per scrape of an endpoint nobody scraped.

import (
	"math"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The runtime/metrics sample set is read as a whole (reading one series costs
// the same as reading all) and shared, so exports that overlap serialise here.
var (
	rtMu      sync.Mutex
	rtSamples []metrics.Sample
	rtIndex   map[string]int
)

// rtNames are the runtime/metrics series consumed below. A name a future Go
// release drops yields a KindBad sample, which reads as 0 rather than panicking.
var rtNames = []string{
	"/memory/classes/heap/objects:bytes",
	"/memory/classes/total:bytes",
	"/memory/classes/heap/released:bytes",
	"/gc/heap/objects:objects",
	"/gc/heap/goal:bytes",
	"/gc/cycles/total:gc-cycles",
	"/gc/pauses:seconds",
	"/sched/goroutines:goroutines",
	"/cpu/classes/gc/total:cpu-seconds",
}

func init() {
	rtIndex = make(map[string]int, len(rtNames))
	rtSamples = make([]metrics.Sample, 0, len(rtNames))
	for _, n := range rtNames {
		rtIndex[n] = len(rtSamples)
		rtSamples = append(rtSamples, metrics.Sample{Name: n})
	}
}

// readRuntime refreshes the shared sample set and returns one series as a float.
func readRuntime(name string) float64 {
	rtMu.Lock()
	defer rtMu.Unlock()
	metrics.Read(rtSamples)
	i, ok := rtIndex[name]
	if !ok {
		return 0
	}
	switch v := rtSamples[i].Value; v.Kind() {
	case metrics.KindUint64:
		return float64(v.Uint64())
	case metrics.KindFloat64:
		return v.Float64()
	case metrics.KindFloat64Histogram:
		return histogramTotal(v.Float64Histogram())
	default:
		return 0
	}
}

// histogramTotal approximates the total observed value of a runtime histogram
// as sum(count x bucket midpoint) — used for cumulative GC pause time, which
// runtime/metrics exposes only as a distribution.
func histogramTotal(h *metrics.Float64Histogram) float64 {
	total := 0.0
	for i, c := range h.Counts {
		if c == 0 {
			continue
		}
		lo, hi := h.Buckets[i], h.Buckets[i+1]
		var mid float64
		switch {
		case math.IsInf(lo, 0) && math.IsInf(hi, 0):
			mid = 0
		case math.IsInf(hi, 0):
			mid = lo
		case math.IsInf(lo, 0):
			mid = hi
		default:
			mid = lo + (hi-lo)/2
		}
		total += float64(c) * mid
	}
	return total
}

// clockTicks is the kernel's USER_HZ, the unit of /proc/self/stat's CPU fields.
// It is 100 on every Linux platform Go supports.
const clockTicks = 100

// procStat returns a field of /proc/self/stat by its proc(5) 1-based number.
// Anything unreadable yields 0, so a restricted environment degrades to zeroes
// rather than failing an export.
func procStat(field int) float64 {
	b, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0
	}
	s := string(b)
	// comm (field 2) is parenthesised and may itself contain spaces, so fields
	// are counted from after its closing paren; field 3 is the first there.
	i := strings.LastIndexByte(s, ')')
	if i < 0 || i+2 >= len(s) {
		return 0
	}
	fs := strings.Fields(s[i+2:])
	if field < 3 || field-3 >= len(fs) {
		return 0
	}
	v, err := strconv.ParseFloat(fs[field-3], 64)
	if err != nil {
		return 0
	}
	return v
}

// procRSS returns the resident set size in bytes.
func procRSS() float64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fs := strings.Fields(string(b))
	if len(fs) < 2 {
		return 0
	}
	pages, err := strconv.ParseFloat(fs[1], 64)
	if err != nil {
		return 0
	}
	return pages * float64(os.Getpagesize())
}

// procFDs counts open file descriptors. The tailer holds rotated-away and
// deleted files open until their offsets commit, so this tracks a budget that
// can genuinely run out.
func procFDs() float64 {
	d, err := os.Open("/proc/self/fd")
	if err != nil {
		return 0
	}
	defer func() { _ = d.Close() }()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return 0
	}
	return float64(len(names))
}

var startTime = time.Now()

// RegisterRuntimeMetrics registers the Go runtime and process series on the
// Registry so they export over OTLP with everything else. Names follow the
// OpenTelemetry process semantic conventions rather than the Prometheus
// go_*/process_* spelling the removed endpoint used, since OTLP is now the only
// egress (a Prometheus backend renders the dots as underscores anyway).
//
// Registry does not deduplicate by name, so this guards against a second call
// registering every series twice.
func RegisterRuntimeMetrics() {
	runtimeOnce.Do(registerRuntimeMetrics)
}

var runtimeOnce sync.Once

func registerRuntimeMetrics() {
	Registry.GaugeFunc("process.runtime.go.goroutines",
		"Goroutines currently existing.",
		func() float64 { return readRuntime("/sched/goroutines:goroutines") })
	Registry.GaugeFunc("process.runtime.go.threads",
		"OS threads created by the Go runtime.",
		func() float64 { n, _ := runtime.ThreadCreateProfile(nil); return float64(n) })
	Registry.GaugeFunc("process.runtime.go.mem.heap_alloc",
		"Bytes of allocated heap objects.",
		func() float64 { return readRuntime("/memory/classes/heap/objects:bytes") })
	Registry.GaugeFunc("process.runtime.go.mem.heap_objects",
		"Allocated heap objects.",
		func() float64 { return readRuntime("/gc/heap/objects:objects") })
	Registry.GaugeFunc("process.runtime.go.mem.heap_goal",
		"Heap size target for the end of the current GC cycle, in bytes.",
		func() float64 { return readRuntime("/gc/heap/goal:bytes") })
	Registry.GaugeFunc("process.runtime.go.mem.total",
		"Bytes of memory mapped by the Go runtime for all purposes.",
		func() float64 { return readRuntime("/memory/classes/total:bytes") })
	Registry.GaugeFunc("process.runtime.go.mem.released",
		"Bytes of heap memory returned to the OS.",
		func() float64 { return readRuntime("/memory/classes/heap/released:bytes") })
	Registry.CounterFunc("process.runtime.go.gc.count",
		"Completed GC cycles since start.",
		func() float64 { return readRuntime("/gc/cycles/total:gc-cycles") })
	Registry.CounterFunc("process.runtime.go.gc.pause_time",
		"Cumulative stop-the-world pause seconds (approximated from the runtime's pause histogram).",
		func() float64 { return readRuntime("/gc/pauses:seconds") })
	Registry.CounterFunc("process.runtime.go.gc.cpu_time",
		"Cumulative CPU seconds spent in garbage collection.",
		func() float64 { return readRuntime("/cpu/classes/gc/total:cpu-seconds") })

	Registry.CounterFunc("process.cpu.time",
		"Cumulative CPU seconds (user + system) consumed by this process.",
		func() float64 { return (procStat(14) + procStat(15)) / clockTicks })
	Registry.GaugeFunc("process.memory.rss",
		"Resident set size in bytes.", procRSS)
	Registry.GaugeFunc("process.memory.virtual",
		"Virtual memory size in bytes.",
		func() float64 { return procStat(23) })
	Registry.GaugeFunc("process.open_file_descriptors",
		"Open file descriptors. The tailer holds rotated-away files open until their offsets commit, so this is a budget that can run out.",
		procFDs)
	Registry.GaugeFunc("process.uptime",
		"Seconds since process start.",
		func() float64 { return time.Since(startTime).Seconds() })
}
