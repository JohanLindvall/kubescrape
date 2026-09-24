package cgroupstats

// The metric names, units and descriptions, and the gauge tables build renders
// from them.

// Prometheus-style names, deliberately, so these land beside the cadvisor
// series they explain (see the package doc). Two details are load-bearing:
//
//   - the CPU names drop cadvisor's `_seconds` suffix. container_cpu_usage_
//     seconds_total is a cumulative count of CPU-seconds; these are a RATE in
//     cores, which is what `rate(container_cpu_usage_seconds_total[5m])`
//     produces and what every dashboard actually plots. Carrying `_seconds` on
//     a value measured in cores would be a lie in the name.
//
//   - the UNIT field is set on the CPU gauges and left EMPTY on the memory
//     ones, which looks inconsistent and is not. The OTLP→Prometheus
//     translation appends the unit as a name suffix, skipping it when the unit
//     is brace-annotated (`{cpu}` is never appended, so those names are safe)
//     or when the name is judged to carry it already. The memory names carry
//     `_bytes` in the MIDDLE (container_memory_working_set_bytes_max), and
//     whether the translator's "already present" test is a substring or a
//     suffix test has varied; a translator that suffixes would render
//     container_memory_working_set_bytes_max_bytes, which joins nothing. The
//     name is the contract here, so the memory gauges state their unit in the
//     DESCRIPTION and leave the field that can rewrite the name alone.
//
// The MEAN and the SAMPLE COUNT are the two that were argued about, so the
// argument is here rather than in a commit message.
//
// The original set was six, on the reasoning that cadvisor supplies the average
// and the join makes it available beside these. That is true of CPU and FALSE
// of memory, and it is conditional on cadvisor:
//
//   - cadvisor's container_cpu_usage_seconds_total is a COUNTER, so
//     rate(...[window]) IS the window's mean CPU rate, exactly. Nothing here
//     improves on it — while -cadvisor is on and the kubelet scrape is working.
//   - cadvisor's container_memory_working_set_bytes is a GAUGE sampled once per
//     scrape. avg_over_time of it across one window is that single instant, not
//     the window's mean. So the average working set — the number that says
//     whether the peak this pipeline exports was a spike or the norm — was
//     available NOWHERE, under any configuration.
//   - `-cadvisor=false` with `-cgroup-stats` on is a supported and sensible
//     deployment (this is then the node's only container CPU/memory signal),
//     and a kubelet scrape can simply fail. Either leaves stddev/max/min with
//     no centre to be read against, and a standard deviation without a mean is
//     not interpretable at all.
//
// The COUNT answers the other half: it separates a 30-sample window from a
// 3-sample one, and — because a held window reports its OWN count rather than
// the re-stated one — it is what tells a fresh measurement from the
// re-statement finish() emits for a sparse window. That gap was previously
// only visible fleet-wide, on obs.CgroupHeldWindows, which cannot answer "is
// THIS container's flat max real".
//
// The price is ten gauges per container per export where there were six. It is
// paid rather than avoided because the alternative reading of the same trade —
// documenting the cadvisor coupling as a hard requirement — would make a
// legal, useful configuration silently produce numbers nobody can read, and
// because the comparison that justifies this pipeline is against shipping
// 30-60 raw samples per container per window, which ten gauges is still an
// order of magnitude below.
const (
	nameCPUStddev  = "container_cpu_usage_stddev"
	nameCPUMax     = "container_cpu_usage_max"
	nameCPUMin     = "container_cpu_usage_min"
	nameCPUMean    = "container_cpu_usage_mean"
	nameCPUSamples = "container_cpu_usage_samples"

	nameMemStddev  = "container_memory_working_set_bytes_stddev"
	nameMemMax     = "container_memory_working_set_bytes_max"
	nameMemMin     = "container_memory_working_set_bytes_min"
	nameMemMean    = "container_memory_working_set_bytes_mean"
	nameMemSamples = "container_memory_working_set_bytes_samples"
)

const (
	// unitCores is brace-annotated on purpose; see the block comment above.
	unitCores = "{cpu}"
	unitBytes = ""
	// unitSamples is brace-annotated for the same reason: a bare "samples"
	// would be appended to the name by the OTLP→Prometheus translation, and
	// container_cpu_usage_samples_samples joins nothing.
	unitSamples = "{sample}"
)

// The descriptions are SHORT, and that is a cost decision made against a
// measurement (TestDescriptionsAreAffordablePerContainer).
//
// A description rides on every metric of every ResourceMetrics, and this
// pipeline emits one ResourceMetrics PER CONTAINER — so unlike a payload with a
// handful of resources, the descriptor text is repeated once per container per
// metric per export. The first cut of these carried a ~480-byte explanatory
// note apiece; measured on a 110-container node that was 81% of the whole
// payload (317 KiB of 390 KiB), i.e. the pipeline spent four bytes explaining
// itself for every byte of data, every scrape interval, forever. The same
// arithmetic is why promscrape's cadvisor and split batchers charge descriptor
// bytes to their chunk estimate (metricMeta.apply / metaFieldBytes).
//
// Shortening rather than chunking, deliberately. Chunking is what bounds a
// payload against a receiver's message limit, and that bound already exists one
// layer down and applies to everything this agent sends: otlpexport measures
// the exact proto size and splits over -otlp-max-send-bytes via pkg/otlpsplit.
// Adding a second chunker here would re-implement it and still ship every one
// of those bytes. Only shortening actually removes them — from the wire and
// from the collector's parse, which is where the repetition is charged.
//
// NOT from the backend's storage, which this comment used to claim: Prometheus
// and Mimir keep HELP per metric FAMILY, not per series, so the ten descriptions
// are stored once however many containers repeat them on the wire. The
// per-container cost is real and it is transmission and parsing; the storage
// claim was not, and an argument that overstates itself is one nobody can
// re-derive.
//
// What is lost is prose that was never useful AT THE POINT OF USE: an operator
// hovering a series in Grafana needs to know what the number is and over what
// window, not the argument for why the pipeline exists. That argument is in the
// package doc, in docs/METRICS.md and in the -cgroup-stats flag help, none of
// which is charged per container per export.
const (
	descCPUStddev = "Standard deviation of the container's CPU rate, in cores, per export window."
	descCPUMax    = "Peak CPU rate for the container, in cores, per export window."
	descCPUMin    = "Lowest CPU rate for the container, in cores, per export window."
	descCPUMean   = "Mean CPU rate for the container, in cores, per export window."
	// The two sample-count descriptions carry the one thing that cannot be
	// derived from the name — below 2 the four statistics beside them are the
	// previous window's — and nothing else, for the reason the others are short.
	descCPUSamples = "CPU rate readings this window; below 2 the four beside it are the previous window's."

	descMemStddev  = "Standard deviation of the working set (memory.current-inactive_file), in bytes, per export window."
	descMemMax     = "Peak working set (memory.current-inactive_file), in bytes, per export window."
	descMemMin     = "Lowest working set (memory.current-inactive_file), in bytes, per export window."
	descMemMean    = "Mean working set (memory.current-inactive_file), in bytes, per export window."
	descMemSamples = "Working-set readings this window; below 2 the four beside it are the previous window's."
)

// gaugesPerSignal is how many gauges one signal contributes to an export: the
// four statistics and the sample count, in signalOut.values' order.
const gaugesPerSignal = 5

// gaugeSpec is one exported gauge's descriptor.
type gaugeSpec struct{ name, desc, unit string }

// cpuGauges and memGauges ARE the emitted set: one row per gauge, in
// signalOut.values' order, and build renders exactly these rows (putSignal).
// A gauge is therefore added to the wire by adding a row here and nowhere else,
// and TestMetricNamesAreExactlyTheDocumentedTen reads the set back off build's
// output.
var (
	cpuGauges = [gaugesPerSignal]gaugeSpec{
		{nameCPUStddev, descCPUStddev, unitCores},
		{nameCPUMax, descCPUMax, unitCores},
		{nameCPUMin, descCPUMin, unitCores},
		{nameCPUMean, descCPUMean, unitCores},
		{nameCPUSamples, descCPUSamples, unitSamples},
	}
	memGauges = [gaugesPerSignal]gaugeSpec{
		{nameMemStddev, descMemStddev, unitBytes},
		{nameMemMax, descMemMax, unitBytes},
		{nameMemMin, descMemMin, unitBytes},
		{nameMemMean, descMemMean, unitBytes},
		{nameMemSamples, descMemSamples, unitSamples},
	}
)
