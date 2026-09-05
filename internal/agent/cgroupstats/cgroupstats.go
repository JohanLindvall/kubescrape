// Package cgroupstats samples container cgroups directly, at a far higher
// frequency than the cadvisor scrape, and exports the DISTRIBUTION of what it
// saw instead of the raw series.
//
// # Why this exists at all
//
// The kubelet's cadvisor endpoint is scraped on -scrape-interval (15-60s), and
// a cumulative counter read twice 60s apart yields exactly one number: the
// average over that minute. A container that pins 4 cores for 2 seconds inside
// a 60s window is reported as ~0.13 cores — the burst that got it OOM-killed,
// throttled, or blamed for a latency spike is arithmetically erased before it
// ever leaves the node. Shipping 1s-resolution raw series instead would restore
// the signal at 30-60x the egress and 30-60x the backend's ingest cost, for
// data that is almost entirely uninteresting.
//
// So the node samples at 1s and ships ten numbers per container per window:
// the standard deviation, maximum, minimum, mean and SAMPLE COUNT of the CPU
// rate and of the memory working set. The burst survives in `_max`, its shape
// in `_stddev`, the centre it is read against in `_mean`, and `_samples` says
// how much of a distribution the window actually measured (below two, the four
// beside it are the previous window's — see finish). The volume on the wire is
// ten gauges per container per scrape interval — the same order as the cadvisor
// series they sit beside, and see the metric-name block for why the set grew
// from six.
//
// # The decision this argues with
//
// internal/metrics/series.go deliberately CLOSES the log-metrics aggregation
// set against exactly these statistics: "anything derivable from these
// (stddev, range, delta, first, ...) belongs in backend recording rules, which
// re-aggregate freely — not as more per-sample state here". That rule is right
// there and it is right here too, with one difference that changes the answer:
//
//	a recording rule can only re-aggregate samples that REACHED the backend,
//	and the samples this package aggregates never leave the node.
//
// stddev_over_time on a 15-60s series is the standard deviation of the
// averages, which is not the quantity anyone wants and cannot be turned into
// it. The high-frequency data existing only inside this process for one window
// is not an implementation detail of the feature — it IS the feature. See
// welford.go for the numerical half of the same argument.
//
// # Identity, and what is NOT exported
//
// A container's resource is REBUILT on every export, through the same code that
// builds a cadvisor row's resource (Resolver, implemented by
// *promscrape.Scraper). That is not an oversight to be optimised away: these
// ten gauges exist to be joined to the cadvisor series they explain, and a
// cached resource diverges from the cadvisor one the moment a pod or namespace
// label is edited in place, or a `.Node` attribute template resolves against
// node metadata that landed after this container was first seen. Two attribute
// sets under one (job, instance) do not join, which defeats the feature exactly
// as thoroughly as a flapping identity would. Rebuilding per export means a
// label edit propagates here on the same schedule it propagates to cadvisor,
// because it is the same lookup through the same one-minute cache (see
// resolveExport for what the rebuild actually costs).
//
// What IS kept from the caching experiment is the property that mattered: a
// container the metadata service cannot place is not exported AT ALL. A series
// with no service.name has no Prometheus `job` and joins nothing, and one that
// appears and disappears with a `job` attached is the flap. Resolution FAILING
// is the flap risk; a label edit is not. That rule is also what keeps every
// pod's SANDBOX (pause) cgroup out — its id appears in no pod's
// containerStatuses, so it never resolves — and it is applied twice, at two
// different seams for two different reasons: at DISCOVERY it decides whether to
// spend three file descriptors on a cgroup, and at EXPORT it decides whether
// the window has a resource to ride on.
//
// # Naming
//
// The ten metric names are Prometheus-style, NOT this repo's OTel-dotted
// convention: they are meant to be read, joined and graphed directly beside
// cadvisor's container_cpu_usage_seconds_total and
// container_memory_working_set_bytes, and a name in a different naming system
// does not sit beside anything. It is the same deliberate divergence
// agent/servicegraph makes for its Tempo-verbatim edge metrics, for the same
// reason: the consumer's vocabulary wins over ours where the whole point is
// that the consumer can put the two side by side. See metricNames below for
// the exact spellings and why the CPU ones drop cadvisor's _seconds suffix.
//
// # Cost
//
// Three reads per container per second (cpu.stat, memory.current,
// memory.stat). On a 200-container node that is 600 reads/s on the one process
// that also tails every log file on the node, so the read path is allocation-
// free and holds its file descriptors open (read.go): the sample path is three
// pread(2) calls into a reused buffer and two integer parses, with no path
// building, no os.File, no fmt and no garbage. The descriptors are the cost —
// three per container, bounded by maxContainers.
//
// # MEASURED
//
// 200 REAL cgroup v2 scopes in the kubepods layout (a delegated subtree, not a
// tmpfs fixture: a kernfs seq_file read IS the cost here, and a fabricated tree
// understates it), 1s sampling / 15s discovery / 30s export, every container
// resolved and sampled. The A/B is the same binary running the same window
// with the sampler NOT started, over the same hierarchy — eight pairs, six of
// 120s and two of 300s, ONE ARM PER PROCESS (RSS does not come back down, so
// two arms in one process measure the first one's high-water mark twice), CPU
// from getrusage(RUSAGE_SELF) and memory from /proc/self/status VmRSS:
//
//	CPU  0.48% of one core   (0.43-0.51% over the eight pairs; the sampler-off
//	                          arm reads 0.001-0.005%)
//	RSS  +5.5 MiB            (+4.7 to +6.3 over the six 120s pairs; the same
//	                          pairs read as the growth WITHIN each arm give
//	                          +5.1, +4.3 to +5.9. The two 300s pairs read
//	                          +7.2 — see WHERE THE RSS COMES FROM)
//
// The SPREAD is part of the measurement, so quote the range rather than the
// mean: eight pairs of one harness over one hierarchy resolve a band close to a
// tenth of a percentage point wide, and every figure here moves with whatever
// else the host is running. Re-measure rather than copying these digits onto
// other hardware.
//
// Where the CPU goes, charged INSIDE one process rather than differenced
// between two (the process CPU clock read around each sweep, each discovery
// pass and each export, on the production schedule; three runs of 300s). The
// three loops charge 4.13 ms of CPU per second between them:
//
//	88%  the sweep          — 600 pread(2) calls a second: 3.65 ms a sweep,
//	                          18.2 us per container
//	 7%  the discovery walk — 4.50 ms a pass, one pass per 15s
//	 4%  the export         — 5.37 ms a window (resolve, build, proto, gzip),
//	                          one per 30s
//
// Differencing the three loops out of separate processes cannot resolve the
// second and third at all: at 200 containers a discovery pass costs 0.03% of
// one core and an export 0.02%, both well inside the 0.43-0.51% the total
// itself wanders over. Those 4.13 ms/s are 0.41% of one core against the 0.48%
// the process A/B reads, and the difference is the three tickers, the scheduler
// and the GC of the export's garbage, which no individual call is charged with.
// At the maxContainers cap the same measurement over 512 scopes charges 1.22%
// of one core, the sweep 10.8 ms (21.1 us per container).
//
// WHICH of the sweep's three preads, because it says where the remaining cost
// can and cannot be attacked. Timed separately over a real cgroup v2 hierarchy
// (275 scopes, held descriptors, 50 rounds, three repeats; a loaded host, so
// quote the SHARES rather than the absolutes):
//
//	memory.stat   inactive_file  10.3-13.9 us   47-65% of the sweep
//	cpu.stat      usage_usec      4.4- 6.3 us
//	memory.current                0.6- 1.0 us
//
// The same harness reads the whole sweep at 21.4-22.8 us per container, i.e.
// 0.429-0.456% of one core at 200 containers — an independent confirmation of
// the 0.43-0.51% above, arrived at without the export or the Welford math.
//
// memory.stat is the expensive one by an order of magnitude over
// memory.current, and it is expensive in the KERNEL: reading it flushes the
// per-CPU memory accounting (the rstat tree) before rendering ~40 lines, of
// which this package wants one field. Nothing on the Go side moves that
// number — BenchmarkSample over a tmpfs tree costs 2.5 us per container, so
// ~89% of the real per-container cost is the kernel generating the files and
// ~11% is everything this package does with them (which is already 0-alloc,
// TestSampleAllocationBudget). The only lever that would move it is reading
// memory.stat on a SLOWER cadence than memory.current, and that is refused:
// working set is current MINUS inactive_file, so a stale inactive_file biases
// exactly the peaks this pipeline exists to catch.
//
// SCOPE, because it decides what the numbers mean. This is the pipeline's own
// cost: it includes the pdata build, the exact proto marshal otlpexport
// measures with and the gzip it compresses with, and it EXCLUDES the socket
// write and the metadata service's HTTP round trips. Excluding the latter is
// deliberate rather than convenient — the identity rebuild goes through the
// cadvisor batcher's one-minute metaclient cache, which the -cadvisor pipeline
// already pays for and which serves this one from memory in steady state — but
// an operator running -cgroup-stats with -cadvisor OFF pays those lookups for
// the first time here, and this figure does not contain them.
//
// WHERE THE RSS COMES FROM, spelled out because it is the figure a DaemonSet is
// sized against and because the three easiest ways to measure it each answer a
// DIFFERENT question rather than answering this one badly:
//
//   - what this package RETAINS is +0.3 MiB, about 1.5 KiB per container
//     (HeapAlloc after a forced GC with the sampler still RUNNING and still
//     holding its state: 0.88 MiB against the sampler-off arm's 0.58). Read it
//     after Run has returned instead and stop() has already dropped the
//     tracked set, which reads 0.61 — the wrong quantity a second time. A
//     heap-based A/B reports a twentieth of what an operator pays, because
//     what an operator pays is RSS.
//   - the EXPORT is most of the difference. The same A/B with the export
//     interval pushed out to an hour — sweep and discovery untouched — grows
//     +3.3 MiB over 300s instead of +7.2: the per-window pdata build, proto
//     marshal and gzip are transient garbage that GOGC turns into arena the
//     process keeps. A measurement window holding no export therefore reads
//     under half, which is the likeliest way to arrive at a figure near
//     +3 MiB.
//   - two arms in ONE process read the second arm high by the first arm's
//     high-water mark, since RSS does not come back down. One arm per process.
//   - the WINDOW LENGTH is still part of the answer after all that, so +5.5 MiB
//     is a floor and not a steady state: the same arm run for 300s instead of
//     120 reads +7.2 MiB with its retained heap unchanged at 0.3, i.e. the
//     high-water mark is still creeping as the allocator settles. Size a
//     DaemonSet with room above the figure, not to it.
//
// So: RSS, one arm per process, a window holding several exports. The earliest
// figure of 0.255% and +0.9 MiB was the first hazard twice over — it timed the
// sweep, the walk and the export in isolation, and it read a heap number rather
// than the process's.
//
// Which is why the discovery walk was worth fixing even though it is not the
// CPU. A/B on the same 200-container tree, ten passes each: os.ReadDir
// allocated 21,950 objects and 1.44 MiB PER PASS — 98% of it DirEntry values
// for cgroup control files discarded on the next line, and the pipeline's
// largest allocation source by far, the per-second sweep allocating nothing at
// all. readdir_linux.go filters on the getdents64 entry type instead: 1,642
// objects and 243 KiB per pass, 13x fewer objects.
package cgroupstats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

const scopeName = "github.com/JohanLindvall/kubescrape/agent/cgroupstats"

// DefaultRoot is where a Kubernetes node's unified cgroup v2 hierarchy is
// mounted. It is only the starting point: the LAYOUT beneath it is discovered,
// never assumed (see discover.go — kind nests the whole tree under
// kubelet.slice, a stock systemd node does not, and a cgroupfs-driver node
// spells every segment differently).
const DefaultRoot = "/sys/fs/cgroup"

// DefaultInterval is the sampling period. One second is the resolution at
// which a CPU burst is still a burst rather than a rounding error, and it is
// cheap enough (three preads per container) to run on the shared node agent.
const DefaultInterval = time.Second

// MinInterval is the floor on the sampling period, and it is a floor in the one
// direction that can hurt the node: a sweep costs three pread(2) calls per
// container and runs on the goroutine that also does discovery, so the period
// is what divides that cost. At MinInterval a 200-container node already issues
// 6000 reads a second; ten times faster is not ten times more insight into a
// burst, it is a busy loop on the shared node agent. Values below it are
// refused where an operator typed one (-cgroup-stats-interval, checkFlagValues)
// and clamped where one arrives programmatically (New).
const MinInterval = 100 * time.Millisecond

// DefaultDiscoverInterval is how often the container set is re-read from the
// filesystem. It is also the retry cadence for a container whose identity has
// not resolved yet (discover.go), which is why it is not longer: a container
// that started half a second before a pass is not in the API server yet.
//
// # It is also this pipeline's BLIND SPOT, and the blind spot is not countable
//
// Discovery is the ONLY way into the sampled set, so a container that starts
// and exits between two passes is never sampled — and leaves no trace that it
// existed: its cgroup directory is created and removed inside the interval, and
// nothing in the hierarchy remembers it. There is no counter for it here and
// there cannot be one, because a counter would need evidence this process never
// receives. The table below is the size of it. cadvisor has the same blind spot
// for the same reason (its own housekeeping interval), so the gap is not a
// regression against what it annotates — but the burstiest objects on a node
// (init containers, CronJob pods, a crashlooping container's runs) are exactly
// the ones below it, which is why the interval is an operator's choice
// (-cgroup-stats-discover-interval) rather than a constant.
//
// What IS countable is one CORNER of it, and the corner is narrow enough that
// the size matters more than the existence: a container the sampler had
// descriptors open on, which left the hierarchy before two readings of either
// signal could be taken, is counted obs.CgroupWindowsDropped{reason="too_short"}
// (see Sampler.snapshot for the three guards that keep the count meaning that,
// for the two neighbours that share the branch without being short-lived at
// all, and for what it deliberately excludes). In the measured case below it
// fires for a container whose DISCOVERY landed within TWO SAMPLING PERIODS of
// its death — a container found earlier in its life is sampled for the
// remainder, and any more than two seconds of remainder describes it — so of
// the containers a pass sees at all it catches exactly 2*Interval/lifetime (up
// to a lifetime of one discovery period; past that a second pass covers what
// the last one missed), and nothing whatsoever of the two larger classes: a
// cgroup that came and went between passes, and one that vanished while still
// unresolved (that set is every pod's sandbox, so counting its deletions would
// be one increment per pod terminated).
//
// MEASURED by driving this package's own discover/sample/export entry points
// over a fixed clock at the 15s default, a 1s sampling period and a 30s export
// (the -scrape-interval default): 6000 trials per row, the container's start a
// CONTINUOUS random phase over the grid's 30s period (stratified, one trial per
// 1/6000th of it). Continuous is the correction that matters — a start snapped
// to whole seconds puts the container's birth and its death exactly ON sample
// ticks, and what happens at an exact coincidence is then the harness's choice
// rather than the node's. "described" is at least one exported window,
// "too_short" is the counter firing, "invisible" is a container no pass ever
// saw:
//
//	lifetime    described   too_short   invisible
//	     2s          0.0%      13.3%       86.7%
//	     5s         20.0%      13.3%       66.7%
//	    10s         53.3%      13.3%       33.3%
//	    15s         86.7%      13.3%        0.0%
//	    16s         93.3%       6.7%        0.0%
//	    17s          100%         0%        0.0%
//	    60s          100%         0%        0.0%
//
// Every cell is a single number rather than a range, and the closed form is
// what makes the rows checkable without the harness: for a lifetime L up to the
// discovery period D, with sampling period I, described is (L-2I)/D, too_short
// is 2I/D and invisible is (D-L)/D. A pass lands inside an L-second life in L
// of every D phases, and the container then needs TWO sample ticks strictly
// inside what remains of that life — which the last 2I of it cannot supply.
// Capture reaches 100% at L = D + 2I = 17s. The form is not fitted to the 15s
// row set: re-run at D = 5s it predicts and measures 40%/40%/20% at a 4s
// lifetime, 60%/40%/0% at 5s and 100% from 7s up.
//
// THE DISCOVERY-VS-SAMPLE RACE IS DECIDED, NOT AVERAGED, and it is worth saying
// so because the earlier version of this table measured the other side of it
// and read one whole discovery phase — 6.7 points — better in every row. A pass
// and a sweep whose ticks coincide are not a coin flip: discover() walks the
// hierarchy before reconcile takes the mutex, and a newly found container's
// descriptors are opened later still, in resolvePending, behind a metadata
// lookup — where a sweep's first act at its tick is to take that mutex. So the
// coinciding sweep has already run by the time the container is installed, its
// first reading is one sampling period after the pass, and the table above is
// that arrangement. Assume the reverse and every row reads 6.7%/26.7%/60.0%/
// 93.3% described with too_short flat at 6.7%; a coin flip between the two
// lands halfway (2.9/22.8/56.1/89.5% described, 10.5% too_short). The
// export-vs-sample tick race, which the earlier table carried a one-phase range
// for, moves nothing here: with the first reading a period after the pass, the
// two readings that decide the verdict cannot straddle an export tick
// (measured, 6000 trials with that order randomised: identical to the last
// digit).
//
// Read the 5s row: of the 80% that produced nothing, the counter accounts for
// thirteen points and misses sixty-seven. That is the shape of the bound —
// real evidence, and weak. Its job is to be the thing that MOVES at all on a
// node running short-lived workloads, which is the argument for lowering the
// interval; the height of the bar is not the size of the loss.
const DefaultDiscoverInterval = 15 * time.Second

// MinDiscoverInterval is the floor on the discovery cadence, and like
// MinInterval it exists in the one direction that costs something. A pass is a
// directory walk plus one metadata lookup for every cgroup that has not
// resolved yet — and on a fresh node that is EVERY pod's sandbox cgroup, for
// maxUnresolvedAge, because a sandbox never resolves by construction. At one
// second a 110-pod node would ask the metadata service for 110 lookups a
// second for three minutes; the walk itself is the cheap half. Values below it
// are refused where an operator typed one
// (-cgroup-stats-discover-interval, checkFlagValues) and clamped where one
// arrives programmatically (New), which is MinInterval's split exactly.
const MinDiscoverInterval = time.Second

// maxUnresolvedAge is how long a cgroup the metadata service DEFINITIVELY does
// not know keeps being offered to it at the discovery cadence before it is
// given up on.
//
// It is deliberately several minutes rather than several passes: the metadata
// service's negative cache is a minute deep, so three retries inside one minute
// are three answers from the same cached miss, and a container whose id reached
// the API server late must still be picked up. Past this a cgroup is retried at
// abandonRetryEvery instead — see resolveFailed for why it is a slow retry and
// not a deletion.
//
// It is measured from the first ANSWERED FAILURE — a 404 — and not from
// discovery, and not from any failure. Two corrections live in that sentence:
//
//   - not from discovery, because a pass is budgeted (resolveBudget), so a node
//     whose metadata service has gone slow rolls most of its pending set to the
//     next pass; a cgroup discovered minutes before it was first asked about
//     would otherwise be abandoned on its very first failure.
//   - not from any failure, because an unreachable service produces failures
//     for every cgroup on the node at once, and abandoning them all would drop
//     a whole node's real containers to the ten-minute cadence over an outage
//     that says nothing about any of them. Only the service's own "I do not
//     know this id" counts toward giving up on it.
//
// An outage in the MIDDLE does not reset it: two definitive misses that far
// apart bracket a cgroup that has been unplaceable for at least that long,
// which is exactly what the grace period is asking about.
const maxUnresolvedAge = 3 * time.Minute

// abandonRetryEvery is the slow retry cadence for a cgroup that has been given
// up on. Every pod contributes one such cgroup forever (its sandbox), so this
// is the steady-state cost of the give-up rule: one metadata lookup per pod per
// ten minutes, and no file descriptors.
const abandonRetryEvery = 10 * time.Minute

// reconsiderEvery is the retry cadence an ABANDONED cgroup falls back to while
// the metadata service cannot answer.
//
// Giving up is a guess about the future made from failures, and a guess has to
// be revisable. What makes it revisable is the CLASSIFICATION of the failure
// rather than any tally of successes: a 404 is a statement about this container
// (a sandbox is one forever), while a transport error is a statement about the
// service — so a retry that comes back unreachable proves the give-up may no
// longer be justified, and the entry goes back on the fast clock instead of
// serving out its ten minutes.
//
// One minute is not an arbitrary throttle: it is the depth of the resolver's
// negative cache. A retry inside it is answered from the same cached miss and
// can prove nothing, so it is the shortest cadence that can still learn
// anything — while remaining, at one lookup per abandoned cgroup per minute, an
// outage-only cost that ends when the service answers.
//
// The earlier design triggered this on EVIDENCE instead: any successful lookup
// anywhere re-armed every abandoned cgroup. On a healthy node every export
// resolves every container, so the evidence arm was permanently satisfied and
// abandonRetryEvery never bound at all — a 110-pod node issued ~110 lookups a
// MINUTE for its sandboxes, each landing just outside that same negative cache
// and costing a real round trip. Classifying the failure removes the need for
// it: a sandbox is on the slow path because its failure is definitive, and a
// genuine outage is on the fast path because its failure is not.
const reconsiderEvery = time.Minute

// resolveTimeout bounds ONE identity lookup, so an unreachable metadata service
// cannot hold the discovery goroutine open indefinitely and stop newly started
// containers from ever being picked up.
const resolveTimeout = 5 * time.Second

// resolveBudget bounds a whole discovery pass's worth of lookups (the first is
// always attempted, so progress cannot stall entirely). What does not fit rolls
// to the next pass.
const resolveBudget = 2 * time.Second

// exportResolveBudget bounds the identity rebuild of a whole EXPORT (see
// resolveExport). It is a deadline on the pass rather than a count, because
// what it protects is the meaning of the numbers: the windows have already been
// snapshotted and reset when the rebuild runs, so a rebuild that outlasts the
// export cadence does not delay one payload, it silently stretches the interval
// the NEXT one claims to describe.
//
// Five seconds is roughly twenty times the steady-state cost of a full node.
// Every lookup goes through the resolver's one-minute cache, and the cadvisor
// scrape warms the same entries under the same keys; a miss underneath it is a
// conditional GET the metadata service answers 304, so 512 containers cost a
// few hundred milliseconds of sequential round trips at worst. A pass that
// cannot finish inside this is a metadata service that cannot answer at the
// rate the node needs, which is exactly the state in which exporting nothing —
// loudly, into obs.CgroupWindowsDropped — beats exporting a stretched window.
const exportResolveBudget = 5 * time.Second

// maxContainers bounds the TRACKED set, and it bounds FILE DESCRIPTORS rather
// than memory: three are held open per sampled container so the sample path can
// pread without allocating. The kubelet's default max-pods is 110, so this is
// well past a full node; past it, a newly resolved container is not promoted
// into the sampled set and is counted (obs.CgroupContainersCapped{cap=tracked}).
//
// Only entries that HOLD descriptors are counted against it. It used to be
// tested against tracked+pending, which spent an fd budget on entries that own
// no fd — and since every pod's sandbox cgroup is permanently pending, a large
// node's sandboxes crowded out its real workload containers, which is precisely
// backwards.
const maxContainers = 512

// maxPending bounds the not-yet-attributed set. It is the MEMORY bound, a
// different resource from maxContainers with a different remedy, and it is much
// larger because a pending entry is ~150 bytes and no descriptor at all.
//
// It has to comfortably exceed a full node's container count for two reasons
// that stack: every pod contributes one permanently unresolvable cgroup (its
// sandbox), and a metadata-service outage puts every real container here at
// once — so the worst legitimate occupancy is EVERY cgroup on the node, not
// just the unresolvable ones. At the kubelet's default 110 pods that is a few
// hundred; this is roughly ten times it, for about 600 KiB. It exists for the
// hierarchy that is not a Kubernetes node's at all (a mis-pointed
// -cgroup-stats-root), where the entry count is bounded by nothing else:
// maxScanDirs bounds the DIRECTORIES a pass descends into, and a container
// scope is never descended into, so one pathological slice can yield
// arbitrarily many.
const maxPending = 4096

// maxHeldWindows bounds the sparse-window hold (see finish).
//
// The hold exists to bridge a SAMPLING gap — a container discovered part-way
// through a window, or the second reading a CPU rate needs before it can yield
// its first value — and one window is already an enormously generous bridge for
// that: a window is a whole -scrape-interval, 15 to 60 seconds, against a gap
// measured in single sample periods. Two consecutive holds cover any real
// hiccup twice over.
//
// The bound is not a refinement, it is the difference between bridging a gap
// and impersonating a live container. Unbounded, a dead-but-still-listed
// container republishes its last measurement as fresh data forever — and that
// state is not hypothetical: repointLocked deliberately refuses to follow a
// container down to a supervisor scope late in its life, so on CRI-O a
// container whose own scope is removed while its conmon scope lingers keeps
// being listed and stops being readable, and a stale directory listing does the
// same anywhere. Past the bound the signal stops being exported and the gap is
// the honest report.
const maxHeldWindows = 2

// maxDeadWindows is how many consecutive export windows a tracked container may
// yield NOTHING — every read of all three of its files failing at every sample
// — before it is retired: descriptors released, entry dropped, gone from
// kubescrape_cgroup_containers.
//
// The state is real and it is permanent without this. repointLocked
// deliberately refuses to follow a container DOWN to a supervisor scope late in
// its life, so on CRI-O a container whose own scope is removed while its conmon
// scope lingers under the same id stays in every listing and stops being
// readable; a stale directory listing does the same anywhere. Such an entry held
// three file descriptors and issued three failing preads per sampling period —
// 3/s on the goroutine that also tails every log file on the node — for the life
// of the process, and went on being counted as a sampled container.
//
// One past maxHeldWindows, and that is arithmetic rather than taste: the hold
// emits for exactly maxHeldWindows windows and then expires, so a container
// whose reads all fail is retired on precisely the first window in which it
// would have exported NOTHING anyway. Retirement therefore never cuts an export
// short, and the expiry transition is still counted
// (obs.CgroupHeldWindows{outcome="expired"}, incremented in the same snapshot,
// before the entry is dropped) rather than being swallowed by the retirement. A
// shorter bound would silence a container that was still emitting; a longer one
// buys nothing but failing reads.
//
// A window in which no sample was ATTEMPTED does not count — that says nothing
// about readability — and one successful read of any of the three files clears
// the streak.
const maxDeadWindows = maxHeldWindows + 1

// emptyWarnEvery re-states the "nothing to sample" complaint while it persists.
// It is a STATE, not an event — the mount is missing for as long as nobody
// fixes it — which is what internal/logdedupe.Throttle is for.
const emptyWarnEvery = 10 * time.Minute

// readWarnEvery throttles the "this cgroup file cannot be read" complaint,
// which without it would be one line per container per sample interval.
const readWarnEvery = time.Minute

// Exporter sends one OTLP metrics payload; satisfied by otlpexport's clients
// and by the agent's buffered/routed chain.
type Exporter interface {
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// Resolver turns a cgroup-derived container identity into the OTLP resource
// the metrics are exported under.
//
// It is an INTERFACE and not a reimplementation on purpose. These ten gauges
// only mean anything if they join the cadvisor series they explain, and two
// series join when their resource attributes — hence the derived Prometheus
// job and instance — are identical. That identity is decided by one body of
// code (the cadvisor batcher's metadata lookup, its TTL cache, its
// unresolved-row fallback and the `cadvisor` attribute builder with its
// instance prefix), so this package borrows that body rather than growing a
// second one that would agree with it only until the first edit.
//
// *promscrape.Scraper implements it (FillContainerResource, which is
// cadvisorBatcher.fillResource's own body called with a cgroup-derived
// identity).
type Resolver interface {
	// FillContainerResource writes the resource attributes describing the
	// container with this runtime id, whose pod slice carried this uid, and
	// reports whether the METADATA SERVICE placed it — and, when it did not,
	// whether it ANSWERED.
	//
	// ok is the export gate. A cgroup path carries no namespace, pod name or
	// container name, so an implementation that cannot resolve the id has
	// nothing to fall back to but the two ids themselves — a resource with no
	// service.name, hence no Prometheus job, hence a series that joins nothing
	// and would change identity the moment a later lookup succeeded. false
	// means "do not export this container"; the sampler retries it.
	//
	// answered is only meaningful when ok is false, and it decides HOW OFTEN
	// the sampler retries, because the two ways to fail are categorically
	// different:
	//
	//   - true: the service replied, and its reply was "no container of any pod
	//     I know has this id" (a 404). Every pod's SANDBOX cgroup gets this
	//     answer forever, by construction — the pause container's id appears in
	//     no pod's containerStatuses — so past a grace period such a cgroup is
	//     given up on and retried rarely (abandonRetryEvery). Nothing about the
	//     node changing will make a pause container become a workload one.
	//   - false: the service could not be reached or could not answer (a
	//     transport failure, a 5xx, a timeout). That says nothing whatsoever
	//     about this container, so it never counts toward giving up, and an
	//     already-abandoned cgroup that gets this answer drops back to the fast
	//     retry (reconsiderEvery) — which is what makes the node resume
	//     promptly when the service comes back.
	//
	// An implementation that cannot tell the two apart must report false: a
	// definitive verdict invented from an outage abandons real containers.
	FillContainerResource(ctx context.Context, res pcommon.Resource, containerID, podUID string) (ok, answered bool)
}

// Config configures the sampler.
type Config struct {
	// Root is the cgroup v2 mount point (empty = DefaultRoot, and see
	// ErrUnsupportedNode: whether Root was set is what decides whether a
	// hierarchy this package cannot read is the operator's error or the node's
	// property).
	Root string
	// Interval is the sampling period (0 or negative = DefaultInterval, below
	// MinInterval = MinInterval).
	Interval time.Duration
	// DiscoverInterval is how often the container set is re-read from the
	// hierarchy (0 or negative = DefaultDiscoverInterval, below
	// MinDiscoverInterval = MinDiscoverInterval). It is the pipeline's blind
	// spot for short-lived containers; see DefaultDiscoverInterval.
	DiscoverInterval time.Duration
	// Resolver builds each container's resource. Required.
	Resolver Resolver
	Logger   *slog.Logger

	// now is the injectable clock (the store.now pattern); nil = time.Now.
	now func() time.Time
	// check is the injectable cgroup-version probe; nil = checkCgroup2. It
	// exists so the two CLASSIFICATIONS of a failed probe — the operator's
	// error and the node's property — can be tested on any machine, rather than
	// only on one whose real /sys/fs/cgroup happens to be the shape the test
	// needs.
	check func(root string) error
}

// Sampler reads container cgroups on its own ticker and exports the
// distribution of each window.
//
// THREE goroutines run under Run: the SAMPLER (the per-interval reads), the
// DISCOVERER (the walk and the identity lookups) and the EXPORTER (snapshot,
// render, send). Each is separated from the sampler for the same reason —
// nothing that can BLOCK may share the goroutine that has to take a reading
// every interval — and in each case the blocking is unbounded by anything this
// process controls: the exporter waits on the collector, the discoverer waits
// on the metadata service.
//
// The discoverer used to be the sampler, and that was a measurable defect
// rather than a theoretical one: resolvePending budgets a whole pass at
// resolveBudget (2s), so a slow metadata service stalled the sweep for up to
// two seconds out of every discovery interval. time.Ticker then delivers the
// buffered tick immediately and the next one on its original schedule, so the
// pair of readings straddling the stall are milliseconds apart — and a CPU rate
// over milliseconds is dominated by the accounting quantum. MEASURED against a
// real cgroup hard-capped at 0.5 cores with a busy loop in it: undisturbed
// 1-second readings give a p95 of 0.5015 and a max of 0.5017 cores, while the
// catch-up pair 5 ms apart after a 2-second stall reads anywhere from 0.0000 to
// 0.9672 — a median of 0.86, sixteen of thirty draws above the true steady-state
// max, and a peak 93% over it. Every one of those is arithmetically correct for
// the sliver it measured, which is exactly why nothing downstream can tell it
// from the burst _max exists to preserve. Moving discovery off REMOVES that trigger;
// minElapsed below is the backstop for every other one (a GC pause, a
// CPU-starved node, mu contention), because the reading is arithmetically
// correct for the interval it measured and therefore indistinguishable from a
// real burst.
//
// One mutex guards the tracked set; nothing is exported while holding it, and
// no metadata lookup is made while holding it.
type Sampler struct {
	root     string
	interval time.Duration
	// minElapsed is the shortest interval a CPU rate may be derived over; see
	// sampleOne.
	minElapsed    time.Duration
	discoverEvery time.Duration
	resolver      Resolver
	log           *slog.Logger
	now           func() time.Time
	// c is the obs label bindings; see counters for why they are here and not
	// in a package-level var block.
	c *counters
	// resolveBudget is resolveBudget, as a field so a test can shrink it.
	resolveBudget time.Duration
	// exportBudget is exportResolveBudget, likewise.
	exportBudget time.Duration
	// maxTracked and maxPending are maxContainers and maxPending, likewise —
	// the two caps are what a test has to be able to reach without fabricating
	// four thousand cgroup directories.
	maxTracked int
	maxPending int

	// exportSem serialises export(): it holds at most one token, and taking it
	// is what makes the export loop and FinalExport mutually exclusive.
	//
	// They ARE concurrent in production, which the "call it after Run has
	// returned" contract this used to carry could not deliver. The agent's
	// shutdown joins its producers under a BOUNDED budget (main.go) and goes on
	// to the final exports when the join times out — deliberately, so one stuck
	// producer cannot cost every other pipeline its last window — so a slow
	// collector puts Run's export loop and the caller's FinalExport in the same
	// body at the same time. Without this they raced on the snapshot scratch
	// and would have interleaved two payloads' worth of window resets.
	//
	// A channel rather than a Mutex because the wait must be bounded by the
	// CALLER's context: FinalExport runs inside the shutdown sequence's shared
	// deadline, and blocking there past it would spend the budget the disk
	// buffer's drain needs.
	exportSem chan struct{}

	mu      sync.Mutex
	tracked map[string]*container // resolved and sampled, by 64-hex runtime id
	// pending is what has been discovered but not resolved: no descriptors, no
	// samples, nothing exported. Every pod contributes one permanently (its
	// sandbox cgroup) — see discover.go.
	pending map[string]*pendingContainer
	// buf is the read scratch shared by every per-sample pread. It is guarded
	// by mu with the tracked set because it is only ever touched by the sample
	// path, which holds mu for the whole sweep.
	buf []byte
	// seen is discovery's reused id set (one pass's listing).
	seen map[string]struct{}
	// capped latches "the pending set is at maxPending" and cappedFDs latches
	// "the tracked set is at maxContainers", so each warning is one line rather
	// than one per discovery cycle. Two latches because the two caps bound
	// different resources and one must not silence the other.
	capped    bool
	cappedFDs bool

	// snap is export()'s reusable snapshot buffer, and it is guarded by
	// exportSem rather than by mu: snapshot() fills it under mu and the payload
	// is then built from it with mu FREE (the point of copying out at all), so
	// the mutex orders it against the sample sweep but says nothing about a
	// second exporter. exportSem is what says that.
	snap []windowPair
	// todo is the DISCOVERY goroutine's reusable resolution work list, and it
	// is unsynchronised because that goroutine is its only user.
	todo []pendingSnap

	// Counts published to the obs gauges. ATOMIC, not a read under mu: the
	// gauge funcs are evaluated on the self-metrics export goroutine, and a
	// sweep holds mu for every container on the node — so taking the sample
	// mutex here delayed the WHOLE process's self-metrics export behind an
	// unrelated pipeline's filesystem reads.
	nSampled  atomic.Int64
	nPending  atomic.Int64
	nDiscover atomic.Int64

	// The two ways to export nothing are throttled SEPARATELY: they have
	// different causes and different fixes, so one must not silence the other
	// for the throttle window (a node that starts empty and then fills with
	// cgroups nobody can resolve would report only the first).
	emptyWarn      logdedupe.Throttle
	unresolvedWarn logdedupe.Throttle
	// readWarn names ONE offending cgroup path per interval when reads start
	// failing. The counters are per FILE, which says what broke but never
	// where; a line per container per second would be a flood proportional to
	// the node's container count, which is exactly what logdedupe exists for.
	readWarn logdedupe.Throttle
	listWarn logdedupe.Throttle
	// retireWarn is its own throttle rather than readWarn's: a retirement is
	// the CONCLUSION of a run of read failures, and sharing the throttle would
	// let the failures suppress the line that says what was done about them.
	retireWarn logdedupe.Throttle
}

// container is one tracked container cgroup: its identity, the three open
// descriptors the sample path preads, and the state of the window in progress.
type container struct {
	id      string
	podUID  string
	dir     string
	baseLen int // len(basename(dir)); see repointLocked

	fds  cgroupFDs
	open bool // fds are valid: false once the cgroup is gone or retired
	// gone marks a cgroup that vanished from the hierarchy. Its descriptors are
	// already released; it stays only until the next export carries the window
	// it accumulated, which is the OOM-killed container's last seconds.
	gone bool

	// prevUsec is the last cpu.stat usage_usec, with the instant it was read:
	// a CPU rate needs two readings, so the first sample of a container's life
	// (and the first after a counter reset) contributes to no rate at all.
	prevUsec uint64
	prevAt   time.Time
	havePrev bool

	cpu window // CPU rate over the window, in cores
	mem window // memory working set over the window, in bytes

	// The last DISTRIBUTION exported for each signal, re-emitted when a window
	// is too sparse to describe one (see finish).
	heldCPU held
	heldMem held

	// Read liveness for the window in progress, folded into the streak at each
	// snapshot: tried says a sweep reached this container, readOK says at least
	// one of its three files answered. Two bools rather than counters because
	// the sample path is allocation- and work-budgeted (TestSampleAllocationBudget)
	// and this is the whole of what the retirement rule needs.
	tried  bool
	readOK bool
	// deadWindows is the run of consecutive windows that were tried and
	// produced nothing; see maxDeadWindows.
	deadWindows int
	// described records that some window of this container's life was
	// exported. It is the difference between "too short to measure" and "died
	// shortly after an export"; see the too_short branch in snapshot.
	described bool
	// atShutdown distinguishes the two ways gone gets set: the cgroup left the
	// hierarchy (reconcile) or this PROCESS is leaving (stop). Only the first
	// is evidence about the container, which is what the too_short branch in
	// snapshot counts.
	atShutdown bool
}

// release closes the container's descriptors, once.
func (c *container) release() {
	if c.open {
		c.fds.close()
		c.open = false
	}
}

// markGone releases the descriptors and marks the container for one final
// flush. Idempotent: discovery passes over a vanished cgroup until the export
// that retires it.
func (c *container) markGone() {
	c.release()
	c.gone = true
}

// endWindow folds the window's read outcome into the dead-window streak and
// reports whether the container has now been unreadable for long enough to be
// retired (maxDeadWindows). Caller holds mu.
func (c *container) endWindow() bool {
	switch {
	case c.readOK:
		c.deadWindows = 0
	case c.tried:
		// Tried and nothing came back: neither cpu.stat, nor memory.current,
		// nor memory.stat answered at any sample in the whole window.
		c.deadWindows++
	}
	// A window nobody sampled (the sampler was not running, or an export
	// happened between two sweeps) says nothing either way and leaves the
	// streak where it was.
	c.tried, c.readOK = false, false
	return c.deadWindows >= maxDeadWindows
}

// unreadable reports whether this container is currently producing nothing, so
// the gauge counts what is actually being SAMPLED rather than what is merely
// tracked. Caller holds mu.
func (c *container) unreadable() bool { return c.deadWindows > 0 }

// pendingContainer is a discovered cgroup whose identity has not resolved.
type pendingContainer struct {
	id      string
	podUID  string
	dir     string
	baseLen int
	// firstFail is the first ANSWERED failure — a 404 — which is what
	// maxUnresolvedAge is measured from; see that constant for why it is
	// neither the discovery time nor the first failure of any kind.
	firstFail time.Time
	// lastTry is the last attempt, and it is what orders the budget-truncated
	// queue (resolvePending). Every path that attempts a lookup must stamp it,
	// the cap refusal in track() included: an entry that keeps its zero value
	// sorts to the front of every pass forever.
	lastTry time.Time
	// nextTry is only meaningful once gaveUp: abandonRetryEvery after a
	// definitive miss, reconsiderEvery after one the service could not answer.
	nextTry time.Time
	gaveUp  bool
}

// pendingSnap is one resolution request, copied out from under the lock. It
// carries lastTry only so the pass can be ordered by it (see resolvePending).
type pendingSnap struct {
	id, podUID string
	lastTry    time.Time
}

// held is the last distribution exported for one signal of one container, and
// how many consecutive windows have now re-stated it (bounded by
// maxHeldWindows).
type held struct {
	ok                     bool
	n                      int
	stddev, max, min, mean float64
}

// signalOut is one signal's rendered contribution to one export.
//
// samples is THIS window's own sample count, and it is what makes every other
// number in the struct readable: below two the four statistics beside it are
// the PREVIOUS window's, re-stated (see finish), and between two and thirty
// they say how much of a distribution the window actually measured. It is
// deliberately not taken from the held value — a re-statement that also
// re-stated its count would be indistinguishable from a fresh measurement,
// which is the whole reason it is exported.
type signalOut struct {
	emit                   bool
	stddev, max, min, mean float64
	samples                int64
}

// windowPair is one container's finished window, copied out from under the
// lock so the identity rebuild and the OTLP build can run without it.
//
// It carries the two IDS rather than a resource: the resource is rebuilt from
// current metadata at every export (see the package doc), so there is nothing
// resource-shaped to copy out, and the ids are exactly what the resolver takes.
type windowPair struct {
	id     string
	podUID string
	cpu    signalOut
	mem    signalOut
}

// New builds a sampler and performs the one-time checks an operator has to
// hear about at startup: that the root is a cgroup v2 hierarchy (see
// checkCgroup2 and ErrUnsupportedNode for which failures are the operator's
// and which are the node's) and that it actually contains pod cgroups this
// process can read (warned, not refused — see below).
//
// It does NOT resolve anything. Resolution is a metadata lookup per container,
// and doing it here would put a whole node's worth of them — against a service
// that may be starting up alongside this process — in front of every other
// pipeline's start. Run's first act is a full discovery pass.
func New(cfg Config) (*Sampler, error) {
	if cfg.Resolver == nil {
		return nil, errors.New("cgroupstats: a Resolver is required")
	}
	root := cfg.Root
	if root == "" {
		root = DefaultRoot
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	interval := cfg.Interval
	switch {
	case interval <= 0:
		interval = DefaultInterval
	case interval < MinInterval:
		// Clamped rather than refused: this is the library guarantee for a
		// Config arriving programmatically, the same shape as the non-positive
		// default above. What an OPERATOR typed is checkFlagValues' question,
		// and it answers it with a refusal — they would otherwise not find out.
		log.Warn("cgroup sampling period raised to the floor: a shorter one is three cgroup reads per container per period",
			"asked", interval, "using", MinInterval)
		interval = MinInterval
	}
	discoverEvery := cfg.DiscoverInterval
	switch {
	case discoverEvery <= 0:
		discoverEvery = DefaultDiscoverInterval
	case discoverEvery < MinDiscoverInterval:
		// Clamped, not refused, for the reason the sampling period is: this is
		// the library guarantee for a Config arriving programmatically, while
		// what an OPERATOR typed is checkFlagValues' question and gets a
		// refusal.
		log.Warn("cgroup discovery interval raised to the floor: every pass re-offers each unresolved cgroup to the metadata service, and every pod contributes one permanently (its sandbox)",
			"asked", discoverEvery, "using", MinDiscoverInterval)
		discoverEvery = MinDiscoverInterval
	}
	check := cfg.check
	if check == nil {
		check = checkCgroup2
	}
	if err := check(root); err != nil {
		if cfg.Root == "" {
			// The DEFAULT root. What we just learned is a property of this
			// NODE, not of anything the operator typed, and it is identical on
			// every node of its kind — so the caller disables this pipeline and
			// keeps the others (ErrUnsupportedNode).
			return nil, fmt.Errorf("%w: %w", ErrUnsupportedNode, err)
		}
		// An explicitly configured root that is not a cgroup v2 hierarchy is an
		// operator error, uniform across the fleet and fixable by editing one
		// value: fatal, as loudly as possible.
		return nil, err
	}
	now := cfg.now
	if now == nil {
		now = time.Now
	}
	s := &Sampler{
		root:     root,
		interval: interval,
		// Half the period. A ticker never fires EARLY relative to its own
		// schedule, so two consecutive readings of one container are a period
		// apart give or take the sweep's own jitter — a container's offset
		// within the sweep, which is bounded by the whole sweep: 3.7 ms over
		// 200 real cgroup v2 scopes and 11.0 ms at the maxContainers cap, worst
		// of 300 timed sweeps at that cap 21.6 ms (MEASURED). Anything at half
		// a period or less means the sampler was blocked and the ticker is
		// catching up. Half leaves 500 ms of the 1s default for jitter, forty
		// times the sweep at that cap and twenty times the worst one measured
		// there, while capping the accounting quantum's contribution to the
		// rate at twice its steady-state value instead of leaving it unbounded.
		minElapsed:    interval / 2,
		discoverEvery: discoverEvery,
		resolver:      cfg.Resolver,
		log:           log,
		now:           now,
		c:             newCounters(),
		resolveBudget: resolveBudget,
		exportBudget:  exportResolveBudget,
		maxTracked:    maxContainers,
		maxPending:    maxPending,
		exportSem:     make(chan struct{}, 1),
		tracked:       map[string]*container{},
		pending:       map[string]*pendingContainer{},
		seen:          map[string]struct{}{},
		buf:           make([]byte, readBufBytes),
	}
	// The walk (not the resolution) runs here, so the startup line and the
	// empty-root warning describe this node rather than an empty map.
	if found, complete, ok := s.walk(); ok {
		s.reconcile(found, complete)
	}
	// The journald lesson, which this pipeline is one missing hostPath away
	// from repeating: an enabled agent that reports ready and silently collects
	// nothing. WARNED rather than refused, unlike a bad explicit root, because
	// an empty root is not necessarily permanent — a freshly joined node
	// genuinely has no workload pods yet, and refusing would turn a transient
	// into a CrashLoop that takes this node's LOGS down with it. The complaint
	// therefore repeats (every discovery cycle) and kubescrape_cgroup_containers
	// is registered exactly when the sampler runs, so a published 0 always
	// means "on and finding nothing" and never "off".
	//
	// Only the EMPTY half here: nothing has been offered to the resolver yet,
	// so "none of them resolved" would be a complaint about work that has not
	// been attempted.
	s.warnIfEmpty()
	return s, nil
}

// warnIfExportingNothing states the "running and collecting nothing" complaint,
// throttled while it persists. The two ways to get there need different
// answers: no cgroups at all is a missing mount, while cgroups that never
// resolve is a metadata-service problem — and one of them is normal for one
// cgroup per pod, so it is only worth a line when NOTHING is being sampled.
func (s *Sampler) warnIfExportingNothing() {
	if s.nSampled.Load() > 0 {
		return
	}
	pending := s.nPending.Load()
	if pending == 0 {
		s.warnIfEmpty()
		return
	}
	if !s.unresolvedWarn.Allow(emptyWarnEvery) {
		return
	}
	s.log.Warn("cgroup sampler resolved none of the container cgroups it found: it is running and will export nothing",
		"root", s.root, "cgroups", pending,
		"hint", "the metadata service must resolve a container id before its samples can be attributed; check kubescrape_cgroup_unresolved_total and the agent's metadata-service connectivity",
	)
}

// warnIfEmpty is the missing-mount half of the same complaint.
func (s *Sampler) warnIfEmpty() {
	if s.nDiscover.Load() > 0 || !s.emptyWarn.Allow(emptyWarnEvery) {
		return
	}
	s.log.Warn("cgroup sampler found no container cgroups: it is running and will export nothing",
		"root", s.root,
		"hint", "mount the host's "+DefaultRoot+" read-only into this container (the shipped DaemonSet does so behind the same flag), or point -cgroup-stats-root at the mount",
	)
}

// Containers reports how many container cgroups are currently sampled. It backs
// kubescrape_cgroup_containers.
func (s *Sampler) Containers() int { return int(s.nSampled.Load()) }

// Unresolved reports how many discovered cgroups have no identity yet and are
// therefore not exported. It backs kubescrape_cgroup_unresolved_containers, and
// on a healthy node it settles at roughly the pod count: one sandbox cgroup per
// pod resolves to nothing, by design.
func (s *Sampler) Unresolved() int { return int(s.nPending.Load()) }

// Discovered reports every container cgroup found in the hierarchy, resolved or
// not, for the startup log line.
func (s *Sampler) Discovered() int { return int(s.nDiscover.Load()) }

// publishCountsLocked refreshes the atomics the gauges read. Caller holds mu.
//
// SAMPLED is not the same as TRACKED, and the gauge is the sampled count: a
// container that is still listed but has stopped answering every read produces
// no data at all, and counting it said the node was measuring 110 containers
// while it was measuring 109. It rejoins the count as soon as a read succeeds
// (and leaves the tracked set altogether at maxDeadWindows). The lag either way
// is one export window, which is inherent — the streak is per window.
func (s *Sampler) publishCountsLocked() {
	sampled, tracked := 0, 0
	for _, c := range s.tracked {
		if c.gone {
			continue
		}
		tracked++
		if !c.unreadable() {
			sampled++
		}
	}
	s.nSampled.Store(int64(sampled))
	s.nPending.Store(int64(len(s.pending)))
	// DISCOVERED is what is in the hierarchy, so an unreadable-but-listed
	// container still counts here: it was found, and it is the difference
	// between this and the sampled gauge that says so.
	s.nDiscover.Store(int64(tracked + len(s.pending)))
}

// Root is the resolved cgroup root, for the startup log line.
func (s *Sampler) Root() string { return s.root }

// Interval and DiscoverInterval are the EFFECTIVE periods — after New's
// defaulting and clamping — and the startup log line reports these rather than
// the flag values. An operator who asked for something the constructor refused
// to honour otherwise has only a WARN line to correlate against, and the line
// that says what the pipeline is doing said something else.
func (s *Sampler) Interval() time.Duration { return s.interval }

// DiscoverInterval is the effective discovery period; see Interval.
func (s *Sampler) DiscoverInterval() time.Duration { return s.discoverEvery }

// Run samples until ctx is done, exporting every exportEvery.
//
// exportEvery is the agent's -scrape-interval: the window these ten gauges
// describe is deliberately the SAME window the cadvisor scrape averages, which
// is what makes "the max inside this scrape interval" a statement about the
// series next to it rather than about an unrelated slice of time.
//
// The LAST window is not exported here. Run releases every descriptor and marks
// every container for a final flush, and the caller ships it with FinalExport
// on its own shutdown budget — the shape internal/metrics.Registry.FinalExport
// and the span-metrics generator already use, and for the same reason: a
// self-manufactured timeout cannot be fitted inside the pod's termination
// grace, which only the caller is tracking.
func (s *Sampler) Run(ctx context.Context, exp Exporter, exportEvery time.Duration) {
	if exportEvery <= 0 {
		exportEvery = 30 * time.Second
	}
	// The FIRST discovery pass is synchronous and runs before either loop:
	// New deliberately resolves nothing, so until it has run the sampler holds
	// descriptors for no one, and a first sweep over an empty set would put
	// every container's first reading a whole interval late.
	s.discover(ctx, s.now())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.exportLoop(ctx, exp, exportEvery)
	}()
	go func() {
		defer wg.Done()
		s.discoverLoop(ctx)
	}()
	s.sampleLoop(ctx)
	wg.Wait()
	s.stop()
}

// sampleLoop is the reader, and it does NOTHING that can block: every read is
// a pread on a descriptor this process already holds. See the Sampler doc for
// why discovery is not here any more.
func (s *Sampler) sampleLoop(ctx context.Context) {
	sampleTick := time.NewTicker(s.interval)
	defer sampleTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sampleTick.C:
			s.sample()
		}
	}
}

// discoverLoop re-reads the container set and resolves identities. It is its
// own goroutine because a pass makes metadata lookups, bounded only by
// resolveBudget — see the Sampler doc.
func (s *Sampler) discoverLoop(ctx context.Context) {
	t := time.NewTicker(s.discoverEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.discover(ctx, s.now())
		}
	}
}

// exportLoop ships one window per tick. It returns on cancellation WITHOUT
// exporting; the final window is FinalExport's, after the sampler has stopped,
// so it cannot race a sweep that is still writing into it.
func (s *Sampler) exportLoop(ctx context.Context, exp Exporter, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.export(ctx, exp); err != nil {
				s.log.Warn("cgroup-stats export failed", "error", err)
			}
		}
	}
}

// sample reads every tracked container once. It runs on the sampler goroutine
// and is allocation-free (TestSampleAllocationBudget); keep it that way — it
// runs once per second per container on the process that also tails every log
// file on the node.
//
// Nothing here may BLOCK. Every read is a pread on a descriptor this process
// already holds, and that is the property that lets the interval mean what it
// says; see the Sampler doc for what sharing this goroutine with discovery
// cost the numbers.
func (s *Sampler) sample() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.tracked {
		if !c.open {
			continue // a vanished cgroup, waiting for its final flush
		}
		s.sampleOne(c)
	}
}

func (s *Sampler) sampleOne(c *container) {
	obs.CgroupSamples.Inc()
	// A sample was attempted, which is what makes a window that produced
	// nothing evidence of an unreadable container rather than of a sampler that
	// was not running (see endWindow).
	c.tried = true

	if usec, err := readField(c.fds.cpuStat, s.buf, keyUsageUsec); err != nil {
		s.c.readCPUStat.Inc()
		s.warnRead(c, fileCPUStat, err)
	} else {
		c.readOK = true
		// The clock is read HERE, per container, right after the read that
		// produced the counter — not once per sweep. A sweep timestamp is the
		// interval between sweep STARTS while each container is read at its own
		// varying offset inside the sweep, so the jitter between two offsets
		// lands in the divisor: measured, a dead-flat container reported a
		// stddev of up to 0.012 cores and an inflated max out of nothing but
		// that. Per-container timing cancels it, because both ends of the
		// interval carry the same offset.
		now := s.now()
		// rebase says this reading becomes the next interval's baseline. It is
		// false in exactly one case, the short interval below, and that case is
		// why the assignment is not unconditional any more.
		rebase := true
		switch {
		case !c.havePrev:
			// First reading of this container: no interval, so no rate.
		case usec < c.prevUsec:
			// A cumulative counter went backwards. On cgroup v2 that means the
			// cgroup was replaced under the same path (a container restart
			// keeping its id is impossible, but a re-created cgroup is not), and
			// the difference would be a large NEGATIVE rate rendered as a huge
			// positive one by the unsigned subtraction. The interval is dropped
			// and the new value becomes the baseline.
			obs.CgroupCounterResets.Inc()
		case now.Sub(c.prevAt) < s.minElapsed:
			// Too short an interval to divide by. usage_usec advances in the
			// scheduler's accounting quanta, so over a few milliseconds the
			// quotient is dominated by where the quanta happened to land — an
			// arithmetically correct rate for the interval measured, and
			// therefore indistinguishable downstream from the real burst _max
			// exists to preserve. It happens when the sampler was blocked and
			// the ticker is catching up (see the Sampler doc for the measured
			// spread: a 0.5-core container's 5 ms catch-up pair reads 0.00 to
			// 0.97 cores, half the draws above its true steady-state max).
			//
			// WHAT IS AND IS NOT LOST, stated precisely because the loose
			// version of this sentence ("nothing is lost") was true only of
			// the CPU-SECONDS and this pipeline's product is the DISTRIBUTION:
			//
			//   - the CPU TIME is not lost. The baseline is KEPT (rebase =
			//     false) and the counter is cumulative, so the sliver's
			//     microseconds are charged to the NEXT interval rather than to
			//     one of their own, and every microsecond cpu.stat reported
			//     still lands in exactly one interval —
			//     TestAShortIntervalIsDeferredNotDiscarded pins that sum.
			//   - the window's MEAN is NOT preserved, and the sentence that
			//     stood here ("the MEAN comes out the same to the last bit")
			//     was simply wrong, as well as contradicting the bullet below
			//     it: mean is the unweighted average of the PER-INTERVAL
			//     RATES, so folding two intervals into one removes a term AND
			//     lengthens the divisor of the term that survives. In that
			//     same test's fixture it reads (1 + 3/1.4 + 1)/3 = 1.38 cores
			//     where the four undeferred intervals would have averaged
			//     (1 + 5 + 1 + 1)/4 = 2.00 — which is the trade, not a defect:
			//     the 5.0 term is the stall artefact.
			//   - the window's SAMPLE is lost: it takes n-1 readings, so
			//     stddev, max and min are computed over one fewer point. A
			//     burst confined to the sliver is not erased — it lands in the
			//     next interval — but it is DILUTED over that longer interval,
			//     which is a smaller `_max` than a burst of the same size
			//     landing inside an ordinary interval.
			//
			// It is not counted, and the argument for that is not that it is
			// free: it is that the loss is already REPORTED, per container and
			// per window, by container_cpu_usage_samples — which exists for
			// exactly this class of question and drops by one for every skip
			// here. A fleet-wide counter would say the same thing less
			// precisely, and the alternative to skipping is worse in the
			// direction that matters: an inflated `_max` is a wrong number
			// where a missing sample is a smaller n, and only the first is
			// indistinguishable from the burst the gauge exists to preserve.
			//
			// Measured on the shipped arrangement (200 real cgroup v2 scopes,
			// discovery and export on their own goroutines) it fired ZERO
			// times in 23,600 rate readings at the 1s default and zero in
			// 239,600 at the 100ms floor: on a healthy node this arm is inert,
			// and it arms only once something has already stalled the sweep.
			rebase = false
		default:
			if el := now.Sub(c.prevAt).Seconds(); el > 0 {
				// usage_usec is MICROseconds of CPU time; divided by the wall
				// seconds it accrued over, that is CPU cores.
				c.cpu.add(float64(usec-c.prevUsec) / 1e6 / el)
			}
		}
		if rebase {
			c.prevUsec, c.prevAt, c.havePrev = usec, now, true
		}
	}

	// The memory working set is a LEVEL, not a rate, so it has no divisor for a
	// short interval to corrupt: two readings close together are two honest
	// readings of what the container held, and the guard above deliberately
	// does not apply here.
	cur, curErr := readValue(c.fds.memCurrent, s.buf)
	if curErr != nil {
		s.c.readMemCurrent.Inc()
		s.warnRead(c, fileMemCurrent, curErr)
	} else {
		c.readOK = true
	}
	inactive, inactErr := readField(c.fds.memStat, s.buf, keyInactiveFile)
	if inactErr != nil {
		s.c.readMemStat.Inc()
		s.warnRead(c, fileMemStat, inactErr)
	} else {
		c.readOK = true
	}
	if curErr == nil && inactErr == nil {
		// cadvisor's own definition of the working set, matched exactly:
		// container_memory_working_set_bytes is memory.current minus the
		// inactive file cache. A different definition here would make _max and
		// _min incomparable with the series they annotate, which is the whole
		// reason this package borrows cadvisor's naming and resource identity.
		var ws float64
		if cur > inactive {
			ws = float64(cur - inactive)
		}
		c.mem.add(ws)
	}
}

// warnRead names one failing cgroup file per readWarnEvery. It is on the
// error arm only, so the healthy sample path never reaches time.Now.
//
// A container torn down between two discovery passes fails every read until
// the next pass drops it, which is a normal few seconds of noise on a node with
// churn — hence the throttle rather than a line per failure, and hence a
// warning that says so.
func (s *Sampler) warnRead(c *container, file string, err error) {
	if !s.readWarn.Allow(readWarnEvery) {
		return
	}
	s.log.Warn("cgroup read failed (throttled; a container removed between discovery passes fails every read until the next one)",
		"cgroup", c.dir, "file", file, "error", err)
}

// snapshot renders every window and resets it, under the lock. The copy is what
// lets the identity rebuild and the OTLP build run with the lock free.
//
// The windows are reset HERE rather than after a successful send: they describe
// one bounded interval, so carrying a failed window forward would either
// double-count it into the next one or silently stretch the interval the
// numbers claim to cover. A lost window is one gap in a distribution; a wrong
// one is worse. With -buffer-dir the send is an enqueue, so the outage case
// this trades away is already covered by the spool. What the reset is NOT
// allowed to be is silent: every window discarded — by a failed send, or by an
// identity that could not be rebuilt — is counted into obs.CgroupWindowsDropped
// by the caller, since a loss nobody can see is the one thing worse than a loss.
//
// It is also where containers are RETIRED, in the two ways one can be: a GONE
// container (its cgroup left the hierarchy) ships its accumulated window once
// and is dropped, and one that is still listed but has answered no read for
// maxDeadWindows windows is dropped too.
//
// The caller holds exportSem, which is what makes reusing s.snap safe.
func (s *Sampler) snapshot() []windowPair {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = s.snap[:0]
	for id, c := range s.tracked {
		var cpu, mem signalOut
		if c.gone {
			// A dead container's LAST datapoint is the one an operator zooms
			// into after an OOM kill, so it has to be real. finish would hold
			// here — re-emitting the PREVIOUS window's numbers stamped at
			// the retirement instant — and the hold exists to bridge a sampling
			// hiccup in a LIVING container, not to invent a final reading for a
			// container that has stopped producing them. A gap is the honest
			// report of a window that measured too little to describe.
			cpu, mem = measured(&c.cpu), measured(&c.mem)
			if !cpu.emit && !mem.emit && !c.described && !c.atShutdown {
				// The literal test: this container was resolved and had three
				// descriptors open, and its cgroup left the hierarchy without
				// either signal ever yielding two readings — so no window of
				// its life can be described. Everything measured about it is
				// discarded here, so it is counted:
				// obs.CgroupWindowsDropped{reason="too_short"}.
				//
				// THE NAME IS THE COMMON CASE, NOT THE WHOLE BRANCH. Two
				// neighbours land here as well. Both are genuine losses of a
				// window this node can say nothing about, so the lower-bound
				// property survives — but neither is a short-lived container,
				// and an operator reading a rate off this series should know
				// which of the three they might be looking at:
				//
				//   - a container a pass discovered whose cgroup was already
				//     gone by the time the sweep reached it. It held three
				//     descriptors and yielded no reading at all — every attempt
				//     failing, or none attempted if the next pass marked it
				//     gone first. "Too short to measure" is right about the
				//     outcome and wrong about the cause: what was late was the
				//     discovery, not short the life.
				//   - a container that lived for MINUTES whose three files
				//     failed every read — a CRI-O container whose own scope was
				//     removed while its conmon scope lingers, a stale listing;
				//     see maxDeadWindows — and whose cgroup then vanished
				//     before the dead-window streak could retire it. That is an
				//     unreadable-cgroup fault, and it is ALREADY counted, three
				//     times per sampling period, as
				//     kubescrape_cgroup_read_errors_total. Read the two
				//     together before concluding that a node is losing
				//     short-lived containers: a rate here with a matching read
				//     error rate is the second case, not the first.
				//
				// Three guards, and each one is what keeps the count from
				// meaning something else:
				//
				//   - `described` excludes the container that has been
				//     exporting all along and dies shortly after an export.
				//     Its final partial window is lost too, but "never
				//     describable" is not what happened, and at a 1s period
				//     inside a 30s window a few percent of every ORDINARY
				//     termination lands there. A signal that fires on normal
				//     shutdowns is one nobody can alert on.
				//   - `atShutdown` excludes THIS PROCESS exiting (see stop):
				//     every container is gone by then, and a SIGTERM before
				//     the second sweep would otherwise publish the verdict for
				//     the whole node.
				//   - the emit tests are the event itself: a container that
				//     produced a distribution of either signal is exported on
				//     the line below and is not a loss at all.
				//
				// WHAT IT DOES NOT COVER, because the sampler has no evidence
				// of it (stated here because the counter's value as an
				// argument for a shorter -cgroup-stats-discover-interval turns
				// on how far below the truth it sits — see
				// DefaultDiscoverInterval):
				//
				//   - a cgroup that appeared and vanished BETWEEN two discovery
				//     passes. Nothing in the hierarchy remembers it.
				//   - a cgroup discovered but never RESOLVED — it lives in
				//     s.pending, holds no descriptors, and reconcile simply
				//     deletes it. That set is dominated by every pod's sandbox
				//     cgroup, which never resolves by construction, so counting
				//     its deletions here would drown the signal in one
				//     increment per pod terminated.
				//   - a container discovered LATE in its life: it is sampled
				//     for the remainder, and two seconds of remainder is enough
				//     to describe it.
				//
				// So the counter is a strict lower bound and a narrow one. It
				// is worth having anyway because it is CERTAIN — every
				// increment is a container this node measured and threw away —
				// where the rest of the blind spot is unmeasurable.
				s.c.droppedTooShort.Inc()
			}
		} else {
			cpu = s.finish(&c.cpu, &c.heldCPU)
			mem = s.finish(&c.mem, &c.heldMem)
		}
		if cpu.emit || mem.emit {
			s.snap = append(s.snap, windowPair{id: c.id, podUID: c.podUID, cpu: cpu, mem: mem})
			// This container has now been described at least once, which is
			// what the too_short verdict above turns on.
			c.described = true
		}
		c.cpu.reset()
		c.mem.reset()
		switch {
		case c.gone:
			c.release()
			delete(s.tracked, id)
		case c.endWindow():
			// Listed and unreadable: released, dropped and counted. See
			// maxDeadWindows.
			c.release()
			delete(s.tracked, id)
			obs.CgroupContainersRetired.Inc()
			s.quarantineLocked(c)
			if s.retireWarn.Allow(readWarnEvery) {
				s.log.Warn("cgroup sampler retired a container that is still listed but has answered no read for several export windows (throttled); its descriptors are released and it is no longer counted as sampled",
					"cgroup", c.dir, "container", c.id, "windows", c.deadWindows)
			}
		}
	}
	s.publishCountsLocked()
	return s.snap
}

// quarantineLocked puts a retired container back into the PENDING set on the
// slow clock. Caller holds mu.
//
// Retirement alone would not stick: the cgroup is still in the hierarchy, so the
// very next discovery pass — fifteen seconds later — finds an id that is in
// neither map, resolves it (its identity is fine; it is the cgroup FILES that
// stopped answering), re-opens three descriptors and starts the same three dead
// windows over. That is a retirement undone before it saved anything: measured
// against the fifteen-second discovery cadence and a thirty-second window, such
// a container would be back to holding descriptors and failing reads for about
// five sixths of the time.
//
// The pending set is already exactly the right place — "discovery knows about
// it, nothing is being sampled from it" — and gaveUp is already how that set
// spells "ask about this one rarely". So the entry lands there past its grace
// period, and abandonRetryEvery later it gets one more chance: if whatever broke
// has healed it is sampled again, and if not it costs three failing reads for
// three windows out of every ten minutes instead of forever.
//
// One nuance worth stating rather than hiding: it now counts in
// kubescrape_cgroup_unresolved_containers, whose name is about IDENTITY, and
// this container's identity resolved perfectly well. The gauge's real subject is
// "discovered and not exported", which is true of it, and the retirement counter
// plus the warning are what name the actual reason.
// The pending CAP is deliberately not consulted: this entry is not a new
// discovery competing for memory, it is a container that already held three
// descriptors, and refusing it would put it straight back into the churn the
// quarantine exists to stop. The overshoot is bounded by the tracked cap
// (maxContainers entries of ~150 bytes) and only in a hierarchy that is already
// at both caps at once.
func (s *Sampler) quarantineLocked(c *container) {
	now := s.now()
	s.pending[c.id] = &pendingContainer{
		id: c.id, podUID: c.podUID, dir: c.dir, baseLen: c.baseLen,
		firstFail: now, lastTry: now, nextTry: now.Add(abandonRetryEvery), gaveUp: true,
	}
}

// measured renders one signal's window WITHOUT the sparse-window hold: a real
// distribution or nothing. It is what a container being retired gets; see
// snapshot and finish.
func measured(w *window) signalOut {
	if w.n < 2 {
		return signalOut{}
	}
	return signalOut{emit: true, stddev: w.stddev(), max: w.max, min: w.min, mean: w.mean, samples: int64(w.n)}
}

// finish renders one signal's window and folds the sparse-window rule in.
//
// A window holding FEWER THAN TWO samples of a signal is not a distribution: a
// single reading has a stddev of zero by construction and a max and a min that
// are the same number — which is the average the cadvisor scrape already
// publishes, dressed up as three new series. So such a window HOLDS: the last
// distribution actually measured is re-emitted, and the series keeps its shape
// instead of alternating between real numbers and degenerate ones.
//
// Per SIGNAL, because the two fill at different rates: a CPU rate needs two raw
// readings before it yields even one value, so CPU is permanently one sample
// behind memory and a per-container rule would throw away a perfectly good
// working-set measurement to protect the CPU one.
//
// With nothing held — a container's very first window — nothing is emitted. A
// gap is honest about a distribution that has not been measured yet; inventing
// one is not. The hold is BOUNDED for the same reason (maxHeldWindows): past
// the bound the signal stops being emitted, because re-stating a measurement
// indefinitely is not bridging a gap, it is reporting a dead container as a
// live one. What happens to such a container one step later is maxDeadWindows'
// business: it is retired, so it stops costing descriptors and reads as well as
// series.
//
// A container being RETIRED does not come through here at all — snapshot uses
// measured() for it. The hold bridges a hiccup in a LIVING container; a dead
// one's last datapoint has to be something that was actually read.
//
// A held window carries NO MARKER distinguishing it downstream, and that is a
// decision rather than an omission. The only two places a marker could go both
// fork the series: a resource attribute changes the derived job and instance,
// which is the exact identity flap the whole design is built to prevent, and a
// data-point attribute on a gauge changes the label set, which breaks the join
// to the cadvisor series these gauges exist to annotate — for precisely the
// windows it marks. So the bound is the answer instead: at most maxHeldWindows
// re-statements, then a gap, and the gap is the signal. What an operator
// staring at a flat max needs is on the AGENT's own metrics, where it costs no
// series identity: obs.CgroupHeldWindows{outcome} counts both the holding and
// the giving up.
func (s *Sampler) finish(w *window, h *held) signalOut {
	if w.n >= 2 {
		*h = held{ok: true, stddev: w.stddev(), max: w.max, min: w.min, mean: w.mean}
		return signalOut{emit: true, stddev: h.stddev, max: h.max, min: h.min, mean: h.mean, samples: int64(w.n)}
	}
	if !h.ok {
		return signalOut{}
	}
	if h.n >= maxHeldWindows {
		// Cleared, not merely skipped: this is what makes `expired` an EVENT
		// counted once at the transition rather than once per window forever,
		// and what makes a later real window start a fresh hold budget.
		*h = held{}
		s.c.heldExpired.Inc()
		return signalOut{}
	}
	h.n++
	s.c.heldWindows.Inc()
	// samples is w.n — 0 or 1 — and NOT the held value's: it is the one field
	// that must describe this window rather than the one being re-stated, so
	// that a consumer can tell the two apart at all. See signalOut.
	return signalOut{emit: true, stddev: h.stddev, max: h.max, min: h.min, mean: h.mean, samples: int64(w.n)}
}

// export snapshots, rebuilds every identity and sends one window.
//
// The whole body is serialised by exportSem: the export loop and FinalExport
// are concurrent in production (see the field), and two of these interleaving
// would race on the snapshot scratch and split one window's containers across
// two payloads.
func (s *Sampler) export(ctx context.Context, exp Exporter) error {
	select {
	case s.exportSem <- struct{}{}:
	case <-ctx.Done():
		// The other exporter has the window and is shipping it; waiting past
		// the caller's deadline would spend a shutdown budget the later steps
		// need. Nothing is dropped here that the holder is not already sending.
		return ctx.Err()
	}
	defer func() { <-s.exportSem }()

	snap := s.snapshot()
	if len(snap) == 0 {
		return nil
	}
	md := s.build(ctx, snap, s.now())
	if md.DataPointCount() == 0 {
		return nil
	}
	if err := exp.ExportMetrics(ctx, md); err != nil {
		s.c.droppedExport.Add(float64(md.ResourceMetrics().Len()))
		return err
	}
	return nil
}

// FinalExport ships the last window, on the CALLER's context.
//
// The caller owns the budget deliberately: this used to manufacture its own
// five seconds, which is the shape internal/metrics.Registry.FinalExport had
// removed from it — a fixed timeout cannot be fitted inside the pod's
// termination grace, which the agent's shutdown sequence is tracking as one
// shared deadline for every final flush. The context must also be DETACHED
// (WithoutCancel, never a bare Background: otlpexport.Own's durability marker
// and the transform handoff marker ride on it).
//
// It is safe to call while Run is still going, and the agent does exactly that:
// the shutdown sequence joins its producers under a bounded budget and proceeds
// when the join times out. exportSem serialises the two, and the ctx bounds the
// wait. Once Run HAS returned every descriptor is released and every container
// is marked for a final flush, so this then carries whatever the last partial
// window measured, including the seconds of a container that vanished just
// before shutdown.
func (s *Sampler) FinalExport(ctx context.Context, exp Exporter) error {
	return s.export(ctx, exp)
}

// build rebuilds each container's identity and renders one ResourceMetrics per
// container that still has one, with up to ten gauges (five per signal, and a
// signal whose window measured nothing contributes none of its five).
//
// The rebuild runs HERE, on the export goroutine with the mutex free, and never
// on the sampler goroutine: an identity lookup can block for as long as the
// metadata service takes, and the sweep must keep measuring the node while it
// does. See resolveExport for the budget.
func (s *Sampler) build(ctx context.Context, snap []windowPair, now time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	ts := pcommon.NewTimestampFromTime(now)
	rctx, cancel := context.WithTimeout(ctx, s.exportBudget)
	defer cancel()
	for i := range snap {
		w := &snap[i]
		res := pcommon.NewResource()
		if !s.resolveExport(rctx, res, w) {
			continue
		}
		rm := md.ResourceMetrics().AppendEmpty()
		res.MoveTo(rm.Resource())
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName(scopeName)
		sm.Scope().SetVersion(obs.ScopeVersion)
		if w.cpu.emit {
			putGauge(sm, nameCPUStddev, descCPUStddev, unitCores, w.cpu.stddev, ts)
			putGauge(sm, nameCPUMax, descCPUMax, unitCores, w.cpu.max, ts)
			putGauge(sm, nameCPUMin, descCPUMin, unitCores, w.cpu.min, ts)
			putGauge(sm, nameCPUMean, descCPUMean, unitCores, w.cpu.mean, ts)
			putGauge(sm, nameCPUSamples, descCPUSamples, unitSamples, float64(w.cpu.samples), ts)
		}
		if w.mem.emit {
			putGauge(sm, nameMemStddev, descMemStddev, unitBytes, w.mem.stddev, ts)
			putGauge(sm, nameMemMax, descMemMax, unitBytes, w.mem.max, ts)
			putGauge(sm, nameMemMin, descMemMin, unitBytes, w.mem.min, ts)
			putGauge(sm, nameMemMean, descMemMean, unitBytes, w.mem.mean, ts)
			putGauge(sm, nameMemSamples, descMemSamples, unitSamples, float64(w.mem.samples), ts)
		}
	}
	return md
}

// resolveExport rebuilds one container's resource for one export, reporting
// whether it may be exported at all.
//
// The cost is one Resolver call per container per export, and the claim that
// this is cheap is load-bearing enough to be measured rather than assumed
// (promscrape's TestFillContainerResourceIsA304InTheSteadyState): the resolver
// keeps a one-minute cache keyed by container id — the SAME entries the
// cadvisor scrape fills, since it resolves the same ids through the same body —
// and a miss underneath it reaches metaclient, which holds the decoded
// document with its ETag and revalidates a stale one as a conditional GET the
// metadata service answers 304. So the steady state is a map lookup, and the
// worst case once a minute per container is a 304 with no body.
//
// A container that does not resolve is NOT exported and its window is LOST —
// counted, because it was measured. That is the same verdict discovery applies
// before spending descriptors, taken again here because a container's identity
// can stop resolving between the two (a metadata-service outage, a pod whose
// tombstone aged out from under a cgroup that has not been cleaned up yet).
//
// A lookup cut short by the pass deadline lands here too, which is the right
// accounting for the LOSS counter (the window really is gone) and a slightly
// generous reading of the unresolved one — but only in the state where the
// metadata service is too slow to answer a node's worth of lookups, which is
// exactly what that counter is there to show.
// The failure CLASSIFICATION is deliberately ignored here. It governs a retry
// cadence for a cgroup that is not being sampled, and everything reaching this
// seam is already tracked and will be offered again by the next export whatever
// the answer was; the pending set's policy is resolveFailed's.
func (s *Sampler) resolveExport(ctx context.Context, res pcommon.Resource, w *windowPair) bool {
	ok, _ := s.resolver.FillContainerResource(ctx, res, w.id, w.podUID)
	if !ok {
		s.c.droppedUnresolved.Inc()
		s.c.unresolvedExport.Inc()
		return false
	}
	return true
}

func putGauge(sm pmetric.ScopeMetrics, name, desc, unit string, v float64, ts pcommon.Timestamp) {
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	m.SetDescription(desc)
	if unit != "" {
		m.SetUnit(unit)
	}
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetDoubleValue(v)
}

// stop releases every held descriptor and marks every container for one final
// flush, which FinalExport then carries. Called by Run once all three loops
// have stopped.
//
// The mark is atShutdown, NOT the plain vanished-from-the-hierarchy one, and
// the difference is a loss counter's credibility. snapshot counts a gone
// container that never produced a distribution as
// obs.CgroupWindowsDropped{reason="too_short"} — evidence that this node runs
// containers shorter than the pipeline can describe. Every container on the
// node is gone by this line, so without the distinction a SIGTERM published
// that verdict for every container discovered within the last sampling period,
// and for the WHOLE NODE'S SET when the process is killed before its second
// sweep (a CrashLooping agent, a rollout that rolls straight back). A shutdown
// is not the container being short-lived and it is not data loss of that kind:
// what is lost is the partial window every container loses at SIGTERM, and the
// ones with two samples still export theirs on the line below.
//
// A container that had ALREADY vanished keeps its verdict — c.gone is checked
// before the mark, so the real short-lived container that died in the last
// second before the signal is still counted.
func (s *Sampler) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.tracked {
		if !c.gone {
			c.atShutdown = true
		}
		c.markGone()
	}
	s.publishCountsLocked()
}

// closeAll releases every descriptor and forgets every container, for tests and
// for a sampler that is discarded without a final export.
func (s *Sampler) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, c := range s.tracked {
		c.release()
		delete(s.tracked, id)
	}
	clear(s.pending)
	s.publishCountsLocked()
}

// --- metric names, units and descriptions ---
//
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

// metricNames is the exported set, for the test that pins the spellings.
var metricNames = []string{
	nameCPUStddev, nameCPUMax, nameCPUMin, nameCPUMean, nameCPUSamples,
	nameMemStddev, nameMemMax, nameMemMin, nameMemMean, nameMemSamples,
}

// counters are this pipeline's label bindings of the obs families, bound ONCE
// rather than per observation so the sample path's error arm costs an atomic
// add and not a label-vector lookup — the failing case is exactly the one that
// repeats every second.
//
// They live on the SAMPLER and are bound in New, not in a package-level var
// block, because BINDING A LABEL SET PUBLISHES ITS SERIES AT ZERO
// (internal/metrics' vec.with → series.materialize, deliberately: a bound
// value is a statement that this process can produce that outcome). A var
// block runs at package initialisation, which happens because
// cmd/kubescrape-agent IMPORTS this package — so every agent in the fleet
// published fifteen kubescrape_cgroup_* series reading 0 whether or not
// -cgroup-stats was on, which is precisely the ambiguity the pipeline's own
// gauge was built to avoid (obs.RegisterCgroupStats: "a published 0 always
// means enabled and finding nothing"). An operator alerting on
// kubescrape_cgroup_read_errors_total or graphing
// kubescrape_cgroup_windows_dropped_total could not tell a healthy sampler
// from an absent one.
//
// New binds them AFTER the cgroup-version check, so a node that refuses the
// pipeline (ErrUnsupportedNode) publishes nothing either.
type counters struct {
	readCPUStat, readMemCurrent, readMemStat *metrics.RegCounter

	unresolvedPending, unresolvedUnreachable *metrics.RegCounter
	unresolvedAbandoned, unresolvedExport    *metrics.RegCounter

	cappedTracked, cappedPending *metrics.RegCounter

	listErrRoot, listErrSubtree *metrics.RegCounter

	droppedUnresolved, droppedExport, droppedTooShort *metrics.RegCounter

	heldWindows, heldExpired *metrics.RegCounter
}

func newCounters() *counters {
	// The UNLABELED families of this pipeline, published at 0 by the same rule
	// and for the same reason. metrics.Registry.Counter deliberately publishes
	// nothing at REGISTRATION (obs registers both binaries' metrics at package
	// init, so a zero there would assert "this never happened" about a feature
	// the process does not contain), and there is no exported way to
	// materialise a scalar series other than adding zero to it — which for a
	// cumulative counter is exactly the statement wanted: this outcome exists
	// here and has occurred no times. Without it, four of these five are rare
	// by design (an open error, a counter reset, a truncated scan, a
	// retirement), so "absent" and "healthy" looked identical on the one
	// pipeline that argues hardest that they must not.
	for _, c := range []*metrics.RegCounter{
		obs.CgroupSamples, obs.CgroupOpenErrors, obs.CgroupCounterResets,
		obs.CgroupScanTruncated, obs.CgroupContainersRetired,
	} {
		c.Add(0)
	}
	return &counters{
		readCPUStat:    obs.CgroupReadErrors.WithLabelValues(fileCPUStat),
		readMemCurrent: obs.CgroupReadErrors.WithLabelValues(fileMemCurrent),
		readMemStat:    obs.CgroupReadErrors.WithLabelValues(fileMemStat),

		unresolvedPending:     obs.CgroupUnresolved.WithLabelValues("pending"),
		unresolvedUnreachable: obs.CgroupUnresolved.WithLabelValues("unreachable"),
		unresolvedAbandoned:   obs.CgroupUnresolved.WithLabelValues("abandoned"),
		unresolvedExport:      obs.CgroupUnresolved.WithLabelValues("export"),

		cappedTracked: obs.CgroupContainersCapped.WithLabelValues("tracked"),
		cappedPending: obs.CgroupContainersCapped.WithLabelValues("pending"),

		listErrRoot:    obs.CgroupDiscoveryErrors.WithLabelValues("root"),
		listErrSubtree: obs.CgroupDiscoveryErrors.WithLabelValues("subtree"),

		droppedUnresolved: obs.CgroupWindowsDropped.WithLabelValues("unresolved"),
		droppedExport:     obs.CgroupWindowsDropped.WithLabelValues("export_failed"),
		// too_short is the observable half of the short-lived-container blind
		// spot; see Sampler.snapshot.
		droppedTooShort: obs.CgroupWindowsDropped.WithLabelValues("too_short"),

		heldWindows: obs.CgroupHeldWindows.WithLabelValues("held"),
		heldExpired: obs.CgroupHeldWindows.WithLabelValues("expired"),
	}
}
