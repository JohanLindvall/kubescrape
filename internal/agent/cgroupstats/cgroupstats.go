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
// series they sit beside, and see the metric-name block (names.go) for why the
// set grew from six.
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
// that the consumer can put the two side by side. See the name constants in
// names.go (nameCPUStddev and its siblings) for the exact spellings and why the
// CPU ones drop cadvisor's _seconds suffix.
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
// container, so the period is what divides that cost. At MinInterval a
// 200-container node already issues 6000 reads a second; ten times faster is
// not ten times more insight into a burst, it is a busy loop on the shared
// node agent. Values below it are refused where an operator typed one
// (-cgroup-stats-interval, checkFlagValues) and clamped where one arrives
// programmatically (New).
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
// (see Sampler.finalWindowLocked for the three guards that keep the count
// meaning that, for the two neighbours that share the verdict without being
// short-lived at all, and for what it deliberately excludes). In the measured
// case below it fires for a container whose DISCOVERY landed within TWO
// SAMPLING PERIODS of its death — a container found earlier in its life is
// sampled for the remainder, and any more than two seconds of remainder
// describes it — so of
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
// Only entries that HOLD descriptors are counted against it (countsLocked's
// live count, not len(tracked)), and that rule has had to be applied twice. It
// used to be tested against tracked+pending, which spent an fd budget on
// entries that own no fd — and since every pod's sandbox cgroup is permanently
// pending, a large node's sandboxes crowded out its real workload containers,
// which is precisely backwards. The same defect then survived inside the tracked map itself: a
// GONE container has already released its three descriptors and lingers only
// until the export that carries its final window, so on a dense node a batch of
// exits refused newly resolved live containers for up to one whole export
// window, on a budget nobody was spending.
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
	// property — except a genuine cgroup v1 hierarchy, which is the node's at
	// any root; see v1Error).
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
	// lastOpenErr is the most recent failure to open a resolved container's
	// files (an *os.PathError naming the file), kept for the nothing-is-sampled
	// warning; see pendingContainer.unreadable. Guarded by mu.
	lastOpenErr error

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

	// The three ways to export nothing are throttled SEPARATELY: they have
	// different causes and different fixes, so one must not silence another
	// for the throttle window (a node that starts empty and then fills with
	// cgroups nobody can resolve would report only the first; a metadata
	// outage that heals onto cgroups whose files will not open would keep
	// blaming the metadata service for ten minutes).
	emptyWarn      logdedupe.Throttle
	unresolvedWarn logdedupe.Throttle
	unreadableWarn logdedupe.Throttle
	// readWarn names ONE offending cgroup path per interval when reads start
	// failing. The counters are per FILE, which says what broke but never
	// where; a line per container per second would be a flood proportional to
	// the node's container count, which is exactly what logdedupe exists for.
	readWarn logdedupe.Throttle
	listWarn logdedupe.Throttle
	// rootWarn is the ROOT-listing complaint, throttled apart from listWarn
	// for the reason walk() already counts the two apart: an unreadable root
	// is a missing mount and a silent pipeline, an unreadable subtree is one
	// directory inside a working hierarchy. A mount that flaps between the two
	// must not have whichever fired first suppress the other.
	rootWarn logdedupe.Throttle
	// retireWarn is its own throttle rather than readWarn's: a retirement is
	// the CONCLUSION of a run of read failures, and sharing the throttle would
	// let the failures suppress the line that says what was done about them.
	retireWarn logdedupe.Throttle
}

// container is one tracked container cgroup: its identity, the three open
// descriptors the sample path preads, and the state of the window in progress.
type container struct {
	cgroupRef // where it lives; dir can move to a better path (repointLocked)

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
	// shortly after an export"; see finalWindowLocked's too_short verdict.
	described bool
	// atShutdown distinguishes the two ways gone gets set: the cgroup left the
	// hierarchy (reconcile) or this PROCESS is leaving (stop). Only the first
	// is evidence about the container, which is what finalWindowLocked's
	// too_short verdict counts.
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
	cgroupRef
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
	// definitive miss or after a resolved retry whose files still would not
	// open, reconsiderEvery after a miss the service could not answer or a
	// resolved retry the descriptor cap refused (see track).
	nextTry time.Time
	gaveUp  bool
	// unreadable records that the LATEST attempt resolved the identity and the
	// cgroup's files then failed — the open in track, or the dead reads that
	// retired it (quarantineLocked). It is what lets warnIfExportingNothing
	// tell "nothing resolves" (a metadata problem) from "everything resolves
	// and nothing opens" (a hierarchy problem); a later unresolved attempt
	// clears it.
	unreadable bool
}

// pendingSnap is one resolution request, copied out from under the lock. It
// carries lastTry only so the pass can be ordered by it (see resolvePending).
type pendingSnap struct {
	id, podUID string
	lastTry    time.Time
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
		var v1 *v1Error
		if cfg.Root == "" || errors.As(err, &v1) {
			// The DEFAULT root, or ANY root holding a genuine cgroup v1
			// hierarchy. What we just learned is a property of this NODE, not
			// of anything the operator typed, and it is identical on every node
			// of its kind — so the caller disables this pipeline and keeps the
			// others (ErrUnsupportedNode). A v1 hierarchy at an explicit root
			// proves the operator pointed at the node's cgroup mount; the chart
			// renders an explicit root by default, so reading it as an operator
			// error would CrashLoop every v1 node of a mixed fleet (v1Error).
			return nil, fmt.Errorf("%w: %w", ErrUnsupportedNode, err)
		}
		// An explicitly configured root that is not a cgroup hierarchy this
		// sampler can use (nothing mounted there, a typo) is an operator error,
		// uniform across the fleet and fixable by editing one value: fatal, as
		// loudly as possible.
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
//
// There is a THIRD way, and it must not borrow the second's words: cgroups
// that RESOLVE and whose files then will not open (or were retired for reads
// that all failed). Those are pending too, so they used to be reported as
// "resolved none", with a hint sending the operator to the metadata service
// and a counter that moves on every healthy node anyway (the sandboxes) —
// while the counter that was actually moving, kubescrape_cgroup_open_errors_total,
// was named nowhere. The per-entry mark (pendingContainer.unreadable) is what
// tells the two apart, and each arm has its OWN throttle, so the arm is chosen
// BEFORE a slot is claimed: claiming one shared slot first let whichever line
// fired first silence the other for the whole window. That means the pending
// set is counted under mu on every discovery pass while nothing is sampled —
// one more walk beside the one resolvePending already makes, bounded by
// maxPending, and only on a node that is exporting nothing.
func (s *Sampler) warnIfExportingNothing() {
	if s.nSampled.Load() > 0 {
		return
	}
	pending := s.nPending.Load()
	if pending == 0 {
		s.warnIfEmpty()
		return
	}
	s.mu.Lock()
	unreadable, lastErr := 0, s.lastOpenErr
	for _, p := range s.pending {
		if p.unreadable {
			unreadable++
		}
	}
	s.mu.Unlock()
	if unreadable > 0 {
		if !s.unreadableWarn.Allow(emptyWarnEvery) {
			return
		}
		args := []any{"root", s.root, "cgroups", unreadable,
			"hint", "their identities resolved; their cgroup files did not open or read — check kubescrape_cgroup_open_errors_total and kubescrape_cgroup_read_errors_total, and that the memory controller is enabled on the container cgroups (without it they have no memory.current or memory.stat)",
		}
		if lastErr != nil {
			args = append(args, "error", lastErr)
		}
		s.log.Warn("cgroup sampler resolved container cgroups but could not read their files: it is running and will export nothing", args...)
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

// countsLocked is the ONE definition of which tracked containers count, shared
// by the descriptor cap (track) and the gauges (publishCountsLocked). Caller
// holds mu.
//
// live is the tracked containers that still HOLD descriptors, which is what the
// maxContainers cap bounds. A gone container is excluded: it released its three
// descriptors in markGone and stays in the map only until the export that
// carries its final window, so charging it against a descriptor budget refuses
// a live container for descriptors nobody holds.
//
// sampled is the live containers currently producing data. SAMPLED is not the
// same as TRACKED: a container that is still listed but has stopped answering
// every read produces no data at all, and counting it said the node was
// measuring 110 containers while it was measuring 109. It rejoins the count as
// soon as a read succeeds (and leaves the tracked set altogether at
// maxDeadWindows). The lag either way is one export window, which is inherent —
// the streak is per window.
func (s *Sampler) countsLocked() (live, sampled int) {
	for _, c := range s.tracked {
		if c.gone {
			continue
		}
		live++
		if !c.unreadable() {
			sampled++
		}
	}
	return live, sampled
}

// publishCountsLocked refreshes the atomics the gauges read; the gauge is the
// SAMPLED count (see countsLocked). Caller holds mu.
func (s *Sampler) publishCountsLocked() {
	live, sampled := s.countsLocked()
	s.nSampled.Store(int64(sampled))
	s.nPending.Store(int64(len(s.pending)))
	// DISCOVERED is what is in the hierarchy, so an unreadable-but-listed
	// container still counts here: it was found, and it is the difference
	// between this and the sampled gauge that says so.
	s.nDiscover.Store(int64(live + len(s.pending)))
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
	wg.Go(func() { s.exportLoop(ctx, exp, exportEvery) })
	wg.Go(func() { s.discoverLoop(ctx) })
	s.sampleLoop(ctx)
	wg.Wait()
	s.stop()
}

// stop releases every held descriptor and marks every container for one final
// flush, which FinalExport then carries. Called by Run once all three loops
// have stopped.
//
// The mark is atShutdown, NOT the plain vanished-from-the-hierarchy one, and
// the difference is a loss counter's credibility. finalWindowLocked counts a
// gone container that never produced a distribution as
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
