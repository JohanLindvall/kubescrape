package promscrape

// The /stats/summary batcher: the metric set, the resource each statistic lands
// on, and the chunking that keeps a payload under the collector's receive limit.
//
// THE ATTRIBUTION RULE, which is what the feature is actually about: every
// statistic lands on the resource for the object it DESCRIBES, and every such
// resource is built by the code a cadvisor row goes through
// (Scraper.fillIdentityResource — the body behind both cadvisorBatcher's
// fillResource and the exported FillContainerResource that
// internal/agent/cgroupstats borrows). A summary series is only worth having
// while it joins the cadvisor series for the same object, and two series join
// exactly when their resource attributes — hence the derived Prometheus job and
// instance — are byte-identical. A second identity implementation would agree
// with this one until the first edit to either.
//
//   - a container stat  -> that container's resource (fs.type on the point)
//   - a pod stat        -> that pod's resource
//   - a volume stat     -> that POD's resource, volume named on the point
//   - a node stat       -> the summary pipeline's own node resource
//
// NAMING is OTLP-dotted, following the OpenTelemetry Collector's
// kubeletstatsreceiver verbatim wherever it has a name. This is the opposite
// call from internal/agent/cgroupstats, and the difference between the two
// cases is the whole argument: cgroupstats' gauges are a higher-resolution
// reading of a series cadvisor already publishes, so they have to sit in the
// same panel expression as it and a name in another naming system sits beside
// nothing. Here there is nothing to be adjacent to — pod ephemeral storage,
// per-container log bytes and per-volume usage appear in no cadvisor family, no
// kube-state-metrics series and (for emptyDir, and per pod) no
// kubelet_volume_stats_* series, which is why the feature exists — and the join
// this pipeline does need is a RESOURCE join, indifferent to the names on
// either side. Copying either reference exporter's unnamespaced global names
// (ephemeral_storage_pod_usage, kubelet_stats_*) would additionally make
// "run both for a week and compare" mean two producers writing one series.
//
// The divergences from the receiver, each with a mechanical reason, so nobody
// rediscovers them as bugs:
//
//   - inode counts take the unit {inode}, not the receiver's 1. The
//     OTLP->Prometheus translation appends the unit to the name and a bare "1"
//     on a GAUGE becomes _ratio, so k8s.volume.inodes renders upstream as
//     k8s_volume_inodes_ratio — a count of inodes wearing the word "ratio".
//     Brace-annotated units are never appended. Same hazard cgroupstats' metric
//     block names: the UNIT field can rewrite the NAME.
//   - k8s.volume.usage rather than the receiver's optional
//     k8s.pod.volume.usage: its four siblings are k8s.volume.*, and the value is
//     the volume's.
//   - k8s.node.filesystem.{inodes,inodes.free,inodes.used} rather than its
//     optional inode.count/inode.free pair (which has no `used` at all): one
//     inode spelling across pods, containers, volumes and nodes.
//   - k8s.node.imagefs.* / k8s.node.containerfs.* and the node process pair are
//     additions; the receiver has imagefs as a literal todo in its accumulator.

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// The units. A count is never dimensionless here: see the naming note above for
// what a bare "1" does to a gauge's name.
const (
	unitBytes     = "By"
	unitInodes    = "{inode}"
	unitProcesses = "{process}"
)

// The data-point attribute keys. fs.type's values PARTITION an additive whole
// (a container's rootfs plus its logs IS its ephemeral storage), which is the
// rule that decides attribute-versus-name throughout this pipeline: an
// attribute where the values partition an additive whole, separate names where
// they do not.
const (
	attrFsType       = "fs.type"
	attrVolumeName   = "k8s.volume.name"
	attrPVCName      = "k8s.persistentvolumeclaim.name"
	attrPVCNamespace = "k8s.persistentvolumeclaim.namespace"
)

// The two container filesystem label sets, shared and never mutated: putLabels
// only reads them, so one package-level slice each costs no per-point
// allocation.
var (
	fsTypeRootfs = []Label{{Name: attrFsType, Value: "rootfs"}}
	fsTypeLogs   = []Label{{Name: attrFsType, Value: "logs"}}
)

// summaryMetric is one exported series: name, unit, and the ONE sentence of
// description it carries.
//
// Descriptions stay short because this pipeline has the one-resource-per-object
// shape whose cost cgroupstats measured: a descriptor is repeated per resource,
// so 480-byte descriptions were 81% of a 110-container payload there. The
// argument for a name belongs in the docs; the wire carries a sentence.
type summaryMetric struct {
	name string
	unit string
	desc string
}

// The per-container set. Names and fs.type values are the kubeletstats
// receiver's optional k8s.container.ephemeral_storage.* verbatim. Its DEFAULT
// container.filesystem.usage is deliberately not also emitted: it is the rootfs
// half of this same number under a second name.
var (
	metContainerEphemeralUsage = summaryMetric{
		"k8s.container.ephemeral_storage.usage", unitBytes,
		"Bytes the container has written to the filesystem named by fs.type (rootfs or logs).",
	}
	metContainerEphemeralInodesUsed = summaryMetric{
		"k8s.container.ephemeral_storage.inodes.used", unitInodes,
		"Inodes the container uses on the filesystem named by fs.type (rootfs or logs).",
	}
)

// The per-pod set. k8s.pod.filesystem.usage is the receiver's spelling for the
// ephemeral-storage block and it is the wrong-sounding name for the right
// number — the description carries the correction, because this is the metric
// issue #10 exists for and nothing else reports it.
var (
	metPodFilesystemUsage = summaryMetric{
		"k8s.pod.filesystem.usage", unitBytes,
		"Pod ephemeral-storage bytes in use (writable layers, container logs and emptyDir) — what eviction and limits.ephemeral-storage measure.",
	}
	metPodFilesystemInodesUsed = summaryMetric{
		"k8s.pod.filesystem.inodes.used", unitInodes,
		"Inodes the pod's ephemeral storage uses.",
	}
	// Overlaps cadvisor's container_processes on the pod cgroup — but that row
	// is exactly what -cadvisor-rollups=false deletes, so this one is the pids
	// signal that survives the configuration where the other disappears.
	metPodProcessCount = summaryMetric{
		"k8s.pod.process.count", unitProcesses,
		"Processes running in the pod.",
	}
)

// fsMetrics is the six-metric family one filesystem contributes, in the order
// addFS reads the fsFields slots.
type fsMetrics struct {
	usage, available, capacity     summaryMetric
	inodes, inodesFree, inodesUsed summaryMetric
}

// The per-volume set, on the POD's resource. Five of the six are the receiver's
// defaults verbatim; k8s.volume.usage is ours (see the naming note).
//
// There is deliberately no k8s.volume.type: the receiver reads it from the pod
// SPEC, and store.trimPod strips `volumes` from the spec the metadata service
// caches, so it is not derivable here. Said out loud rather than left as a
// missing attribute to be rediscovered.
var volumeMetrics = fsMetrics{
	usage:      summaryMetric{"k8s.volume.usage", unitBytes, "Bytes in use on the volume."},
	available:  summaryMetric{"k8s.volume.available", unitBytes, "Bytes available on the volume."},
	capacity:   summaryMetric{"k8s.volume.capacity", unitBytes, "Total bytes of the volume."},
	inodes:     summaryMetric{"k8s.volume.inodes", unitInodes, "Total inodes of the volume."},
	inodesFree: summaryMetric{"k8s.volume.inodes.free", unitInodes, "Free inodes on the volume."},
	inodesUsed: summaryMetric{"k8s.volume.inodes.used", unitInodes, "Inodes in use on the volume."},
}

// The three node filesystems. Three families rather than one with an fs.name
// attribute, because they partition nothing: on a node whose runtime does not
// split imagefs they are the same device under three keys, so a sum across them
// is wrong and separate names make that query unwriteable.
var (
	nodeFsMetrics = fsMetrics{
		usage:      summaryMetric{"k8s.node.filesystem.usage", unitBytes, "Bytes in use on the node filesystem (nodefs)."},
		available:  summaryMetric{"k8s.node.filesystem.available", unitBytes, "Bytes available on the node filesystem (nodefs)."},
		capacity:   summaryMetric{"k8s.node.filesystem.capacity", unitBytes, "Total bytes of the node filesystem (nodefs)."},
		inodes:     summaryMetric{"k8s.node.filesystem.inodes", unitInodes, "Total inodes of the node filesystem (nodefs)."},
		inodesFree: summaryMetric{"k8s.node.filesystem.inodes.free", unitInodes, "Free inodes on the node filesystem (nodefs)."},
		inodesUsed: summaryMetric{"k8s.node.filesystem.inodes.used", unitInodes, "Inodes in use on the node filesystem (nodefs)."},
	}
	nodeImageFsMetrics = fsMetrics{
		usage:      summaryMetric{"k8s.node.imagefs.usage", unitBytes, "Bytes in use on the node's image filesystem (imagefs)."},
		available:  summaryMetric{"k8s.node.imagefs.available", unitBytes, "Bytes available on the node's image filesystem (imagefs)."},
		capacity:   summaryMetric{"k8s.node.imagefs.capacity", unitBytes, "Total bytes of the node's image filesystem (imagefs)."},
		inodes:     summaryMetric{"k8s.node.imagefs.inodes", unitInodes, "Total inodes of the node's image filesystem (imagefs)."},
		inodesFree: summaryMetric{"k8s.node.imagefs.inodes.free", unitInodes, "Free inodes on the node's image filesystem (imagefs)."},
		inodesUsed: summaryMetric{"k8s.node.imagefs.inodes.used", unitInodes, "Inodes in use on the node's image filesystem (imagefs)."},
	}
	nodeContainerFsMetrics = fsMetrics{
		usage:      summaryMetric{"k8s.node.containerfs.usage", unitBytes, "Bytes in use on the node's container filesystem (containerfs)."},
		available:  summaryMetric{"k8s.node.containerfs.available", unitBytes, "Bytes available on the node's container filesystem (containerfs)."},
		capacity:   summaryMetric{"k8s.node.containerfs.capacity", unitBytes, "Total bytes of the node's container filesystem (containerfs)."},
		inodes:     summaryMetric{"k8s.node.containerfs.inodes", unitInodes, "Total inodes of the node's container filesystem (containerfs)."},
		inodesFree: summaryMetric{"k8s.node.containerfs.inodes.free", unitInodes, "Free inodes on the node's container filesystem (containerfs)."},
		inodesUsed: summaryMetric{"k8s.node.containerfs.inodes.used", unitInodes, "Inodes in use on the node's container filesystem (containerfs)."},
	}
	metNodeProcessCount = summaryMetric{"k8s.node.process.count", unitProcesses, "Processes running on the node."}
	metNodeProcessMax   = summaryMetric{"k8s.node.process.max", unitProcesses, "Maximum PIDs the node allows."}
)

// summaryMetrics is the exported set with the object each metric describes, for
// the test that pins the spellings and the units (cgroupstats' metricNames, one
// column wider). A name is wire-visible and a unit rewrites a name, so both
// belong under an exact-match assertion rather than a length check.
var summaryMetrics = []struct {
	m     summaryMetric
	level string
}{
	{metContainerEphemeralUsage, levelContainer},
	{metContainerEphemeralInodesUsed, levelContainer},

	{metPodFilesystemUsage, levelPod},
	{metPodFilesystemInodesUsed, levelPod},
	{metPodProcessCount, levelPod},

	{volumeMetrics.usage, levelVolume},
	{volumeMetrics.available, levelVolume},
	{volumeMetrics.capacity, levelVolume},
	{volumeMetrics.inodes, levelVolume},
	{volumeMetrics.inodesFree, levelVolume},
	{volumeMetrics.inodesUsed, levelVolume},

	{nodeFsMetrics.usage, levelNode},
	{nodeFsMetrics.available, levelNode},
	{nodeFsMetrics.capacity, levelNode},
	{nodeFsMetrics.inodes, levelNode},
	{nodeFsMetrics.inodesFree, levelNode},
	{nodeFsMetrics.inodesUsed, levelNode},
	{nodeImageFsMetrics.usage, levelNode},
	{nodeImageFsMetrics.available, levelNode},
	{nodeImageFsMetrics.capacity, levelNode},
	{nodeImageFsMetrics.inodes, levelNode},
	{nodeImageFsMetrics.inodesFree, levelNode},
	{nodeImageFsMetrics.inodesUsed, levelNode},
	{nodeContainerFsMetrics.usage, levelNode},
	{nodeContainerFsMetrics.available, levelNode},
	{nodeContainerFsMetrics.capacity, levelNode},
	{nodeContainerFsMetrics.inodes, levelNode},
	{nodeContainerFsMetrics.inodesFree, levelNode},
	{nodeContainerFsMetrics.inodesUsed, levelNode},
	{metNodeProcessCount, levelNode},
	{metNodeProcessMax, levelNode},
}

// The object levels, used for the unresolved accounting and by the metric
// table above.
const (
	levelPod       = "pod"
	levelContainer = "container"
	levelVolume    = "volume"
	levelNode      = "node"
)

// summaryTarget names the resource one statistic lands on. It is resolved on
// every add rather than held as a handle, because a chunk flush replaces the
// payload and would leave a held ScopeMetrics pointing into the batch that has
// already been sent.
type summaryTarget struct {
	ident cadvisorIdentity
	node  bool
	// level is the object this target describes, for the unresolved accounting;
	// only pod and container can fail to resolve.
	level string
}

// summaryNodeKey is the node resource's slot in the batch. cadvisorIdentity's
// appendKey always starts with 'u' or 'n', so a NUL-prefixed key cannot collide
// with an object's.
const summaryNodeKey = "\x00node"

// summaryScope is one object's place in the current batch: the scope its
// metrics are appended to, plus the materialized resource key its metrics are
// memoized under.
type summaryScope struct {
	sm  pmetric.ScopeMetrics
	key string
}

// summaryBatcher accumulates one /stats/summary conversion, chunked by the same
// BatchPoints/BatchBytes bound every other batcher obeys.
//
// It deliberately does NOT reuse cadvisorBatcher.addNumber. That path runs
// drop(), whose pod-level arm deletes a pod-level row of any family that is not
// container_network_* — so under -cadvisor-rollups=false every pod-level summary
// series would silently disappear, on a non-default flag, with nothing counting
// it. The rollup filter is about cadvisor's cgroup hierarchy and has no meaning
// for a payload the kubelet has already attributed.
type summaryBatcher struct {
	s        *Scraper
	ctx      context.Context
	url      string
	scrapeTS pcommon.Timestamp

	md     pmetric.Metrics
	scopes map[string]summaryScope
	byKey  map[string]pmetric.Metric
	points int
	bytes  int
	// scraped is every point the document PRODUCED, across chunks and BEFORE
	// the filter: points resets with the batch and is the chunk bound's input,
	// while this is what the scrape reports as its sample count. Pre-filter
	// because kubescrape_scrape_samples_total says "before filtering" and
	// kubescrape_scrape_samples_dropped_total is the term subtracted from it —
	// see add().
	scraped int
	keyBuf  []byte

	// filter is the per-scrape memoizing view of metrics.pipelines.summary. It
	// sees the OTLP metric name and the DATA POINT attributes, so a rule can
	// select on k8s.volume.name — the pipeline's cardinality lever.
	filter  *filterSession
	dropped int

	// exportErr is sticky so a failed chunk export can travel out of a
	// lightning ArrayEach callback without being read as a decode failure.
	exportErr error

	malformed struct{ pods, containers, volumes, node int }
	// unresolved counts objects exported with their LABEL identity because the
	// metadata service could not place them; see object().
	unresolved struct{ pods, containers int }

	// The GetPaths output buffers, one per element shape so a nested walk cannot
	// clobber the slots its caller is still reading.
	podSlots, ctrSlots, volSlots, nodeSlots [][]byte
	// volLabels backs one volume's data-point attributes; putLabels copies them
	// into the point immediately, so one buffer serves every volume.
	volLabels []Label
}

func newSummaryBatcher(s *Scraper, ctx context.Context, url string, scrape time.Time) *summaryBatcher {
	sb := &summaryBatcher{
		s:         s,
		ctx:       ctx,
		url:       url,
		scrapeTS:  pcommon.NewTimestampFromTime(scrape),
		filter:    s.cfg.Filters.filterFor(pipelineSummary).session(),
		volLabels: make([]Label, 0, 3),
	}
	sb.reset()
	return sb
}

func (sb *summaryBatcher) reset() {
	sb.md = pmetric.NewMetrics()
	if sb.scopes == nil {
		sb.scopes = make(map[string]summaryScope)
		sb.byKey = make(map[string]pmetric.Metric)
	} else {
		// The handles point into the payload that was just taken.
		clear(sb.scopes)
		clear(sb.byKey)
	}
	sb.points = 0
	sb.bytes = 0
}

func (sb *summaryBatcher) take() pmetric.Metrics {
	md := sb.md
	sb.reset()
	return md
}

func (sb *summaryBatcher) count() int { return sb.points }
func (sb *summaryBatcher) size() int  { return sb.bytes }

// flushIfFull ships the accumulated chunk once it reaches either batch bound.
// It is safe between any two points: nothing holds a scope across a call,
// because every add resolves its resource afresh.
func (sb *summaryBatcher) flushIfFull() error {
	if !sb.s.chunkFull(sb) {
		return nil
	}
	return sb.flush()
}

// flush exports the accumulated chunk, if any. An empty payload is not sent: it
// costs an OTLP RPC and, with -buffer-dir, a durable queue record.
func (sb *summaryBatcher) flush() error {
	if sb.points == 0 {
		return nil
	}
	if err := sb.s.cfg.Exporter.ExportMetrics(sb.ctx, sb.take()); err != nil {
		sb.exportErr = err
		return err
	}
	return nil
}

// scope returns the ScopeMetrics for a target's resource, creating it on first
// use per chunk.
func (sb *summaryBatcher) scope(t summaryTarget) summaryScope {
	if t.node {
		if sc, ok := sb.scopes[summaryNodeKey]; ok {
			return sc
		}
		return sb.newScope(summaryNodeKey, sb.fillNodeResource)
	}
	sb.keyBuf = t.ident.appendKey(sb.keyBuf[:0])
	if sc, ok := sb.scopes[string(sb.keyBuf)]; ok { // no alloc: the map read elides the copy
		return sc
	}
	return sb.newScope(string(sb.keyBuf), func(res pcommon.Resource) {
		// ONE identity path, shared with the cadvisor batcher and with
		// FillContainerResource: that is what makes these series join the cadvisor
		// series for the same object, and a copy would agree until the first edit.
		//
		// An UNRESOLVED object is still exported, which is cadvisor's policy and
		// deliberately not cgroupstats'. The asymmetry is the reason both exist: a
		// cgroup path yields two hex ids and no service.name, so such a series has
		// no Prometheus job and joins nothing — while the summary carries pod name,
		// namespace, uid and container name, i.e. MORE identity than a cadvisor
		// row, so fillIdentityResource's label fallback produces exactly what an
		// unresolved cadvisor row for the same object produces. Consistency in both
		// states is the point: resolved or not, the two rows are built by one
		// function from the same inputs.
		if resolved, _ := sb.s.fillIdentityResource(sb.ctx, res, t.ident); !resolved {
			switch t.level {
			case levelPod:
				sb.unresolved.pods++
			case levelContainer:
				sb.unresolved.containers++
			}
		}
	})
}

func (sb *summaryBatcher) newScope(key string, fill func(pcommon.Resource)) summaryScope {
	rm := sb.md.ResourceMetrics().AppendEmpty()
	fill(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(scopeNameSummary)
	sm.Scope().SetVersion(obs.ScopeVersion)
	sc := summaryScope{sm: sm, key: key}
	sb.scopes[key] = sc
	// One resource per described object: its attributes are charged where it is
	// created, or a payload of hundreds of enriched resources flushes past the
	// collector's receive limit the estimate exists to respect.
	sb.bytes += resourceBytes(rm.Resource(), scopeNameSummary)
	return sc
}

// fillNodeResource builds the node-level resource: the /metrics scrape's shape
// (service.name=kubelet + the node attributes) under the SUMMARY pipeline's
// builder, whose default instance prefix is what keeps the two apart. Without
// it both would derive (job=kubelet, instance=<node>) while differing on
// url.full — one series described by two attribute sets, flapping every cycle.
//
// The node itself is always s.nodeInfo(), never the nodeName the payload
// reports, so this resource stays consistent with every other pipeline's on
// this agent; a disagreement between the two is warned about where it is
// noticed (addNode).
func (sb *summaryBatcher) fillNodeResource(res pcommon.Resource) {
	a := res.Attributes()
	a.PutStr("service.name", "kubelet")
	a.PutStr("url.full", sb.url)
	sb.s.attrsFor(pipelineSummary).Build(res, attrs.Context{Node: sb.s.nodeInfo()})
}

// metric returns the (per-resource) gauge for one metric name, creating it on
// first use per chunk.
func (sb *summaryBatcher) metric(sc summaryScope, met summaryMetric) pmetric.Metric {
	sb.keyBuf = append(sb.keyBuf[:0], sc.key...)
	sb.keyBuf = append(sb.keyBuf, 0)
	sb.keyBuf = append(sb.keyBuf, met.name...)
	if m, ok := sb.byKey[string(sb.keyBuf)]; ok { // no alloc: the map read elides the copy
		return m
	}
	key := string(sb.keyBuf) // materialize once per new metric per chunk
	m := sc.sm.Metrics().AppendEmpty()
	m.SetName(met.name)
	// Every metric here is a Gauge: no start timestamp to carry, no cumulative
	// reset to represent, and the only cumulative-looking candidates the kubelet
	// schema offers (usageCoreNanoSeconds, pageFaults) are in the cpu/memory set
	// this pipeline does not export.
	m.SetEmptyGauge()
	sb.byKey[key] = m
	// The description and unit are charged with the descriptor: a
	// resource-per-object batch repeats every descriptor per object, and
	// uncharged text is what flushes a chunk past the receive limit.
	sb.bytes += chargeDescriptor(m, met.name, metricMeta{help: met.desc, unit: met.unit})
	return m
}

// addOpt emits one OPTIONAL statistic: the single place a decoded slot becomes
// a data point, and therefore the single place the kubelet's nil-is-not-zero
// rule is enforced. A slot that is absent, null or unreadable produces no point
// at all — never a zero, which would read as a container using no ephemeral
// storage and be both wrong and invisible.
func (sb *summaryBatcher) addOpt(t summaryTarget, met summaryMetric, raw []byte, tsMs int64, labels []Label) {
	v, ok := parseCount(raw)
	if !ok {
		return
	}
	sb.add(t, met, v, tsMs, labels)
}

func (sb *summaryBatcher) add(t summaryTarget, met summaryMetric, v uint64, tsMs int64, labels []Label) {
	// Counted BEFORE the filter, at the seam scrapeSession.accept counts an
	// exposition sample at. kubescrape_scrape_samples_total documents itself as
	// the count "before filtering" and kubescrape_scrape_samples_dropped_total
	// is the term subtracted from it; counting a point only once it had survived
	// the filter made that arithmetic under-report this pipeline by exactly the
	// number of points an operator's own keep/drop rules removed — so a filter
	// that emptied the pipeline showed as a pipeline with nothing to say. A stat
	// the kubelet did not report is not counted anywhere, which is right: there
	// is no sample to have (addOpt returns before reaching this).
	sb.scraped++
	if !sb.filter.Keep(met.name, labels) {
		sb.dropped++
		return
	}
	m := sb.metric(sb.scope(t), met)
	dp := m.Gauge().DataPoints().AppendEmpty()
	// Int, not Double: every value here is a count of bytes, inodes or
	// processes, and parseCount has already refused anything a float64 could not
	// hold exactly.
	dp.SetIntValue(int64(v))
	dp.SetTimestamp(pointTS(tsMs, sb.scrapeTS))
	putLabels(dp.Attributes(), labels)
	sb.points++
	sb.bytes += numberBytes(Sample{Labels: labels})
}

// addFS emits one filesystem's six statistics from the fsFields slots at base,
// all stamped with that filesystem's own time.
func (sb *summaryBatcher) addFS(t summaryTarget, fm fsMetrics, slots [][]byte, base int, labels []Label) {
	ts := statTime(slots[base+fTime])
	sb.addOpt(t, fm.usage, slots[base+fUsed], ts, labels)
	sb.addOpt(t, fm.available, slots[base+fAvailable], ts, labels)
	sb.addOpt(t, fm.capacity, slots[base+fCapacity], ts, labels)
	sb.addOpt(t, fm.inodes, slots[base+fInodes], ts, labels)
	sb.addOpt(t, fm.inodesFree, slots[base+fInodesFree], ts, labels)
	sb.addOpt(t, fm.inodesUsed, slots[base+fInodesUsed], ts, labels)
}

// report tallies the scrape's accounting, once, from a defer — the abort paths
// have to report too: a scrape that failed on its last chunk still filtered and
// still skipped whatever it skipped.
func (sb *summaryBatcher) report() {
	if sb.dropped > 0 {
		obs.ScrapeSamplesDropped.WithLabelValues(pipelineSummary, "filter").Add(float64(sb.dropped))
	}
	m := sb.malformed
	if n := m.pods + m.containers + m.volumes + m.node; n > 0 {
		// Per-element degradation counted the way a malformed exposition line is:
		// data was dropped, and this is the metric that names the cause. The log
		// line is deduped per endpoint — nothing about a kubelet's broken payload
		// changes between cycles, and the counter is the ongoing signal.
		obs.ScrapeMalformed.WithLabelValues(pipelineSummary).Add(float64(n))
		sb.s.warnOnce("summarymalformed:"+sb.url,
			"the kubelet's /stats/summary had elements that could not be decoded; they were skipped and the rest of the document was exported",
			"url", sb.url, "pods", m.pods, "containers", m.containers, "volumes", m.volumes, "node", m.node)
	}
	if u := sb.unresolved; u.pods+u.containers > 0 {
		// Counted per OBJECT, not per point, and split by which kind of object:
		// the two have different causes (a pod the service has not seen, versus a
		// pod it has whose container name is not in it) and the log line below
		// says it once for the whole process. A condition nobody can alert on is
		// a condition nobody knows about — the closest sibling is
		// kubescrape_cgroup_unresolved_total, which exists for exactly this.
		if u.pods > 0 {
			obs.SummaryUnresolved.WithLabelValues(levelPod).Add(float64(u.pods))
		}
		if u.containers > 0 {
			obs.SummaryUnresolved.WithLabelValues(levelContainer).Add(float64(u.containers))
		}
		// What is lost here is the JOIN, not the series: an unresolved container
		// resource has no container.id, so its service.instance.id is
		// cadvisor-<uid>/<name> where the cadvisor row for the same container has
		// cadvisor-<id>, and the two stop lining up until a lookup succeeds. It is
		// bounded and self-healing, so the LOG line is said once and the counter
		// above is the ongoing signal — a persisting condition, complained about
		// the way internal/logdedupe says to.
		sb.s.warnOnce("summaryunresolved:"+sb.url,
			"the metadata service could not place objects in the kubelet's /stats/summary; they are exported with their label identity, but until the lookups succeed their series do not join the cadvisor series for the same objects",
			"url", sb.url, "pods", u.pods, "containers", u.containers)
	}
}
