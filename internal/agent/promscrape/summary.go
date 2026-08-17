package promscrape

// The kubelet's /stats/summary scrape: the THIRD kubelet endpoint, and the one
// whose payload is JSON rather than Prometheus exposition.
//
// What it is for. The claim worth making is narrow, because the obvious wider
// one is false: /metrics/cadvisor is NOT silent about filesystems. It carries
// container_fs_usage_bytes, container_fs_limit_bytes, container_fs_inodes_free
// and container_fs_inodes_total (the kubelet enables cadvisor's DiskUsage set
// whenever LocalStorageCapacityIsolation is on, which is its default), keyed by
// DEVICE. What cadvisor has no concept of is
//
//   - the kubelet's EPHEMERAL-STORAGE accounting — a container's writable layer
//     plus its logs, a pod's containers plus its on-disk emptyDirs — which is
//     the quantity the eviction manager and limits["ephemeral-storage"] are
//     measured against, and which kube-state-metrics has only the LIMIT of;
//   - a container's LOG bytes, which live on nodefs outside its cgroup;
//   - a VOLUME of any kind (kubelet_volume_stats_* on /metrics covers only
//     PVC-backed ones, labelled by namespace and PVC, never by pod);
//   - an inodes-USED number, at any level: it publishes free and total only;
//   - which node filesystem is nodefs, imagefs or containerfs — the roles the
//     eviction thresholds are written against, where cadvisor has device names.
//
// The rest of what this pipeline emits DOES overlap, and is written down here
// rather than left to be rediscovered as a bug: the three node filesystem
// families restate cadvisor's root-cgroup (id="/") rows — available and
// inodes.used as a subtraction — and add only the role above;
// k8s.pod.process.count is the kubelet's sum of its containers' cadvisor
// process counts, which container_processes already carries per container (and
// still carries under -cadvisor-rollups=false); and on a PVC-backed volume
// the six k8s.volume.* restate the six kubelet_volume_stats_* that the -node-
// metrics scrape already collects, adding the pod attribution those lack.
// Everything the two endpoints BOTH cover outright (cpu, memory, network, swap)
// is deliberately not exported here; see summarybatch.go.
//
// Three properties are load-bearing and each is defended by a rule below:
//
//   - THE TRANSPORT IS THE CADVISOR SCRAPE'S. Same kubeletGet, same rotating
//     ServiceAccount token, same skip-verify TLS client, same kubeletTimeout
//     clamp, same schedule slot mechanics. A second copy would drift on the
//     first edit to either, and the failure would be a node whose third kubelet
//     scrape outlives the cycle it runs in.
//
//   - AN ABSENT FIELD IS NOT A ZERO. The kubelet's schema is pointers
//     throughout and a nil means NOT REPORTED. The decode is lightning GetPaths
//     against a fixed path table, so an absent leaf is a nil SLOT — a shape no
//     struct field can have — and addOpt is the one place that turns a slot into
//     a data point. A container reported as using 0 bytes of ephemeral storage
//     looks healthy and is unmeasured, which is the worst outcome this pipeline
//     can produce.
//
//   - PER-ELEMENT DEGRADATION, pkg/promparse' rule applied to JSON: an element
//     that does not decode costs its own metrics and nothing else, while a
//     DOCUMENT-level failure fails the scrape. That split is exactly the one
//     internal/agent/azurediag makes between a malformed envelope and a
//     malformed record, and it has the same boundary: the array WALK is strict,
//     so a byte-level syntax error inside the pods array is a document failure
//     (nothing can say where the next element starts), while an element that
//     walks but is not a decodable pod is skipped and counted.
//
// k8s.io/kubelet is deliberately NOT a dependency. Beyond the module cost, a
// hand-declared copy of a ~400-line upstream schema is a copy that drifts, and
// decoding into it inflates a ~500 KB document into tens of thousands of
// objects every scrape interval — for the ~20 leaves this pipeline exports.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	ljson "github.com/JohanLindvall/lightning/pkg/json"

	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/logline"
)

// summaryPath is the kubelet's stats endpoint, relative to Kubelet.Endpoint.
const summaryPath = "/stats/summary"

// maxSummaryBytes bounds the body read into memory. lightning is slice-based,
// so the whole document is resident during the walk; a kubelet is the node's
// own process, but it is still a response this agent does not control, and
// every other body-bearing seam in this repo carries a cap (this is the same 16
// MiB otlpingest uses for a push). No flag: a bound nobody can usefully tune is
// a constant, and CLAUDE.md's rule is that a config surface becomes a config
// SECTION rather than a new flag.
//
// A dense node's summary is ~0.5 MB, so the cap is ~30x the realistic worst
// case; hitting it fails the scrape rather than exporting a prefix, because a
// truncated JSON document has no meaning at all.
const maxSummaryBytes = 16 << 20

// maxExactCount is the largest integer float64 holds exactly. Every value this
// pipeline emits is a count of bytes, inodes or processes and is far below it
// (2^53 bytes is 9 PB), so parsing through float64 is lossless — and a value
// ABOVE it is refused rather than rounded, since a number we cannot read
// faithfully is not a number we should publish.
const maxExactCount = 1 << 53

// scrapeSummary scrapes <kubelet>/stats/summary and exports it as the pod,
// container, volume and node gauges of summarybatch.go. It returns the number
// of data points the document produced BEFORE the series filter, which is what
// kubescrape_scrape_samples_total counts for every pipeline (there are no
// exposition samples to count here, so a reported statistic is the sample);
// what the filter then removed is the dropped counter, and the difference is
// what reached the collector.
func (s *Scraper) scrapeSummary(ctx context.Context) (int, error) {
	ctx, cancel := s.scrapeContext(ctx, s.kubeletTimeout(), pipelineSummary)
	defer cancel()

	url := strings.TrimRight(s.cfg.Kubelet.Endpoint, "/") + summaryPath
	resp, err := s.kubeletGet(ctx, url, acceptJSON)
	if err != nil {
		s.reportSummaryRefusal(url, err)
		return 0, err
	}
	defer drainClose(resp.Body)

	body, err := readCapped(resp.Body, maxSummaryBytes)
	if err != nil {
		return 0, err
	}
	return s.convertSummary(ctx, url, body, time.Now())
}

// reportSummaryRefusal names the RBAC rule behind a 403.
//
// The kubelet authorizes each endpoint against its own subresource: /metrics
// and /metrics/cadvisor against nodes/metrics, which is all the agent's
// ClusterRole has historically held, and /stats/* against nodes/stats. So the
// first-contact failure of this pipeline is a 403 on every node in the fleet
// while the other two kubelet scrapes keep working — and "status 403" against a
// kubelet is indistinguishable from an expired token, a bad audience or a
// webhook authorizer outage. Once per process, keyed on the endpoint like every
// other per-target complaint (there is nothing new to say until someone edits
// the ClusterRole).
func (s *Scraper) reportSummaryRefusal(url string, err error) {
	var se *kubeletStatusError
	if !errors.As(err, &se) || se.code != http.StatusForbidden {
		return
	}
	s.warnOnce("summaryrbac:"+s.cfg.Kubelet.Endpoint,
		`the kubelet refused /stats/summary: it authorizes that endpoint against the nodes/stats subresource, not the nodes/metrics the cadvisor and node scrapes use — add {apiGroups: [""], resources: ["nodes/stats"], verbs: ["get"]} to the agent ClusterRole`,
		"url", url)
}

// readCapped reads r whole, refusing a body over limit. The limit+1 read is
// what tells "exactly at the limit" from "over it": a LimitReader stopping at
// the limit yields a truncated document that would decode as a document-level
// error and blame the kubelet for a bound of ours.
func readCapped(r io.Reader, limit int) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > limit {
		return nil, fmt.Errorf("body over the %d-byte maxSummaryBytes cap", limit)
	}
	return b, nil
}

// convertSummary decodes one summary document into gauges and exports them.
//
// Order is node first, then pods, which is the document's own; what matters is
// that the FINAL export happens only on success. A document-level failure
// therefore drops whatever was accumulated instead of shipping it: unlike a
// streamed exposition — where everything before the abort was parsed from
// complete lines and salvage is worth it — a document that does not have the
// shape assumed says nothing trustworthy about the part already walked. A chunk
// that already flushed mid-walk (a node far past BatchBytes) stays flushed;
// every metric here is a Gauge, so a partial payload costs only the missing
// series.
func (s *Scraper) convertSummary(ctx context.Context, url string, body []byte, scrape time.Time) (int, error) {
	// Handoff to the transform seam, exactly as newScrapeSession marks it: every
	// chunk is take()n out of the batcher and never re-sent — a failure fails the
	// scrape and the next cycle re-fetches from the kubelet — so the transform
	// wrapper may run its script in place instead of deep-copying the payload.
	ctx = transform.Handoff(ctx)
	sb := newSummaryBatcher(s, ctx, url, scrape)
	defer sb.report()

	if err := sb.addNode(body); err != nil {
		return sb.scraped, err
	}
	if err := sb.addPods(body); err != nil {
		return sb.scraped, err
	}
	if err := sb.flush(); err != nil {
		return sb.scraped, err
	}
	return sb.scraped, nil
}

// addPods walks the document's pods array. A pod element that does not decode
// is counted and skipped; anything wrong with the ARRAY — absent, not an array,
// a broken token between two elements — fails the scrape, because it is a
// statement about the document rather than about one pod.
func (sb *summaryBatcher) addPods(body []byte) error {
	err := ljson.ArrayEach(body, func(elem []byte) error {
		if !sb.addPod(elem) {
			sb.malformed.pods++
		}
		// The only error a callback may raise: an export failure has to abort the
		// walk, and it must not be mistaken for a decode failure on the way out
		// (hence the sticky exportErr the caller below reads).
		return sb.flushIfFull()
	}, "pods")
	if err != nil && sb.exportErr == nil {
		return fmt.Errorf("decoding the pods array: %w", err)
	}
	return err
}

// addPod emits one pod element's own statistics and walks its containers and
// volumes. It reports whether the element decoded; a false is the caller's
// signal to count it, never to fail the scrape.
func (sb *summaryBatcher) addPod(elem []byte) bool {
	slots, err := ljson.GetPaths(elem, podPaths, sb.podSlots)
	sb.podSlots = slots
	if err != nil {
		return false
	}
	ns, pod, uid := rawString(slots[pPodNamespace]), rawString(slots[pPodName]), rawString(slots[pPodUID])
	if ns == "" || pod == "" {
		// Nothing can place a pod the payload does not name, and a resource with
		// no identity would collect every such element into one anonymous series.
		return false
	}
	t := summaryTarget{
		ident: cadvisorIdentity{namespace: ns, pod: pod, podUID: uid},
		level: levelPod,
	}
	// The pod's ephemeral-storage block. Its available/capacity/inodes/inodesFree
	// are NOT read: the kubelet fills them from the node's rootfs stats verbatim,
	// so they are the node's numbers copied onto every pod — 880 constant series
	// on a 110-pod node, exported once at node level instead.
	ephTS := statTime(slots[pPodEphTime])
	sb.addOpt(t, metPodFilesystemUsage, slots[pPodEphUsed], ephTS, nil)
	sb.addOpt(t, metPodFilesystemInodesUsed, slots[pPodEphInodesUsed], ephTS, nil)
	// ProcessStats carries no time of its own; pointTS then stamps the scrape's.
	sb.addOpt(t, metPodProcessCount, slots[pPodProcessCount], 0, nil)

	sb.addVolumes(elem, t.ident)
	sb.addContainers(elem, t.ident)
	return true
}

// addVolumes emits one pod's volume statistics ONTO THE POD'S RESOURCE, with
// the volume named on the data points.
//
// This is the deliberate departure from the OTel kubeletstats receiver, which
// mints a resource per volume, and the reason is this repo's identity model:
// attrs.Identity derives service.instance.id from the resource and has no
// volume component, so N per-volume resources plus the pod's own would be N+1
// resources sharing one (job, instance) with different attribute sets — a
// flapping target_info, the very collision attrs.PrefixInstance and the cadvisor
// sandbox fold exist to prevent. It is also the datapointAttributes rule read
// straight: a volume is a property of the pod, not a second identity for it.
func (sb *summaryBatcher) addVolumes(elem []byte, ident cadvisorIdentity) {
	t := summaryTarget{ident: ident, level: levelPod}
	err := ljson.ArrayEach(elem, func(v []byte) error {
		if !sb.addVolume(t, v) {
			sb.malformed.volumes++
		}
		return nil
	}, "volume")
	// A pod with no measured volumes has no array at all, which is the common
	// case and not a fault.
	if err != nil && !errors.Is(err, ljson.ErrKeyNotFound) {
		sb.malformed.volumes++
	}
}

func (sb *summaryBatcher) addVolume(t summaryTarget, v []byte) bool {
	slots, err := ljson.GetPaths(v, volumePaths, sb.volSlots)
	sb.volSlots = slots
	if err != nil {
		return false
	}
	name := rawString(slots[pVolName])
	if name == "" {
		// Without a name the point is indistinguishable from every other volume
		// of the pod: same resource, same metric, same attribute set.
		return false
	}
	labels := append(sb.volLabels[:0], Label{Name: attrVolumeName, Value: name})
	if pvc := rawString(slots[pVolPVCName]); pvc != "" {
		labels = append(labels, Label{Name: attrPVCName, Value: pvc})
		if pns := rawString(slots[pVolPVCNamespace]); pns != "" {
			labels = append(labels, Label{Name: attrPVCNamespace, Value: pns})
		}
	}
	sb.volLabels = labels
	sb.addFS(t, volumeMetrics, slots, pVolFs, labels)
	return true
}

// addContainers emits one pod's per-container statistics, each on ITS OWN
// container resource. PodStats.Containers never lists the pause container, so
// none of the sandbox machinery the cadvisor batcher needs applies here — this
// is the one place where the kubelet has already done the attribution cadvisor
// makes us reconstruct from a cgroup path.
func (sb *summaryBatcher) addContainers(elem []byte, ident cadvisorIdentity) {
	err := ljson.ArrayEach(elem, func(c []byte) error {
		if !sb.addContainer(ident, c) {
			sb.malformed.containers++
		}
		return nil
	}, "containers")
	if err != nil && !errors.Is(err, ljson.ErrKeyNotFound) {
		sb.malformed.containers++
	}
}

func (sb *summaryBatcher) addContainer(ident cadvisorIdentity, c []byte) bool {
	slots, err := ljson.GetPaths(c, containerPaths, sb.ctrSlots)
	sb.ctrSlots = slots
	if err != nil {
		return false
	}
	name := rawString(slots[pCtrName])
	if name == "" {
		return false
	}
	// The container identity a cadvisor row for this container would carry,
	// minus the cgroup path's container id — which resolveContext supplies from
	// the pod document, by matching this name inside it. That is what makes the
	// two resources byte-identical and the series joinable.
	ident.container = name
	t := summaryTarget{ident: ident, level: levelContainer}

	// rootfs and logs are the two halves of ONE additive whole (a container's
	// ephemeral storage), which is why they are an attribute on one metric name
	// rather than two names: summing over fs.type is the correct query. The node
	// filesystems below are the opposite case and get separate names.
	rootTS := statTime(slots[pCtrRootfsTime])
	sb.addOpt(t, metContainerEphemeralUsage, slots[pCtrRootfsUsed], rootTS, fsTypeRootfs)
	sb.addOpt(t, metContainerEphemeralInodesUsed, slots[pCtrRootfsInodesUsed], rootTS, fsTypeRootfs)
	logsTS := statTime(slots[pCtrLogsTime])
	sb.addOpt(t, metContainerEphemeralUsage, slots[pCtrLogsUsed], logsTS, fsTypeLogs)
	sb.addOpt(t, metContainerEphemeralInodesUsed, slots[pCtrLogsInodesUsed], logsTS, fsTypeLogs)
	return true
}

// addNode emits the node-level block. An absent "node" key is not a fault (the
// document is still a document); a node object that does not decode is counted
// and skipped, so a kubelet that broke its own node stats still ships every
// pod's.
func (sb *summaryBatcher) addNode(body []byte) error {
	node, err := ljson.Lookup(body, "node")
	if err != nil {
		if errors.Is(err, ljson.ErrKeyNotFound) {
			return nil
		}
		// Everything else this descent can fail with is about the DOCUMENT and
		// not about the node object: a root that is not a JSON object at all, or
		// a root object broken before the key is reached. Counting it as a
		// skipped element would report a kubelet serving garbage as one serving a
		// slightly damaged payload.
		return fmt.Errorf("decoding the summary document: %w", err)
	}
	slots, err := ljson.GetPaths(node, nodePaths, sb.nodeSlots)
	sb.nodeSlots = slots
	if err != nil {
		sb.malformed.node++
		return nil
	}
	if reported := rawString(slots[pNodeName]); reported != "" && reported != sb.s.cfg.Node {
		// The resource still uses this agent's node (s.nodeInfo()), so the series
		// stay consistent with every other pipeline's — but an agent scraping
		// another node's kubelet attributes that node's filesystems to itself,
		// and nothing else in the payload says so.
		sb.s.warnOnce("summarynode:"+sb.url,
			"the kubelet's /stats/summary reports a different node than this agent's -node; its statistics are being exported under this agent's node",
			"reported", reported, "node", sb.s.cfg.Node, "url", sb.url)
	}
	t := summaryTarget{node: true}
	// Three filesystems, three metric families, deliberately not one family with
	// an fs.name attribute: nodefs, imagefs and containerfs do not PARTITION
	// anything — on a node whose runtime does not split imagefs they are the same
	// device reported three times — so a sum over such an attribute would be
	// wrong. Separate names make the wrong query unwriteable instead of merely
	// documented.
	sb.addFS(t, nodeFsMetrics, slots, pNodeFs, nil)
	sb.addFS(t, nodeImageFsMetrics, slots, pNodeImageFs, nil)
	sb.addFS(t, nodeContainerFsMetrics, slots, pNodeContainerFs, nil)
	rlimitTS := statTime(slots[pNodeRlimitTime])
	sb.addOpt(t, metNodeProcessCount, slots[pNodeProcessCount], rlimitTS, nil)
	sb.addOpt(t, metNodeProcessMax, slots[pNodeProcessMax], rlimitTS, nil)
	return sb.flushIfFull()
}

// The decode path tables. One GetPaths pass per element against a fixed table
// with named slot indices — internal/agent/azurediag/decode.go's idiom, down to
// the const slot names. An absent path leaves a NIL slot, which is what makes
// "not reported" representable at all: there is no struct field that could hold
// it, and every alternative spelling defaults it to zero.

// fsFields are the leaves of the kubelet's FsStats, in the order the f* slot
// offsets below name them. Three node filesystems and every volume share that
// shape, so they share this list and the addFS emitter over it.
var fsFields = []string{"time", "usedBytes", "availableBytes", "capacityBytes", "inodes", "inodesFree", "inodesUsed"}

const (
	fTime = iota
	fUsed
	fAvailable
	fCapacity
	fInodes
	fInodesFree
	fInodesUsed
	nFsFields
)

// fsPathsUnder returns the FsStats paths rooted at parent (nil parent = the
// element itself, which is how VolumeStats inlines them).
func fsPathsUnder(parent ...string) [][]string {
	out := make([][]string, 0, nFsFields)
	for _, leaf := range fsFields {
		path := make([]string, 0, len(parent)+1)
		out = append(out, append(append(path, parent...), leaf))
	}
	return out
}

// Pod element (PodStats).
const (
	pPodName = iota
	pPodNamespace
	pPodUID
	pPodEphUsed
	pPodEphInodesUsed
	pPodEphTime
	pPodProcessCount
)

var podPaths = [][]string{
	pPodName:          {"podRef", "name"},
	pPodNamespace:     {"podRef", "namespace"},
	pPodUID:           {"podRef", "uid"},
	pPodEphUsed:       {"ephemeral-storage", "usedBytes"},
	pPodEphInodesUsed: {"ephemeral-storage", "inodesUsed"},
	pPodEphTime:       {"ephemeral-storage", "time"},
	pPodProcessCount:  {"process_stats", "process_count"},
}

// Container element (ContainerStats). Only the leaves this pipeline exports are
// requested: ContainerStats.Rootfs and .Logs carry the imagefs/nodefs
// available/capacity/inodes numbers verbatim, so asking for them would be an
// invitation to export the node's numbers once per container.
const (
	pCtrName = iota
	pCtrRootfsUsed
	pCtrRootfsInodesUsed
	pCtrRootfsTime
	pCtrLogsUsed
	pCtrLogsInodesUsed
	pCtrLogsTime
)

var containerPaths = [][]string{
	pCtrName:             {"name"},
	pCtrRootfsUsed:       {"rootfs", "usedBytes"},
	pCtrRootfsInodesUsed: {"rootfs", "inodesUsed"},
	pCtrRootfsTime:       {"rootfs", "time"},
	pCtrLogsUsed:         {"logs", "usedBytes"},
	pCtrLogsInodesUsed:   {"logs", "inodesUsed"},
	pCtrLogsTime:         {"logs", "time"},
}

// Volume element (VolumeStats, which inlines FsStats at the element root).
const (
	pVolFs           = 0
	pVolName         = nFsFields
	pVolPVCName      = pVolName + 1
	pVolPVCNamespace = pVolPVCName + 1
	nVolumePaths     = pVolPVCNamespace + 1
)

var volumePaths = func() [][]string {
	out := make([][]string, 0, nVolumePaths)
	out = append(out, fsPathsUnder()...)
	out = append(out, []string{"name"}, []string{"pvcRef", "name"}, []string{"pvcRef", "namespace"})
	return out
}()

// Node object (NodeStats). Runtime is absent on some runtimes and
// Runtime.ContainerFs before Kubernetes 1.30 and whenever the runtime does not
// split it; Rlimit is Linux-only. All three are nil slots, not errors.
const (
	pNodeName         = 0
	pNodeFs           = 1
	pNodeImageFs      = pNodeFs + nFsFields
	pNodeContainerFs  = pNodeImageFs + nFsFields
	pNodeProcessCount = pNodeContainerFs + nFsFields
	pNodeProcessMax   = pNodeProcessCount + 1
	pNodeRlimitTime   = pNodeProcessMax + 1
	nNodePaths        = pNodeRlimitTime + 1
)

var nodePaths = func() [][]string {
	out := make([][]string, 0, nNodePaths)
	out = append(out, []string{"nodeName"})
	out = append(out, fsPathsUnder("fs")...)
	out = append(out, fsPathsUnder("runtime", "imageFs")...)
	out = append(out, fsPathsUnder("runtime", "containerFs")...)
	// RlimitStats spells them curproc/maxpid.
	out = append(out, []string{"rlimit", "curproc"}, []string{"rlimit", "maxpid"}, []string{"rlimit", "time"})
	return out
}()

// rawString decodes one raw JSON scalar slot as a string; an absent slot, a
// null and a non-scalar all read as "". logline.RawScalarString is the repo's
// one spelling of that (azurediag decodes its envelope through it), and it
// aliases the body rather than copying when the string carries no escapes.
func rawString(raw []byte) string {
	s, ok := logline.RawScalarString(raw)
	if !ok {
		return ""
	}
	return s
}

// parseCount reads one optional unsigned count. The bool is the ONLY thing
// standing between "the kubelet did not report this" and "the kubelet reported
// zero", so every way of not having a number — absent slot, JSON null,
// unparseable token, negative, non-integral, or past float64's exact range —
// returns false and produces no data point. A value present and zero returns
// true: a container that genuinely wrote nothing to its rootfs is a fact worth
// exporting.
func parseCount(raw []byte) (uint64, bool) {
	if len(raw) == 0 || string(raw) == "null" { // comparison, not a conversion: no allocation
		return 0, false
	}
	f, err := ljson.ParseFloat(raw)
	if err != nil {
		return 0, false
	}
	// >= not >: 2^53+1 is not representable and ParseFloat rounds it DOWN to
	// exactly 2^53, so a strict > accepted it and reported a DIFFERENT (one
	// lower) number — the one input where this function turns a number into
	// another number rather than into "absent", which is the single thing it
	// exists to prevent. Refusing 2^53 itself costs nothing real: these are
	// byte, inode and process counts.
	if f < 0 || f >= maxExactCount || f != math.Trunc(f) {
		return 0, false
	}
	return uint64(f), true
}

// statTime is the stat's OWN timestamp in milliseconds, or 0 (which pointTS
// reads as the scrape time) when it carries none.
//
// Every point takes the stat's time rather than the scrape's, and it matters
// most for volumes: the kubelet's volume-stats calculator refreshes on its own
// jittered ~1 minute period and the summary serves that cache, so at
// -scrape-interval=15s three scrapes in four carry a reading up to a minute
// old. Stamping the scrape time would fabricate freshness. The visible
// consequence, which will be reported as a bug and is not one: a volume series
// legitimately updates once a minute while its neighbours update every 15s, and
// an unrefreshed stat re-sends an identical (series, timestamp, value) triple —
// which Prometheus and Mimir accept as an idempotent duplicate precisely
// because the value did not change.
func statTime(raw []byte) int64 {
	s := rawString(raw)
	if s == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0
	}
	ms := t.UnixMilli()
	if ms <= 0 {
		return 0 // pre-epoch or unrepresentable: the scrape time, as for an absent one
	}
	return ms
}
