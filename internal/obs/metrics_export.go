package obs

// OTLP exporter (agent).
var (
	Exports = Registry.CounterVec("kubescrape_export_requests_total",
		"OTLP export attempts by signal and outcome: ok, transient (the collector is unreachable or "+
			"back-pressuring and the payload is retried or spooled), permanent (the collector rejected THIS "+
			"payload and no retry can help, so a producer drops it). A sustained transient rate means the "+
			"destination is down; any permanent rate means telemetry is being lost — the accompanying warning "+
			"names the endpoint, the class and the likeliest cause.", "signal", "outcome")
	// ExportDuration is what the outcome counter cannot say: a collector that
	// is SLOW rather than down. Every attempt takes -otlp-timeout to fail when
	// the collector hangs, and a p90 climbing towards that timeout is the
	// warning before Exports{outcome="transient"} starts moving.
	ExportDuration = Registry.HistogramVec("kubescrape_export_duration_seconds",
		"Wall-clock duration of one OTLP export attempt by signal, successful or not (retries and split parts "+
			"are separate attempts). The outcome counter says whether the collector answered; this says how "+
			"long it took to, and a p90 climbing towards -otlp-timeout (15s) is the collector back-pressuring "+
			"or a network path degrading before a single attempt fails — the tailer's flush and the ingest "+
			"receiver's push both wait on exactly this.", durationBuckets, "signal")
	// ExportSplitParts reports the size-split firing. It is otherwise
	// invisible: a payload over -otlp-max-send-bytes is quietly delivered in
	// pieces, each its own round trip, auth build and gzip pass — and the one
	// shape the splitter cannot rescue (a SINGLE record larger than the cap) is
	// sent alone and rejected by the collector, which surfaces only as a
	// permanent export failure with no hint of where the size came from.
	ExportSplitParts = Registry.CounterVec("kubescrape_export_split_parts_total",
		"Extra parts an over-cap OTLP payload was split into before sending (a payload sent whole adds "+
			"nothing). Counted per export ATTEMPT, like kubescrape_export_requests_total: the split is "+
			"re-derived on every try, so a retried payload adds its parts again — read this against the "+
			"attempt counter, not as a payload count. A sustained rate means a producer is batching past "+
			"-otlp-max-send-bytes: lower its batch size, or raise the cap if the collector's receive limit "+
			"allows it.", "signal")
	// The other half of the split's story: what it could NOT rescue. Both
	// reasons ship a part the collector is expected to reject wholesale, which
	// on its own surfaces only as a permanent export failure naming no size —
	// and the framing arm is a REFUSAL TO KEEP SPLITTING, so without a series
	// of its own the loss it trades for would be invisible.
	ExportOversizeParts = Registry.CounterVec("kubescrape_export_oversize_parts_total",
		"OTLP parts sent knowingly larger than -otlp-max-send-bytes, by signal and reason — the collector "+
			"rejects each one, so any rate here is telemetry being lost. Counted per export ATTEMPT, like "+
			"kubescrape_export_requests_total: one oversized record is re-counted on every retry of its "+
			"payload (with -buffer-dir, up to eight wire attempts), so this sizes the RATE of the condition "+
			"and never the number of records lost. item: a SINGLE log record, span or "+
			"metric data point is itself over the cap and nothing can shrink it, so it ships alone (find the "+
			"producer of that record — a multi-megabyte log line, a histogram with an enormous label set — or "+
			"raise the cap if the collector's receive limit allows). framing: the split was ABANDONED because "+
			"the framing every part re-copies (the resource attributes, the scope identity, or a metric's "+
			"description) left too little room under the cap for content, so the remainder shipped as one "+
			"over-cap part instead of as thousands of near-empty ones — on the unauthenticated -ingest and "+
			"trace-tier listeners that shape is what a hostile sender constructs, so read a rate here as a "+
			"sender pushing a resource nearly as large as the cap; the accompanying throttled warning names "+
			"the signal and the sizes.", "signal", "reason")
	ExportRejected = Registry.CounterVec("kubescrape_export_rejected_records_total",
		"Records the collector REJECTED inside a payload it otherwise accepted (OTLP partial_success), by signal. "+
			"The export succeeded, so every producer advanced its offset, cursor or position past them — these are "+
			"lost, permanently, and retrying cannot help (OTLP defines them as invalid rather than deferred). "+
			"Any nonzero rate means telemetry is being discarded downstream; the collector's own message is on the "+
			"accompanying warning.", "signal")
)

// Metadata client (agent).
var (
	MetadataRequests = Registry.CounterVec("kubescrape_metadata_requests_total",
		"Requests to the metadata service by outcome.", "outcome")
)

// BearerTokenReadErrors counts failed reads of a mounted bearer-token file, by
// which half of the rotation contract was reading (internal/bearer).
//
// Both halves SURVIVE the failure — that is the package's whole point — so
// neither produces an immediate symptom, and the counter is what turns a
// broken projection into something alertable before the delayed symptom
// arrives. role="client" is the token this process PRESENTS (the kubelet
// scrape, the OTLP export, the agent's /v1/scrape-auth calls): it keeps
// sending the last good value, or — when nothing has ever been read — sends
// none at all and every request it feeds is rejected unauthenticated.
// role="receiver" is the ACCEPT set (the metadata service's /v1/scrape-auth,
// the trace tier's internal hop): it keeps accepting the last good token, so
// the failure surfaces only once the clients have rotated past it, as a
// fleet-wide 401 with nothing on the receiver to explain it. A nonzero rate
// sustained past one rotation means an unreadable, empty or unmounted Secret
// projection: fix the mount. Each failure also carries a throttled Warn naming
// the path and the error.
var BearerTokenReadErrors = Registry.CounterVec("kubescrape_bearer_token_read_errors_total",
	"Failed re-reads of a mounted bearer-token file, by rotation role. client = the token this process "+
		"presents (it keeps presenting the last good one, or none at all if it never read one, and every "+
		"request it feeds is then rejected unauthenticated); receiver = the set this process ACCEPTS (it "+
		"keeps accepting the last good token, so the symptom is a fleet-wide 401 once the clients rotate "+
		"past it). Sustained nonzero means an unreadable, empty or unmounted Secret projection — fix the "+
		"mount; a throttled Warn names the path and the error.",
	"role")

// Export seam (agent): routing and transforms.
var (
	Routed = Registry.CounterVec("kubescrape_routed_payload_parts_total",
		"Payload parts forwarded to a non-default routing destination.", "route", "signal")
	// RouteFailures is the missing half of Routed. Routed counts only parts a
	// destination ACCEPTED, so a route that has never worked reads as absence —
	// indistinguishable from a route nothing matched, which is the likelier
	// first-run mistake and needs the opposite response. Route destinations are
	// unbuffered by design, so a failure here is back-pressure onto the
	// producer (and, for a producer that cannot rewind, loss).
	RouteFailures = Registry.CounterVec("kubescrape_routed_failures_total",
		"Payload parts a routing destination refused, by route and signal. The whole export fails when any "+
			"destination does, so the producer retries and destinations that already accepted the payload see "+
			"duplicates; a sustained rate means that tenant's telemetry is not arriving — the accompanying "+
			"warning names the class and the likeliest cause.", "route", "signal")
	// RouteUnknown deliberately carries NO label. The name a script routes to
	// is script-chosen and unbounded — exactly what a label must not be — so it
	// rides the (throttled) log line instead, the same rule the ingest door
	// follows for a sender's peer address.
	RouteUnknown = Registry.Counter("kubescrape_routed_unknown_total",
		"Payloads a transform script routed to a name no route defines. They fall back to the default chain "+
			"rather than being dropped, so the effect is silent mis-tenanting; the throttled warning names the "+
			"route the script asked for.")
	TransformErrors = Registry.CounterVec("kubescrape_transform_errors_total",
		"Transform program invocations that failed (the batch is NOT exported; the error propagates to the producer's retry path).", "signal")
	// A transform drop is INTENDED loss, which is exactly why it needs a
	// counter: the intent lives in an operator-edited Starlark file that hot-
	// reloads, so a one-character edit can silently discard a node's whole log
	// stream with no error logged and every other metric green. That has
	// already happened once (see transform/hostobj.go).
	TransformDropped = Registry.CounterVec("kubescrape_transform_dropped_total",
		"Records a transform script called drop() on, by signal: logs (log records), metrics (data points — a dropped metric counts all of its points), traces (spans) and targets (whole scrape targets the targets: hook dropped, which stop being scraped from that cycle on; there is no other signal for one, since a target that is never fetched has no up series to go to 0). Counted once the batch's records SETTLE: its export is ACKED (a delivered forward, or a payload transformed to nothing and acked without a send), so a retry that brings the same records back to the script never re-counts them — a copy-path producer's retry, an ingest sender's retransmission, the log-metrics set's retained samples, a tailer re-reading the files a failed flush rewound (the tailer counts when its offsets commit). A FAILED forward also counts when it is final for its records (a transform.Consumed payload: promscrape's take()n chunks, cgroupstats' reset windows, and the cumulative renders of the self-metrics, span-metrics and service-graph exporters, whose next export is a new point), or those drops would be counted nowhere during exactly the collector outage this counter is read in; so does the tailer dropping a permanently rejected batch.", "signal")
	TransformEmitSkipped = Registry.Counter("kubescrape_transform_emit_skipped_total",
		"emit_metric observations a transform script made that were NOT recorded because the item's resource carried more than 1024 attributes — too wide to key a log-metric series on without stalling the export. The item itself is still exported; only the observation is lost, so a sustained rate means a sender (usually an OTLP push) is attaching an unbounded attribute set to its resource. The accompanying throttled warning names the metric and the width.")
	TransformReloads = Registry.CounterVec("kubescrape_transform_reloads_total",
		"Transforms-file reloads by outcome (applied, failed — a failed compile keeps the last good program). BOTH outcomes are deduped by the file's content hash, so a persistently broken file counts once rather than once per poll tick: the counter reads as distinct broken EDITS, not as the reload cadence, and it says nothing about whether the file on disk is STILL broken once the edit has left the rate window — kubescrape_transform_reload_failing is that state, and the one to alert on.", "outcome")
)

// RegisterTransformReloadFailing publishes how many watched transforms files
// currently fail to compile (or cannot be read) — on an agent, which watches
// one file, 0 or 1. It is the STATE beside kubescrape_transform_reloads_total
// {outcome="failed"}, which is deduped by content hash and so moves once per
// distinct broken EDIT: an increase() alert on it resolves one window after
// the edit while every node keeps running the last good program against a
// broken file on disk — and the startup compile is fatal, so the next
// restart, reboot or eviction of any node CrashLoops it. Registered by the
// reloader itself, exactly when it runs (-transforms-file set), so a published
// 0 means "watching, and the file compiles", never "off".
func RegisterTransformReloadFailing(failing func() float64) {
	Registry.GaugeFunc("kubescrape_transform_reload_failing",
		"Watched transforms files whose current content fails to compile or cannot be read (an agent watches one, "+
			"so 0 or 1). The state half of kubescrape_transform_reloads_total{outcome=\"failed\"}, which counts "+
			"each distinct broken edit once: while this is 1 every node runs its last good program against a "+
			"broken file, and because the startup compile is fatal, the next restart, reboot or eviction of a node "+
			"CrashLoops it. Alert on == 1 sustained past an edit-fix cycle. Registered only while -transforms-file "+
			"is watched, so 0 means watching and clean, never off.", failing)
}
