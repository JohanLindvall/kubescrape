package obs

// Kubernetes events (the cluster-singleton events collector).
var (
	Leader = Registry.Gauge("kubescrape_leader",
		"1 while this replica holds the cluster-singleton lease, 0 otherwise; sum != 1 means split brain or nobody leading.")
	EventsObserved = Registry.CounterVec("kubescrape_events_observed_total",
		"Kubernetes events received from the watch, by event type (normal, warning, other — anything else the API server reports).", "type")
	EventsExported = Registry.Counter("kubescrape_events_exported_total",
		"Kubernetes event records exported (after the rules).")
	EventsDropped = Registry.Counter("kubescrape_events_dropped_batches_total",
		"Kubernetes event batches dropped after a permanent collector rejection (the position advances past them).")
	EventsDroppedRecords = Registry.Counter("kubescrape_events_dropped_records_total",
		"Kubernetes event records lost with those batches (the magnitude of the loss the batch counter only signals).")
	EventsOverflowDropped = Registry.Counter("kubescrape_events_overflow_dropped_total",
		"Kubernetes events dropped UNEXPORTED because the retained batch hit its cap before anything could commit (a collector outage on a fresh install); the watch will not re-deliver them, so each is outright loss.")
	EventsExportFailures = Registry.Counter("kubescrape_events_export_failures_total",
		"Kubernetes event batch exports that failed transiently. The batch is KEPT and the watch stays open "+
			"(tryFlush), so this counts ATTEMPTS — one per flush, not lost events: a steady rate is a collector "+
			"outage the reader is riding out with its position uncommitted, and the loss counters are "+
			"kubescrape_events_dropped_batches_total (a permanent rejection) and "+
			"kubescrape_events_overflow_dropped_total (the retained batch reaching its cap). Deliberately NOT "+
			"kubescrape_log_export_failures_total, which is the tailer's files-rewound counter and cannot apply "+
			"here — this reader rewinds no file, and the singleton that collects events runs with -logs=false, so "+
			"those increments landed on a metric whose help described something that had not happened.")
	EventWatchRestarts = Registry.Counter("kubescrape_event_watch_restarts_total",
		"Event watch restarts (a closed stream, an error, or an expired resourceVersion).")
	EventRelists = Registry.CounterVec("kubescrape_event_relists_total",
		"Event watches that fell back to a relist because the stored resourceVersion had aged out of the API server's watch window.", "stage")
	EventGapDiscarded = Registry.CounterVec("kubescrape_event_gap_discarded_total",
		"Event watch expiries with no relist to fall back to (nothing exported yet and none armed): the next stream restarts at the CURRENT revision and whatever the dead watch never delivered is discarded — the events pipeline's one silent-loss arm, worth an alert wherever kubescrape_event_relists_total has one.", "stage")
	EventPositionErrors = Registry.CounterVec("kubescrape_event_position_errors_total",
		"Failures reading or writing the event position ConfigMap, by operation (load, save).", "operation")
	// The whole reason events are collected HERE rather than as a flat stream
	// is that an event about a pod lands on that pod's resource attributes. A
	// failed resolution still exports the event — with the identity the event
	// itself carries — so nothing is lost except the join, which is exactly the
	// kind of degradation that has no other symptom: the records keep flowing
	// and every other counter stays green. kubescrape_metadata_requests_total
	// cannot answer it either, since the uid_mismatch arm issues no request at
	// all and a lookup error is indistinguishable there from any other caller's.
	EventsUnresolved = Registry.CounterVec("kubescrape_events_unresolved_total",
		"Events about a Pod that were exported WITHOUT that pod's resolved identity (owner chain, labels, node, service.name), by reason: lookup = the metadata service could not answer (unreachable, or the pod is gone and past its tombstone TTL), uid_mismatch = a pod of that name exists but is a different incarnation, so adopting it would attribute the event to the wrong pod. Counted once per failed resolution: per distinct involved object per batch, and again at most every 30s while it keeps failing (a failure is retried then, so events that follow a metadata-service recovery resolve). A lookup cut short by the reader's own shutdown or lost lease is not counted. The event is still exported under the identity it carries, so this is lost CORRELATION, not lost data.", "reason")
)

// Azure diagnostics (the Event Hubs consumer in the cluster-singleton deployment).
var (
	// signal/plural, matching AzureExported and every other producer. This
	// counter used to spell the same dimension "kind" with singular values, so
	// the decoded and exported counts of one pipeline could not be joined or
	// even reliably grepped for.
	AzureRecords = Registry.CounterVec("kubescrape_azure_records_total",
		"Azure diagnostic records decoded from Event Hubs messages, by signal (logs, metrics).", "signal")
	AzureDecodeErrors = Registry.Counter("kubescrape_azure_decode_errors_total",
		"Event Hubs messages or records that could not be decoded as Azure diagnostics JSON (skipped, committed past).")
	AzureExported = Registry.CounterVec("kubescrape_azure_exported_total",
		"Azure diagnostic records exported, by signal (logs, metrics).", "signal")
	AzureDropped = Registry.Counter("kubescrape_azure_dropped_batches_total",
		"Azure diagnostic payloads dropped after a permanent collector rejection (the offsets advance past them).")
	AzureDroppedRecords = Registry.CounterVec("kubescrape_azure_dropped_records_total",
		"Azure diagnostic records (log records or metric data points) lost with those payloads, by signal.", "signal")
	AzureExportFailures = Registry.CounterVec("kubescrape_azure_export_failures_total",
		"Azure diagnostic payload exports that failed transiently and are being retried IN PLACE, by signal "+
			"(logs, metrics). The payload is kept and the Kafka offsets do not advance until the collector acks, "+
			"so this counts ATTEMPTS, not lost records; the loss counters are "+
			"kubescrape_azure_dropped_batches_total and kubescrape_azure_dropped_records_total. Deliberately NOT "+
			"kubescrape_log_export_failures_total, the tailer's files-rewound counter: this reader owns no file, "+
			"it runs in the singleton Deployment with -logs=false, and only its LOGS signal was ever counted "+
			"there — so a hub carrying platform metrics retried invisibly.", "signal")
	AzureFetchErrors = Registry.Counter("kubescrape_azure_fetch_errors_total",
		"Kafka fetch errors from the Event Hubs consumer (retried; partial fetches are still processed).")
	AzureCommitErrors = Registry.Counter("kubescrape_azure_commit_errors_total",
		"Offset commit failures (the records were delivered; a redelivery produces at-least-once duplicates).")
	AzureTokenRefreshes = Registry.CounterVec("kubescrape_azure_token_refreshes_total",
		"Microsoft Entra token refreshes for the Event Hubs connection, by outcome (ok, error).", "outcome")
)

// RegisterAzurePartitions publishes how many Event Hubs partitions the
// consumer currently owns, summed over every configured source. It is
// registered exactly when the pipeline runs, so a published 0 always means
// "joined and owns nothing" — the state a topic pattern matching no hub, an
// entity-scoped credential in a shared group, or a rebalance that assigned
// this member nothing leaves behind, and which the records counter cannot tell
// from a quiet hub.
func RegisterAzurePartitions(assigned func() float64) {
	Registry.GaugeFunc("kubescrape_azure_partitions_assigned",
		"Event Hubs partitions currently assigned to this consumer, summed over every configured source. "+
			"Registered only while -azure-diagnostics runs, so 0 means the consumer group was joined and this "+
			"member owns nothing — a -azure-eventhub-topics pattern that matches no hub, an entity-scoped "+
			"credential in a group shared with siblings that cannot see its hub, or a rebalance in progress — "+
			"which kubescrape_azure_records_total cannot tell from a hub that is simply quiet. The assignment "+
			"Info line names the topics and partitions each time it changes.", assigned)
}
