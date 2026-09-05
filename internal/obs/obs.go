// Package obs holds the internal (self-observability) metrics of both
// binaries. They are produced through internal/metrics' Registry and exported
// over OTLP alongside everything else — there is no Prometheus exposition.
// New failure paths should count into an existing metric here or add one.
package obs

import (
	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// Registry collects every metric below; the binaries export it periodically
// via metrics.Registry.Run with their own resource identity.
var Registry = metrics.NewRegistry()

// Log pipeline (agent).
var (
	LogEntries = Registry.Counter("kubescrape_log_entries_total",
		"Log entries exported. With -buffer-dir this counts acceptance into the disk buffer, not collector delivery — reconcile against kubescrape_buffer_dropped_records_total{signal=\"logs\"} for what was later dropped drain-side.")
	LogBytes = Registry.Counter("kubescrape_log_bytes_total",
		"Raw log bytes read from live files and archives. Segment replays (re-reading a rotated file's owed range after a restart or rewind) are not re-counted.")
	LogExportFailures = Registry.Counter("kubescrape_log_export_failures_total",
		"Log batch exports that failed after retries (files rewound).")
	// LogPermanentDropped is the tailer's counterpart to the other producers'
	// permanent-rejection drops. Retrying a definitive rejection cannot
	// succeed, and because one sweep goroutine serves every file on the node,
	// retrying it forever stops ALL log shipping there — so the batch is
	// dropped and the offsets advance. That is real data loss and must be
	// alertable.
	//
	// With -buffer-dir the tailer's Export returns the ENQUEUE verdict, not
	// the collector's, so on that (documented, durable) configuration this
	// counter moves only for a batch larger than the whole buffer cap; the
	// collector's own permanent rejections land on
	// kubescrape_buffer_dropped_batches_total{signal="logs"} (with
	// ..._records_total beside it for the magnitude) instead, which is where
	// an alert for the buffered chain belongs.
	LogPermanentDropped = Registry.Counter("kubescrape_log_permanent_dropped_total",
		"Log records dropped after a definitive collector rejection (retrying could not succeed; offsets advanced so the pipeline survives).")
	LogFiles = Registry.Gauge("kubescrape_log_files",
		"Log files currently tracked.")
	// LogFilesUnresolved answers the operator's first question — "why is this
	// pod's log missing?" — for the half of the answers that live INSIDE the
	// tailer. A tracked file is never read before it can be attributed, so a
	// file whose container metadata will not resolve produces nothing at all
	// while every other log metric looks healthy: the file is tracked
	// (kubescrape_log_files counts it), no byte is read
	// (kubescrape_log_bytes_total does not move for it) and nothing is lost
	// (the data waits on disk). A value that stays above zero for longer than a
	// container's first seconds means the metadata service is unreachable or
	// the containers are unknown to it; the tailer names the waiting files in a
	// throttled warning and on GET /debug/tailer.
	LogFilesUnresolved = Registry.Gauge("kubescrape_log_files_unresolved",
		"Tracked log files whose metadata has not resolved yet, so nothing is read from them (their content waits on disk and is not lost).")
	// LogFilesSkipped is the OTHER half of that question: a file the discovery
	// pass saw and deliberately did not track. Every one of these was silent by
	// design, so an operator whose pod produced no logs had nothing to look at
	// — the file simply never appeared anywhere. Counted ONCE PER FILE per
	// reason (not once per discovery pass, which runs every couple of seconds),
	// so a rate is "files newly skipped", and a file that changes reason counts
	// again under the new one. The matching Debug line names the path.
	LogFilesSkipped = Registry.CounterVec("kubescrape_log_files_skipped_total",
		"Log files seen by discovery and deliberately not tracked, by reason: source_exclude (the source's exclude "+
			"globs), excluded_namespace (-logs-exclude-namespaces or the source's excludeNamespaces), "+
			"namespace_not_selected (the source's namespaces allowlist; another source may still claim the file), "+
			"unparseable_name (not a CRI <pod>_<namespace>_<container>-<id>.log name), too_old (the source's "+
			"ignoreOlder cutoff), non_regular (a FIFO/socket/device, never opened because the open would block the "+
			"sweep goroutine node-wide) and stat_error (the path was listed but could not be stat'd — an EACCES or "+
			"EIO here is a genuine collection failure, not a selection). Counted once per file per reason.", "reason")
	// LogMetadataBudgetExhausted is promscrape's ScrapeMetaBudgetExhausted one
	// pipeline over, and it exists for the same reason: a metadata lookup can
	// BLOCK server-side for the whole -metadata-wait, every file on the node is
	// resolved by the SINGLE sweep goroutine, and past the sweep's shared
	// resolve budget files are simply not reached. No request is issued, so
	// kubescrape_metadata_requests_total cannot move, and the files stay
	// unresolved with nothing at all to show for it. Counted ONCE PER SWEEP
	// (not once per unreached file) so a rate is comparable with the sweep
	// cadence.
	LogMetadataBudgetExhausted = Registry.Counter("kubescrape_log_metadata_budget_exhausted_total",
		"Tailer sweeps that ran out of their shared metadata-resolution budget, leaving files unresolved and unread until a later sweep (nothing is lost; a sustained rate means the metadata service is slow or unreachable).")
	LogRotations = Registry.Counter("kubescrape_log_rotations_total",
		"Log file rotations and truncations handled.")
	LogPrefixLost = Registry.Counter("kubescrape_log_prefix_lost_total",
		"Log content given up on as unrecoverable: a rotated-away segment that could not be re-read "+
			"(the file was deleted or compressed before its lines were exported, and no open fd survived "+
			"a restart), a segment or vanished file whose reads kept erring without progress past the "+
			"stall budget, or a compressed archive replaced across a restart before its old stream fully "+
			"shipped. One count per given-up segment/file/archive; these lines are lost.")
	LogEnriched = Registry.CounterVec("kubescrape_log_enriched_total",
		"Log records by the enrichment strategy that matched (json, logfmt, pattern, none).", "format")
	LogEnrichTimeRejected = Registry.Counter("kubescrape_log_enrich_time_rejected_total",
		"Timestamps parsed from a log line that did NOT replace the producer's own (the CRI/journal/event time) "+
			"because the line's timestamp carried no zone. Such a stamp is a wall clock enrichment must read as UTC, "+
			"so a workload running with TZ set to anything else would misdate every record by that offset; the "+
			"accurate ingest time is kept instead. A timestamp that states its own offset (RFC3339, an epoch, any "+
			"zoned layout) always wins, however old it is, and a record with no producer timestamp at all takes the "+
			"parsed one either way.")
	// The two lag gauges were named the other way round: the unqualified
	// kubescrape_log_lag_bytes was the per-file MAXIMUM and the _total_bytes
	// suffix carried the sum. Both are gauges, and _total is reserved for
	// counters — so the name promised a counter, and the name WITHOUT the
	// qualifier was the one that was not the total.
	LogLagMaxBytes = Registry.Gauge("kubescrape_log_lag_max_bytes",
		"Largest per-file backlog: bytes on disk not yet exported and committed (per-file breakdown on /debug/tailer).")
	LogLagTotalBytes = Registry.Gauge("kubescrape_log_lag_bytes",
		"Total backlog across tracked files: bytes on disk not yet exported and committed.")
	LogRateLimited = Registry.CounterVec("kubescrape_log_rate_limited_total",
		"Per-file line rate limit hits: lines discarded (action=drop) or reads paused (action=pause).", "action")
	LogRulesDropped = Registry.Counter("kubescrape_log_rules_dropped_total",
		"Log records dropped by the logs rules (including sampled-away lines), whichever path carried the "+
			"record: the tailer, journald, Kubernetes events, Azure diagnostics, or an OTLP push into -ingest "+
			"(a dropped pushed record is still acked to its sender — it was delivered; the operator chose "+
			"to drop it). Counted ONCE PER RECORD, not once per attempt: the tailer rewinds and re-reads "+
			"the same bytes after a failed export, and counting every pass multiplied this by the number of "+
			"rewinds an outage spanned. Per RECORD rather than per DELIVERY is the exact claim — a record "+
			"withheld by a commit clamp and then rewound is DELIVERED twice and counted once, which is the "+
			"direction to be wrong in: under-claiming only inflates, while over-claiming would destroy "+
			"observations invisibly. The residual skew is a `sample` rule, whose verdict is a per-filter "+
			"counter rather than a function of the bytes — after a rewind the re-read samples a DIFFERENT set "+
			"of lines, and the drops are attributed to the pass that first put those bytes through the chain. "+
			"The -ingest path holds the same line by a different mechanism, and its unit is per DELIVERY: the "+
			"tally is staged while the chain runs and applied only once the push is ACKED, so the retransmits "+
			"of a NACKed push cost nothing — but a push whose ack is LOST is redelivered and recounted, which "+
			"no receiver can dedupe.")
	// MonitorFieldsIgnored counts ServiceMonitor/PodMonitor upserts carrying
	// endpoint fields kubescrape does not interpret — the metric form of the
	// startup warning, so a partially-applied CR is alertable and not just
	// visible in one pod's logs.
	MonitorFieldsIgnored = Registry.CounterVec("kubescrape_monitor_fields_ignored_total",
		"Monitor upserts whose endpoints set fields kubescrape does not interpret.", "kind")
	// MonitorParseErrors counts ServiceMonitor/PodMonitor upserts that failed
	// to parse. This is the SEVERE sibling of MonitorFieldsIgnored: a parse
	// failure removes the monitor from the index, so every target it
	// contributed disappears. It had only a log line until now, which left
	// the worse outcome unalertable while the milder one was counted.
	MonitorParseErrors = Registry.CounterVec("kubescrape_monitor_parse_errors_total",
		"Monitor upserts that failed to parse and were dropped from the index.", "kind")
	// MonitorNamespaceRefused counts monitors dropped because their namespace
	// is not in -monitor-namespaces. It is the ONE outcome on that code path
	// that had neither a metric nor a log line, which on a multi-tenant
	// cluster makes an admin's deliberate refusal look exactly like a selector
	// typo, a missing CRD, or a monitor that matches nothing.
	MonitorNamespaceRefused = Registry.CounterVec("kubescrape_monitor_namespace_refused_total",
		"Monitor upserts ignored because their namespace is not permitted by -monitor-namespaces (an informer re-delivery re-counts the same monitor, exactly like the sibling monitor_* counters).", "kind")
	// MonitorTargetShadowed counts monitor endpoints whose auth/TLS material
	// CONFLICTS with the monitor already holding the same URL on the same pod.
	// Two monitors resolving to one URL are served as ONE merged target that
	// honours both (a kubescrape target's exported identity has no monitor
	// component, so scraping twice would put two byte-identical series
	// identities in one payload): relabel chains concatenate, the finer
	// explicit cadence wins, one-sided auth/TLS is adopted, and a bare or
	// identical endpoint merges silently, uncounted. Auth/TLS material is the
	// one group a single scrape cannot honour twice — when both sides declare
	// it and it differs, the first monitor's is served and the other's counts
	// here. A nonzero rate means a scrape is running with a credential or TLS
	// config one of its CRs did not choose, and wants the two monitors
	// reconciled.
	//
	// Counted per served targets request, like every other decision on that
	// path: the RATE is the signal, not the absolute value.
	MonitorTargetShadowed = Registry.CounterVec("kubescrape_monitor_target_shadowed_total",
		"Monitor endpoints whose auth/TLS conflicts with the monitor already holding the same URL on that pod (the holder's is served; the rest of the endpoint's configuration still merges).", "kind")
	// MonitorRelabelChainCapped counts monitor endpoints whose
	// metricRelabelings were only PARTLY folded into the target holding their
	// URL, because the merged chain reached scrape.MaxRelabelChainRules /
	// MaxRelabelChainBytes. Series the refused rules asked to keep or drop are
	// therefore NOT filtered, which is invisible in the data — the metrics
	// simply arrive.
	//
	// The ceiling exists because the chain is tenant-supplied, is copied into
	// every served target (so it multiplies through the node-targets document
	// the metadata singleton marshals in one piece) and is walked per sample by
	// every agent that scrapes the target. Two monitors' chains concatenate, so
	// N monitors colliding on one URL multiply it; the per-endpoint half of the
	// bound lives in internal/servicemonitors and reports itself through
	// kubescrape_monitor_fields_ignored_total instead.
	//
	// A nonzero rate means either a genuinely enormous chain (fix the CR) or
	// several monitors piling onto one URL (reconcile them). Counted per served
	// targets request, like every other decision on that path: the RATE is the
	// signal, not the absolute value.
	MonitorRelabelChainCapped = Registry.CounterVec("kubescrape_monitor_relabel_chain_capped_total",
		"Monitor endpoints whose metricRelabelings were partly refused because the merged chain for that scrape URL is at the per-target ceiling (the rules that fit are applied; the rest filter nothing).", "kind")
	// MonitorContributorsCapped counts monitor endpoints whose configuration
	// merged into the target already holding their URL but whose monitor NAME
	// was refused from that target's contributor list (the wire-visible
	// `monitors` field), because the list is at
	// scrape.MaxContributorsPerTarget.
	//
	// The SCRAPE is unaffected — the endpoint's relabel rules, cadence and
	// auth merged — so this is a loss of ATTRIBUTION, which is exactly why it
	// needs a counter: nothing in the served document or in the collected
	// metrics can reveal it. It is a separate series from
	// kubescrape_monitor_relabel_chain_capped_total because it is a separate
	// bound with a separate remedy, and because it binds on the cheaper
	// attack: a contribution costs a finer `interval` and no relabel rules at
	// all, so N colliding monitors reached the contributor list unbounded
	// while both relabel bounds stayed silent.
	//
	// A nonzero rate means many monitors resolve to one URL on one pod —
	// reconcile them; /v1/explain names the pod's monitors and says which
	// stopped being listed. Counted per served targets request, like every
	// other decision on that path: the RATE is the signal, not the absolute
	// value.
	MonitorContributorsCapped = Registry.CounterVec("kubescrape_monitor_contributors_capped_total",
		"Monitor endpoints whose configuration merged into the target holding their URL but whose monitor name was refused from that target's contributor list at the per-target ceiling (attribution only; the scrape is unaffected).", "kind")
	// ScrapeTargetsCapped counts scrape targets REFUSED because one pod
	// already produced scrape.MaxPortsPerPod of them. Every ScrapeTarget
	// embeds the whole pod document, so N targets carry N copies of the pod's
	// annotations: without a ceiling a tenant who can annotate a pod (or
	// author a ServiceMonitor with many endpoints, which needs no annotation
	// at all) makes the singleton metadata service marshal an O(N²) response
	// and OOM, taking target discovery for the whole fleet with it. The cap
	// sits where targets are ACCUMULATED so it covers every door — pod
	// annotation, Service annotation, ServiceMonitor and PodMonitor alike.
	//
	// A nonzero value means some endpoint of that pod is NOT being scraped;
	// /v1/explain names the pod and says so.
	// LogReadErrors counts sweeps that failed to read a tracked log file for a
	// reason other than "it is gone" (permission denied, EIO on a failing
	// disk, an SELinux denial). The WARN beside it is throttled per path and
	// SATURATES past a bounded number of distinct paths, so without a counter a
	// broad failure becomes invisible rather than merely quiet.
	LogReadErrors = Registry.Counter("kubescrape_log_read_errors_total",
		"Failed reads of a tracked log file (excluding the file being gone); the per-path warning is throttled, this is not.")
	ScrapeTargetsCapped = Registry.Counter("kubescrape_scrape_targets_capped_total",
		"Scrape targets refused because a single pod exceeded the per-pod target ceiling; those endpoints are not scraped (see /v1/explain for the pod).")
	TargetIdentityCollisions = Registry.Counter("kubescrape_scrape_target_identity_collisions_total",
		"Groups of scrape targets on one node that resolve to the SAME exported series identity — the same "+
			"Prometheus (job, instance), where job is the workload and instance is host:port. Every target in "+
			"such a group is still served and still scraped, because each was configured deliberately and "+
			"dropping one is invisible in the data (indistinguishable from an app that stopped exporting); what "+
			"collides is what they EXPORT. Two shapes reach it: two paths on one port (a pod annotation beside a "+
			"Service annotation, or two monitor endpoints), where url.full at least differs; and two hostNetwork "+
			"pods of ONE workload annotated with the same port, where even url.full is identical. The symptom "+
			"without this counter is anonymous — a metric name the endpoints share arrives as one series "+
			"alternating between their values so rate() sees resets, and up{} arrives as both 0 and 1 at one "+
			"timestamp, which a backend rejects as a duplicate sample. READ THE VALUE, NOT A SHORT RATE: it "+
			"increments once per colliding group per target DERIVATION, so its rate tracks how often the node's "+
			"target list is rebuilt, not how broken the configuration is. Alert on increase over an hour, or "+
			"simply on the throttled WARN beside it, which names the job, the instance and every colliding URL; "+
			"GET /v1/explain/{ns}/{pod} lists the same collisions per target under `collidesWith`. The remedy is "+
			"always the operator's: give the endpoints separate container ports, or drop one declaration.")
	// ScrapeAuthFailures counts /v1/scrape-auth requests that reached the
	// Secret read and failed there, by CAUSE. The route is the only one that
	// hard-fails on external state, and every cause used to answer 404: an
	// RBAC denial (the likeliest real failure, since -scrape-auth-secrets
	// needs a grant added by hand) was indistinguishable from a typo in a
	// monitor's secret ref, and both landed in metadata_requests_total's
	// not_found stream, which is documented as the container-attribution
	// signal. `upstream` is the one to alert on.
	// The label is `reason`, NOT `kind`: `kind` is the monitor-kind dimension
	// on the three sibling monitor_* metrics (values servicemonitor/podmonitor),
	// and reusing the name for a failure cause would make one label mean two
	// unrelated things across metrics an operator reads together.
	ScrapeAuthFailures = Registry.CounterVec("kubescrape_scrape_auth_failures_total",
		"/v1/scrape-auth requests that did NOT yield a credential, by cause — every one of them means a monitor "+
			"endpoint is about to be scraped without the auth or TLS material its CR declares, i.e. up=0 for that "+
			"target, and the agent sees only a status code. not_found = no such Secret or key; upstream = "+
			"forbidden, timeout or unreachable API server (the one to alert on); not_utf8 = value cannot be "+
			"served as a JSON string; disabled = this service does not run -scrape-auth-secrets, so it serves no "+
			"credentials at all; no_monitors = -servicemonitors is off, so nothing can be allowlisted; "+
			"unauthorized = missing or wrong bearer token, which is what a -scrape-auth-token-file mismatch "+
			"between the agents and this service looks like; not_allowed = the ref is not referenced by any "+
			"INDEXED monitor endpoint (the monitor failed to parse, was refused by -monitor-namespaces, or the "+
			"ref is a typo); bad_request = a path segment that cannot name a Kubernetes object. Each cause also "+
			"logs, throttled, naming the ref or the peer.", "reason")
	// ContainerLookupTimeouts is the container endpoint's ATTRIBUTION-failure
	// signal, and it is deliberately separate from the 404 it produces.
	//
	// kubescrape_http_requests_total{pattern="/v1/containers",code="404"} counts
	// an instant miss and a lookup that blocked for the whole -wait-timeout
	// identically, and the two mean opposite things: the first is the agent
	// asking about a container this replica has never heard of (an id from a
	// rotated log file, a pod on another node), while the second is the store
	// failing to learn about a container the agent is holding log lines for
	// RIGHT NOW — one blocked-lookup slot spent per occurrence, and the lines
	// stay unattributed until it resolves.
	ContainerLookupTimeouts = Registry.Counter("kubescrape_container_lookup_timeouts_total",
		"Blocking container lookups whose wait budget expired without the container ID appearing in the store. A "+
			"low rate is normal (the wait covers the ~1s gap between a container starting and the kubelet posting "+
			"its ID, and a rotated log file's id may never come back); a SUSTAINED rate means this replica's pod "+
			"informer is not seeing the pods whose logs the agents are shipping — check kubescrape_apiserver_reachable "+
			"and the pod informer's RBAC. The throttled WARN beside it names one example container id. Requests "+
			"that never blocked (?wait=0, the cadvisor and ingest pollers) are not counted here, and a client that "+
			"disconnects mid-wait is not either.")
	// SelfLookupRefused splits the /v1/self 404s that
	// kubescrape_http_requests_total cannot tell apart. The answer is an
	// IDENTITY, so each refusal has a different remedy and the agent's own
	// counter (kubescrape_self_metadata_lookups_total{outcome="by_name"}) only
	// says that the fallback ran, never why.
	SelfLookupRefused = Registry.CounterVec("kubescrape_self_lookups_refused_total",
		"GET /v1/self requests answered 4xx instead of an identity, by reason. no_pod = the connection's source "+
			"address owns no live pod, which is EXPECTED and permanent for a hostNetwork agent (it shares the node "+
			"address) and for one behind SNAT — those fall back to a lookup by name and are fine; forwarded = the "+
			"request carried Forwarded/X-Forwarded-For/X-Real-Ip, so kubescrape refuses to attribute a connection "+
			"a hop declared is not its caller's (a service mesh adding the header in the caller's own network "+
			"namespace lands here too, at the cost of one extra by-name lookup per -self-attributes-refresh); "+
			"unparseable_peer = the connection had no readable address, which should not happen and is warned. A "+
			"fleet-wide no_pod rate with self-metrics still carrying pod attributes is the by-name fallback "+
			"working; the same rate with kubescrape_self_metadata_resolved at 0 is not.", "reason")
	// InformerWatchErrors counts the list/watch failures the reflector REPORTS
	// to its error handler — a strictly smaller set than "the watch is
	// unhealthy", and the difference is the point of this comment.
	//
	// client-go reaches the handler only when ListAndWatchWithContext RETURNS
	// an error (reflector.go RunWithContext calls watchErrorHandler on any
	// non-nil return). Two things do return one, and both land here: a refusal
	// the API server ANSWERS on the LIST — revoked RBAC, a deleted CRD, a watch
	// the server rejects — and a RELIST that fails for any reason at all
	// (`err = r.list(ctx); if err != nil { return err }`).
	//
	// What it does NOT do is fire RELIABLY while the API server is
	// UNREACHABLE, which is the shape an operator most wants to see. As long as
	// the watch REQUEST itself keeps failing retriably (connection refused,
	// 429 — isWatchErrorRetriable), the reflector backs off and `continue`s
	// inside watchWithResync: it never returns, so it never relists, so nothing
	// is reported. Measured across four outage shapes of up to five minutes: no
	// series at all, /readyz 200 throughout, every log line INFO — and in one
	// real outage the counter stepped only five seconds AFTER recovery. It may
	// equally step during the next relist an outage happens to trigger; that is
	// a signal you cannot alert on the absence of.
	//
	// The half it does cover is worth alerting on, because readiness LATCHES —
	// /readyz gates on the initial sync and is never re-evaluated, deliberately,
	// since an unready service loses its endpoints and cuts every agent off a
	// cache that is still serving useful data — so once the process is up, this
	// and the reachability probe are all that speak.
	//
	// For the unreachable half the signal is kubescrape_apiserver_reachable
	// (RegisterAPIServerProbe), which exists because no PASSIVE signal is
	// dependable:
	// the store gauges do not even freeze at plausible values. The tombstone
	// sweeper keeps running over a store nothing refills, so
	// kubescrape_store_pods DECAYS during an outage (measured: 89 -> 85 over
	// five minutes) — an alert on a FLAT gauge reads healthy exactly when it
	// should not.
	InformerWatchErrors = Registry.CounterVec("kubescrape_informer_watch_errors_total",
		"List/watch failures the informers REPORT, by resource: the refusals the API server ANSWERS (revoked RBAC, a deleted CRD, a rejected watch), plus any relist that fails. It does NOT reliably move while the API server is UNREACHABLE — client-go retries a refused watch internally and never relists, so this stayed flat through outages of up to five minutes — so alert on kubescrape_apiserver_reachable for that half.", "resource")

	// OwnerResolveFailures is internal/owners' ONE signal, and until it existed
	// that package had none at all — no metric, no log, at any level.
	//
	// The owner chain is not decoration: attrs.ServiceName derives service.name
	// from the workload owner, so a Deployment whose metadata cannot be read
	// leaves every one of its pods described by its POD NAME instead, which
	// changes half the Prometheus job of every series the fleet exports for it.
	// The resolver answers a failed read by returning nil and appending the bare
	// owner reference, which renders as a perfectly well-formed response — so a
	// missing RBAC rule, a metadata informer that never synced, or a lister
	// wired for the wrong type degraded attribution CLUSTER-WIDE while every
	// counter in the process stayed flat.
	//
	// Read the reasons, not the total: they are not equally alarming.
	// not_found is the only one that is ever normal (a pod tombstone outliving
	// its deleted owner, or an informer still filling), so a low steady rate
	// there is expected and a SUSTAINED high one means the owner informer is
	// not seeing objects the pods reference — check the ClusterRole and
	// kubescrape_informer_watch_errors_total. lister_error, no_informer and
	// wrong_type are never normal: they are a broken cache or a wiring bug, and
	// each carries a throttled Warn naming the object. bad_api_version and
	// uid_mismatch are per-object oddities (a malformed ownerReference; an
	// owner deleted and recreated under its old name) that cost that one pod
	// its owner's labels.
	OwnerResolveFailures = Registry.CounterVec("kubescrape_owner_resolve_failures_total",
		"Owner-chain and namespace/node metadata reads that did NOT yield the object's metadata, by kind "+
			"(ReplicaSet, Deployment, StatefulSet, DaemonSet, Job, CronJob, Namespace, Node) and reason. The "+
			"affected pod is still served — with the bare owner reference and no owner labels or annotations — so "+
			"nothing fails visibly while service.name, the workload labels and half the Prometheus job silently "+
			"degrade. not_found = the object is not in the informer cache (normal at a low rate: a deleted owner "+
			"under a pod tombstone, or a cache still filling; sustained means the informer is not seeing it); "+
			"lister_error = the cache returned an error other than NotFound (the RBAC-shaped case — alert on "+
			"this one); no_informer = no metadata informer is wired for that resource at all (a wiring bug: the "+
			"kind is in owners.AllGVRs but main did not start it); wrong_type = the cached object is not "+
			"PartialObjectMetadata (a wiring bug); bad_api_version = the ownerReference's apiVersion does not "+
			"parse, so the reference can never match a watched kind; uid_mismatch = an object of that name IS "+
			"cached but under a different UID, so it is refused rather than lending a recreated owner's labels "+
			"to the old reference; owners_capped = the object named more owners than owners.MaxOwners serves, so "+
			"the tail of its chain is not described (the served document says so through pod.ownersOmitted). The "+
			"three wiring/RBAC reasons also log, throttled per object; owners_capped logs once per resolution "+
			"through a keyless throttle.", "kind", "reason")

	// MetadataAnnotationsOmitted counts objects whose annotations this API
	// served SHORT — kubemeta.MaxAnnotationValueBytes refused an oversized
	// value, or kubemeta.MaxAnnotationBytes refused the tail of an oversized
	// set.
	//
	// It exists because the ceiling is otherwise invisible from outside one
	// document: a pod, an owner or a namespace that quietly stopped carrying an
	// attribution annotation looks exactly like one that never had it, and the
	// thing an operator notices is a resource attribute that stopped being
	// stamped weeks after the annotation was added. The served document is
	// truthful on its own (kubemeta.OmittedAnnotation names what went), but a
	// document nobody reads is not a signal.
	//
	// The kind label is the OBJECT's, bounded by owners.AllGVRs plus Pod and
	// Service — never an ownerReference's own Kind, which names arbitrary CRDs.
	// Any nonzero value is worth looking at: on a cluster whose deploy-tool
	// blobs are already dropped, no real object comes close to either ceiling.
	MetadataAnnotationsOmitted = Registry.CounterVec("kubescrape_metadata_annotations_omitted_total",
		"Objects whose annotations were served SHORT, by kind: a single value over kubemeta.MaxAnnotationValueBytes, or a set over kubemeta.MaxAnnotationBytes. Every ScrapeTarget embeds the whole pod document — the pod's annotations, its namespace's and one set per resolved owner — so an unbounded annotation set is an unbounded response on the route every agent polls each scrape cycle. The served object names the omitted keys in its own kubescrape.io/annotations-omitted annotation; nothing else is truncated and no value is ever served shortened.", "kind")

	// BufferTruncated counts bytes the disk buffer lost to damage discovered
	// at OPEN (truncated tails, dropped or foreign segments — diskqueue's
	// open-time loss counters). A crash mid-append costs one torn record;
	// anything larger means corruption cost fsynced records.
	BufferTruncated = Registry.CounterVec("kubescrape_buffer_truncated_bytes_total",
		"Bytes the disk buffer lost to damage discovered at open (truncated, dropped or foreign segments).", "signal")

	// Data loss is counted TWICE over: once per batch and once per record.
	//
	// A batch here is 1..1024 records, so a batch counter alone answers "did we
	// lose anything" and cannot answer "how much" — and this pair is the alert
	// the documented durable configuration points at (with -buffer-dir the
	// tailer sees the ENQUEUE verdict, so the collector's permanent rejections
	// surface here rather than on kubescrape_log_permanent_dropped_total, which
	// has always counted records). Every other producer's drop counter gets the
	// same treatment, and the ones that count batches now say so in the name.
	BufferDroppedBatches = Registry.CounterVec("kubescrape_buffer_dropped_batches_total",
		"Buffered batches dropped after a permanent collector rejection (bad payload, auth, unimplemented).", "signal")
	BufferDroppedRecords = Registry.CounterVec("kubescrape_buffer_dropped_records_total",
		"Records lost with those batches: log records, metric data points or spans, by signal. A batch whose payload no longer DECODES is counted in kubescrape_buffer_dropped_batches_total only — its record count is not recoverable — so this is a lower bound whenever kubescrape_buffer_read_errors_total is also moving.", "signal")
	BufferRequeued = Registry.CounterVec("kubescrape_buffer_requeued_total",
		"Buffered batches moved to the back of the queue after repeated transient failures (keeps one stuck batch from blocking the signal).", "signal")
	BufferFull = Registry.CounterVec("kubescrape_buffer_full_total",
		"Batches the disk buffer refused: the undelivered backlog is at its cap, or one batch exceeds the whole cap. Back-pressure for logs (the tailer rewinds and re-reads), a lost batch for producers that cannot rewind (scrape, self-metrics, log-metrics). Counted per refusal, not per batch: the tailer's in-flush retries can refuse one batch up to three times.", "signal")
	// BufferEnqueueErrors counts write-side refusals that are NOT capacity:
	// a latched fsync failure, a closed queue, ENOSPC from segment
	// preallocation. For a producer that cannot rewind (scrape, self-metrics,
	// log-metrics) the batch is gone, and every other buffer metric stays flat
	// while it happens.
	BufferEnqueueErrors = Registry.CounterVec("kubescrape_buffer_enqueue_errors_total",
		"Batches the disk buffer refused for a reason other than capacity (I/O error, closed queue, no space left on device).", "signal")
	BufferReadErrors = Registry.CounterVec("kubescrape_buffer_read_errors_total",
		"Disk-buffer read failures while draining. lost=true is reported corruption the queue advanced past (its Stats carry the magnitude); lost=false left the queue in place for a retry.", "signal", "lost")
	PositionsCorrupt = Registry.Counter("kubescrape_positions_corrupt_total",
		"Positions files that failed to parse at startup (whatever decoded is kept; the affected inputs re-read "+
			"their window). Recurring bumps across restarts point at a failing disk, not a one-off crash.")
	// PositionsSaveErrors is the write-side counterpart: offsets are silently
	// NOT being persisted, so a restart re-reads (or, with an empty store,
	// skips) per -logs-unknown-files while every other metric stays flat — the
	// same dark-node failure kubescrape_buffer_enqueue_errors_total exists for.
	PositionsSaveErrors = Registry.Counter("kubescrape_positions_save_errors_total",
		"Failed writes of the positions file (committed offsets and the journald cursor are not being persisted). Any sustained rate means a bad path, a read-only mount or a full disk.")
	LogUnresolvedLost = Registry.Counter("kubescrape_log_unresolved_lost_total",
		"Log files deleted before their metadata ever resolved (the metadata service was unreachable "+
			"or the container unknown for the file's whole life). Their content was never read and is lost.")
	LogOversizedDropped = Registry.Counter("kubescrape_log_oversized_dropped_total",
		"Unterminated lines discarded for exceeding the per-entry size bound (no newline within MaxEntryBytes+4096).")
	LogTornFinalLines = Registry.Counter("kubescrape_log_torn_final_lines_total",
		"Unterminated final lines of RENAMED-away files (the fragment can never complete and is dropped). In-place truncation destroys its unread tail unmeasurably — there is nothing left to count — so truncation losses do not appear here or anywhere.")
	LogScrubbed = Registry.CounterVec("kubescrape_log_scrubbed_total",
		"Log bodies redacted by a scrub pattern (one bump per pattern per record, not per match).", "pattern")
	LogArchiveErrors = Registry.Counter("kubescrape_log_archive_errors_total",
		"Compressed log files whose remainder was lost: the stream failed to decode mid-read (truncated gzip, "+
			"trailing garbage), or the file vanished with uncommitted data and no retained fd. What decoded "+
			"before the failure is delivered; the remainder is unrecoverable and the archive settles.")
	LogDrainErrors = Registry.CounterVec("kubescrape_log_drain_errors_total",
		"Reads that failed part-way through DRAINING a file incarnation that is going away (a rotated inode, a "+
			"compressed archive). The drain cannot be retried — the next sweep would fail identically while "+
			"holding the fd — so the unread remainder of that incarnation is unrecoverable and lost. Distinct "+
			"from a clean EOF, which is the drain succeeding.", "source")
	LogPodConfigInvalid = Registry.Counter("kubescrape_log_pod_config_invalid_total",
		"Files whose pod's kubescrape.io/logs annotation failed to PARSE and was ignored (counted at metadata "+
			"resolution, once per file). Logs keep flowing under the source defaults — the failure mode this "+
			"guards against is silent: an operator edits the annotation, nothing changes, and the only signal "+
			"was one Warn line on one node. The offending files and their parse errors are listed on the "+
			"agent's GET /debug/tailer as podConfigError.")
	LogPodAttrsRefused = Registry.CounterVec("kubescrape_log_pod_attrs_refused_total",
		"Resource-attribute keys a pod's kubescrape.io/logs annotation tried to set that name RESOLVED KUBERNETES "+
			"IDENTITY (namespace, pod, container, node) and were refused. The annotation is authoritative about the "+
			"workload's own description, never about which object — or which tenant — the records belong to: "+
			"k8s.namespace.name is the routing key, so honouring it let any pod redirect its logs into another "+
			"tenant. A nonzero rate is a workload attempting it, whether by mistake or not.", "key")
	// LogSegmentsStalled is the ONE state in the tailer where a file stops
	// collecting without losing anything and without any counter moving: a
	// rotated segment must be replayed before the live tail may be read (or the
	// joiner fuses fragments across the gap into records that never existed),
	// and a source that will not open — EACCES on the rotated file, EMFILE at
	// RLIMIT_NOFILE, EIO on a failing disk — leaves that gate closed. Lag grows,
	// the fd stays pinned, and the only other signal is a Warn repeating at
	// sweep cadence. A sustained nonzero value is the alert; under EMFILE it is
	// fleet-correlated. The gate is bounded, so a stall that outlives the bound
	// gives the segment up and shows on kubescrape_log_prefix_lost_total.
	LogSegmentsStalled = Registry.Gauge("kubescrape_log_segments_stalled",
		"Tracked files whose live tail is currently NOT being read because a rotated segment's replay cannot proceed.")
)

// Scrape pipeline (agent).
var (
	Scrapes = Registry.CounterVec("kubescrape_scrapes_total",
		"Scrapes by pipeline and outcome.", "pipeline", "outcome")
	ScrapeDuration = Registry.HistogramVec("kubescrape_scrape_duration_seconds",
		"Scrape duration by pipeline.", nil, "pipeline")
	// The breakdown of kubescrape_scrapes_total{outcome="error"}. A separate
	// family rather than more `outcome` values on that one, because the two
	// answer different questions and are read at different times: the outcome
	// label is the up/down ratio a dashboard plots, this is what an operator
	// looks at once the ratio is already wrong. Their sums agree by
	// construction (both move from the same place, once per scrape).
	ScrapeFailures = Registry.CounterVec("kubescrape_scrape_failures_total",
		"Failed scrapes by pipeline and CAUSE — the number to look at first when targets are up=0, because the "+
			"reasons take different remedies and several are not the target's fault at all. The target could not "+
			"be REACHED: dns (the name does not resolve), connect (refused, unreachable, or the connection was "+
			"reset), tls (the certificate did not verify, the name did not match, or the port speaks plaintext), "+
			"timeout (the scrape budget expired - raise -scrape-timeout, or the endpoint is slow) and canceled "+
			"(the agent is shutting down; not a fault). The target ANSWERED and refused: unauthorized (401 or "+
			"403 - a missing credential, or for the kubelet pipelines a missing RBAC rule; the accompanying warn "+
			"names which) and status (any other non-200). The scrape never left this agent: auth (a bearer, "+
			"basicAuth or TLS secret ref could not be resolved - the metadata service needs -scrape-auth-secrets "+
			"and this agent a matching token file) and relabel (a monitor's metricRelabelings regex would not "+
			"compile; the scrape fails deliberately rather than export what the rule asked to drop). The target "+
			"answered WRONG: proto_refused (it served the protobuf exposition without -scrape-native-histograms "+
			"having asked for it - refused unparsed, since decoding materialises the whole gzip-amplified "+
			"message), sample_limit (-scrape-max-samples was exceeded; what was converted before the abort is "+
			"still exported) and body (a response body over this pipeline's cap). Finally export: the payload "+
			"was scraped and converted and the COLLECTOR - or, with -buffer-dir, the spool - refused it, the one "+
			"reason where nothing is wrong with the target and kubescrape_export_requests_total is the family to "+
			"read. other is anything unclassified; a rate on it means this list needs a new value.",
		"pipeline", "reason")
	// A gauge rather than a counter because the question is "how many are there
	// NOW", and ABSENT rather than 0 when target scraping is off: it is only
	// ever Set by a cycle that fetched the list, so a published 0 always means
	// "the scraper asked and this node has no targets" and never "-metrics is
	// false". The empty target list is the most common first-run failure and it
	// moves no other metric at all.
	ScrapeTargets = Registry.Gauge("kubescrape_scrape_targets",
		"Scrape targets the metadata service returned for THIS node on the last successful fetch, after the "+
			"transforms file's targets: hook. Absent when annotation and monitor scraping is off (-metrics "+
			"false); 0 means the fetch succeeded and returned nothing - no pod on this node carries "+
			"prometheus.io/scrape, no annotated Service selects one, and no ServiceMonitor or PodMonitor "+
			"resolves to one here. It does NOT count the kubelet pipelines, which are configured rather than "+
			"discovered. A fetch that FAILS leaves the previous value standing; GET /debug/targets on the agent "+
			"is the per-target view behind this number.")
	ScrapeSamples = Registry.CounterVec("kubescrape_scrape_samples_total",
		"Samples parsed by pipeline (before filtering).", "pipeline")
	// The counterpart to that "before filtering". Filtering happens between
	// the parse and the conversion, so a keep rule that matches nothing — or a
	// ServiceMonitor metricRelabeling that drops everything — empties a
	// pipeline while kubescrape_scrapes_total reports success and
	// kubescrape_scrape_samples_total keeps climbing. Nothing moved when that
	// happened; scraped minus dropped is now the number that reaches the
	// collector.
	ScrapeSamplesDropped = Registry.CounterVec("kubescrape_scrape_samples_dropped_total",
		"Parsed samples that never became data points, by pipeline and by what discarded them. filter (the "+
			"config's metrics keep/drop rules) and relabel (a monitor's metricRelabelings) discard a sample "+
			"BEFORE it can become a data point and are the operator's own decision, so a rate on either is the "+
			"config working. accumulator is refused INSIDE the conversion and is not: one histogram/summary "+
			"family exceeded maxFamilyAccBytes, the 16 MiB of accumulators the converter may RETAIN for a "+
			"single family — the bound that keeps a target exposing one such family across ~900k label sets "+
			"from OOM-killing the agent while its scrape still records outcome ok. Nobody asked for that drop, "+
			"so unlike the other two it also carries a per-target warn, and any rate on it means a target is "+
			"losing well-formed series. A dashboard filtering the two config reasons hides the only one that "+
			"reports a refusal of OURS.",
		"pipeline", "reason")
	SummaryUnresolved = Registry.CounterVec("kubescrape_summary_unresolved_total",
		"Objects in the kubelet's /stats/summary that the metadata service could not place, by the LEVEL of "+
			"the object: `pod` (no pod of that namespace and name, or one whose UID neither matches nor MIRRORS "+
			"the UID the kubelet reported — a static pod's kubelet-minted UID is proved against the mirror pod's "+
			"kubernetes.io/config.mirror or config.hash annotation, and a pod merely REUSING the name carries "+
			"neither) and `container` (the container's resource carries no container.id, so it does not line up "+
			"with the cadvisor row for the same container: its pod could not be placed, or the pod was placed "+
			"and the container was not — a name the cached pod document does not list, or one it lists from the "+
			"pod SPEC whose status has not reached the API server yet). "+
			"Counted once per OBJECT per scrape, never per data point — a pod with four statistics is one "+
			"unplaceable pod. The statistics are still exported, carrying the identity the payload itself gave "+
			"them, so nothing is lost; what an unplaceable object loses is the JOIN, since a series with no pod "+
			"identity cannot line up with the cadvisor row for the same container. A steady low rate is ordinary "+
			"— a pod that ended between the kubelet building the summary and the lookup landing — while a "+
			"sustained or fleet-wide rate means the metadata service is not answering, and the ephemeral-storage "+
			"series it exists for are arriving unattributable.", "level")
	// The cadvisor pipeline's counterpart to SummaryUnresolved. It did not
	// exist, and metabudget.go's own comment names the consequence: with the
	// allowance intact (the ordinary case) a cadvisor scrape shedding
	// attribution moved NOTHING — the rows still export, still carry their
	// label identity, and still look healthy on kubescrape_scrapes_total, so an
	// operator whose cadvisor series had lost every workload label had no
	// number to point at.
	CadvisorUnresolved = Registry.CounterVec("kubescrape_cadvisor_unresolved_total",
		"cadvisor RESOURCES built without a metadata-service answer, by the level of the object: `container` "+
			"(the row named a container id the store does not know) and `pod` (a pod-level row whose namespace "+
			"and name resolved to nothing, or to a pod carrying a different UID than the cgroup path). Counted "+
			"once per resource per exported chunk, never per sample. The rows ARE still exported, carrying the "+
			"identity their own labels gave them, so this costs attribution rather than data: no owner chain, no "+
			"pod labels, and a service.name that falls back to the pod name. A steady low rate is ordinary - a "+
			"just-started container whose id the kubelet has not posted to the API server yet resolves on a "+
			"later cycle - while a sustained or fleet-wide rate means the metadata service is not answering, and "+
			"is usually accompanied by kubescrape_scrape_metadata_budget_exhausted_total.", "level")
	ScrapeMetaBudgetExhausted = Registry.CounterVec("kubescrape_scrape_metadata_budget_exhausted_total",
		"Scrapes that spent their whole per-scrape metadata allowance, by pipeline. The allowance is half the "+
			"scrape timeout; past it the remaining objects are NOT looked up at all, so they export under the "+
			"identity the payload itself carried and lose only the join. It exists because a metadata service "+
			"that HANGS — a partition or a dropping firewall, as opposed to one that refuses, which fails "+
			"instantly and is harmless — would otherwise consume the entire scrape budget and take the kubelet "+
			"pipelines down with it, discarding stats already parsed. So this counter is the difference between "+
			"degraded attribution and no data: a sustained rate means the metadata service is slow or "+
			"unreachable and this node's cadvisor and summary series are arriving unjoinable. It is the only "+
			"signal for the objects that were never asked about — kubescrape_metadata_requests_total cannot "+
			"move for a request that is never issued.", "pipeline")
	ScrapeMalformed = Registry.CounterVec("kubescrape_scrape_malformed_total",
		"Exposition samples dropped as malformed by pipeline (unparseable lines, histogram buckets without le, summary rows without quantile).", "pipeline")
	ScrapeExemplarsMalformed = Registry.CounterVec("kubescrape_scrape_exemplars_malformed_total",
		"Unparseable OpenMetrics exemplar suffixes by pipeline. NO data was lost: the samples carrying them were exported without the exemplar, which is why this is separate from kubescrape_scrape_malformed_total. Only ever nonzero where exemplar scraping is enabled.", "pipeline")
	ScrapeCollisions = Registry.Counter("kubescrape_scrape_name_collisions_total",
		"Data points dropped because their family name was already claimed by a metric of another shape in the same batch (a target redeclaring a family's TYPE mid-exposition).")
	// The attributable half of the collision above, for the one case the
	// protobuf exposition makes reachable without any TYPE redeclaration: a
	// HISTOGRAM family whose metrics carry different REPRESENTATIONS. One name
	// carries one OTLP type, so the family resolves to native and the other
	// representation's data is dropped here rather than silently in the
	// batcher's type guard.
	ScrapeHistogramMixed = Registry.CounterVec("kubescrape_scrape_histogram_mixed_total",
		"Protobuf histogram metrics dropped because their family resolved to another representation, by pipeline and by the representation that lost: classic = per-bucket rows, nhcb = custom-bucket native (schema -53, whose bounds this client_model cannot read at all).", "pipeline", "dropped")
)

// OTLP exporter (agent).
var (
	Exports = Registry.CounterVec("kubescrape_export_requests_total",
		"OTLP export attempts by signal and outcome: ok, transient (the collector is unreachable or "+
			"back-pressuring and the payload is retried or spooled), permanent (the collector rejected THIS "+
			"payload and no retry can help, so a producer drops it). A sustained transient rate means the "+
			"destination is down; any permanent rate means telemetry is being lost — the accompanying warning "+
			"names the endpoint, the class and the likeliest cause.", "signal", "outcome")
	// ExportSplitParts reports the size-split firing. It is otherwise
	// invisible: a payload over -otlp-max-send-bytes is quietly delivered in
	// pieces, each its own round trip, auth build and gzip pass — and the one
	// shape the splitter cannot rescue (a SINGLE record larger than the cap) is
	// sent alone and rejected by the collector, which surfaces only as a
	// permanent export failure with no hint of where the size came from.
	ExportSplitParts = Registry.CounterVec("kubescrape_export_split_parts_total",
		"Extra parts an over-cap OTLP payload was split into before sending (a payload sent whole adds "+
			"nothing). A sustained rate means a producer is batching past -otlp-max-send-bytes: lower its "+
			"batch size, or raise the cap if the collector's receive limit allows it.", "signal")
	// The other half of the split's story: what it could NOT rescue. Both
	// reasons ship a part the collector is expected to reject wholesale, which
	// on its own surfaces only as a permanent export failure naming no size —
	// and the framing arm is a REFUSAL TO KEEP SPLITTING, so without a series
	// of its own the loss it trades for would be invisible.
	ExportOversizeParts = Registry.CounterVec("kubescrape_export_oversize_parts_total",
		"OTLP parts sent knowingly larger than -otlp-max-send-bytes, by signal and reason — the collector "+
			"rejects each one, so any rate here is telemetry being lost. item: a SINGLE log record, span or "+
			"metric data point is itself over the cap and nothing can shrink it, so it ships alone (find the "+
			"producer of that record — a multi-megabyte log line, a histogram with an enormous label set — or "+
			"raise the cap if the collector's receive limit allows). framing: the split was ABANDONED because "+
			"the framing every part re-copies (the resource attributes, the scope identity, or a metric's "+
			"description) left too little room under the cap for content, so the remainder shipped as one "+
			"over-cap part instead of as thousands of near-empty ones — on the unauthenticated -ingest and "+
			"trace-tier listeners that shape is what a hostile sender constructs, so read a rate here as a "+
			"sender pushing a resource nearly as large as the cap; the accompanying throttled warning names "+
			"the signal and the sizes.", "signal", "reason")
)

// OTLP ingest (agent).
var (
	// IngestRejected counts pushes refused for exceeding one of the receiver's
	// THREE admission bounds: the concurrently-processed count, the raw payload
	// bytes both transports may buffer while reading and decoding, and the
	// estimated DECODED structure those bytes inflate into. They are retryable
	// and the sender still holds the payload, but a persistently non-zero rate
	// means the node cannot keep up with what is being pushed at it.
	//
	// The help enumerates all three because it is the only description the
	// series has, and the three take DIFFERENT operator responses — the one
	// knob an operator reaches for first (-ingest-max-in-flight) does nothing
	// at all for the other two.
	IngestAdmissionRejected = Registry.Counter("kubescrape_ingest_admission_rejected_total",
		"Pushed RESOURCES the transforms file's ingest admission hook (ingest: admit(resource)) rejected — "+
			"removed before enrichment, push still acked. The hook is the operator's per-sender policy on "+
			"listeners nothing authenticates; a script error fails OPEN (the resource is admitted) and counts "+
			"into kubescrape_transform_errors_total{signal=\"ingest\"} instead.")
	IngestBodyRejected = Registry.CounterVec("kubescrape_ingest_body_rejected_total",
		"OTLP request bodies refused at the receiver's door, before anything was decoded, by reason. Every "+
			"reason but one is the OTLP/HTTP door's alone — media_type, content_encoding and aborted have no "+
			"gRPC equivalent at all, and that arm's own size and decode failures are grpc-go's to answer — but "+
			"the family is NOT HTTP-only: too_deep is counted from BOTH transports, the gRPC codec being the "+
			"one hook that runs before pdata's decoder does. FIVE of the six describe a request "+
			"that is WRONG, so the sender must change something before a retry can work: too_large (413, over "+
			"the receiver's cap in either the compressed or the decompressed direction), media_type (415, a "+
			"Content-Type that is not application/x-protobuf), content_encoding (400, a Content-Encoding that "+
			"is neither gzip nor identity), malformed (400, a body that would not decompress, or bytes that are "+
			"not a valid OTLP payload) and too_deep (400 on HTTP, gRPC Internal from the codec: a body inside "+
			"every SIZE cap whose length-delimited NESTING passes 100 levels, which pdata's decoder — it has no "+
			"recursion limit of its own — would follow into an unbounded goroutine stack. It is deliberately "+
			"NOT malformed: the body decodes perfectly, which is the problem). Each of those carries a "+
			"throttled Warn, naming the peer on the HTTP arm — which on a listener nothing authenticates is the "+
			"only way to tell a misconfigured sender apart from a probe — while the gRPC codec runs with no "+
			"peer in hand and names the depth bound instead. `aborted` is the ODD ONE OUT and carries no "+
			"Warn: the client went away mid-upload (a killed pod, a rolled deployment, an SDK export timeout), "+
			"so nothing was wrong with the request and the retry is exactly what happens next — a rolling "+
			"deployment would otherwise log "+
			"one accusation per evicted pod. It is answered 503, deliberately neither 400 nor 408. Also "+
			"deliberately SEPARATE from kubescrape_ingest_rejected_total, which is the receiver protecting "+
			"ITSELF (its in-flight count, its raw byte budget or its decoded-structure budget) and is "+
			"retryable as sent. Only the APPLICATION-"+
			"facing listeners feed this family: the trace tier runs its authenticated internal hop in the same "+
			"process, and folding sibling-shard traffic in would put bearer-authenticated pushes into the series "+
			"an operator reads as \"somebody out there is pushing wrong\" — a failed hop is already one "+
			"kubescrape_service_graph_sends_failed_total on the SENDING shard, where the peer is known.", "reason")
	IngestChainSkipped = Registry.CounterVec("kubescrape_ingest_log_chain_skipped_total",
		"Ingested log RECORDS or RESOURCES on which PART of the line-derived chain was skipped by an abuse "+
			"bound — the data itself is always still forwarded. WHICH part is what the reason says, and the two "+
			"halves are not interchangeable: debugging the wrong one is the cost of reading this as a single "+
			"thing. body_too_large is per RECORD (a body whose text view exceeds 1 MiB; the tailer never feeds "+
			"lines past -logs-max-entry-bytes either) and skips everything that READS the line — the "+
			"logAttributes lift, body enrichment, and the content half of the log-metric labels and of the "+
			"keep/drop rules, both of which still run on the record's attributes and severity, the record still "+
			"being observed with an empty line. resource_too_wide (a resource carrying more than 64 attributes "+
			"— the metric store retains a serialization of the whole resource per series, so sender-chosen "+
			"width is sender-chosen retained heap) and resources_capped (a resource past the first 256 of one "+
			"push) are per RESOURCE and skip the log-metrics OBSERVATION only: enrichment, the lift and the "+
			"rules run in full on every record of such a resource, which is also why neither can move at all "+
			"unless a logMetrics section is configured. The listeners are unauthenticated; a nonzero rate is a "+
			"sender worth finding.", "reason")
	IngestRejected = Registry.Counter("kubescrape_ingest_rejected_total",
		"Pushed OTLP requests refused because a receiver admission bound was reached (retryable: 429 / "+
			"ResourceExhausted — the payload is intact and the sender owns the retry). THREE bounds feed "+
			"this one series and they bound different things, so read the metric with all three in mind: "+
			"the CONCURRENT PUSH COUNT (-ingest-max-in-flight) bounds how many pushes are processed at "+
			"once and bounds no memory at all; the RAW BYTE BUDGET bounds the payload bytes both "+
			"transports hold while reading and decoding (four full-size bodies, scaled up in step with "+
			"-ingest-grpc-max-recv-bytes); and the DECODED BUDGET (decodedSize's estimate against "+
			"decodedBudgetFactor x the raw budget) bounds what those bytes inflate INTO, which the other "+
			"two cannot — pdata's decoded structure runs several times the wire size, so a body inside "+
			"every size cap can still be the thing that fills the node. Raising -ingest-max-in-flight or "+
			"the body cap therefore does nothing for a decoded-budget refusal. One case is retryable in "+
			"FORM but permanent in PRACTICE and is the reason this counter can sit at a steady rate from "+
			"one sender: a single push whose decoded structure alone estimates past the whole decoded "+
			"budget is refused on every retry — it carries a throttled Warn naming the estimate and the "+
			"budget, and the fix is for that sender to batch smaller, never a larger bound here.")
	IngestReserveExpired = Registry.Counter("kubescrape_ingest_reserve_expired_total",
		"gRPC pre-decode buffer reservations reclaimed because the peer sent no message inside the decode "+
			"window; the reclaim also cancels that stream (the sender sees Canceled, which OTLP lists as "+
			"retryable). Deliberately NOT kubescrape_ingest_rejected_total: nothing was refused and the budget "+
			"had room, so folding the two would let one headers-only prober, at zero cost in bytes, drive the "+
			"rate an operator scales the node on. A sustained rate here is one slow or probing sender on a "+
			"listener nothing authenticates.")
	IngestReservedStripped = Registry.CounterVec("kubescrape_ingest_reserved_stripped_total",
		"Attribute occurrences removed at first receipt because a sender shipped a key reserved for "+
			"kubescrape's own plumbing, by key — the namespace router's script marker (honored on a resource "+
			"before any routing glob, so a wire-supplied copy would steer the payload onto any configured "+
			"route and its tenant headers) or the transform engine's drop marker (whose presence-only prune "+
			"would delete the element and count it as an operator-intended transform drop whenever a script "+
			"for that signal is active). The data itself is still forwarded, minus the reserved key. Any "+
			"rate here is a sender shipping a key nothing but kubescrape has a reason to set, i.e. one "+
			"worth finding — which is why the sender's own IDENTITY keys, which every conformant SDK "+
			"sets, are counted separately as kubescrape_ingest_identity_stripped_total.", "key")
	IngestIdentityStripped = Registry.CounterVec("kubescrape_ingest_identity_stripped_total",
		"Resource-attribute occurrences removed at first receipt because a sender DECLARED its own "+
			"Kubernetes identity to an application-facing listener, by key — k8s.namespace.name and the "+
			"pod/node/container keys a resolved lookup overwrites anyway. This is an EXPECTED condition, "+
			"not an accusation: a workload instrumented by the OpenTelemetry Operator sets several of "+
			"these on every push, so a healthy cluster moves this counter continuously and there is "+
			"nothing here to alert on. It is deliberately not kubescrape_ingest_reserved_stripped_total, "+
			"whose keys are kubescrape's own plumbing markers and where any rate at all is a sender worth "+
			"finding. The strip exists because internal/agent/route keys TENANCY on k8s.namespace.name "+
			"and these listeners authenticate nothing, so a declared namespace would choose another "+
			"tenant's endpoint and headers; enrichment owns the same keys for a resource it resolves, and "+
			"a resource it cannot resolve has no correction available. The data itself is still "+
			"forwarded, minus the key — and service.name, service.namespace and service.instance.id are "+
			"left to the sender. What the strip does NOT close, stated so this counter is not mistaken "+
			"for a closed question: the lookup keys (container.id, k8s.pod.uid) are exempt because "+
			"stripping them would disable attribution entirely, and the metadata service's "+
			"/v1/pods/{ns}/{name} is unauthenticated while its container index is cluster-wide — so a "+
			"sender that reads another pod's container id and declares no namespace of its own is "+
			"RESOLVED into that pod's namespace and routed to its tenant. Nothing on the wire is "+
			"forged there, so nothing is stripped and this counter does not move. Authenticating the "+
			"listener or the metadata service is what closes it.", "key")
	IngestEmptyMetricsDropped = Registry.Counter("kubescrape_ingest_empty_metrics_dropped_total",
		"Pushed metrics removed at first receipt because they carried no data points. An empty metric is "+
			"legal OTLP, so nothing downstream rejects one: it would ride enrichment, the split regrouping, "+
			"the transform scripts, the router, the disk buffer and the wire, spending its name, description, "+
			"unit and framing against the send cap on every push, and deliver no measurement. kubescrape's own "+
			"producers cannot emit one (every metric-building path appends a metric's first data point in the "+
			"same function that creates its shell), so any rate here is a SENDER creating metric descriptors "+
			"it never records into — and this counter is the only report of it that exists. The rest of the "+
			"payload is forwarded unchanged; a push consisting only of empty metrics is acked without a send.")
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

// SelfMetadataLookups counts the agent's own-pod lookups by outcome, SEPARATELY
// from kubescrape_metadata_requests_total.
//
// That counter is documented as the container-attribution health signal, and
// the self lookup retries forever: a fleet where /v1/self cannot resolve
// (hostNetwork, a NAT hop) would otherwise contribute a permanent stream of
// not_found to it and fire an alert about an attribution problem that does not
// exist. Separation is not achieved by this counter alone — the agent gives
// the self lookup its OWN metaclient.Client, without the Observe hook, since
// the hook is per-client and fires inside every fetch (main.go). Counting
// here and observing there would have double-counted the outcome into the
// very metric the split exists to keep clean.
var SelfMetadataLookups = Registry.CounterVec("kubescrape_self_metadata_lookups_total",
	"Own-pod metadata lookups for -self-attributes, by outcome.", "outcome")

// SelfMetadataLookups' outcome label values, shared by the two binaries'
// resolvers so their series stay unionable (each binary re-typing the strings
// is how one grows a spelling the other's dashboards do not match).
const (
	SelfLookupSelf   = "self"    // resolved via GET /v1/self (the agent's first try)
	SelfLookupByName = "by_name" // resolved via the namespace/name fallback
	SelfLookupError  = "error"   // not resolved; the error is returned to the poller
)

// RegisterMonitorsRejected publishes kubescrape_monitors_rejected — the count
// of monitors whose CURRENT object does not parse, by kind — from the index's
// own state (servicemonitors.Index.Rejected, adapted by the caller to the
// kinds actually watched).
//
// It is the STATE beside kubescrape_monitor_parse_errors_total's events. The
// counter says a breakage happened and is news-gated, so a monitor that stays
// broken for weeks is one warn line and one increment — nothing an operator
// can alert "still true" on. An unparseable update DELETES the monitor from
// the index, dropping every target it contributed, so a nonzero value here
// means some configuration is presently contributing nothing; it returns to 0
// when the object is fixed or deleted.
//
// Called exactly when -servicemonitors runs with a monitoring CRD present, and
// the caller's hook emits a kind only while that kind's CRD is watched — so a
// published 0 always means "watched, and none rejected", never "off", and an
// unwatched kind is absent rather than a forever-0 series (the
// self-metadata gauge's rule). Alert on nonzero sustained longer than a
// rollout: a blip is an operator mid-edit, a plateau is a monitor nobody has
// noticed is dead.
func RegisterMonitorsRejected(rejected func() map[string]int) {
	Registry.GaugeFuncVec("kubescrape_monitors_rejected",
		"Monitors whose current object fails to parse and is therefore ABSENT from the index — every target "+
			"it contributed is dropped while this is nonzero. The state half of "+
			"kubescrape_monitor_parse_errors_total (the news-gated event): the counter says a breakage "+
			"happened, this says one is still true, and it returns to 0 when the object is fixed or deleted. "+
			"Registered only while -servicemonitors is on with the CRD present, and a kind appears only while "+
			"that CRD is watched, so 0 means watched-and-clean, never off.",
		"kind", func() map[string]float64 {
			counts := rejected()
			out := make(map[string]float64, len(counts))
			for k, v := range counts {
				out[k] = float64(v)
			}
			return out
		})
}

// RegisterSelfMetadata exposes whether this process has resolved the pod it
// runs in, whose attributes it stamps on the metrics it generates about itself
// (-self-attributes). Both binaries register it whenever the lookup RUNS, and
// only then: a registered gauge means "this process is trying", so a 0 always
// means unresolved and never "the feature is off".
//
// Without it an unattributed process is invisible: the agent's failed lookups
// were indistinguishable from any other in kubescrape_metadata_requests_total,
// and the metadata service's own lookup touches no counter at all — so "my
// agents' own metrics carry no pod" is unalertable, and the symptom (a missing
// label on one job) is easy to read as a dashboard problem.
func RegisterSelfMetadata(resolved func() bool) {
	Registry.GaugeFunc("kubescrape_self_metadata_resolved",
		"1 when this process has resolved its own pod's metadata for -self-attributes, 0 while it has not.",
		func() float64 {
			if resolved() {
				return 1
			}
			return 0
		})
}

// RegisterReadiness publishes one gauge per startup gate /readyz waits on, so
// a fleet stuck unready is diagnosable from the metrics as well as from the
// probe body — registered by both binaries, exactly when they have gates.
//
// The probe already names its pending gates, but only to whoever curls it: a
// rolling update that stops at the first node shows up as a Deployment/DaemonSet
// that will not progress, and the pod that could answer the question is the one
// nobody has a shell on. The self-metrics push runs from startup regardless of
// readiness, so an unready process still reports this.
//
// A gate is published from the moment it is REQUIRED, which is what makes 0
// mean "waiting" rather than "absent"; a gate that never appears was never
// wired for this deployment (its pipeline is off). The value flips to 1 once
// and stays there — these are STARTUP gates, not liveness.
func RegisterReadiness(gates func() map[string]bool) {
	Registry.GaugeFuncVec("kubescrape_readiness_gate",
		"1 when this startup gate is satisfied, 0 while it is still pending, by gate. /readyz is 200 only when every gate reads 1, and a rolling update advances on /readyz — so a gate sitting at 0 across the fleet IS the stalled rollout, and the label says which subsystem to look at. Gates are the pipelines this process actually wired, so an absent gate means that pipeline is off rather than healthy. Alert on a gate at 0 for longer than a pod's startup budget.",
		"gate", func() map[string]float64 {
			state := gates()
			out := make(map[string]float64, len(state))
			for name, ok := range state {
				if ok {
					out[name] = 1
				} else {
					out[name] = 0
				}
			}
			return out
		})
}

// High-frequency cgroup sampler (agent, -cgroup-stats).
var (
	CgroupSamples = Registry.Counter("kubescrape_cgroup_samples_total",
		"Per-container cgroup sampling rounds. Divided by the number of containers and the elapsed time this is the effective sampling rate, which is what tells a stalled sampler (a slow export goroutine cannot stall it, but a wedged filesystem can) from a quiet node.")
	CgroupReadErrors = Registry.CounterVec("kubescrape_cgroup_read_errors_total",
		"Failed reads of a held cgroup file, by file. A container removed between two samples produces a burst of these until the next discovery pass drops it, so a small steady rate on a node with churn is normal; a rate proportional to the container count is not.", "file")
	CgroupOpenErrors = Registry.Counter("kubescrape_cgroup_open_errors_total",
		"Discovered container cgroups whose files could not be opened (the container went away between the directory listing and the open, or its controllers are not enabled). The container is retried on the next discovery pass.")
	CgroupCounterResets = Registry.Counter("kubescrape_cgroup_counter_resets_total",
		"Intervals discarded because cpu.stat's usage_usec went backwards — the cgroup was replaced under a path still being sampled. The reading becomes the new baseline; no rate is derived across the reset, since the unsigned difference would render an enormous positive burst that never happened.")
	CgroupContainersCapped = Registry.CounterVec("kubescrape_cgroup_containers_capped_total",
		"Discovered container cgroups refused because a cap was reached, by which cap. The two bound different resources and have different remedies: `tracked` is the FILE DESCRIPTOR cap (three are held open per sampled container so the per-second read path allocates nothing), and a container refused there is discovered and attributable but never sampled; `pending` is the MEMORY cap on the not-yet-attributed set, which holds no descriptors at all and is sized for every pod's sandbox cgroup plus a metadata outage's worth of real containers. A nonzero `tracked` rate above the kubelet's max-pods means either an unusual node or a discovery walking something it should not; a nonzero `pending` rate means the hierarchy holds thousands of cgroups nothing can place.", "cap")
	CgroupDiscoveryErrors = Registry.CounterVec("kubescrape_cgroup_discovery_errors_total",
		"Discovery passes that could not read part of the cgroup hierarchy, by scope. `root` is the whole pass failing to list -cgroup-stats-root: the root is unmounted or unreadable, NOTHING is discovered and the sampler is exporting nothing — the same condition the startup warning names. `subtree` is a directory below the root failing to list: the pass still returns what it found, but it is INCOMPLETE, so nothing is retired from it (an unreadable directory is indistinguishable from a container that went away) and a container that really did go away keeps its descriptors until a complete listing proves otherwise.", "scope")
	CgroupWindowsDropped = Registry.CounterVec("kubescrape_cgroup_windows_dropped_total",
		"Container windows measured and then discarded rather than exported, by reason. ALERT ON THE REASON, NOT THE FAMILY: two of the three are faults and the third is expected on an ordinary node. `unresolved` means the metadata service could not place the container when the payload was built, so the window had no resource to carry it (the samples are already reset; a window describes one bounded interval and is never carried forward, which would silently stretch the interval the numbers claim to cover). `export_failed` means the collector — or, with -buffer-dir, the spool — refused the payload. Those two are the pipeline's LOSS signal: a sustained rate is data an operator asked for and did not get. `too_short` is NOT a fault — it is a container that entered the sampled set and left the cgroup hierarchy before any window of its life could be described, which is the only evidence a node can produce that -cgroup-stats-discover-interval is missing short-lived containers (a container that starts AND exits between two discovery passes is never seen at all and leaves nothing to count). It fires on ~13% of containers at every lifetime up to the discovery interval, so any node running CronJobs or init containers sits permanently nonzero; the remedy is a shorter discovery interval, paid for in metadata-service lookups. A rate(...) > 0 alert over the whole family therefore pages on normal pod churn.", "reason")
	CgroupHeldWindows = Registry.CounterVec("kubescrape_cgroup_held_windows_total",
		"Signal windows that took fewer than two samples, by what was done about it. A single reading is not a distribution (its stddev is 0 by construction and its max and min are the same number — the average the cadvisor scrape already publishes), so `held` re-states the last distribution actually measured, which bridges a sampling gap of a second or two without the series alternating between real and degenerate points. The hold is BOUNDED: `expired` counts a signal reaching the bound and ceasing to be exported, which is what keeps a dead-but-still-listed container (a CRI-O conmon scope outliving its container, or a stale listing) from republishing its last measurement as fresh data forever.", "outcome")
	CgroupScanTruncated = Registry.Counter("kubescrape_cgroup_scan_truncated_total",
		"Discovery passes that hit the directory budget and stopped early, so the container set is INCOMPLETE. Never expected on a Kubernetes node; if it moves, -cgroup-stats-root is pointing at something far larger than a cgroup hierarchy.")
	CgroupUnresolved = Registry.CounterVec("kubescrape_cgroup_unresolved_total",
		"Identity lookups that did not place a container cgroup, by outcome. Such a cgroup is NOT exported: a resource with no service.name has no Prometheus job, joins none of the cadvisor series these gauges exist to explain, and would appear and disappear with a job attached as lookups succeeded and stopped succeeding. `pending` counts one DEFINITIVE miss at discovery — the metadata service answered and does not know this container id — where the verdict decides whether to spend three file descriptors on the cgroup; `unreachable` counts a lookup the service could not answer at all (transport failure, 5xx, timeout), which is deliberately kept apart because it says nothing about the cgroup and therefore never counts toward giving up on it; `abandoned` counts giving up on one after a grace period of definitive misses, after which it is retried far more slowly — a cadence an unreachable lookup drops it back out of, so the node resumes within a minute of the service returning. `export` counts a failure at the other seam, where the resource is rebuilt from current metadata to carry a window that has already been measured — so unlike the others it costs data, counted again as kubescrape_cgroup_windows_dropped_total{reason=\"unresolved\"}. A steady low `pending` rate is normal and expected (every pod's sandbox cgroup lives there permanently, its container id appearing in no pod's containerStatuses); any sustained `unreachable` or `export` rate means the metadata service is not answering.", "outcome")
	CgroupContainersRetired = Registry.Counter("kubescrape_cgroup_containers_retired_total",
		"Containers dropped from the sampled set because every read of all three of their cgroup files failed for several consecutive export windows: the cgroup is still listed but no longer readable. It is what a CRI-O container whose own scope was removed while its conmon scope lingers looks like, and what a stale directory listing looks like anywhere. Their descriptors are released and they stop being counted in kubescrape_cgroup_containers; without the retirement they held three file descriptors and issued three failing reads per sampling period for the life of the process. A sustained rate means containers are disappearing in a way discovery cannot see.")
)

// RegisterCgroupStats publishes the tracked-container gauge, registered exactly
// when the sampler RUNS — the RegisterSelfMetadata rule, for the same reason. A
// published 0 then always means "enabled and finding nothing", which is the
// whole failure this pipeline had to be built not to hide: an agent with no
// /sys/fs/cgroup mount starts cleanly, answers /readyz 200 and exports no
// container at all, and an absent metric family is indistinguishable from a
// feature nobody turned on.
func RegisterCgroupStats(containers, unresolved func() int) {
	Registry.GaugeFunc("kubescrape_cgroup_containers",
		"Container cgroups currently sampled by -cgroup-stats. Registered whenever the sampler runs, so 0 means it is running and finding nothing (typically a missing read-only /sys/fs/cgroup mount) rather than disabled.",
		func() float64 { return float64(containers()) })
	Registry.GaugeFunc("kubescrape_cgroup_unresolved_containers",
		"Container cgroups discovered by -cgroup-stats whose identity the metadata service has not placed, and which are therefore not exported. One per pod is EXPECTED and permanent (the sandbox/pause cgroup appears in no pod's containerStatuses); a value that tracks the container count while kubescrape_cgroup_containers sits near 0 means the metadata service is not answering.",
		func() float64 { return float64(unresolved()) })
}

// Journald input (agent).
var (
	JournalEntries = Registry.Counter("kubescrape_journal_entries_total",
		"Journal entries exported.")
	JournalRestarts = Registry.Counter("kubescrape_journal_restarts_total",
		"Journal reader restarts.")
	JournalTruncated = Registry.Counter("kubescrape_journal_truncated_total",
		"Journal messages truncated at MaxEntryBytes (the record carries log.truncated).")
	// JournalEntryDefects covers the read-side fallbacks that were silent: the
	// journal stores raw BYTES, so a message can be invalid UTF-8 and is
	// rewritten with U+FFFD before it can be exported, and an entry can carry
	// no realtime stamp at all, in which case the record is dated with the
	// agent's own clock. Both change the record the operator receives, and
	// neither is visible in it (a replaced byte looks like the producer wrote
	// it; a substituted timestamp looks authoritative). One CounterVec rather
	// than two families because they are the same class of event and an
	// operator alerts on the family, not on either value.
	//
	// Deliberately NOT here: an unknown PRIORITY, which the converter maps to
	// severity Unspecified. That one IS visible — in the exported record's own
	// severity field — so it needs no counter to be noticed.
	JournalEntryDefects = Registry.CounterVec("kubescrape_journal_entry_defects_total",
		"Journal entries the reader had to repair before exporting, by defect: invalid_utf8 (raw bytes the journal "+
			"stored that are not valid UTF-8; each is replaced with U+FFFD, so the exported body differs from what "+
			"the producer wrote) and no_timestamp (the entry carried no realtime stamp, so the record is dated with "+
			"the agent's clock at read time instead of the producer's). Each carries a throttled warning naming the "+
			"unit.", "defect")
	JournalExportFailures = Registry.Counter("kubescrape_journal_export_failures_total",
		"Journal batch exports that failed and are being retried IN PLACE. The batch is kept and never re-read, "+
			"so this counts ATTEMPTS — not lost entries, and not re-reads: a steady rate is a collector outage the "+
			"reader is riding out with its cursor uncommitted, and the loss counter is "+
			"kubescrape_journal_dropped_batches_total. Deliberately NOT kubescrape_log_export_failures_total, which "+
			"is the tailer's files-rewound counter and cannot apply here — journald rewinds no file, and the "+
			"singleton that reads a journal typically runs with -logs=false, so those increments landed on a "+
			"metric whose help described something that had not happened.")
)

// OTLP ingest (agent).
var (
	Ingested = Registry.CounterVec("kubescrape_ingest_resources_total",
		"RESOURCES by enrichment outcome — the resource being the unit enrichment is APPLIED to, which is not "+
			"the same denominator on both paths and cannot be. On the resource path (all logs, all traces, "+
			"-ingest-metrics-mode=resource, and an auto push not demoted to split) it is one count per PUSHED "+
			"ResourceLogs/ResourceMetrics, so five resources naming one container id count five even though the "+
			"metadata lookup behind them ran once: the lookup memoises per request, the counter deliberately "+
			"does not, or the series could not say how much of a push went unattributed. On the "+
			"datapoint/split path it is one count per DISTINCT DESCRIBED OBJECT per request, that being what "+
			"the splitter mints a resource for. So read it as resources enriched, never as senders and never "+
			"as metadata lookups (kubescrape_metadata_requests_total counts those). enriched = an id resolved; "+
			"peer_ip = no id, attributed by the connection's source address; peer_ip_rejected = that address "+
			"resolved to the RECEIVER's own workload, so it was rewritten in flight (a proxy, a mesh sidecar, "+
			"or an internal hop addressed to the application port) and nothing was attributed — anything above "+
			"zero means peer-IP attribution cannot work on that path; unresolved = nothing identified the "+
			"object the resource describes, which is the SENDER on the resource path but a DESCRIBED OBJECT on "+
			"the split path, where the sender's own resource may have resolved perfectly; split_capped = the "+
			"push exceeded what one payload may inflate into — either its distinct-object count "+
			"(maxSplitGroups) or the bytes those per-object resource copies would cost (maxSplitCopyBytes) — "+
			"so the remainder shares the sender's resource unenriched rather than costing one full resource "+
			"copy each.", "outcome")
	ExportRejected = Registry.CounterVec("kubescrape_export_rejected_records_total",
		"Records the collector REJECTED inside a payload it otherwise accepted (OTLP partial_success), by signal. "+
			"The export succeeded, so every producer advanced its offset, cursor or position past them — these are "+
			"lost, permanently, and retrying cannot help (OTLP defines them as invalid rather than deferred). "+
			"Any nonzero rate means telemetry is being discarded downstream; the collector's own message is on the "+
			"accompanying warning.", "signal")

	// Export seam (agent): routing and transforms. (Grouped here with the
	// ingest metrics historically; the banner above does not cover them.)
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
		"Records a transform script called drop() on, by signal: logs (log records), metrics (data points — a dropped metric counts all of its points), traces (spans) and targets (whole scrape targets the targets: hook dropped, which stop being scraped from that cycle on; there is no other signal for one, since a target that is never fetched has no up series to go to 0). Counted when the batch's export is ACKED (a delivered forward, or a payload transformed to nothing and acked without a send), so a producer's transient retries never re-count. The one exception is the tailer's in-place seam, which counts at transform time: a rewound re-read after a failed export re-runs the (possibly hot-reloaded) script and re-counts.", "signal")
	TransformReloads = Registry.CounterVec("kubescrape_transform_reloads_total",
		"Transforms-file reloads by outcome (applied, failed — a failed compile keeps the last good program).", "outcome")

	// Trace tier (the -service-graph shard's sampler and span metrics).
	TraceSpansDropped = Registry.CounterVec("kubescrape_trace_spans_dropped_total",
		"Ingested spans dropped by the trace sampler (probability = the consistent trace-ID decision, rate = the spans/second cap).", "reason")
	SpanMetricsDropped = Registry.Counter("kubescrape_span_metrics_dropped_total",
		"Spans not aggregated into span metrics because the dimension-cardinality cap was reached.")
	SpanMetricsEvicted = Registry.Counter("kubescrape_span_metrics_evicted_total",
		"Span-metric series dropped at export because their dimensions went unobserved for traceMetrics.staleAfter (this is what frees cardinality-cap slots).")
)

// Service-graph edges (the -service-graph shard). Every one of these counts a
// place where the graph is INCOMPLETE, and each names its cause — a config
// bound for all but the unnamed-spans counter, because those bounds trade
// completeness for memory and an operator has to be able to see which one is
// binding: an edge missing from Grafana's Service Graph view looks exactly
// like a call that never happened.
var (
	ServiceGraphDropped = Registry.Counter("kubescrape_service_graph_dropped_total",
		"Edges not aggregated because the serviceGraph.maxCardinality series cap was reached (existing edges keep reporting; a new one is lost until eviction frees a slot).")
	ServiceGraphEvicted = Registry.Counter("kubescrape_service_graph_evicted_total",
		"Edge series dropped at export because they went unobserved for serviceGraph.staleAfter (this is what frees cardinality-cap slots).")
	// ServiceGraphStoreFull is the pairing store's back-pressure. It is bumped
	// per SPAN, not per edge: the span is never stored, so its partner will
	// later expire unpaired too, and both counters move for one lost request.
	ServiceGraphStoreFull = Registry.Counter("kubescrape_service_graph_store_full_total",
		"Spans dropped because the pairing store held serviceGraph.maxItems half-edges (the request cannot become an edge).")
	// ServiceGraphExpired is the OTHER shape of an incomplete graph, and a
	// non-zero baseline is normal: a client half whose server is uninstrumented
	// expires by design (and may then become a virtual-node edge). A RISING rate
	// means serviceGraph.wait is shorter than the real client-to-server span
	// delivery gap, or the two halves are landing on different shards.
	ServiceGraphExpired = Registry.Counter("kubescrape_service_graph_expired_total",
		"Half-edges that expired after serviceGraph.wait without their partner arriving.")
	// ServiceGraphUnnamed is the one counter in this block with no config bound
	// behind it: the incompleteness is the SENDER's. A resource without
	// service.name gives the graph no label value to name a node with, and
	// pairing its spans anyway minted one anonymous ""-labeled vertex that
	// every unattributable sender in the cluster shared, accreting edges to
	// real services (Tempo skips such resources too).
	ServiceGraphUnnamed = Registry.Counter("kubescrape_service_graph_unnamed_spans_total",
		"Spans skipped by the service-graph processor because their resource carries no service.name — nothing to name a graph node with. The shipped tier enriches at entry and derives service.name for every attributable sender, so a sustained rate means unattributable senders whose spans can mint no edge (they still forward to the collector; only the graph skips them).")
)

// RegisterServiceGraphStats publishes the pairing store's own numbers on the
// shard (-service-graph): what pairing ACHIEVED, plus the one leading
// indicator the counters above cannot give.
//
// The counters above all move only once the graph has already lost something.
// The pending gauge is the backlog against serviceGraph.maxItems — "the store
// is at 80% and climbing" — the same reason RegisterBufferStats exists beside
// the disk buffer's drop counters. Completed is what makes the rest readable:
// an expiry rate means nothing without the pairing rate it is a fraction of.
//
// Published through a registration function because the values are owned by
// servicegraph's store, which obs cannot reach — that package imports obs, so
// the dependency runs one way only. ServiceGraphStat mirrors
// servicegraph.Stats for exactly that reason.
//
// Stats.Dropped and Stats.Unpaired are deliberately NOT published here: the
// first is already kubescrape_service_graph_store_full_total, and the second is
// kubescrape_service_graph_expired_total minus the virtual-node counter below
// (every expiry goes exactly one of those two ways). A second series carrying a
// number two others already determine is one more thing to keep consistent.
func RegisterServiceGraphStats(stats func() ServiceGraphStat) {
	Registry.GaugeFunc("kubescrape_service_graph_pending_edges",
		"Half-edges currently awaiting their partner. serviceGraph.maxItems caps this; at the cap spans are refused (see kubescrape_service_graph_store_full_total), so the ratio against the cap is the leading indicator.",
		func() float64 { return float64(stats().Pending) })
	Registry.CounterFunc("kubescrape_service_graph_completed_total",
		"Edges completed by BOTH halves arriving within serviceGraph.wait. The denominator for the loss counters: an expiry or drop rate only means something against the rate of pairings that worked.",
		func() float64 { return float64(stats().Completed) })
	Registry.CounterFunc("kubescrape_service_graph_virtual_node_total",
		"Half-edges that expired unpaired but named their far side through serviceGraph.virtualNodePeerAttributes, and so still reached the graph (as a virtual-node edge). The remainder of kubescrape_service_graph_expired_total is the genuinely lost part — nothing named the missing side, so that request is on no edge at all.",
		func() float64 { return float64(stats().VirtualNode) })
	Registry.CounterFunc("kubescrape_service_graph_unkeyable_total",
		"Spans that could not be keyed for pairing: no trace id, or a client/producer span with no span id of its own. Never stored — every zero id shares one key space, so they would cross-pair unrelated requests into invented edges (a zero-id client span keys exactly where every ROOT SERVER span of its trace does). A moving rate means an SDK is emitting malformed spans.",
		func() float64 { return float64(stats().Unkeyable) })
}

// ServiceGraphStat is the pairing store's snapshot (servicegraph.Stats' shape).
type ServiceGraphStat struct {
	Pending     int
	Completed   uint64
	VirtualNode uint64
	Unkeyable   uint64
}

// RegisterServiceGraphResharder publishes the tier's INTERNAL hop: how a shard
// that received an application push distributed those spans to the shards that
// own their traces.
//
// These are the numbers only the resharder knows. The wire outcome of each hop
// lands in kubescrape_export_requests_total{signal="traces"} together with the
// ordinary collector exports, where a hop failure is indistinguishable from a
// collector failure — and the two mean very different things: one is a shard
// that cannot be reached, the other is a destination outside the cluster.
//
// A failed hop is NOT silent, unlike the best-effort side channel this replaced:
// the entry shard holds the only copy of a pushed span, so it refuses the
// application's push and the sender retries. SendsFailed therefore moves in step
// with the senders' own error rate.
func RegisterServiceGraphResharder(stats func() ServiceGraphReshardStat) {
	Registry.CounterFunc("kubescrape_service_graph_spans_forwarded_total",
		"Spans this shard handed to ANOTHER shard because that shard owns their trace, and which it accepted. Roughly (N-1)/N of everything pushed to this pod on an N-shard tier: the remainder is kubescrape_service_graph_spans_local_total. This is the tier's internal bandwidth.",
		func() float64 { return float64(stats().SpansForwarded) })
	Registry.CounterFunc("kubescrape_service_graph_spans_local_total",
		"Spans this shard already owned and kept in-process, taking no second hop. Its ratio against the forwarded count should track 1/N for an N-shard tier; a persistent skew means the ring is unbalanced or one shard is receiving most of the pushes.",
		func() float64 { return float64(stats().SpansLocal) })
	Registry.CounterFunc("kubescrape_service_graph_spans_unkeyed_total",
		"Pushed spans with no trace id. They cannot be hashed onto the ring, so they are kept locally and exported from here; they can never pair into an edge. A moving rate means an SDK is emitting malformed spans.",
		func() float64 { return float64(stats().SpansUnkeyed) })
	Registry.CounterFunc("kubescrape_service_graph_sends_failed_total",
		"Failed internal hops (one per owning shard per batch). Every one of them FAILS the application's push — the entry shard holds the only copy of those spans — so this moves together with the senders' export errors, and a rate that tracks one shard means that shard is down or its token is wrong.",
		func() float64 { return float64(stats().SendsFailed) })
	Registry.CounterFunc("kubescrape_service_graph_loops_blocked_total",
		"Spans in application pushes refused for carrying the internal forwarded marker: an internal hop addressed to the tier's APPLICATION port instead of its authenticated receiver, which without the refusal would re-enrich and re-shard every span on every hop until the network is the incident. Always zero in a correct deployment; anything else is a config error to fix now.",
		func() float64 { return float64(stats().LoopsBlocked) })
}

// ServiceGraphReshardStat is the resharder's snapshot
// (servicegraph.ReshardStats' shape; see RegisterServiceGraphStats on why obs
// declares its own).
type ServiceGraphReshardStat struct {
	SpansForwarded uint64
	SpansLocal     uint64
	SpansUnkeyed   uint64
	SendsFailed    uint64
	LoopsBlocked   uint64
}

// Tail sampling (the -service-graph shard's buffering layer). Two things need
// counting here that no other layer knows: WHICH rule decided a trace — the
// policy engine returns it precisely so the answer is not "some subset of the
// list" — and what the memory bounds cost when they bind, since every bound
// trades trace completeness for a smaller buffer.
var (
	TailSampleTraces = Registry.CounterVec("kubescrape_tail_sampling_traces_total",
		"Traces decided by the tail sampler, by verdict (keep, drop) and by the policy that decided. policy=\"none\" is the default drop — no policy had an opinion — which is what a policy list matching nothing looks like. Every configured policy gets its series at startup, so a policy that has never fired reads as zero rather than as absent. This counts DECISIONS: whether the kept trace then reached the collector is kubescrape_tail_sampling_spans_total.", "decision", "policy")
	TailSampleSpans = Registry.CounterVec("kubescrape_tail_sampling_spans_total",
		"Spans leaving the tail-sampling buffer, by fate: kept = the trace was sampled and the export was acked (with -buffer-dir, acked means SPOOLED — a decided keep is durable and a collector outage becomes a backlog); dropped = the trace was not sampled; lost = the final attempt to move the trace's spans out of this process failed and nothing here holds a copy any longer (the sender was acked when the spans were buffered). Under a routed or size-split export, earlier shares may already have been delivered or spooled, so lost is an UPPER BOUND on loss — it never under-counts. A moving lost rate is data loss, not back-pressure; without -buffer-dir a collector outage produces it directly, with one it means the spool itself refused the payload.", "outcome")
	TailSampleEarly = Registry.CounterVec("kubescrape_tail_sampling_early_decisions_total",
		"Traces decided BEFORE their decisionWait elapsed, by the bound that forced it (spans_per_trace, max_traces, max_spans) or shutdown (a graceful stop flushing the buffer, plus any straggler push decided on arrival after that flush). An early decision judges the spans present, so it degrades gracefully — a slow trace can be missed, a fast one is never invented — but a sustained rate against a bound means that bound is sized below the shard's span rate.", "reason")
	TailSampleLate = Registry.CounterVec("kubescrape_tail_sampling_late_spans_total",
		"Spans that arrived after their trace was decided and followed the cached verdict, by outcome (kept = forwarded immediately, dropped). A large kept+dropped share means decisionWait is shorter than the spread of a trace's arrival.", "outcome")
	TailSampleCacheEvicted = Registry.Counter("kubescrape_tail_sampling_cache_evictions_total",
		"Verdicts evicted from the decision cache by its SIZE cap while still LIVE (evicting an entry already past its TTL is the cache reclaiming space, not a signal, and is not counted). A span arriving for an evicted trace starts a fresh window AND re-charges the rateLimiting and composite budgets — the cache remembers a trace was charged for as long as it holds the entry at all, so eviction is the only thing that still double-charges. Raise tailSampling.decisionCacheSize if this moves.")
)

// RegisterTailSamplingStats publishes what the tail-sampling buffer is holding.
//
// This is the one number the counters above cannot give and the one an operator
// most needs: buffered spans are ACKED but not durable (see agent/tailbuffer's
// package doc), so this gauge is exactly what a hard kill of the shard would
// lose at that instant. It is also the leading indicator for the memory bounds,
// the same reason RegisterBufferStats exists beside the disk buffer's drop
// counters — the early-decision counter only moves once a bound has ALREADY
// bound.
func RegisterTailSamplingStats(stats func() TailSamplingStat) {
	Registry.GaugeFunc("kubescrape_tail_sampling_buffered_traces",
		"Traces currently assembling in the tail-sampling buffer, awaiting their decision. tailSampling.maxTraces caps it; at the cap the oldest is decided early (kubescrape_tail_sampling_early_decisions_total).",
		func() float64 { return float64(stats().Traces) })
	Registry.GaugeFunc("kubescrape_tail_sampling_buffered_spans",
		"Spans currently held in the tail-sampling buffer. These are acked to their senders but not yet decided and not durable anywhere (a DECIDED keep is spooled when -buffer-dir is set; an undecided one is not): this is what a hard kill of this pod would lose. tailSampling.maxSpans caps it, and is itself checked against the pod's memory limit at startup — the likeliest hard kill here is the OOM this buffer causes.",
		func() float64 { return float64(stats().Spans) })
}

// TailSamplingStat is the buffer's occupancy (tailbuffer.Stats' shape; see
// RegisterServiceGraphStats on why obs declares its own).
type TailSamplingStat struct {
	Traces int
	Spans  int
}

// Journald drops (agent).
var (
	JournalDropped = Registry.Counter("kubescrape_journal_dropped_batches_total",
		"Journal batches dropped after a permanent collector rejection (the cursor advances past them).")
	JournalDroppedRecords = Registry.Counter("kubescrape_journal_dropped_records_total",
		"Journal records lost with those batches. The magnitude of the loss: a batch is up to Config.BatchSize entries.")
)

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
		"Events about a Pod that were exported WITHOUT that pod's resolved identity (owner chain, labels, node, service.name), by reason: lookup = the metadata service could not answer (unreachable, or the pod is gone and past its tombstone TTL), uid_mismatch = a pod of that name exists but is a different incarnation, so adopting it would attribute the event to the wrong pod. Counted once per distinct involved object per batch. The event is still exported under the identity it carries, so this is lost CORRELATION, not lost data.", "reason")
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

// Debug surfaces (agent, -listen). The data-bearing three — /debug/otlp, its
// UI and /debug/tailer — stream or enumerate this node's whole telemetry feed
// on a port every pod in the cluster can reach, so they are gated (a local
// connection, or the -debug-token-file bearer token) and the gate is counted:
// a refusal an operator cannot see is a refusal that gets configured away, and
// an ACCEPTED read leaves no other trace than a throttled attach line.

// DebugRefused counts those refusals, by reason.
var DebugRefused = Registry.CounterVec("kubescrape_debug_refused_total",
	"Requests for the agent's data-bearing debug surfaces (/debug/otlp, /debug/otlp/ui, /debug/tailer) that "+
		"were refused, by reason. no_token = no -debug-token-file is configured, so these are served only to a "+
		"local connection (kubectl port-forward, or a container in this pod) and this one came from elsewhere — "+
		"set the flag and hand the token to whoever needs to read an agent remotely; unauthenticated = a token "+
		"file IS configured and the request carried no valid bearer token, i.e. a stale token after a rotation, "+
		"the wrong Secret mounted, or somebody probing the port; forwarded = the connection is local but the "+
		"request carries a forwarding header, so the address belongs to a relay and cannot stand in for the "+
		"caller; host = the connection is local but its Host header names something other than localhost, "+
		"which is what a DNS-rebound browser page reaching a kubectl port-forward looks like (a client that "+
		"dialled this port directly sends localhost or 127.0.0.1). A steady rate on no_token or "+
		"unauthenticated from pods that are not yours is somebody trying to read this node's log lines, and "+
		"any rate on host is a browser being pointed at an operator's port-forward.", "reason")

// HTTP server (metadata service).
var (
	HTTPRequests = Registry.CounterVec("kubescrape_http_requests_total",
		"Metadata API requests by pattern and status code.", "pattern", "code")
)

func init() {
	// The build version reaches internal/metrics' own exported scopes through
	// here: it owns BuildVersion and imports that package, so the value is
	// pushed down rather than imported back.
	metrics.SetScopeVersion(BuildVersion())

	// Registered here rather than through a Register* hook because it is a
	// property of THIS registry, which always exists — there is no wiring
	// decision to condition it on, so a published 0 always means "nothing was
	// skipped". The value is read at export time from the registry's own
	// atomic; while Dump is serving a scrape it is one dump behind (the func
	// metrics render after the stored series), which costs a scrape and never a
	// count.
	Registry.CounterFunc("kubescrape_self_metrics_points_skipped_total",
		"Data points of this process's OWN metrics that could not be rendered because their stored label set "+
			"failed to parse back, and were therefore left out. This should never move: the label sets of these "+
			"series come from code, not from data. It matters because of WHERE the loss lands — the Prometheus "+
			"/metrics exposition this process serves for itself, which is the delivery path when "+
			"-self-metrics-interval=0 and the signal an operator uses to diagnose everything else. A skipped "+
			"point is simply ABSENT from the response, so without this counter the operator's own telemetry "+
			"shrinks invisibly. It counts the SCRAPE path only: the OTLP push reads the same stored string but "+
			"degrades differently (it emits the point with whatever labels parsed, rather than dropping it), so "+
			"a nonzero value here means the pushed copy of that series is mislabelled rather than missing. Any "+
			"nonzero value means memory corruption or a bug in the label round-trip; the throttled WARN beside "+
			"it names the metric.",
		func() float64 { return float64(Registry.DumpLabelErrors()) })
}

// RegisterLogMetricsDrops exposes one log-metrics set's refused observations as
// export-time counters — cumulative since process start.
//
// The counts used to be PROCESS-GLOBAL atomics in internal/metrics, registered
// unconditionally here, purely because obs imports that package and the
// counters therefore could not be declared in obs. Registering a getter over
// the set is what dissolves that, and it is the pattern the store stats, the
// buffer stats and the self-metadata gauge already use. It also makes the
// family mean what it says: registered exactly when a log-metrics set EXISTS,
// so a published 0 is "configured and dropping nothing" rather than "the
// feature is off", and two sets in one process no longer merge their counts
// into one number.
func RegisterLogMetricsDrops(set *metrics.DynamicMetricSet) {
	if set == nil {
		return
	}
	// One family, labeled by metric name — sum() over the label is the
	// aggregate. There used to be two: an unlabeled
	// kubescrape_log_metrics_dropped_capped_total counter beside a
	// kubescrape_log_metrics_dropped_capped_by_metric GAUGE that carried a
	// monotonic since-start total and spelled its label name inside the metric
	// name. Both halves were wrong: a gauge does not mark the reset at a
	// restart, so rate()/increase() over it silently swallowed one, and the cap
	// frees slots only through idleness — a burst blinds ONE metric for up to
	// maxAge + grace (24h by default) and an alert has to be able to name it,
	// which the aggregate could not. A counter vec is both.
	//
	// The cost of the merge, stated plainly: the label set is data-driven (a
	// metric name appears only once it has dropped something), so with nothing
	// dropped the family is ABSENT rather than reading 0, where the old
	// unlabeled counter always published a zero.
	Registry.CounterFuncVec("kubescrape_log_metrics_dropped_capped_total",
		"Log-metric observations dropped because that metric's label-set cardinality cap was reached, by metric name. sum() over the label is the total. Absent until something is dropped: the label set is data-driven.",
		"metric", set.DroppedCappedByMetric)
	// kubescrape_log_metrics_dropped_collision_total was REMOVED here, and the
	// removal is the point rather than a tidy-up. It counted observations the
	// store refused because a second "check" hash disagreed on a primary-hash
	// hit — a guard that existed because the primary key was 64 bits. The key is
	// 128 bits now, which puts a collision at ~1.5e-31 per series map (10000
	// label sets, birthday bound), so the guard was removed with it and this
	// counter could never move again. A counter that is structurally incapable
	// of moving is worse than no counter: it reads as evidence of absence, and
	// an operator alerting on it would believe they were watching something.
	// See metrics/labels.go's strHash for the full argument.
	Registry.CounterFunc("kubescrape_log_metrics_dropped_nan_total",
		"Log-metric observations dropped since start because the extracted value was NaN or +/-Inf (neither is representable as a sample).",
		func() float64 { return float64(set.DroppedNaN()) })
	Registry.CounterFunc("kubescrape_log_metrics_dropped_undelivered_total",
		"Undelivered log-metric resources dropped because the re-offer buffer filled or the collector rejected them "+
			"PERMANENTLY. Taking a snapshot is DESTRUCTIVE (it seals aggregation windows, zeroes idled samples and "+
			"deletes expired ones), so a transiently failed export retains its samples for the next one; this counts "+
			"what a collector outage longer than that buffer could hold, plus definitively rejected chunks that "+
			"retrying could never deliver. These are genuinely lost observations — the ones the retention cannot save.",
		func() float64 { return float64(set.DroppedUndelivered()) })
}

// RegisterStoreStats exposes store sizes as gauges evaluated at export time.
func RegisterStoreStats(stats func() (pods, containers int)) {
	Registry.GaugeFunc("kubescrape_store_pods",
		"Pods currently in the store (including tombstones).",
		func() float64 { pods, _ := stats(); return float64(pods) })
	Registry.GaugeFunc("kubescrape_store_containers",
		"Container IDs currently indexed (including tombstones).",
		func() float64 { _, containers := stats(); return float64(containers) })
}

// RegisterStoreAnomalies publishes the two index anomalies that are silent by
// construction: they are decided under the store's write lock, on the informer
// goroutine, where nothing may log and no context is available.
//
// Both are guards that keep the served data CORRECT, so neither is a bug an
// operator has to chase — they are worth publishing because they are the only
// evidence that the condition they guard against is happening at all, and each
// has a second, un-guarded consequence somewhere else.
//
// A hook rather than counters the store bumps directly: internal/store and
// internal/services publish their STATE through hooks, the same way the store
// sizes, the waiter state and the buffer stats are wired — a gauge over a
// value obs would otherwise have to poll. (Both packages do import obs for one
// thing: MetadataAnnotationsOmitted, a plain event counter bumped once per
// informer event. A hook cannot serve that one, because its label set spans
// three packages and a CounterVec and a CounterFuncVec cannot share a name.)
func RegisterStoreAnomalies(podNameReuse, serviceNameReuse, podIPContested func() int64) {
	Registry.CounterFuncVec("kubescrape_index_name_reuse_total",
		"Objects that arrived under a namespace/name a DIFFERENT, still-live UID held, by kind (pod, service). "+
			"The old record is tombstoned or dropped on the spot, so nothing stale is served — but reaching this "+
			"at all means a Delete was never delivered (a relist gap: an API-server restart, an etcd compaction, "+
			"an expired resourceVersion), because the ordinary recreate order tombstones the predecessor first. "+
			"A low rate around API-server disruption is expected and harmless; a sustained one says this "+
			"process's watches keep breaking, which is also the condition under which OTHER deletes — the ones "+
			"no name index can catch — leave a deleted pod being served as a live scrape target until its "+
			"tombstone expires. Read it beside kubescrape_informer_watch_errors_total.",
		"kind",
		func() map[string]float64 {
			return map[string]float64{
				"pod":     float64(podNameReuse()),
				"service": float64(serviceNameReuse()),
			}
		})
	Registry.CounterFunc("kubescrape_pod_ip_contested_total",
		"Pod-IP claims decided between two pods that were BOTH live (neither terminating) — i.e. the CNI handed "+
			"an address to a new pod while the store still had a running pod reporting it. The index keeps the "+
			"later acquisition, which is the right answer, so /v1/pod-ips and /v1/self stay correct; what this "+
			"counts is the window in which they could have been wrong. Expect a trickle on a churning cluster. A "+
			"sustained rate means addresses are recycling faster than the pod watch reports the release, and the "+
			"cost lands on the agent's opt-in peer-IP attribution (-ingest-peer-ip-fallback), where a resource "+
			"stamped with the previous holder's identity is never revisited.",
		func() float64 { return float64(podIPContested()) })
}

// RegisterAPIServerProbe publishes the API-server reachability watchdog's two
// series: a 1/0 gauge for the last probe's verdict and a counter of failed
// probes.
//
// Registered exactly when the probe RUNS (-apiserver-probe-interval > 0), the
// way RegisterSelfMetadata is: a published 0 then always means UNREACHABLE and
// never "the probe is off", and an ABSENT family means nobody is looking —
// which is a distinct thing for an alert to select on.
//
// It exists because no passive signal reliably sees the commonest outage.
// kubescrape_informer_watch_errors_total stays flat while the API server is
// merely unreachable (client-go retries a refused watch internally and never
// relists — see that metric); /readyz latches at the initial sync by design;
// and kubescrape_store_pods DECAYS rather than freezing, because the tombstone
// sweeper keeps running over a store nothing refills.
//
// What the gauge measures is EXACTLY one thing: whether a NEW connection from
// this pod, with this ServiceAccount, reached the API server on the last probe.
// That is a proxy for "the cache can refill", not a reading of the cache
// itself, and it is wrong in both directions at the margins. False negative:
// an ESTABLISHED watch blackholed by a NetworkPolicy or a dead conntrack entry
// leaves the cache frozen while a fresh connection still succeeds and this
// reads 1 — the classic silent stall. False positive: one failed probe (a
// dropped packet, an API server mid-restart, a 10s timeout under load) reads 0
// without proving the informers missed anything. Alert on it SUSTAINED, and
// read the failure counter for the history of an outage the process survived.
func RegisterAPIServerProbe(reachable func() bool, failures func() int64) {
	Registry.GaugeFunc("kubescrape_apiserver_reachable",
		"1 when the last API-server probe opened a NEW connection and got an answer, 0 when it did not. A proxy for 'the informer caches can still refill', not a reading of the caches: a blackholed established watch can leave them frozen while this reads 1, and a single failed probe does not prove they stopped. Alert on it sustained. The service keeps serving its in-memory cache either way — readiness latches at the initial sync on purpose. Absent when -apiserver-probe-interval=0 (no probe runs); never 0 for that reason.",
		func() float64 {
			if reachable() {
				return 1
			}
			return 0
		})
	Registry.CounterFunc("kubescrape_apiserver_probe_failures_total",
		"API-server reachability probes that failed (a metadata-only LIST of one namespace, the same TCP/TLS/authn/authz path the informers use). Nonzero with the gauge back at 1 means the outage healed; a rising rate with the gauge at 0 is an outage in progress.",
		func() float64 { return float64(failures()) })
}

// RegisterWaiterStats exposes the container-lookup waiter state: how many
// lookups are blocked right now, and how many have been SHED by the cap.
//
// The shed is a load-shedding 503, and without its own series it is
// indistinguishable from the not-ready 503 in
// kubescrape_http_requests_total{pattern="/v1/containers",code="503"}. The cap
// binding means abuse or an extreme anomaly rather than ordinary fleet load —
// exactly the moment the operator needs the two told apart — and the gauge is
// what shows the pressure building before the cap is reached.
//
// The shutdown drain gets its OWN counter rather than a second reason on the
// shed one. Both answer the same retryable 503, but they mean opposite things
// to an operator: the cap binding is abuse or an anomaly worth paging on, while
// the drain is one line per rolling update — and blurring them would make every
// deploy fire the abuse alert.
//
// A hook rather than a counter the store bumps directly: the store publishes
// STATE through hooks, the same way the buffer stats and the self-metadata
// gauge are wired (see RegisterStoreAnomalies for the one direct counter it
// does bump, and why).
func RegisterWaiterStats(blocked func() int, shed, drained func() int64) {
	Registry.GaugeFunc("kubescrape_container_lookups_blocked",
		"Container lookups currently parked, in EITHER of the two places one can park: waiting for a container "+
			"ID to appear in the store, or — only until the informer caches finish their initial sync — waiting "+
			"for the store to become readable at all. Both draw on the one -max-blocked-lookups budget, because "+
			"both hold a request on a route nothing authenticates; the second is transient by nature, but it is "+
			"exactly when an agent fleet arrives at once.",
		func() float64 { return float64(blocked()) })
	Registry.CounterFunc("kubescrape_container_lookups_shed_total",
		"Blocking container lookups refused because the store's concurrent-waiter cap was reached.",
		func() float64 { return float64(shed()) })
	Registry.CounterFunc("kubescrape_container_lookups_drained_total",
		"Blocking container lookups answered with a retryable 503 because the process began shutting down. Without the drain they stayed parked until the process exit killed the connection mid-wait (curl reports an empty reply); this counts the answers, and it is expected to be nonzero on a rolling update, unlike kubescrape_container_lookups_shed_total.",
		func() float64 { return float64(drained()) })
}

// RegisterBufferStats exposes the disk buffer's per-signal backlog as gauges
// evaluated at export time.
//
// Every other buffer metric is a counter that only moves once data is ALREADY
// being dropped or refused (dropped/full/read_errors): by then the collector
// has been degrading for a while and the tailer is back-pressured. The backlog
// against its cap is the leading indicator — "the spool is at 70% and
// climbing" — and it was previously unobservable even though the spool had
// tracked the number all along.
func RegisterBufferStats(stats func() map[string]BufferStat) {
	Registry.GaugeFuncVec("kubescrape_buffer_backlog_bytes",
		"Undelivered bytes currently queued in the disk buffer, per signal (what -buffer-max-bytes caps). signal=\"traces\" exists only on the trace tier with tail sampling on — the one trace payload this agent owns rather than forwards.",
		"signal", func() map[string]float64 {
			out := map[string]float64{}
			for sig, st := range stats() {
				out[sig] = float64(st.Backlog)
			}
			return out
		})
	Registry.GaugeFuncVec("kubescrape_buffer_max_bytes",
		"Configured disk-buffer cap per signal (0 = uncapped); backlog/max is the utilisation to alert on.",
		"signal", func() map[string]float64 {
			out := map[string]float64{}
			for sig, st := range stats() {
				out[sig] = float64(st.Cap)
			}
			return out
		})
	Registry.GaugeFuncVec("kubescrape_buffer_segments",
		"Disk-buffer segment files on disk per signal. Physical footprint can exceed the backlog by up to one segment (a delivered but unreclaimed prefix).",
		"signal", func() map[string]float64 {
			out := map[string]float64{}
			for sig, st := range stats() {
				out[sig] = float64(st.Segments)
			}
			return out
		})
}

// BufferStat is one signal's disk-buffer occupancy.
type BufferStat struct {
	Backlog  int64
	Cap      int64
	Segments int
}
