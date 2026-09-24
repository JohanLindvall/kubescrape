package obs

import (
	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// Metadata service: the monitor index and the scrape targets derived from it
// (with their ceilings), /v1/scrape-auth, the container and /v1/self lookups,
// the informers, owner resolution and the served document's ceilings.
var (
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
	// typo, a missing CRD, or a monitor that matches nothing. Counted once per
	// CHANGE of the refused monitor, like its monitor_* siblings (cmd/kubescrape
	// monitorRefusals): per delivery, its rate was the informer's resync period.
	MonitorNamespaceRefused = Registry.CounterVec("kubescrape_monitor_namespace_refused_total",
		"Monitors ignored because their namespace is not permitted by -monitor-namespaces, counted once per created or edited monitor (an informer resync or relist re-delivering an unchanged monitor does not re-count it, exactly like the sibling monitor_* counters). Refused monitors never reach the index, so kubescrape_monitors_rejected does not include them.", "kind")
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
	// here. A nonzero increase means a scrape is running with a credential or
	// TLS config one of its CRs did not choose, and wants the two monitors
	// reconciled.
	//
	// READ THE VALUE, NOT A SHORT RATE (like TargetIdentityCollisions): it
	// increments once per conflicting endpoint per target DERIVATION, and the
	// change-token memo answers an agent's revalidation without deriving while
	// nothing the node's list reads has changed. The rate therefore tracks how
	// often lists are rebuilt (source churn anywhere in the cluster, a cold
	// agent), not how long the conflict has lasted — a persisting conflict in a
	// quiet cluster shows a near-zero rate. Alert on increase over a long
	// window; /v1/explain shows the current state.
	MonitorTargetShadowed = Registry.CounterVec("kubescrape_monitor_target_shadowed_total",
		"Monitor endpoints whose auth/TLS conflicts with the monitor already holding the same URL on that pod (the holder's is served; the rest of the endpoint's configuration still merges). Increments once per target derivation, which the change-token memo skips while nothing changes: alert on increase over a long window, not on a short rate.", "kind")
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
	// A nonzero increase means either a genuinely enormous chain (fix the CR)
	// or several monitors piling onto one URL (reconcile them). READ THE VALUE,
	// NOT A SHORT RATE, for MonitorTargetShadowed's reason: it increments once
	// per capped endpoint per target DERIVATION, which the change-token memo
	// skips while nothing changes, so the rate tracks rebuilds rather than how
	// long the cap has bound. Alert on increase over a long window; /v1/explain
	// shows the current state.
	MonitorRelabelChainCapped = Registry.CounterVec("kubescrape_monitor_relabel_chain_capped_total",
		"Monitor endpoints whose metricRelabelings were partly refused because the merged chain for that scrape URL is at the per-target ceiling (the rules that fit are applied; the rest filter nothing). Increments once per target derivation, which the change-token memo skips while nothing changes: alert on increase over a long window, not on a short rate.", "kind")
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
	// A nonzero increase means many monitors resolve to one URL on one pod —
	// reconcile them; /v1/explain names the pod's monitors and says which
	// stopped being listed. READ THE VALUE, NOT A SHORT RATE, for
	// MonitorTargetShadowed's reason: it increments once per refused name per
	// target DERIVATION, which the change-token memo skips while nothing
	// changes, so the rate tracks rebuilds rather than how long the list has
	// been full. Alert on increase over a long window.
	MonitorContributorsCapped = Registry.CounterVec("kubescrape_monitor_contributors_capped_total",
		"Monitor endpoints whose configuration merged into the target holding their URL but whose monitor name was refused from that target's contributor list at the per-target ceiling (attribution only; the scrape is unaffected). Increments once per target derivation, which the change-token memo skips while nothing changes: alert on increase over a long window, not on a short rate.", "kind")
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
	// ContainerLookupWait is the timeouts counter's leading edge: how long the
	// lookups that DID park waited. The informer lagging the kubelet shows here
	// as a rising p90 well before waits start expiring, and resolved against
	// timeout says whether the wait budget fits this cluster.
	ContainerLookupWait = Registry.HistogramVec("kubescrape_container_lookup_wait_seconds",
		"How long a blocking container lookup was parked before it was answered, by outcome: resolved (the "+
			"container ID appeared and the wait paid off) or timeout (the budget expired first). Only lookups "+
			"that actually parked are observed — an ID already in the store costs no wait and is not here — so "+
			"the count is the parked-lookup rate and the distribution is the pod informer's lag behind the "+
			"kubelet: a p90 creeping up under a timeout rate of zero is the informer falling behind before "+
			"kubescrape_container_lookup_timeouts_total starts to move.", nil, "outcome")
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
			"request carried Forwarded/Via/X-Forwarded-For/X-Real-Ip (presence alone), so kubescrape refuses to "+
			"attribute a connection "+
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

	// NodeTargetsBuilds makes the node-targets ETag memo observable in
	// production: the benchmark measured the win, and nothing else could say
	// whether a running fleet gets it.
	NodeTargetsBuilds = Registry.CounterVec("kubescrape_node_targets_builds_total",
		"GET /v1/nodes/{node}/targets answers by how they were produced: built (the whole derivation, sort and "+
			"marshal for that node ran) or memo_hit (a conditional GET whose ETag was current by the change "+
			"token or the clock, answered 304 without building). Agents poll this route every scrape interval "+
			"per node, so with the change tokens wired the steady state is almost all memo_hit and a built rate "+
			"tracking real pod, Service and monitor churn; a built rate near the poll rate means the memo is "+
			"not reaching the caller it exists for (a source publishing no change token, agents that send no "+
			"If-None-Match, or a memo evicted at its cap).", "result")

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
			"(ReplicaSet, Deployment, StatefulSet, DaemonSet, Job, CronJob, Namespace, Node, or unknown — a "+
			"reference to a kind this service does not watch, on the owners_capped and bad_api_version reasons "+
			"only, which take the kind from the reference rather than from a cache) and reason. The "+
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

	// MetadataLabelsOmitted is MetadataAnnotationsOmitted's sibling for an
	// OWNER's labels (kubemeta.CopyOwnerMeta, kubemeta.MaxOwnerLabelBytes).
	// Only owners are bounded: a pod's and a Service's labels are selection
	// input and stay verbatim, while nothing selects on an owner's — and they
	// ride every pod document once per resolved reference. Counted once per
	// resolution, like the annotation counter's owner half; the kind label is
	// the owner's GVR kind.
	MetadataLabelsOmitted = Registry.CounterVec("kubescrape_metadata_labels_omitted_total",
		"Owner objects whose labels were served SHORT, by kind: a label set over kubemeta.MaxOwnerLabelBytes. An owner's labels ride every pod document that names it, once per resolved ownerReference, and nothing selects on them, so they are bounded where a pod's and a Service's are not. The served owner names the omitted keys in its own kubescrape.io/labels-omitted annotation; no label value is ever shortened.", "kind")
)

// HTTP server (metadata service).
var (
	HTTPRequests = Registry.CounterVec("kubescrape_http_requests_total",
		"Metadata API requests by pattern and status code.", "pattern", "code")
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

// RegisterInformerFreshness publishes the last time each informer delivered an
// event, as a unix timestamp per resource. It is the FRESHNESS half of the
// reachability probe: kubescrape_apiserver_reachable says a NEW connection
// reaches the API server, which an established watch that has silently stopped
// delivering (a blackholed connection, a proxy dropping the stream) does not
// contradict — and this is what does. On a cluster whose pods change at all,
// time() minus this gauge staying above a few minutes for pods is a watch that
// is no longer advancing; on a quiet cluster the age is genuinely ambiguous,
// which is why it is a gauge to read beside the probe rather than an alert on
// its own.
func RegisterInformerFreshness(last func() map[string]float64) {
	Registry.GaugeFuncVec("kubescrape_informer_last_event_timestamp_seconds",
		"Unix time of the last add, update or delete each informer delivered, by resource; a periodic -resync "+
			"replay of the informer's own cache is not a delivery and does not advance it. The freshness half "+
			"of kubescrape_apiserver_reachable: the probe says a NEW connection reaches the API server, which an "+
			"established watch that has silently stopped delivering does not contradict, while this gauge stops "+
			"advancing exactly then. time() minus this for pods staying above a few minutes on a cluster whose "+
			"pods change at all is a watch that is no longer advancing; on a quiet cluster the age is ambiguous, "+
			"so read it beside the probe and kubescrape_informer_watch_errors_total rather than alerting on it "+
			"alone. 0 until the first event.", "resource", last)
}

// RegisterStoreStats exposes store sizes as gauges evaluated at export time,
// from ONE stats call per export or scrape (metrics.PerPass): it takes the
// store's read lock, and the two sizes must describe one instant.
func RegisterStoreStats(stats func() (pods, containers int)) {
	type sizes struct{ pods, containers int }
	snap := metrics.PerPass(Registry, func() sizes {
		pods, containers := stats()
		return sizes{pods, containers}
	})
	Registry.GaugeFunc("kubescrape_store_pods",
		"Pods currently in the store (including tombstones).",
		func() float64 { return float64(snap().pods) })
	Registry.GaugeFunc("kubescrape_store_containers",
		"Container IDs currently indexed (including tombstones).",
		func() float64 { return float64(snap().containers) })
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
