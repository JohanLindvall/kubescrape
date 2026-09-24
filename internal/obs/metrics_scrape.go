package obs

// Scrape pipeline (agent).
var (
	Scrapes = Registry.CounterVec("kubescrape_scrapes_total",
		"Scrapes by pipeline and outcome.", "pipeline", "outcome")
	ScrapeDuration = Registry.HistogramVec("kubescrape_scrape_duration_seconds",
		"Scrape duration by pipeline.", durationBuckets, "pipeline")
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
			"reset), tls (the certificate did not verify, the name did not match, the port speaks plaintext, "+
			"or the target refused this agent's client certificate - tlsConfig.cert/keySecret), "+
			"timeout (the scrape budget expired - raise -scrape-timeout, or the endpoint is slow) and canceled "+
			"(the agent is shutting down; not a fault). The target ANSWERED and refused: unauthorized (401 or "+
			"403 - a missing credential, or for the kubelet pipelines a missing RBAC rule; the accompanying warn "+
			"names which) and status (any other non-200). The scrape never left this agent: auth (a bearer, "+
			"basicAuth or TLS secret ref could not be resolved - the metadata service needs -scrape-auth-secrets "+
			"and this agent a matching token file; on the kubelet pipelines, the agent's own -kubelet-token-file "+
			"has never been readable) and relabel (a monitor's metricRelabelings regex would not "+
			"compile; the scrape fails deliberately rather than export what the rule asked to drop). The target "+
			"answered WRONG: proto_refused (it served the protobuf exposition without -scrape-native-histograms "+
			"having asked for it - refused unparsed, since decoding materialises the whole gzip-amplified "+
			"message), sample_limit (-scrape-max-samples was exceeded; what was converted before the abort is "+
			"still exported) and body (a response body over one of this pipeline's size caps - a protobuf "+
			"message over 4 MiB, or one that would decode past its heap budget, included - or a body that ended "+
			"mid-stream: the target or a proxy cut the response). Finally export (the payload "+
			"was scraped and converted and the COLLECTOR - or, with -buffer-dir, the spool - refused it: the one "+
			"reason where nothing is wrong with the target, and kubescrape_export_requests_total is the family to "+
			"read) and other (anything unclassified; a rate on it means this list needs a new value).",
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
			"and the container was not — a name the cached pod document does not list, one it lists from the "+
			"pod SPEC whose status has not reached the API server yet, or, just after a RESTART, one whose "+
			"document still names the previous incarnation: withheld rather than stamped with the dead "+
			"container's id). "+
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
			"once per resource per exported chunk, never per sample, and only for an object the metadata service "+
			"was asked about: a row cadvisor itself could not attribute (a CRI-O conmon scope, a kata helper - no "+
			"namespace, pod or container label) is never looked up and is not counted. The rows ARE still "+
			"exported, carrying the identity their own labels gave them, so this costs attribution rather than "+
			"data: no owner chain, no "+
			"pod labels, and a service.name that falls back to the pod name. A steady low rate is ordinary - a "+
			"just-started container whose id the kubelet has not posted to the API server yet resolves on a "+
			"later cycle - while a sustained or fleet-wide rate means the metadata service is not answering, and "+
			"is usually accompanied by kubescrape_scrape_metadata_budget_exhausted_total.", "level")
	ScrapeMetaBudgetExhausted = Registry.CounterVec("kubescrape_scrape_metadata_budget_exhausted_total",
		"Scrapes that spent their whole per-scrape metadata allowance, by pipeline. The allowance is half the "+
			"scrape's budget - the smaller of -scrape-timeout and -scrape-interval (and, for a monitor target, "+
			"its own scrapeTimeout and interval); past it the remaining objects are NOT looked up at all, so they export under the "+
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
		"Unparseable or refused exemplars by pipeline - refused meaning a label set over OpenMetrics' 128 code points of names plus values, which no Prometheus-compatible backend keeps, or one with an empty or repeated label name. The protobuf exposition's exemplars count here too, and a protobuf-only target moves this only through those refusals: it serves no text exemplar suffix to be unparseable. NO data was lost: the samples carrying them were exported without the exemplar, which is why this is separate from kubescrape_scrape_malformed_total. Only ever nonzero where exemplar scraping is enabled.", "pipeline")
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
