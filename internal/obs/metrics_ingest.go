package obs

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
			"so the remainder's points share ONE overflow resource per input resource, stripped of the "+
			"sender's identity and unenriched (each point keeps its own id attributes), rather than costing "+
			"one full resource copy each.", "outcome")
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
			"bound — the data itself is always still forwarded. WHICH part is what the reason says, and the "+
			"parts are not interchangeable: debugging the wrong one is the cost of reading this as a single "+
			"thing. body_too_large is per RECORD (a body whose text view exceeds 1 MiB; the tailer truncates a "+
			"joined entry at -logs-max-entry-bytes the same way): a STRING body is read on its first 1 MiB — the "+
			"logAttributes lift, body enrichment, the log-metric labels and the keep/drop rules all see that "+
			"prefix — while a STRUCTURED body, whose rendering is the cost being bounded, is read as an EMPTY "+
			"line, so the lift and enrichment skip it and the labels and rules run on the record's attributes "+
			"and severity alone. Either way the rules still run and a line-keyed rule can drop the record. "+
			"resource_value_too_large is per RESOURCE (a map, array or bytes resource attribute whose text "+
			"exceeds 64 KiB; the rules and labels read resource keys per record, so such a value is rendered once "+
			"per resource, and one this large not at all): that attribute resolves EMPTY for the keep/drop rules "+
			"and the log-metric labels and is left off the log-derived series' resource, while the forwarded "+
			"payload keeps it — it moves only with a logs.rules or logMetrics section configured. "+
			"resource_too_wide (a resource carrying more than 64 attributes "+
			"— the metric store retains a serialization of the whole resource per series, so sender-chosen "+
			"width is sender-chosen retained heap) and resources_capped (a resource past the first 256 of one "+
			"push) are per RESOURCE and skip the log-metrics OBSERVATION only: enrichment, the lift and the "+
			"rules run in full on every record of such a resource, which is also why neither can move at all "+
			"unless a logMetrics section is configured. The listeners are unauthenticated; a nonzero rate is a "+
			"sender worth finding.", "reason")
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
	IngestRejected = Registry.CounterVec("kubescrape_ingest_rejected_total",
		"Pushed OTLP requests refused because a receiver admission bound was reached (retryable: 429 / "+
			"ResourceExhausted — the payload is intact and the sender owns the retry), by the bound that "+
			"refused, which is also the remedy: in_flight is the CONCURRENT PUSH COUNT (-ingest-max-in-flight), "+
			"which bounds how many pushes are processed at once and bounds no memory at all; buffer_bytes is the "+
			"RAW BYTE BUDGET, the payload bytes both transports hold while reading and decoding (four full-size "+
			"bodies, scaled up in step with -ingest-grpc-max-recv-bytes); decoded_bytes is the DECODED BUDGET "+
			"(an estimate of the decoded structure, read off the wire bytes before the decode, against twice "+
			"the raw budget), what those bytes inflate "+
			"INTO, which the other two cannot bound — pdata's decoded structure runs several times the wire "+
			"size, so a body inside every size cap can still be the thing that fills the node, and raising "+
			"-ingest-max-in-flight or the body cap does nothing for a decoded_bytes refusal. One decoded_bytes "+
			"case is retryable in FORM but permanent in PRACTICE and is the reason this series can sit at a "+
			"steady rate from one sender: a single push whose decoded structure alone estimates past the whole "+
			"decoded budget is refused on every retry — it carries a throttled Warn naming the estimate and the "+
			"budget, and the fix is for that sender to batch smaller, never a larger bound here.", "reason")
	IngestReserveExpired = Registry.Counter("kubescrape_ingest_reserve_expired_total",
		"gRPC pre-decode buffer reservations reclaimed because the peer did not complete a message inside "+
			"the decode window — either a probe that sent nothing after its headers, or a sender slower than "+
			"the reservation divided by the window (the window scales with -ingest-grpc-max-recv-bytes, so "+
			"this is a floor on the sender's upload rate); the reclaim also cancels that stream (the sender sees Canceled, which OTLP lists as "+
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
