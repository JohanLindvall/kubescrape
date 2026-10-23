# Optional pipelines

[Repository guidance](../AGENTS.md) · [Build boundaries](AGENTS-DEVELOPMENT.md#toolchain-and-optional-builds)

These packages are imported by the command only through their positive build
tags. Their flags always exist; enabling an excluded pipeline is a dry-run and
startup error. Reuse the [shared log chain](AGENTS-LOGS.md#shared-record-processing).

## Journald

`internal/agent/journald` reads natively through
`go-systemd/v22/sdjournal`; it is the agent's sole cgo dependency. Keep cgo in
`source_sdjournal.go` behind the source interface; conversion and tests use
that interface without native journal reads.

Mount `/run/log/journal` and the node's `/etc/machine-id` when enabled. The
persistent journal under `/var/log/journal/<id>` requires the machine ID under
`SD_JOURNAL_LOCAL_ONLY`. The machine-id hostPath must be `File`, never
`FileOrCreate`. Preserve `journalfiles.go`'s startup gate and the mount tests;
kind's volatile journal does not exercise persistent-journal access.

Resources group by systemd unit plus lifted resource/scope values. Use the
named journald scope. Truncate oversized bodies on a rune boundary, mark their
original length, and count truncation. Report invalid UTF-8 and missing
timestamps as entry defects.

Resume with SeekCursor and skip its entry once, or start with SeekTail plus
Previous when no cursor exists. Commit the cursor after successful export and
persist through the unified positions store. A hard kill may replay the
unpersisted interval. Reader errors reopen with backoff; export failures retry
the **same payload** so per-record observations are not multiplied. Settle
permanent rejection explicitly. Measure flush intervals from the last flush
and bound the detached shutdown flush.

Rules and log metrics apply even with container logs disabled, so compile
them in `compileConfig`, not `startLogs`. Metrics observe before rules; a
dropped journal entry still advances its cursor.

## Kubernetes events

`internal/agent/events` and `internal/leader` run in the singleton Deployment,
with per-node pipelines disabled. The namespaced Role restricts get/update of
the position ConfigMap and Lease through resourceNames; create cannot use that
restriction. Keep RBAC names aligned with `-events-lease` and
`-events-position-configmap` and their chart tests.

`leader.Run` re-enters election after leadership loss. Latch acquisition
synchronously on the resource lock, join the work goroutine before reporting
leadership down, and do not overlap workers across elections.

Position is resourceVersion plus timestamp watermark in a ConfigMap. Advance
and persist it only after export ACK. It is a lower bound on delivered data,
so stale writers can cause replay but cannot skip unseen data.

When the watch expires, replay a paginated LIST filtered by the watermark.
Secure replay only after this process read every page and exported every
entry. Hold committed position while list order remains unsecured; flush the
tail before opening the watch, clamp count triggers to retained capacity, and
retry failed exports in place. Bookmarks do not prove a backlog was delivered
and apply immediately only with an empty batch. A redelivering stream drops
its retained batch before restarting; preserve cold-start skip-backlog rules.

Modified events are occurrences; Deleted events are TTL expiry. A Pod event
uses the involved pod's resource after a name lookup and UID check. On failure
export the event's own identity, count and throttle the complaint, bound lookup
time, and memoize failures briefly. Group unresolved and resolved records
separately; do not attach the reader's node. Graceful shutdown persists a final
position with a detached context.

## Azure diagnostics

`internal/agent/azurediag` consumes Event Hubs over Kafka/TLS with franz-go in
the same singleton workload. Consumer groups provide coordination; do not
add leader election. Commit an input only when both its log and metric outputs
ACK. Retry transient failure in place; count and commit past malformed inputs
or permanent rejection. Keep the source interface and real-group `kfake` tests.

Connection-string parsing is case-insensitive and splits each component at
its first `=`. Keep the password string verbatim, including EntityPath.
EntityPath selects the topic when no explicit topic is set; an explicit list
omitting it warns. Multiple credentials need separate readers/clients and
readiness gates. Reject duplicate sources and ambiguous namespace lists.
Disambiguate groups for several clients in the same namespace; adding a client
can change group identity and therefore the `-azure-start` position.

Connection strings use SASL PLAIN and reread the file per session. Managed
identity uses OAUTHBEARER, selecting workload identity when its token file,
client and tenant are available, otherwise IMDS. Cache tokens to issuer expiry,
refresh early, back off failures and never serve an expired token. Invalidate
the token when rebuilding after fatal group/cluster authorization errors.
Do not introduce Azure SDK dependencies for these flows.

Decode records strictly and classify each record independently. Logs retain
their record JSON and run the full log chain; scrub resultDescription too.
The ARM envelope is authoritative for resource identity, so remove redundant
body-derived resource IDs only when that identity exists. Keep azure.category
and azure.event_category distinct.

Preaggregated metric windows export gauges for count/total/minimum/maximum/
average, with the timegrain on data points. Preserve cloud.resource_id verbatim;
normalize subscription/group/type/name identity as defined in `resourceid.go`.
Resource name/group determine service identity. Readiness clears on the first
poll. Untagged calls into this package must not relink it into every variant.
