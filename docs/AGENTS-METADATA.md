# Metadata service

[Repository guidance](../AGENTS.md) · [HTTP reference](REFERENCE.md#api)

## Informers, store and owners

`cmd/kubescrape` serves requests from client-go informer caches. Use one initial
LIST plus WATCH per resource per replica; no request should query the API server.

- Full pod objects feed `internal/store.Store`. Index by UID, normalized
  container ID, node, namespace/name and canonical pod IP. Preserve the
  same-name replacement guard.
- The pod-IP index uses `ipClaimants` for every record reporting an address;
  release promotes a survivor without scanning all pods under the write lock.
  Preserve `ipSeq` ordering and give no sequence to a first-seen draining pod.
  `podip_delete_test.go` and `ipclaimants_test.go` define the lifecycle.
- `trimPod` removes managed fields and unused pod spec bulk, while preserving
  everything consumed by `kubemeta.Pod`. Keep Service specs intact for
  `internal/services.Index`; `TestTrimPodPreservesEverythingFromPodReads`
  checks the pod projection.
- Full Service objects feed selector-based Service-to-pod discovery.
- ReplicaSets, Deployments, StatefulSets, DaemonSets, Jobs, CronJobs, Namespaces
  and Nodes use `PartialObjectMetadata`. Resolve owner chains and namespace
  metadata lazily at request time, never store their resolved values on pods.
  A new owner kind needs an `owners.ownerKinds` row, `attrs.kindTable`, and
  both deployed and chart ClusterRoles.

All store mutations hold its write lock; upserts replace records and only
deletion timestamps mutate in place. `FromPod` deep-copies Kubernetes-owned
data. Shallow returned values remain immutable. Store and Service index locks
must cover handlers, informer callbacks, sweepers, readiness statistics,
self-metadata lookup and gauge reads. Use the injectable store clock in tests.

## Readiness and failure signals

Core readiness gates use the **handler registrations'** `HasSynced`: pod and
Service handlers must have populated their derived stores before serving 200.
Owner informers use their indexers directly and may gate on informer sync.
ServiceMonitor and PodMonitor each need a gate. Discovery errors are fatal;
only NotFound/NoMatch means an unavailable CRD. Inspect served resource names,
not merely whether the API group exists.

Readiness latches after initial sync so API-server outages do not disconnect
agents from useful cached data. `apiserver.go` probes a `limit=1` Namespace
metadata list without resourceVersion. `-apiserver-probe-interval` controls it;
zero disables it. Reachability and probe-failure metrics plus transition,
throttled-repeat and recovery logs report outages. Watch errors and store
gauges are not reliable cache-freshness indicators; tombstones keep expiring.

Owner failures keep the bare owner reference and count a bounded kind/reason.
`not_found` can be normal for retained pods or initial cache fill;
`lister_error`, `no_informer` and `wrong_type` require throttled warnings.
`bad_api_version` and `uid_mismatch` describe per-object failures. Never borrow
metadata from an owner recreated under the same name. Reason constants live
in `owners.go`; keep metric help text aligned with them.

## Lookup and discovery invariants

- Unknown container IDs wait per exact ID, bounded by `-wait-timeout`; a
  client's wait can shorten that budget, never extend it. A pod upsert wakes
  all waiters for its IDs, not unrelated waiters. Bound parked lookups.
- `Server.Drain` runs before HTTP shutdown and releases container waiters and
  pre-sync requests with 503 and `Retry-After: 1`. A spent shutdown budget must
  report outstanding work rather than imply it completed.
- Deleted pods and replaced container IDs stay resolvable for `-cache-ttl`.
  Include `lastState.terminated` while the pod lives. A previous-incarnation
  view clears image fields and uses restart count N-1.
- Scrape targets exclude deleted, completed and terminating pods. They remain
  available to metadata lookups. A not-ready but otherwise live pod may be
  scraped; `Ready` is metadata, not this eligibility gate.
- `prometheus.io/port` accepts comma-separated names or numbers on pods and
  Services. Translate Service ports through targetPort. A regular container's
  named port wins over init/sidecar/ephemeral containers; otherwise first match
  wins. All discovery paths use the same resolver.
- Deduplicate targets by URL. Pod annotations win over Service annotations;
  a monitor upgrades an annotation target wholesale.
- `/v1/explain` uses the same eligibility, merge and bound decisions as target
  discovery, returns 503 before readiness, and does not increment derivation
  counters. Its shared owner/namespace resolution may still count.

## Monitors and credentials

`internal/servicemonitors.Index` parses both monitor kinds. Apply
`-monitor-namespaces` at indexing time so refused monitors cannot expand
credential access. ServiceMonitors resolve monitor → Services → pods;
PodMonitors select pods directly. Endpoints naming neither port nor targetPort
resolve to nothing, including when a pod has an unnamed port.

Implement supported auth/TLS fields and keep/drop metric relabeling exactly
as documented. Source labels join with `;`; regexes are anchored; `__name__`
is the metric name. A custom separator matters only with multiple source
labels. Scheme comparison is case-insensitive. Unsupported fields must be
reported, not silently interpreted as a different rule. A malformed monitor
is removed from the index; update the ignored-fields inventories in code.

An admitted bad regex or an unapplied mutating rule on which a keep/drop rule
depends refuses the endpoint. Agent relabel compilation failures fail the
scrape. Cache compiled chains by fingerprint and bound regex cost before
compilation.

Two monitors on the same pod/URL merge deterministically. Concatenate relabel
chains, choose the finer explicit interval with its own timeout, and treat
auth/TLS as one indivisible group. Adopt credentials when only one contributes
them; conflicting groups retain the first and report shadowing. Preserve the
winning `Monitor` and the bounded contributor list. Invalid intervals trigger
the caller's warning/fallback; clamp scrape timeout to its interval.

Credential references are namespaced to the monitor. Use
`Endpoint.secretRefs` for both parsing and `AuthSecretRefs`. Serve only
referenced keys through `/v1/scrape-auth/...`, only with
`-scrape-auth-secrets` and its required bearer token. Authenticate before
allowlist checks; never expose credentials in logs or cacheable responses.
Service and agent caches refresh at one minute; an agent may keep last-good
material for at most ten minutes during service failure, never past a
401/403/404. TLS clients are cached by material and bounded by `maxTLSClients`.

The receiver refreshes its token more frequently than clients and accepts a
predecessor for a grace period through `internal/bearer`. Other metadata routes
remain unauthenticated; their security implications belong in the accepted
residuals roster.

## Bounds and response identity

Apply bounds where data is accumulated, including merge paths. Count and
explain every refusal using the existing verdicts and shared wording.

| Bound | Required behavior |
|---|---|
| `scrape.MaxPortsPerPod` | Enforce in `server.targetDedup.add`; a merge into an existing URL does not spend a new target |
| Relabel rules, bytes and instruction cost | Enforce parse and merged-chain budgets; keep the aggregate prefix, but refuse an endpoint with an oversized individual rule |
| `maxEndpointsPerMonitor` | Keep the bounded endpoint prefix and report omitted endpoints; malformed/over-budget selectors refuse the whole monitor |
| `scrape.MaxContributorsPerTarget` | Bound attribution without discarding already-merged configuration |
| `scrape.MaxTargetBytesPerPod` | Measure the pod projection once; first target is unconditional, merged views are charged, oversized Service views reduce to identity |
| Annotation values and sets | Omit oversized values whole; preserve priority for `prometheus.io/*` and `kubescrape.io/*` without exempting them from limits |
| `owners.MaxOwners` and owner-label bytes | Bound owner count and labels; report omissions |

Measure every admitted relabel rule's individual cost even after an aggregate
ceiling is reached. Bound annotation ordering deterministically so ETags stay
stable. Strip cluster-supplied omission markers before generating authoritative
ones. Pod labels remain selection input and are not filtered; namespace labels
are also unbounded apart from Kubernetes' object limit. Keep those residuals
documented.

`server.writeCached` adds Cache-Control and ETag to metadata and node-target
responses, excluding pod-IP lookups. Sort targets before hashing.
`metaclient` keys its cache on the query-less URL, revalidates with
If-None-Match, and never caches an undecodable response.

Node-target memo validity includes pod/node generation, owner/namespace
changes, Services and monitors. Writers publish a generation after mutation;
readers sample it before derivation. Bump on changes to served data, not status
or byte-identical resyncs. A changed authoritative generation rebuilds even a
young memo; an unchanged one permits 304 with a full max-age. Use clock-based
expiry only for unwired change sources. Preserve the generation and fallback
tests in `internal/server`.
