package promscrape

// The ONE identity path. Every object an enriched pipeline describes — a
// cadvisor row, a /stats/summary element, a splitter's row, a cgroup the
// sampler found (internal/agent/cgroupstats, through FillContainerResource) —
// is resolved to a pod and container through the metadata service here
// (resolveContext), and the cadvisor-shaped resource is built from the answer
// here (fillIdentityResource). The pipelines' series are worth having only
// while they JOIN, and two resources join exactly when they are byte-identical:
// a second implementation would agree with this one until the first edit to
// either. Underneath sits the TTL cache over the metadata service (podMeta,
// containerMeta), bounded by size and by the scrape's metadata allowance
// (metabudget.go).

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

// MetaSource resolves pod and container metadata; implemented by
// metaclient.Client.
type MetaSource interface {
	PodByName(ctx context.Context, namespace, name string) (*kubemeta.Pod, error)
	Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error)
}

// metaSource returns the metadata source shared by the kubelet scrapes and
// the splitters. Splitters require Kubelet.Meta to be set even when the
// kubelet scrapes are disabled.
func (s *Scraper) metaSource() MetaSource {
	return s.cfg.Kubelet.Meta
}

// FillContainerResource builds the resource attributes describing one container
// identified by its CGROUP PATH — the runtime container id and the pod uid of
// the slice it sits in — using exactly the machinery a cadvisor row goes
// through: the same metadata lookup, the same TTL cache, the same
// unresolved-row fallback and the same `cadvisor` attribute builder (hence the
// same instance prefix).
//
// It exists for internal/agent/cgroupstats, whose ten gauges are only worth
// anything if they JOIN the cadvisor series they explain — and two series join
// when their resource attributes, and so the derived Prometheus job and
// instance, are byte-identical. A second implementation would agree with this
// one until the first edit to either; sharing the body means the sampler cannot
// drift from the scrape even in principle.
//
// The first bool is what a cgroup path costs. A cadvisor row carries namespace,
// pod and container LABELS, so an unresolvable one still falls back to a
// resource with a service.name (the pod name) and keeps its Prometheus `job`. A
// cgroup path yields only two ids, so the same fallback here produces a resource
// with no service.name at all — a series attributed to nothing, joining none of
// the cadvisor series the sampler exists to explain, and one successful later
// lookup away from silently becoming a DIFFERENT series. So the verdict is
// reported and the caller declines to export what did not resolve; see
// cgroupstats.Resolver.
//
// The second bool CLASSIFIES a failure, and it exists because the two ways to
// not resolve want opposite retry policies. See cgroupstats.Resolver for the
// contract and containerMeta for where the classification comes from.
func (s *Scraper) FillContainerResource(ctx context.Context, res pcommon.Resource, containerID, podUID string) (ok, answered bool) {
	return s.fillIdentityResource(ctx, res, cadvisorIdentity{containerID: containerID, podUID: podUID, pathVouched: true})
}

// fillIdentityResource is cadvisorBatcher.fillResource's body, lifted onto the
// Scraper so the exported seam above, that batcher and the /stats/summary one
// are literally one code path. It
// reports whether the metadata service PLACED the identity (as opposed to the
// resource having been built from the caller's own labels), and whether it
// ANSWERED at all.
func (s *Scraper) fillIdentityResource(ctx context.Context, res pcommon.Resource, ident cadvisorIdentity) (bool, bool) {
	return s.fillIdentityResourceCreated(ctx, res, ident, time.Time{})
}

// fillIdentityResourceCreated is fillIdentityResource for a caller that knows
// when the container it describes was CREATED — the /stats/summary batcher,
// whose only way to a container's identity is a name match inside the pod
// document (see Scraper.containerIncarnation). Zero means unknown, which is
// every other caller: a cgroup row resolves its incarnation by container id.
func (s *Scraper) fillIdentityResourceCreated(ctx context.Context, res pcommon.Resource, ident cadvisorIdentity, created time.Time) (bool, bool) {
	// Exact container incarnation via the cgroup container ID, else the pod.
	// lookupContainerID, not ident.containerID: a container id a row merely
	// PARSED out of a cgroup path it does not name itself must not be resolved,
	// or a supervisor scope inherits the identity of the container it supervises.
	actx, resolved, answered := s.resolveContextCreated(ctx, ident.lookupContainerID(), ident.namespace, ident.pod, ident.podUID, ident.container, created)
	actx.Node = s.nodeInfo()
	if resolved && actx.Container == nil && ident.container != "" {
		// A placed pod whose document does not name the container — or names
		// only an EARLIER incarnation (containerIncarnation) — still names it on
		// the resource, from the row's own identity: attrs.Container, which
		// would, has nothing to build from.
		res.Attributes().PutStr("k8s.container.name", ident.container)
	}

	if !resolved && (ident.pod != "" || ident.podUID != "" || ident.containerID != "") {
		// Metadata unavailable (or a same-name pod replaced this one, or a
		// standalone non-k8s container the metadata service cannot know): keep
		// the identity from the labels and the cgroup path — container.id is a
		// containerID-only row's ONLY distinguisher, since its id/name/image
		// labels are elided from the data points as pod-scoped-redundant.
		a := res.Attributes()
		if ident.namespace != "" {
			a.PutStr("k8s.namespace.name", ident.namespace)
		}
		if ident.pod != "" {
			a.PutStr("k8s.pod.name", ident.pod)
		}
		if ident.podUID != "" {
			a.PutStr("k8s.pod.uid", ident.podUID)
		}
		if ident.container != "" {
			a.PutStr("k8s.container.name", ident.container)
		}
		if ident.containerID != "" {
			a.PutStr("container.id", ident.containerID)
		}
		// The image label is elided from container-row data points as
		// resource-redundant; on an unresolved resource it is the only source.
		if ident.image != "" && (ident.container != "" || ident.containerID != "") {
			a.PutStr("container.image.name", ident.image)
		}
		// service.name is otherwise obtained only as a SIDE EFFECT of attrs.Pod
		// running inside Build, which needs resolved metadata — so an
		// unresolved row carried no service.name at all, and the OTLP→Prometheus
		// translation gives it no `job` label. Metadata resolution is not
		// stable (a lookup fails, a pod is replaced by one of the same name),
		// so the SAME series gained and lost its job label as rows resolved and
		// stopped resolving: two different Prometheus series, flapping.
		//
		// attrs.ServiceName's documented fallback chain ends at the pod name,
		// which is exactly what is known here. It survives only when no pod
		// resolved at all: whenever resolveContext hands back a pod context —
		// including the container-id-miss arm, which returns the pod with
		// resolved=false — the default builder's attrs.Pod OVERWRITES
		// service.name with the owner-derived name. A later successful
		// resolution skips this branch entirely.
		if ident.pod != "" {
			a.PutStr("service.name", ident.pod)
		}
	}
	s.attrsFor(pipelineCadvisor).Build(res, actx)
	return resolved, answered
}

// resolveContext resolves a described object's pod/container through the metadata
// service — an exact container incarnation by container id, else the pod by
// namespace+name (accepted only when podAnswersFor says the object the API
// server holds is the one the kubelet described under uid) with a named
// container matched within it. It returns the built attrs.Context (Node NOT
// set — the caller adds it) and whether the row's IDENTITY resolved; on false
// the caller writes its own identity fallback. A resolved pod whose document
// does not name the container comes back with a nil Container, and the caller
// names the container itself (fillIdentityResourceCreated from the row's
// identity, the splitter from its groupBy labels) — this used to take the
// caller's resource only to stamp that one attribute on it.
// The two questions are not the same one: a row naming a container id the store
// does not know yet gets the POD context (Build still enriches from it) together
// with false, because its container identity can only come from the caller's own
// labels. Shared by the cadvisor and split batchers.
//
// The THIRD value classifies a failure for the one caller whose retry policy
// turns on it (FillContainerResource, for internal/agent/cgroupstats): true
// means the metadata service ANSWERED and its answer placed nothing, false
// means it could not answer at all. With no id to look up at all it is true —
// nothing was asked, so there is no outage to wait out and no later attempt
// that could go differently.
func (s *Scraper) resolveContext(ctx context.Context, containerID, namespace, pod, uid, container string) (attrs.Context, bool, bool) {
	return s.resolveContextCreated(ctx, containerID, namespace, pod, uid, container, time.Time{})
}

// resolveContextCreated is resolveContext for a caller that also knows when
// the named container was CREATED (zero = unknown): the by-name match is then
// held to that incarnation (containerIncarnation).
func (s *Scraper) resolveContextCreated(ctx context.Context, containerID, namespace, pod, uid, container string, created time.Time) (attrs.Context, bool, bool) {
	var actx attrs.Context
	// ONE object, however many lookups it takes and however many chunks it
	// appears in: the metadata allowance's shed count is per object (see
	// objectShed).
	obj := objectShed{containerID: containerID, namespace: namespace, pod: pod, uid: uid, container: container}
	if containerID != "" {
		pmeta, cmeta, answered := s.containerMeta(ctx, containerID, &obj)
		if pmeta != nil {
			actx.Pod, actx.Container = pmeta, cmeta
			return actx, true, true
		}
		// The store does not know THIS incarnation — typically because the kubelet
		// has not posted a just-started container's id to the API server yet, and
		// the miss is negative-cached for a minute. Falling through to the pod
		// branch below would match the pod's CURRENT container by NAME and stamp
		// that incarnation's container.id, image and restart_count — hence
		// service.instance.id — onto this row: a dead incarnation's identity on
		// the live one's cumulative counters, and a resource byte-identical to the
		// one the other incarnation is keyed under (both keys carry the container
		// id), i.e. two ResourceMetrics with the same identity in one payload.
		// Take the pod for its owner/label enrichment, but report UNRESOLVED so
		// the caller's identity fallback keeps the row's OWN container id, name
		// and image.
		//
		// The classification stays the CONTAINER lookup's: the service has said
		// this container id is unknown, and whether the pod lookup beside it
		// also reached the service says nothing about that verdict.
		if pod != "" {
			if meta, _ := s.podMeta(ctx, namespace, pod, uid, &obj); meta != nil && podAnswersFor(meta, uid) {
				actx.Pod = meta
			}
		}
		return actx, false, answered
	}
	if pod != "" {
		meta, answered := s.podMeta(ctx, namespace, pod, uid, &obj)
		if meta != nil && podAnswersFor(meta, uid) {
			if container != "" {
				meta, actx.Container = s.containerIncarnation(ctx, meta, namespace, pod, uid, container, created, &obj)
			}
			actx.Pod = meta
			return actx, true, true
		}
		// A pod that resolved under a DIFFERENT uid is an answer too: this name
		// belongs to another pod now, and asking again in a second changes
		// nothing. (That holds for a CURRENT answer; a cached one cannot go stale
		// across a same-name recreation, because podMeta keys its cache by the
		// uid asked about as well as the name.)
		return actx, false, answered
	}
	return actx, false, true
}

// containerIncarnation matches container BY NAME inside the pod document meta
// and returns the document it settled on with the match — nil when the document
// does not name the container, or demonstrably describes an EARLIER incarnation
// than the one the kubelet reports as created at `created` (zero = unknown,
// which matches by name alone, as every caller but the summary does).
//
// A name is not an incarnation. A container id resolves exactly; a name match
// is exact only while the document is at least as new as the container, and
// the /stats/summary batcher — the one caller passing a creation time — has no
// other way to a container's identity. A document fetched before a restart and
// served from podCache for up to podMetaCacheTTL names the PREVIOUS
// incarnation's id and restart count, so the summary stamped
// container.id=<old id> (service.instance.id=cadvisor-<old id>) on the new
// incarnation's statistics while cadvisor, resolving the same container by its
// new cgroup id, did not: the two resources stopped joining for up to a minute
// after every restart, and nothing counted it, since container.id was present.
//
// A document that names an earlier incarnation is dropped from the cache and,
// when the cache is why (it was fetched before the container existed), asked
// for once more. A document that is STILL older — the second or so before the
// kubelet posts the new status — withholds the match rather than stamp the
// dead incarnation's identity; the resource then carries k8s.container.name
// and no container.id, which is what the summary's unresolved-container
// accounting counts. The entry is not kept either, so the next scrape asks
// again instead of re-serving it for a minute.
//
// Checked only inside incarnationWindow of the container's creation: that is
// the whole span a cached document can be stale for, and it bounds the cost of
// a creation time that disagrees with the document's start time for some
// other reason (cadvisor's CreationTime is a cgroup directory's mtime, which a
// child cgroup moves) to a withheld container.id for that first minute rather
// than for the container's life.
func (s *Scraper) containerIncarnation(ctx context.Context, meta *kubemeta.Pod, namespace, pod, uid, container string, created time.Time, obj *objectShed) (*kubemeta.Pod, *kubemeta.Container) {
	c := containerNamed(meta, container)
	if c == nil || created.IsZero() || time.Since(created) >= incarnationWindow || !namesEarlierIncarnation(c, created) {
		return meta, c
	}
	key := podCacheKey(namespace, pod, uid)
	if s.cacheDrop(key, created) {
		// The CACHE is why: the document was fetched before this container
		// existed, so a fresh answer can differ.
		if fresh, _ := s.podMeta(ctx, namespace, pod, uid, obj); fresh != nil && podAnswersFor(fresh, uid) {
			meta = fresh
			if c = containerNamed(meta, container); c == nil || !namesEarlierIncarnation(c, created) {
				return meta, c
			}
			s.cacheDrop(key, time.Time{})
		}
	}
	return meta, nil
}

// incarnationWindow is how long after a container's creation containerIncarnation
// checks a by-name match against it: podMetaCacheTTL, the longest a document
// fetched before the creation can be served, plus slack for the
// second-resolution floor on both timestamps.
const incarnationWindow = podMetaCacheTTL + 5*time.Second

// namesEarlierIncarnation reports whether the pod document's container c
// demonstrably describes an incarnation OLDER than the container created at
// `created`. Both timestamps are second-resolution by the time they reach this
// agent (metav1.Time serialises no fraction), and a container is created before
// it starts, so a start strictly before the creation's second is another
// container; a restart is separated by the previous run plus the kubelet's
// back-off, far more than the second the comparison gives away.
func namesEarlierIncarnation(c *kubemeta.Container, created time.Time) bool {
	if c.ID == "" {
		// The first-start window: the document names no incarnation at all,
		// so attrs.Container writes no container.id and nothing wrong can be
		// stamped.
		return false
	}
	if c.StartedAt != nil {
		return c.StartedAt.Truncate(time.Second).Before(created.Truncate(time.Second))
	}
	// An id with no start: waiting (CrashLoopBackOff keeps naming the dead
	// incarnation's id) or terminated without ever starting — a document that
	// vouches for no started container cannot vouch for this one. A document
	// with no state at all says nothing either way and is taken as it is.
	return c.State == "waiting" || c.State == "terminated"
}

// containerNamed returns meta's container called name, or nil.
func containerNamed(meta *kubemeta.Pod, name string) *kubemeta.Container {
	for i := range meta.Containers {
		if meta.Containers[i].Name == name {
			return &meta.Containers[i]
		}
	}
	return nil
}

// The two annotations the kubelet stamps on a mirror pod, both carrying the
// STATIC pod's own UID: config.hash is written onto the static pod as the
// kubelet generates that UID from the manifest, and the mirror client copies
// that value into config.mirror when it creates the API object. Either one on
// its own is the proof podAnswersFor needs, so both are read — they are written
// by different code, and a pod that has one has the claim.
const (
	annConfigMirror = "kubernetes.io/config.mirror"
	annConfigHash   = "kubernetes.io/config.hash"
)

// podAnswersFor reports whether the pod object the API server holds is the pod
// the KUBELET described under uid. It is the ONE rule behind every by-name
// resolution in this package, whichever kubelet endpoint the identity came from
// — the cgroup path in a /metrics/cadvisor row's `id` label, or podRef.uid in a
// /stats/summary element — because the two pipelines' series are only worth
// having while they JOIN, and a pod one of them refuses to place must be a pod
// the other refuses to place too.
//
// A UID match is the ordinary case. The MIRROR case is the one that needs
// explaining: a static pod's UID is minted by the kubelet from its manifest and
// is what every statistic about it carries, while the object the API server
// holds is a MIRROR pod under a UID the API server assigned — so a plain UID
// comparison refuses the answer, and kube-apiserver, etcd, kube-scheduler and
// kube-controller-manager resolve to nothing on every scrape for the life of
// the cluster. A mirror pod NAMES the static pod it was minted from, so there
// is something to check against after all.
//
// The cross-check is REDIRECTED, never dropped: resolving a UID miss by name
// alone lets a pod that merely shares the name lend its identity to statistics
// about another object — the hazard internal/agent/events resolves its involved
// objects by UID for, and the one the store's byPodName guard is about. A pod
// claiming no mirror stays unresolved and is exported with its label identity.
//
// An EMPTY uid is the one permissive branch, and it is the caller's statement
// that the kubelet reported no uid at all — a cgroup layout with no parseable
// pod segment. There is nothing to check against and no evidence of a mismatch
// either, so the name is all there is; podRef.uid is always present, so this
// branch belongs to the cadvisor side alone.
func podAnswersFor(meta *kubemeta.Pod, uid string) bool {
	if uid == "" || meta.UID == uid {
		return true
	}
	return isMirrorOf(meta, uid)
}

// isMirrorOf reports whether the pod the API server holds is the mirror of the
// static pod whose UID the kubelet reported.
func isMirrorOf(meta *kubemeta.Pod, staticUID string) bool {
	if staticUID == "" {
		// A pod carrying neither annotation would otherwise match "", which is
		// every pod the metadata service cannot place a UID for.
		return false
	}
	return meta.Annotations[annConfigMirror] == staticUID || meta.Annotations[annConfigHash] == staticUID
}

// podMetaCacheTTL bounds how long resolved (or not-found) metadata is reused
// across cadvisor scrape cycles.
const podMetaCacheTTL = time.Minute

// podCacheMaxEntries bounds the metadata cache. Each entry holds a full
// kubemeta.Pod document, and the objects a splitter describes are the whole
// cluster's, not this node's.
const podCacheMaxEntries = 8192

// podCacheLowWater is what a size trim leaves behind. Trimming BELOW the cap
// rather than down to it amortizes the O(n) sweep over ~2000 inserts instead of
// running it on every insert while full, which matters because the sweep holds
// cacheMu — shared by every concurrent scrape goroutine's lookup. Same shape,
// and the same reason, as metaclient's evictLowWater.
const podCacheLowWater = podCacheMaxEntries * 3 / 4

// podCacheSweepEvery is how often expired entries are swept below the cap.
const podCacheSweepEvery = time.Minute

type podCacheEntry struct {
	pod       *kubemeta.Pod       // nil: lookup failed / unknown
	container *kubemeta.Container // set for container-ID entries
	fetched   time.Time
	// answered records WHY a negative entry is negative, which is the one thing
	// a caller cannot reconstruct from a nil pod: true means the metadata
	// service replied and its reply was 404 (this id is not a container of any
	// pod it knows — a pod sandbox's permanent condition), false means it could
	// not be reached or could not answer (transport failure, 5xx, an
	// undecodable body).
	//
	// It has to live IN the cache, not just in the return value, because a
	// negative entry is served for podMetaCacheTTL: reconstructing the reason
	// as "definitive" on a cache hit would turn one unreachable minute into a
	// definitive verdict for every lookup inside it — which is exactly the
	// distinction internal/agent/cgroupstats' retry policy is built on.
	answered bool
}

// podMeta resolves pod metadata by name with a small TTL cache; nil when
// unknown. The second value is podCacheEntry.answered: on a nil pod it says
// whether the metadata service ANSWERED (a 404) or could not be asked.
//
// uid is the pod UID the CALLER is asking about ("" when the payload carried
// none). It does not change the request — the lookup is by name — but it keys
// the cache, and that is load-bearing: keyed by name alone, a StatefulSet pod
// recreated under its old name (web-0) hit the still-fresh entry for its
// PREDECESSOR, which podAnswersFor then refused, and the new incarnation went
// out unresolved — service.name falling back to "web-0" instead of "web" — for
// up to podMetaCacheTTL without the metadata service ever being asked. Every
// kubelet row carries a uid, so this adds no entries on the kubelet pipelines;
// a genuine name reuse is still cached, as the mismatching answer, under its
// own uid.
func (s *Scraper) podMeta(ctx context.Context, namespace, pod, uid string, obj *objectShed) (*kubemeta.Pod, bool) {
	key := podCacheKey(namespace, pod, uid)
	if e, ok := s.cacheGet(key); ok {
		return e.pod, e.pod != nil || e.answered
	}
	lctx, spent, may := s.metaLookup(ctx, obj)
	if !may {
		// The scrape has spent its metadata allowance (metabudget.go): the object
		// keeps its label identity, exactly as it would against a service that
		// REFUSED the connection. Nothing was asked, so nothing is cached.
		return nil, false
	}
	defer spent()
	meta, err := s.metaSource().PodByName(lctx, namespace, pod)
	answered := true
	if err != nil {
		meta = nil
		if lctx.Err() != nil {
			// A cancellation is not an answer, and it is not cached either. The
			// LOOKUP's context is what is read, so a lookup cut short by the
			// allowance is classified the same way — the service said nothing.
			return nil, false
		}
		answered = metaclient.IsNotFound(err)
	}
	s.cachePut(key, podCacheEntry{pod: meta, fetched: time.Now(), answered: answered})
	return meta, meta != nil || answered
}

// podCacheKey is podMeta's cache key. Length-prefixed (appendLP, the package's
// injective-join rule): namespace and pod arrive as exporter LABELS on the
// cadvisor and split paths, and uid as a label or a JSON string, so no
// delimiter is safe.
func podCacheKey(namespace, pod, uid string) string {
	// A stack scratch for the common size, so the key costs the one allocation
	// the string itself needs, as the concatenation it replaced did.
	var scratch [128]byte
	b := append(scratch[:0], 'n', 0)
	b = appendLP(b, namespace)
	b = appendLP(b, pod)
	return string(appendLP(b, uid))
}

// containerMeta resolves the exact container incarnation by runtime ID: the pod
// and the container within it, both nil when unknown. The lookup is
// non-blocking (wait 0): the scraped series only reference containers that
// already exist.
//
// The two pointers are the CACHED values, exactly as podMeta returns its cached
// *kubemeta.Pod — shared under the treat-as-immutable contract metaclient's own
// cache holds them to, and never written through by any caller. A hit used to
// copy both into a fresh ContainerMetadata only for the caller to take the
// fields' addresses again: two allocations per container resource per chunk,
// defending nothing (a Pod's maps and slices were shared by the copy anyway).
//
// The second value classifies a MISS, and it is what makes the cgroup sampler's
// retry policy possible (internal/agent/cgroupstats): a 404 is a definitive
// statement about this container id — nothing about the node changing will make
// a pause container's id appear in a pod's containerStatuses — while a
// transport failure, a 5xx or an undecodable body is a statement about the
// metadata SERVICE, and the retry that follows must be soon rather than rare.
// Both still produce a nil here, and the cadvisor path still treats them
// alike: the row is exported with its label identity either way.
func (s *Scraper) containerMeta(ctx context.Context, containerID string, obj *objectShed) (pod *kubemeta.Pod, ctr *kubemeta.Container, answered bool) {
	key := "c\x00" + containerID
	if e, ok := s.cacheGet(key); ok {
		if e.pod == nil {
			return nil, nil, e.answered
		}
		return e.pod, e.container, true
	}
	lctx, spent, may := s.metaLookup(ctx, obj)
	if !may {
		return nil, nil, false // allowance spent; see podMeta
	}
	defer spent()
	md, err := s.metaSource().Container(lctx, containerID, 0)
	if err != nil {
		if lctx.Err() != nil {
			return nil, nil, false // do not negative-cache cancellations
		}
		answered := metaclient.IsNotFound(err)
		s.cachePut(key, podCacheEntry{fetched: time.Now(), answered: answered})
		return nil, nil, answered
	}
	s.cachePut(key, podCacheEntry{pod: &md.Pod, container: &md.Container, fetched: time.Now(), answered: true})
	return &md.Pod, &md.Container, true
}

func (s *Scraper) cacheGet(key string) (podCacheEntry, bool) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	e, ok := s.podCache[key]
	if !ok || time.Since(e.fetched) >= podMetaCacheTTL {
		return podCacheEntry{}, false
	}
	return e, true
}

func (s *Scraper) cachePut(key string, e podCacheEntry) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.podCache[key] = e
	s.evictCacheLocked(time.Now())
}

// cacheDrop forgets key and reports whether the entry it dropped had been
// fetched before `before` — i.e. whether asking again could return anything
// newer than what the cache was serving. A zero `before` never reports true.
func (s *Scraper) cacheDrop(key string, before time.Time) bool {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	e, ok := s.podCache[key]
	if !ok {
		return false
	}
	delete(s.podCache, key)
	return e.fetched.Before(before)
}

// evictCacheLocked bounds the cache: expired entries first, then arbitrary ones
// down to the low-water mark. Caller holds cacheMu.
//
// Expiry alone is NOT a bound and cannot be one. The entries a splitter's
// enrichment inserts are all fetched inside one TTL window, so above the cap
// EVERY insert swept the whole map and deleted nothing — 1.3 s per insert at 12k
// objects, 5.2 s at 20k, all of it holding the mutex the concurrent scrape
// goroutines share — and the map never left the branch, so a 12k-pod cluster
// also retained 12k full pod documents. The cadence keeps the sweep amortized
// and the low-water trim is what makes the cap real; an arbitrary eviction costs
// the next lookup a metadata request, which is the price of the hard bound.
func (s *Scraper) evictCacheLocked(now time.Time) {
	if len(s.podCache) <= podCacheMaxEntries {
		if now.Sub(s.cacheSwept) < podCacheSweepEvery {
			return
		}
		s.cacheSwept = now
		s.sweepExpiredLocked(now)
		return
	}
	// The hard trim sweeps on its way to the cap, so the cadence counts from it
	// too: leaving cacheSwept behind made the next below-cap insert re-run a full
	// sweep however recently this one finished.
	s.cacheSwept = now
	s.sweepExpiredLocked(now)
	for k := range s.podCache {
		if len(s.podCache) <= podCacheLowWater {
			break
		}
		delete(s.podCache, k)
	}
}

// sweepExpiredLocked drops entries past podMetaCacheTTL — the same predicate
// cacheGet reads them by, so nothing usable is thrown away. Caller holds
// cacheMu.
func (s *Scraper) sweepExpiredLocked(now time.Time) {
	for k, e := range s.podCache {
		if now.Sub(e.fetched) >= podMetaCacheTTL {
			delete(s.podCache, k)
		}
	}
}
