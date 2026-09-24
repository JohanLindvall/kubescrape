package main

// Informer wiring: the pod and Service handlers that fill the store and the
// service index, the owner/namespace metadata informers behind owners.Resolver
// and the change token they feed, and the per-resource freshness and watch-error
// reporting.

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/metadata/metadatainformer"
	"k8s.io/client-go/tools/cache"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/owners"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// typedHandler builds informer callbacks that type-assert every payload to T:
// Add and Update call upsert; Delete unwraps a DeletedFinalStateUnknown
// tombstone first, then calls del. A payload of the wrong type is ignored.
func typedHandler[T any](note func(), upsert, del func(T)) cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			note()
			if v, ok := obj.(T); ok {
				upsert(v)
			}
		},
		UpdateFunc: func(oldObj, obj any) {
			if !isResync(oldObj, obj) {
				note()
			}
			if v, ok := obj.(T); ok {
				upsert(v)
			}
		},
		DeleteFunc: func(obj any) {
			note()
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			if v, ok := obj.(T); ok {
				del(v)
			}
		},
	}
}

// isResync reports that an update is a periodic RESYNC: client-go re-delivering
// an object out of its own local cache, not the watch delivering anything.
//
// It is POINTER identity, and that is exact rather than a heuristic: a resync
// hands the handler the cached object as BOTH halves (DeltaFIFO's Sync delta is
// the indexer's own object — it skips the transformer precisely because that
// object was already transformed — and RealFIFO's SyncAll calls
// OnUpdate(obj, obj)), while anything the API server sent, a relist of an
// unchanged object included, is a freshly decoded object. The informers here
// deliver pointer types only, so the comparison cannot hit a non-comparable
// dynamic type.
//
// Why it matters: with -resync set, stamping a resync kept
// kubescrape_informer_last_event_timestamp_seconds fresh on a timer while the
// watch behind it had silently stopped — the exact failure that gauge exists to
// show, and the documented stalled-watch alert could never fire at any -resync
// shorter than its window, since the resync goroutine keeps running through a
// hung watch.
func isResync(oldObj, newObj any) bool { return oldObj == newObj }

// informerFreshness is the last-event clock of every informer this process
// runs, published as kubescrape_informer_last_event_timestamp_seconds. Each
// handler stamps its resource's slot on every event it delivers — one atomic
// store on the informer goroutine — and the gauge reads the slots at export
// time. It is the freshness half of the reachability probe (apiserver.go): the
// probe proves a NEW connection reaches the API server, and this is what
// notices an ESTABLISHED watch that has silently stopped delivering, which
// the probe cannot. A periodic resync is deliberately NOT an event here
// (isResync): it is the informer replaying its own cache, and it keeps running
// through exactly the stall this clock exists to show.
type informerFreshness struct {
	mu    sync.Mutex
	slots map[string]*atomic.Int64
}

// slot registers a resource and returns the stamp its handlers call per event.
func (f *informerFreshness) slot(resource string) func() {
	s := new(atomic.Int64)
	f.mu.Lock()
	if f.slots == nil {
		f.slots = map[string]*atomic.Int64{}
	}
	f.slots[resource] = s
	f.mu.Unlock()
	return func() { s.Store(time.Now().Unix()) }
}

// snapshot is the gauge's read: every registered resource, 0 until its first
// event.
func (f *informerFreshness) snapshot() map[string]float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]float64, len(f.slots))
	for resource, s := range f.slots {
		out[resource] = float64(s.Load())
	}
	return out
}

// freshness is the process's one informerFreshness: the informers are wired in
// three functions, and a package-level clock is what lets each stamp its own
// resource without threading a value through all of them.
var freshness informerFreshness

// registerCoreInformers wires the pod and service informers into the store
// and the service index, returning their HasSynced funcs.
//
// These are the HANDLER REGISTRATIONS' HasSynced, never the informer's. The
// informer's flips as soon as its DeltaFIFO has drained the initial LIST,
// which says nothing about whether OUR handlers have run: the shared
// processor delivers to each listener asynchronously through a pending-
// notification ring, and everything a request actually reads — the store's
// indexes, the service index — is filled from inside those handlers. Gating
// readiness on the informer therefore lets /readyz report 200 while the store
// is still filling, and an agent that polls in that window gets a 200 with a
// half-built (or empty) target list instead of a 503 telling it to come back.
func registerCoreInformers(factory informers.SharedInformerFactory, st *store.Store, svcIndex *services.Index) ([]syncGate, error) {
	podInformer := factory.Core().V1().Pods().Informer()
	if err := watchErrors(podInformer, "pods"); err != nil {
		return nil, fmt.Errorf("pod watch error handler: %w", err)
	}
	podReg, err := podInformer.AddEventHandler(typedHandler(freshness.slot("pods"),
		func(pod *corev1.Pod) { st.UpsertPod(pod) },
		func(pod *corev1.Pod) { st.DeletePod(pod.UID) },
	))
	if err != nil {
		return nil, fmt.Errorf("registering pod event handler: %w", err)
	}

	// Services are matched against pods for service-annotation based scrape
	// discovery; their specs are small, so the full objects are cached.
	svcInformer := factory.Core().V1().Services().Informer()
	if err := watchErrors(svcInformer, "services"); err != nil {
		return nil, fmt.Errorf("service watch error handler: %w", err)
	}
	svcReg, err := svcInformer.AddEventHandler(typedHandler(freshness.slot("services"),
		func(svc *corev1.Service) { svcIndex.Upsert(svc) },
		func(svc *corev1.Service) { svcIndex.Delete(svc.Namespace, svc.UID) },
	))
	if err != nil {
		return nil, fmt.Errorf("registering service event handler: %w", err)
	}
	// NAMED, because readiness is what a rolling update advances on and "some
	// cache has not synced" is the half of that message that cannot be acted
	// on (waitForCaches).
	return []syncGate{{podGate, podReg.HasSynced}, {"services", svcReg.HasSynced}}, nil
}

// podGate names the pod cache's readiness gate. A constant because two places
// wait on it by name — readiness, and the self-pod lookup (gatesSynced) — and a
// misspelling in the second would wait for nothing at all.
const podGate = "pods"

// registerOwnerInformers wires a metadata-only informer per owner GVR,
// returning the listers the owner resolver reads and their HasSynced funcs.
func registerOwnerInformers(metaFactory metadatainformer.SharedInformerFactory, changes *owners.Changes) (map[schema.GroupVersionResource]cache.GenericLister, []syncGate, error) {
	listers := make(map[schema.GroupVersionResource]cache.GenericLister, len(owners.AllGVRs))
	var synced []syncGate
	for _, gvr := range owners.AllGVRs {
		inf := metaFactory.ForResource(gvr)
		if err := inf.Informer().SetTransform(stripManagedFields); err != nil {
			return nil, nil, fmt.Errorf("setting %s informer transform: %w", gvr.Resource, err)
		}
		if err := watchErrors(inf.Informer(), gvr.Resource); err != nil {
			return nil, nil, fmt.Errorf("%s watch error handler: %w", gvr.Resource, err)
		}
		// These informers exist for their LISTERS — the resolver reads them at
		// request time and nothing here keeps derived state — so this handler
		// does one thing: advance the change token that lets the node-targets
		// ETag memo prove a client's copy is current without re-deriving it.
		if _, err := inf.Informer().AddEventHandler(ownerChangeHandler(ownerTokenFor(gvr, changes), freshness.slot(gvr.Resource))); err != nil {
			return nil, nil, fmt.Errorf("%s change handler: %w", gvr.Resource, err)
		}
		listers[gvr] = inf.Lister()
		synced = append(synced, syncGate{gvr.Resource, inf.Informer().HasSynced})
	}
	return listers, synced, nil
}

// ownerTokenFor is the change token gvr's informer feeds: the shared owner
// token for every resource a node-targets derivation reads through the
// resolver (the owner kinds, via Resolve, and Namespaces, via Namespace), and
// NONE for Nodes. No targets derivation reads Node metadata — the only reader
// is GET /v1/nodes/{node}/metadata, whose ETag is a body hash and consults no
// token — so bumping on a Node event lapsed every node's targets memo for a
// change that cannot alter any targets document, and Node churn (an
// autoscaler's joins and leaves, a node label edit) is routine. The informer
// still stamps its freshness slot; only the token is withheld (a nil *Changes
// accepts Bump). IF A TARGETS DERIVATION EVER READS NODE METADATA, THIS
// EXCLUSION MUST GO, or the memo will serve a stale list.
func ownerTokenFor(gvr schema.GroupVersionResource, changes *owners.Changes) *owners.Changes {
	if gvr == owners.NodeGVR {
		return nil
	}
	return changes
}

// ownerChangeHandler bumps the owner change token on changes that can actually
// be SEEN, which is a narrower thing than "the object was written".
//
// Everything this package's informers serve is UID + labels + annotations +
// OWNER REFERENCES (owners.Resolver.clusterScoped and Resolve, via
// kubemeta.CopyMeta) — so an update touching none of those cannot change any
// response and must not advance the token.
//
// The UID IS compared, although it is immutable per OBJECT: the informer is
// keyed by namespace/name, not by UID, and an owner deleted and recreated
// under the same name inside a relist gap arrives as an UPDATE carrying a new
// UID and possibly identical maps (the same shape services.Index documents for
// Services). Resolve's answer changes with it — the uid_mismatch arm stops
// lending the owner's labels to pods naming the old UID, and a Namespace's UID
// is served verbatim — so it must bump. It costs nothing on the paths the
// token exists to ignore: a resync or a status write never changes a UID.
//
// The owner references are easy to forget because they are not the object's own
// metadata in the way the two maps are: Resolve FOLLOWS them (owners.go's
// `for _, parent := range m.OwnerReferences { add(parent, false) }`), so a
// ReplicaSet losing or gaining its Deployment ref — a re-adoption, a controller
// rewriting the field, a `kubectl patch --type=json` removing it — changes the
// Owners chain of every pod that RS owns, and with it attrs.ServiceName and
// therefore half the Prometheus job of every series the fleet exports for that
// workload. Nothing else would bump: the pods are untouched, so the store's
// generation does not move either, and the memo answers 304 with a full max-age
// for as long as nothing unrelated happens to change. Only the fields Resolve
// SERVES are compared (owners.SameServedRefs, which reads the same projection
// Resolve builds its Owners from) — blockOwnerDeletion is not one.
//
// COMPARING resourceVersion IS NOT THAT, and the difference is the whole
// mechanism working or not. The API server changes the resourceVersion on every
// write including status-only ones, and the objects behind AllGVRs are written
// constantly for reasons their metadata never reflects: a kubelet rewrites its
// Node's status on nodeStatusReportFrequency (5 minutes by default), so a
// 200-node cluster produces a node write about every 1.5 seconds; a Deployment
// or ReplicaSet's status moves on every scale and rollout; a Job's counts move
// throughout its life. This token is SHARED by every node's memo, so each of
// those would invalidate the whole fleet's — against agents polling every 30s,
// an RV-keyed token is bumped tens of times between one agent's polls and the
// memo never validates, while a benchmark (which has no such churn) still
// reports the full win. Comparing the served maps is both cheaper to be right
// about and immune to whatever else the object carries.
//
// It also subsumes the resync case for free: client-go re-delivers every cached
// object each `-resync` period, and an identical object compares equal.
//
// An unrecognised object type bumps rather than assumes — an unexpected shape
// is not evidence that nothing changed. A tombstone on delete carries the
// object, but the token only has to move, so its shape is irrelevant there.
func ownerChangeHandler(changes *owners.Changes, note func()) cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(any) { note(); changes.Bump() },
		UpdateFunc: func(oldObj, newObj any) {
			// Stamped before the status-only shortcut: an update that bumps no
			// token is still the watch delivering. A resync is not (isResync).
			if !isResync(oldObj, newObj) {
				note()
			}
			o, okOld := oldObj.(*metav1.PartialObjectMetadata)
			n, okNew := newObj.(*metav1.PartialObjectMetadata)
			// Raw maps, not the filtered ones kubemeta.CopyMeta would produce:
			// running the annotation filter on every event to spot a change
			// only in an annotation that gets dropped anyway would cost more
			// than the spare bump it saves, and the spare bump is safe.
			if okOld && okNew &&
				o.UID == n.UID &&
				maps.Equal(o.Labels, n.Labels) &&
				maps.Equal(o.Annotations, n.Annotations) &&
				owners.SameServedRefs(o.OwnerReferences, n.OwnerReferences) {
				return
			}
			changes.Bump()
		},
		DeleteFunc: func(any) { note(); changes.Bump() },
	}
}

// watchErrors installs a watch-error handler that counts before delegating to
// client-go's default (which keeps the standard logging and the
// expired-resourceVersion handling).
//
// Readiness LATCHES: /readyz gates on the initial sync and is never
// re-evaluated. So a list/watch that breaks AFTER that — revoked RBAC, a
// deleted CRD, an apiserver rejecting the watch — leaves the reflector
// retrying forever while /readyz stays 200 and every response is served from a
// cache that has quietly stopped advancing. Nor do the store gauges freeze at
// plausible values: the tombstone sweeper keeps running over a store nothing
// refills, so kubescrape_store_pods DECAYS (measured 89 -> 85 over five
// minutes) and an alert on a FLAT gauge reads healthy exactly when it must not.
// Without this the only trace is a klog line, which is not alertable; the
// startup half of exactly this failure was already found and fixed once (the
// PodMonitor informer that 403-looped behind a green /readyz).
//
// It covers the refusals the API server ANSWERS, and a failed relist. It does
// NOT reliably cover an UNREACHABLE server — that is what the reachability
// probe in apiserver.go is for; obs.InformerWatchErrors carries the mechanism.
//
// Must be called before the informer is started.
func watchErrors(inf cache.SharedInformer, resource string) error {
	return inf.SetWatchErrorHandlerWithContext(func(ctx context.Context, r *cache.Reflector, err error) {
		obs.InformerWatchErrors.WithLabelValues(resource).Inc()
		cache.DefaultWatchErrorHandler(ctx, r, err)
	})
}
