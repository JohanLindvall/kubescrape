package promscrape

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

// resourceAttrs flattens a resource for comparison.
func resourceAttrs(res pcommon.Resource) map[string]any { return res.Attributes().AsRaw() }

// FillContainerResource exists so internal/agent/cgroupstats' ten gauges JOIN
// the cadvisor series they explain, and joining is decided entirely by whether
// the two resources carry the same attributes — the OTLP→Prometheus translation
// derives `job` and `instance` from them. This is the assertion that says so:
// the resource the exported seam builds from a cgroup path must be
// byte-identical to the one a cadvisor row for the same container gets.
//
// It is not testing that one function calls another (they share a body). It is
// testing the thing a future refactor could break without noticing: that the
// identity a CGROUP PATH yields — container id and pod uid, and nothing else —
// is enough to reach the same resource as a cadvisor row that also carried
// namespace, pod and container LABELS.
func TestFillContainerResourceMatchesTheCadvisorRow(t *testing.T) {
	s := newKubeletScraper(t, "http://unused", &fakeMetaSource{}, &captureExporter{}, false)

	// What the cadvisor batcher builds for the app container's row, labels and
	// all.
	cadvisorRes := pcommon.NewResource()
	s.fillIdentityResource(context.Background(), cadvisorRes, cadvisorIdentity{
		namespace:   "ns1",
		pod:         "pod1",
		container:   "app",
		podUID:      uid1,
		containerID: appCID,
		image:       "img:1",
		hasCgroup:   true,
	})

	// What the cgroup sampler can know: the two ids in the path
	// /kubepods/burstable/pod<uid1>/<appCID>.
	sampled := pcommon.NewResource()
	if ok, _ := s.FillContainerResource(context.Background(), sampled, appCID, uid1); !ok {
		t.Fatal("a container the metadata service knows reported UNRESOLVED, so the sampler would never export it")
	}

	want, got := resourceAttrs(cadvisorRes), resourceAttrs(sampled)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("the sampler's resource differs from the cadvisor row's, so the series will not join:\ncadvisor: %v\nsampler:  %v", want, got)
	}
	// And it is not vacuously equal: the identity that decides the join has to
	// actually be in there.
	for _, k := range []string{"k8s.namespace.name", "k8s.pod.name", "k8s.container.name", "container.id", "service.name", "service.instance.id"} {
		if _, ok := got[k]; !ok {
			t.Errorf("%s is missing from the resolved resource: %v", k, got)
		}
	}
}

// A container the metadata service does not know is reported UNRESOLVED, and
// that verdict is the whole reason the seam returns a bool.
//
// A cadvisor row in the same position keeps its Prometheus `job`, because its
// LABELS carry a pod name that attrs.ServiceName can fall back to. A cgroup
// path carries no such thing — only two ids — so the resource below has no
// service.name at all: a series attributed to nothing, joining none of the
// cadvisor series the sampler exists to explain, and one successful later
// lookup away from silently becoming a different series. cgroupstats therefore
// declines to export it and retries; see cgroupstats.Resolver.
func TestFillContainerResourceReportsAnUnresolvableContainer(t *testing.T) {
	s := newKubeletScraper(t, "http://unused", &fakeMetaSource{}, &captureExporter{}, false)

	res := pcommon.NewResource()
	unknown := fmt.Sprintf("%064x", 0xdeadbeef)
	ok, answered := s.FillContainerResource(context.Background(), res, unknown, uid1)
	if ok {
		t.Fatal("a container id the metadata service does not know reported RESOLVED")
	}
	if !answered {
		t.Error("a 404 from the metadata service reported that it had not ANSWERED; the sampler would then keep the cgroup on the fast retry forever instead of giving up on it")
	}

	attrs := resourceAttrs(res)
	if attrs["container.id"] != unknown {
		t.Errorf("container.id = %v, want the cgroup's own id", attrs["container.id"])
	}
	if attrs["k8s.pod.uid"] != uid1 {
		t.Errorf("k8s.pod.uid = %v, want %q", attrs["k8s.pod.uid"], uid1)
	}
	if _, ok := attrs["service.name"]; ok {
		t.Errorf("the unresolved resource carries service.name %v — if a cgroup path could produce one, the sampler would not have to decline it", attrs["service.name"])
	}
}

// The pod SANDBOX, the specific unresolvable container that every pod has.
//
// cadvisorbatch.go spends ~80 lines excluding it from the scrape path
// (isSandbox), and it reaches the cgroup sampler through a completely different
// door: the pause container's cgroup is an ordinary child of the pod slice, so
// discovery finds it and cgroupid.Parse says "container". What keeps it out of
// the payload is that it does not RESOLVE — its id appears in no pod's
// containerStatuses — which is this assertion. (That the metadata service
// genuinely 404s such an id is proved against the real store, HTTP handler and
// metaclient by internal/server's TestSandboxContainerIDDoesNotResolve; here
// the point is that the scraper's seam passes the verdict on rather than
// inventing an identity from the pod uid it was also given.)
func TestFillContainerResourceDoesNotResolveASandboxID(t *testing.T) {
	s := newKubeletScraper(t, "http://unused", &fakeMetaSource{}, &captureExporter{}, false)

	res := pcommon.NewResource()
	// pauseCID is the sandbox of the very pod uid1 names, and that pod IS
	// resolvable by name — the temptation to fall back to it is exactly what
	// would resurrect the phantom per-pod resource.
	ok, answered := s.FillContainerResource(context.Background(), res, pauseCID, uid1)
	if ok {
		t.Fatalf("the pod sandbox resolved, so every pod would mint a second resource carrying the pause container's numbers: %v", resourceAttrs(res))
	}
	// And the refusal is DEFINITIVE, which is what puts the sandbox on the slow
	// retry: the service answered, and its answer will be the same forever. A
	// sandbox reported as "could not ask" would be retried at the fast cadence
	// for the life of the node, once per pod.
	if !answered {
		t.Error("the sandbox's 404 was classified as an unanswered lookup")
	}
}

// The failure CLASSIFICATION, against the real metaclient and a real HTTP
// server rather than a fake that was told which answer to give.
//
// internal/agent/cgroupstats retries a cgroup rarely when the metadata service
// says "I do not know this id" (every pod's sandbox is that, forever) and
// promptly when the service could not answer at all. That policy is only as
// good as this seam's ability to tell the two apart, and what it rests on is a
// typed error surviving the whole path: the service's status code →
// metaclient.StatusError → metaclient.IsNotFound → containerMeta → here. A
// refactor that flattened any hop into a bare error would turn every outage
// into a fleet-wide abandonment, silently, and this is what would fail instead.
func TestFillContainerResourceDistinguishesA404FromAnUnreachableService(t *testing.T) {
	var status atomic.Int64
	status.Store(http.StatusNotFound)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", int(status.Load()))
	}))
	defer srv.Close()
	mc := metaclient.New(metaclient.Config{Base: srv.URL, Timeout: 5 * time.Second})
	s := newKubeletScraper(t, "http://unused", mc, &captureExporter{}, false)

	// A 404: the service ANSWERED, and its answer is definitive.
	if ok, answered := s.FillContainerResource(context.Background(), pcommon.NewResource(), fmt.Sprintf("%064x", 1), uid1); ok || !answered {
		t.Errorf("a 404 reported (ok=%v, answered=%v), want (false, true): an unplaceable container the service KNOWS nothing about must be able to be given up on", ok, answered)
	}

	// A 503: the service could not answer, and nothing about this container has
	// been learned.
	status.Store(http.StatusServiceUnavailable)
	id503 := fmt.Sprintf("%064x", 2)
	if ok, answered := s.FillContainerResource(context.Background(), pcommon.NewResource(), id503, uid1); ok || answered {
		t.Errorf("a 503 reported (ok=%v, answered=%v), want (false, false): a service that cannot answer must not produce a verdict about a container", ok, answered)
	}
	// And the negative CACHE keeps the classification. It is served for a
	// minute; reconstructing "definitive" from a cached nil would turn one
	// unreachable minute into a definitive verdict for every lookup inside it,
	// which is the abandonment the classification exists to prevent.
	if ok, answered := s.FillContainerResource(context.Background(), pcommon.NewResource(), id503, uid1); ok || answered {
		t.Errorf("the cached 503 reported (ok=%v, answered=%v), want (false, false)", ok, answered)
	}

	// And a service that is not there at all.
	srv.Close()
	if ok, answered := s.FillContainerResource(context.Background(), pcommon.NewResource(), fmt.Sprintf("%064x", 3), uid1); ok || answered {
		t.Errorf("an unreachable metadata service reported (ok=%v, answered=%v), want (false, false)", ok, answered)
	}
}

// The cgroup sampler rebuilds every container's resource on EVERY export
// rather than caching it, because a cached one diverges from the cadvisor
// resource it has to join the moment a pod or namespace label is edited. That
// design rests on a cost claim — "a per-export resolve per container is
// essentially free, because it is the same lookup through the same cache the
// cadvisor scrape already fills, and a miss underneath it is a 304" — and a
// cost claim nobody measured is how the caching went in in the first place.
//
// So: measured, against a real metaclient and a real HTTP server.
func TestFillContainerResourceIsA304InTheSteadyState(t *testing.T) {
	const etag = `"v1"`
	md := kubemeta.ContainerMetadata{
		ContainerID: appCID,
		Container:   kubemeta.Container{Name: "app", Image: "img:1"},
		Pod:         kubemeta.Pod{Name: "pod1", Namespace: "ns1", UID: uid1, NodeName: "node1"},
	}
	body, err := json.Marshal(md)
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var requests, conditional, bodies int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		revalidating := r.Header.Get("If-None-Match") == etag
		if revalidating {
			conditional++
		} else {
			bodies++
		}
		mu.Unlock()
		w.Header().Set("ETag", etag)
		// The metadata routes stamp Cache-Control from -metadata-cache-ttl.
		// One second here only so the test can outlive it without sleeping for
		// the ten the service defaults to.
		w.Header().Set("Cache-Control", "max-age=1")
		if revalidating {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	// ONE metaclient, as the agent has: it is the thing that holds the ETag.
	mc := metaclient.New(metaclient.Config{Base: srv.URL, Timeout: 5 * time.Second})

	// Twenty exports' worth of rebuilds inside one minute. promscrape's own
	// TTL cache — the same entries, under the same keys, that a cadvisor row
	// for this container fills — absorbs all but the first.
	s := newKubeletScraper(t, "http://unused", mc, &captureExporter{}, false)
	for range 20 {
		if ok, _ := s.FillContainerResource(context.Background(), pcommon.NewResource(), appCID, uid1); !ok {
			t.Fatal("the container did not resolve")
		}
	}
	mu.Lock()
	got, gotBodies := requests, bodies
	mu.Unlock()
	if got != 1 || gotBodies != 1 {
		t.Fatalf("20 per-export rebuilds cost %d requests (%d with a body), want exactly 1: the resolver's own cache is what makes rebuilding per export affordable", got, gotBodies)
	}

	// Past that cache — a fresh Scraper has an empty one, which is what the
	// agent looks like once a minute — the lookup reaches metaclient, whose
	// entry is now stale but still holds the ETag. The one wall-clock wait in
	// this test is that staleness: metaclient's clock is injectable only from
	// inside its own package, and the claim under test is specifically that a
	// STALE entry costs a conditional GET rather than a body.
	time.Sleep(1100 * time.Millisecond)
	s2 := newKubeletScraper(t, "http://unused", mc, &captureExporter{}, false)
	if ok, _ := s2.FillContainerResource(context.Background(), pcommon.NewResource(), appCID, uid1); !ok {
		t.Fatal("the container did not resolve after the cache expired")
	}
	mu.Lock()
	got, gotBodies, gotCond := requests, bodies, conditional
	mu.Unlock()
	if got != 2 || gotCond != 1 {
		t.Errorf("the expired-cache rebuild made %d requests total with %d conditional, want 2 and 1", got, gotCond)
	}
	if gotBodies != 1 {
		t.Errorf("the metadata service sent %d bodies, want 1: a stale entry must revalidate with If-None-Match and be answered 304, not re-fetched whole", gotBodies)
	}
}
