package events

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// audit_test.go: targeted tests from the 2026-07 audit.

// The initial-list skip boundary: event timestamps have second granularity and
// e.start is truncated to the second, so an event stamped EXACTLY at the start
// second must be kept (a genuinely-new event created during the startup second
// truncates to that boundary). Strictly-older events are history and skipped.
func TestInitialListSkipBoundary(t *testing.T) {
	e := New(Config{Store: testStoreWithPod(t), Exporter: &capture{}})
	start := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	e.start = start

	e.enqueue(testEvent("AtBoundary", "Pod", "pod1", "pod-uid", start))
	if got := len(e.ch); got != 1 {
		t.Fatalf("event exactly at start: queued %d, want 1 (boundary must be inclusive)", got)
	}
	e.enqueue(testEvent("JustBefore", "Pod", "pod1", "pod-uid", start.Add(-time.Second)))
	if got := len(e.ch); got != 1 {
		t.Fatalf("pre-start event queued (len=%d); initial-list history must be skipped", got)
	}
	// An old creation time whose count-bump timestamp is fresh is a live
	// recurrence, not history: eventTime picks the newest timestamp.
	old := testEvent("OldButBumped", "Pod", "pod1", "pod-uid", start.Add(-time.Hour))
	old.LastTimestamp.Time = start.Add(time.Second)
	e.enqueue(old)
	if got := len(e.ch); got != 2 {
		t.Fatalf("recurrence of an old event not queued (len=%d)", got)
	}
}

// Two count bumps in quick succession re-emit twice — each bump is a distinct
// recurrence of the underlying condition, deliberately not deduped.
func TestCountBumpTwiceEmitsTwice(t *testing.T) {
	e := New(Config{Store: testStoreWithPod(t), Exporter: &capture{}})
	e.start = time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	when := e.start.Add(time.Minute)

	ev1 := testEvent("BackOff", "Pod", "pod1", "pod-uid", when)
	ev2 := testEvent("BackOff", "Pod", "pod1", "pod-uid", when)
	ev2.Count = 2
	ev3 := testEvent("BackOff", "Pod", "pod1", "pod-uid", when)
	ev3.Count = 3

	e.OnUpdate(ev1, ev2)
	e.OnUpdate(ev2, ev3)
	if got := len(e.ch); got != 2 {
		t.Fatalf("queued %d records for two count bumps, want 2", got)
	}
	// An update with no bump (resync: same count, same timestamps) is not
	// re-emitted.
	e.OnUpdate(ev3, ev3)
	if got := len(e.ch); got != 2 {
		t.Fatalf("no-op update re-emitted (len=%d)", got)
	}
}

// Regression guard: an event about a pod used to be attributed straight off
// the store record. The store never resolves the owner chain (that is the
// server's lazy per-request enrich), so np.Pod.Owners was always nil, with
// two consequences on EVERY pod-event resource:
//
//   - no k8s.deployment.name / k8s.replicaset.name / k8s.statefulset.name /
//     k8s.daemonset.name / k8s.job.name / k8s.cronjob.name, and no namespace
//     labels (NamespaceMetadata is nil for the same reason);
//   - attrs.ServiceName falls back to the POD name, so a pod's events land
//     under service.name = "web-abc-xyz" while that same pod's logs (tailer,
//     via metaclient, owners resolved) land under service.name = "web". The
//     events of a workload are therefore not joinable with its logs, and each
//     pod replica gets its own service.name (per-replica cardinality that
//     churns on every rollout).
//
// The resource also never went through attrs.Identity, so it had no
// service.namespace or service.instance.id either. The fix: Config carries an
// owners.Resolver and the exporter finishes each resource with attrs.Identity.
func TestEventPodResourceCarriesWorkloadAttrs(t *testing.T) {
	ctrl := true
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1", Namespace: "ns1", UID: "pod-uid", ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
			}},
		},
		Spec: corev1.PodSpec{NodeName: "node1"},
	})
	e := New(Config{Store: st, Exporter: &capture{}, Owners: stubOwners{}})
	ld := e.convert([]*corev1.Event{testEvent("BackOff", "Pod", "pod1", "pod-uid", time.Now())})

	a := ld.ResourceLogs().At(0).Resource().Attributes()
	if _, ok := a.Get("k8s.replicaset.name"); !ok {
		t.Error("pod-event resource has no k8s.replicaset.name: the owner chain is never resolved")
	}
	if v, _ := a.Get("service.name"); v.Str() == "pod1" {
		t.Errorf("service.name = %q (the pod name); the workload owner never resolves, so a pod's events "+
			"do not share service.name with that pod's logs", v.Str())
	}
	if _, ok := a.Get("service.instance.id"); !ok {
		t.Error("pod-event resource has no service.instance.id: attrs.Build/attrs.Identity is never called")
	}
}

// stubOwners resolves the ReplicaSet -> Deployment chain the way the real
// resolver does off the metadata informers.
type stubOwners struct{}

func (stubOwners) Resolve(_ string, refs []metav1.OwnerReference) []kubemeta.Owner {
	var out []kubemeta.Owner
	for _, r := range refs {
		out = append(out, kubemeta.Owner{Kind: r.Kind, Name: r.Name, UID: string(r.UID)})
		if r.Kind == "ReplicaSet" {
			out = append(out, kubemeta.Owner{Kind: "Deployment", Name: "web", UID: "deploy-uid"})
		}
	}
	return out
}

func (stubOwners) Namespace(string) *kubemeta.ObjectMeta {
	return &kubemeta.ObjectMeta{UID: "ns-uid", Labels: map[string]string{"team": "core"}}
}
