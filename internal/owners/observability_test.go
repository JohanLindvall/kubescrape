package owners

import (
	"bytes"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// This package had NO observability at all — no metric, no log line, at any
// level — while every one of its failure paths answers by returning nil and
// letting the caller serve a bare owner reference. That response is
// well-formed, so a missing RBAC rule or an informer that never synced
// degraded service.name (and half the Prometheus job) for every pod under the
// owner, with nothing anywhere to say so.

// errLister is a GenericLister whose Get always fails, standing in for a cache
// that cannot answer (the shape a revoked RBAC rule takes: the informer's LIST
// 403-loops and the lister errors).
type errLister struct{ err error }

func (l errLister) List(labels.Selector) ([]runtime.Object, error) { return nil, l.err }
func (l errLister) Get(string) (runtime.Object, error)             { return nil, l.err }
func (l errLister) ByNamespace(string) cache.GenericNamespaceLister {
	return errNSLister(l)
}

type errNSLister struct{ err error }

func (l errNSLister) List(labels.Selector) ([]runtime.Object, error) { return nil, l.err }
func (l errNSLister) Get(string) (runtime.Object, error)             { return nil, l.err }

// capture builds a Debug-level logger over a buffer, so a test can assert on
// the Debug lines too — the level most of these failures log at.
func capture(t *testing.T) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// withLister builds a Resolver over one fake lister, with the log captured.
func withLister(t *testing.T, gvr schema.GroupVersionResource, l cache.GenericLister) (*Resolver, *bytes.Buffer) {
	t.Helper()
	r := NewFromListers(map[schema.GroupVersionResource]cache.GenericLister{gvr: l})
	log, buf := capture(t)
	r.log = log
	return r, buf
}

func counter(kind, reason string) float64 {
	return obs.OwnerResolveFailures.WithLabelValues(kind, reason).Value()
}

// A cache that ERRORS (not "does not have it") is the RBAC-shaped case: it must
// warn, not merely tick a Debug line, because nobody meant it to happen.
func TestListerErrorIsCountedAndWarned(t *testing.T) {
	before := counter("ReplicaSet", reasonListerError)
	r, buf := withLister(t, ReplicaSetGVR, errLister{errors.New("forbidden: replicasets is forbidden")})

	ctrl := true
	got, _ := r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
	}})
	if len(got) != 1 || got[0].Labels != nil {
		t.Fatalf("resolve = %+v, want the bare reference (the failure must not change the answer)", got)
	}
	if delta := counter("ReplicaSet", reasonListerError) - before; delta != 1 {
		t.Fatalf("lister_error counter moved by %v, want 1", delta)
	}
	line := buf.String()
	if !strings.Contains(line, "level=WARN") {
		t.Fatalf("a cache error must WARN, got %q", line)
	}
	for _, want := range []string{"kind=ReplicaSet", "reason=lister_error", "name=web-abc", "namespace=default"} {
		if !strings.Contains(line, want) {
			t.Fatalf("log line %q is missing %q", line, want)
		}
	}
}

// NotFound is the one ordinary failure: counted under its own reason and logged
// at Debug, so a cluster with tombstoned owners does not warn all day.
func TestNotFoundIsCountedAtDebug(t *testing.T) {
	before := counter("ReplicaSet", reasonNotFound)
	notFound := apierrors.NewNotFound(ReplicaSetGVR.GroupResource(), "web-abc")
	r, buf := withLister(t, ReplicaSetGVR, errLister{notFound})

	ctrl := true
	r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
	}})
	if delta := counter("ReplicaSet", reasonNotFound) - before; delta != 1 {
		t.Fatalf("not_found counter moved by %v, want 1", delta)
	}
	if line := buf.String(); !strings.Contains(line, "level=DEBUG") || !strings.Contains(line, "reason=not_found") {
		t.Fatalf("not_found should log at Debug, got %q", line)
	}
	if strings.Contains(buf.String(), "level=WARN") {
		t.Fatalf("not_found must not warn: %q", buf.String())
	}
}

// A kind in AllGVRs with no informer wired for it is a wiring bug that strips
// every pod of that owner's labels; it warns.
func TestMissingInformerIsCountedAndWarned(t *testing.T) {
	before := counter("Job", reasonNoInformer)
	r, buf := withLister(t, ReplicaSetGVR, errLister{errors.New("unused")})

	ctrl := true
	r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "batch/v1", Kind: "Job", Name: "j1", UID: "j-uid", Controller: &ctrl,
	}})
	if delta := counter("Job", reasonNoInformer) - before; delta != 1 {
		t.Fatalf("no_informer counter moved by %v, want 1", delta)
	}
	if line := buf.String(); !strings.Contains(line, "level=WARN") || !strings.Contains(line, "reason=no_informer") {
		t.Fatalf("no_informer should warn, got %q", line)
	}
}

// An informer built for the wrong type answers with an object this package
// cannot read. Also a wiring bug, also a warning.
func TestWrongCachedTypeIsCountedAndWarned(t *testing.T) {
	before := counter("Namespace", reasonWrongType)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	// A Pod, not a PartialObjectMetadata: exactly what a metadata informer
	// wired from the typed factory by mistake would cache.
	if err := indexer.Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "prod"}}); err != nil {
		t.Fatal(err)
	}
	r, buf := withLister(t, NamespaceGVR, cache.NewGenericLister(indexer, NamespaceGVR.GroupResource()))

	if meta := r.Namespace("prod"); meta != nil {
		t.Fatalf("namespace = %+v, want nil", meta)
	}
	if delta := counter("Namespace", reasonWrongType) - before; delta != 1 {
		t.Fatalf("wrong_type counter moved by %v, want 1", delta)
	}
	if line := buf.String(); !strings.Contains(line, "level=WARN") || !strings.Contains(line, "reason=wrong_type") {
		t.Fatalf("wrong_type should warn, got %q", line)
	}
}

// An unparseable apiVersion can never match a watched kind. It must be counted
// exactly ONCE per reference: Resolve reaches ownerKind twice (kindGVR and
// followable), and a report in the shared helper would double every occurrence.
func TestBadAPIVersionIsCountedOnce(t *testing.T) {
	before := counter("ReplicaSet", reasonBadAPIVersion)
	r, buf := withLister(t, ReplicaSetGVR, errLister{errors.New("unused")})

	ctrl := true
	got, _ := r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1/beta", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
	}})
	if len(got) != 1 {
		t.Fatalf("resolve = %+v, want the bare reference", got)
	}
	if delta := counter("ReplicaSet", reasonBadAPIVersion) - before; delta != 1 {
		t.Fatalf("bad_api_version counter moved by %v, want exactly 1", delta)
	}
	if line := buf.String(); !strings.Contains(line, "reason=bad_api_version") {
		t.Fatalf("log line %q missing the reason", line)
	}
}

// A Kind nobody watches must not mint a label value: ownerReferences name
// arbitrary CRDs, and the metric's cardinality has to stay a property of
// ownerKinds.
func TestBadAPIVersionOfAnUnwatchedKindIsLabelledUnknown(t *testing.T) {
	before := counter("unknown", reasonBadAPIVersion)
	r, _ := withLister(t, ReplicaSetGVR, errLister{errors.New("unused")})

	ctrl := true
	r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "argoproj.io/v1alpha1/x", Kind: "Rollout", Name: "r1", UID: "r-uid", Controller: &ctrl,
	}})
	if delta := counter("unknown", reasonBadAPIVersion) - before; delta != 1 {
		t.Fatalf("unknown-kind counter moved by %v, want 1", delta)
	}
	if got := counter("Rollout", reasonBadAPIVersion); got != 0 {
		t.Fatalf("the reference's own Kind became a label value (%v); cardinality is unbounded", got)
	}
}

// The UID cross-check REFUSES a read that succeeded — the owner was recreated
// under its old name — and the pod loses its owner's labels either way. It is a
// different story from a miss, so it has its own reason.
func TestUIDMismatchIsCounted(t *testing.T) {
	before := counter("ReplicaSet", reasonUIDMismatch)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	rs := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{
		Name: "web-abc", Namespace: "default", UID: "NEW-uid", Labels: map[string]string{"gen": "2"},
	}}
	if err := indexer.Add(rs); err != nil {
		t.Fatal(err)
	}
	r, buf := withLister(t, ReplicaSetGVR, cache.NewGenericLister(indexer, ReplicaSetGVR.GroupResource()))

	ctrl := true
	got, _ := r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "OLD-uid", Controller: &ctrl,
	}})
	if len(got) != 1 || got[0].Labels != nil {
		t.Fatalf("resolve = %+v, want the bare reference with no borrowed labels", got)
	}
	if delta := counter("ReplicaSet", reasonUIDMismatch) - before; delta != 1 {
		t.Fatalf("uid_mismatch counter moved by %v, want 1", delta)
	}
	if line := buf.String(); !strings.Contains(line, "reason=uid_mismatch") {
		t.Fatalf("log line %q missing the reason", line)
	}
}

// A successful resolve reports nothing: this runs once per owner reference per
// pod per targets request, on the route every agent polls each cycle.
func TestSuccessfulResolveIsSilent(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	rs := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{
		Name: "web-abc", Namespace: "default", UID: "rs-uid",
	}}
	if err := indexer.Add(rs); err != nil {
		t.Fatal(err)
	}
	r, buf := withLister(t, ReplicaSetGVR, cache.NewGenericLister(indexer, ReplicaSetGVR.GroupResource()))

	ctrl := true
	r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
	}})
	if buf.Len() != 0 {
		t.Fatalf("a successful resolve logged %q", buf.String())
	}
}

// The throttle is what keeps a fleet-wide condition to one line per object per
// window; without it every agent's every poll re-logs the same failure.
func TestRepeatedFailureLogsOnceButCountsEveryTime(t *testing.T) {
	before := counter("ReplicaSet", reasonListerError)
	r, buf := withLister(t, ReplicaSetGVR, errLister{errors.New("forbidden")})

	ctrl := true
	ref := metav1.OwnerReference{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
	}
	for range 5 {
		r.Resolve("default", []metav1.OwnerReference{ref})
	}
	if delta := counter("ReplicaSet", reasonListerError) - before; delta != 5 {
		t.Fatalf("counter moved by %v, want 5 (the counter carries the rate)", delta)
	}
	if n := strings.Count(buf.String(), "reading an owner's cached metadata failed"); n != 1 {
		t.Fatalf("logged %d times, want 1 (the throttle window is %v)", n, resolveWarnEvery)
	}
}

// A Resolver built without the constructor (the tests' hand-written get) still
// counts; it just cannot log. Nothing may panic on the nil table.
func TestResolverWithoutThrottleStillCounts(t *testing.T) {
	before := counter("Job", reasonNoInformer)
	r := &Resolver{get: func(schema.GroupVersionResource, string, string) *metav1.PartialObjectMetadata { return nil }}
	r.report(JobGVR, "default", "j1", reasonNoInformer, nil)
	if delta := counter("Job", reasonNoInformer) - before; delta != 1 {
		t.Fatalf("counter moved by %v, want 1", delta)
	}
}

// A Debug line the handler is going to DROP must not spend a throttle slot:
// the table is bounded and saturation suppresses permanently, so slots burned
// at Info would silently disable the reporting the moment someone raises the
// level to look.
func TestInfoLevelDoesNotSpendADebugThrottleSlot(t *testing.T) {
	notFound := apierrors.NewNotFound(ReplicaSetGVR.GroupResource(), "web-abc")
	r, _ := withLister(t, ReplicaSetGVR, errLister{notFound})
	infoBuf := &bytes.Buffer{}
	r.log = slog.New(slog.NewTextHandler(infoBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctrl := true
	ref := metav1.OwnerReference{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
	}
	r.Resolve("default", []metav1.OwnerReference{ref})
	if infoBuf.Len() != 0 {
		t.Fatalf("Info level emitted a Debug-level condition: %q", infoBuf.String())
	}

	debugLog, debugBuf := capture(t)
	r.log = debugLog
	r.Resolve("default", []metav1.OwnerReference{ref})
	if !strings.Contains(debugBuf.String(), "reason=not_found") {
		t.Fatalf("the Info-level pass consumed this object's throttle slot; raising the level reports nothing: %q",
			debugBuf.String())
	}
}

// The RBAC-shaped warning must not be starved by ordinary not_found chatter.
// They are independent conditions, so they get independent tables: one shared
// table plus logdedupe's suppress-never-clear rule would make the starvation
// permanent, and silent.
func TestOrdinaryMissesCannotStarveTheRBACWarning(t *testing.T) {
	notFound := apierrors.NewNotFound(ReplicaSetGVR.GroupResource(), "x")
	r, buf := withLister(t, ReplicaSetGVR, errLister{notFound})

	ctrl := true
	// Saturate the DEBUG table with distinct objects, well past its cap.
	for i := range maxResolveWarnKeys + 10 {
		r.Resolve("default", []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "ReplicaSet",
			Name: "rs-" + strconv.Itoa(i), UID: types.UID("uid-" + strconv.Itoa(i)), Controller: &ctrl,
		}})
	}

	// Now the condition an operator actually needs to see.
	r.get = func(schema.GroupVersionResource, string, string) *metav1.PartialObjectMetadata {
		r.report(ReplicaSetGVR, "default", "critical", reasonListerError, errors.New("forbidden"))
		return nil
	}
	r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "critical", UID: "c-uid", Controller: &ctrl,
	}})

	if !strings.Contains(buf.String(), "reading an owner's cached metadata failed") ||
		!strings.Contains(buf.String(), "name=critical") {
		t.Fatal("a saturated not_found table suppressed the RBAC-shaped warning")
	}
}
