package attrs

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/owners"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// TestKindTableCoversOwnerResolver pins kindTable against the resolver's
// informer list: a kind added to internal/owners (a new metadata informer /
// owner-chain entry) without a row in kindTable would resolve owners whose
// k8s.<kind>.name attribute is then silently dropped from every resource.
func TestKindTableCoversOwnerResolver(t *testing.T) {
	// GVR resource (plural) -> owner kind; "" marks a resolver GVR that is not
	// an owner kind (Namespace backs namespace METADATA, not the owner chain).
	kindByResource := map[string]string{
		"replicasets":  "ReplicaSet",
		"deployments":  "Deployment",
		"statefulsets": "StatefulSet",
		"daemonsets":   "DaemonSet",
		"jobs":         "Job",
		"cronjobs":     "CronJob",
		"nodes":        "Node",
		"namespaces":   "",
	}
	known := make(map[string]bool)
	for _, gvr := range owners.AllGVRs {
		kind, listed := kindByResource[gvr.Resource]
		if !listed {
			t.Errorf("owners.AllGVRs gained %q: add its kind to kindTable in internal/agent/attrs/attrs.go (and to this map)", gvr.Resource)
			continue
		}
		if kind == "" {
			continue
		}
		known[kind] = true
		if _, ok := KindAttribute(kind); !ok {
			t.Errorf("owner kind %q (owners.AllGVRs %q) has no kindTable row in internal/agent/attrs/attrs.go", kind, gvr.Resource)
		}
	}
	// The reverse: no dead rows describing kinds the resolver never caches.
	for kind := range kindTable {
		if !known[kind] {
			t.Errorf("kindTable kind %q has no owners.AllGVRs informer backing it", kind)
		}
	}
}

func TestPrefixInstance(t *testing.T) {
	// Prepend to an existing instance.
	res := pcommon.NewResource()
	res.Attributes().PutStr("service.instance.id", "cid")
	PrefixInstance(res, "cadvisor")
	if v, _ := res.Attributes().Get("service.instance.id"); v.Str() != "cadvisor-cid" {
		t.Errorf("prefix over existing = %q, want cadvisor-cid", v.Str())
	}
	// No instance derived: the bare prefix must NOT be stamped (a shared
	// meaningless instance is worse than none).
	res = pcommon.NewResource()
	PrefixInstance(res, "cadvisor")
	if v, ok := res.Attributes().Get("service.instance.id"); ok {
		t.Errorf("bare prefix stamped = %q, want unset", v.Str())
	}
	// Empty prefix is a no-op.
	res = pcommon.NewResource()
	res.Attributes().PutStr("service.instance.id", "x")
	PrefixInstance(res, "")
	if v, _ := res.Attributes().Get("service.instance.id"); v.Str() != "x" {
		t.Errorf("empty prefix changed instance to %q", v.Str())
	}
}

func TestPodIPAndServiceName(t *testing.T) {
	res := pcommon.NewResource()
	Pod(res, kubemeta.Pod{
		Name: "p", Namespace: "ns", UID: "u", PodIP: "10.0.0.1",
		Owners: []kubemeta.Owner{{Kind: "ReplicaSet", Name: "rs"}, {Kind: "Deployment", Name: "dep"}},
	})
	a := res.Attributes()
	if v, _ := a.Get("k8s.pod.ip"); v.Str() != "10.0.0.1" {
		t.Errorf("k8s.pod.ip = %q, want 10.0.0.1", v.Str())
	}
	if v, _ := a.Get("service.name"); v.Str() != "dep" {
		t.Errorf("service.name = %q, want dep (owner)", v.Str())
	}
	// No PodIP -> attribute omitted.
	res = pcommon.NewResource()
	Pod(res, kubemeta.Pod{Name: "p", Namespace: "ns", UID: "u"})
	if _, ok := res.Attributes().Get("k8s.pod.ip"); ok {
		t.Error("k8s.pod.ip set despite empty PodIP")
	}
}

// A pod with many labels takes the bulk path (putLabelsBulk), and it must stamp
// exactly what the per-label PutStr loop stamps: every label, prefixed; a label
// OVERWRITING a same-named attribute already on the resource; and every other
// pre-existing attribute keeping its type and value (the bulk path round-trips
// the whole map through AsRaw/FromRaw).
func TestPodBulkLabelPathMatchesThePerLabelPath(t *testing.T) {
	labels := func(prefix string, n int) map[string]string {
		m := make(map[string]string, n)
		for i := range n {
			m[fmt.Sprintf("%s%d", prefix, i)] = fmt.Sprintf("v%d", i)
		}
		return m
	}
	pod := kubemeta.Pod{
		Name: "p", Namespace: "ns", UID: "u",
		Labels:            labels("app", bulkLabelThreshold),
		NamespaceMetadata: &kubemeta.ObjectMeta{Labels: labels("team", 8)},
	}
	if len(pod.Labels)+len(pod.NamespaceMetadata.Labels) <= bulkLabelThreshold {
		t.Fatal("the fixture does not reach the bulk path")
	}
	build := func(p kubemeta.Pod) pcommon.Map {
		res := pcommon.NewResource()
		a := res.Attributes()
		// Pre-existing attributes, as a splitter pre-puts them: a non-string
		// one, a nested one, and one a label is about to overwrite.
		a.PutInt("preint", 7)
		a.PutEmptyMap("premap").PutStr("inner", "x")
		a.PutEmptyBytes("prebytes").FromRaw([]byte{1, 2, 3})
		a.PutStr(podLabelPrefix+"app3", "stale")
		Pod(res, p)
		return a
	}
	got := build(pod)

	// The oracle: the per-label loop, reached by splitting the same labels
	// into calls that each stay under the threshold.
	want := pcommon.NewMap()
	build(kubemeta.Pod{Name: "p", Namespace: "ns", UID: "u"}).CopyTo(want)
	for k, v := range pod.Labels {
		want.PutStr(podLabelPrefix+k, v)
	}
	for k, v := range pod.NamespaceMetadata.Labels {
		want.PutStr(nsLabelPrefix+k, v)
	}
	if got.Len() != want.Len() {
		t.Fatalf("bulk path produced %d attributes, want %d", got.Len(), want.Len())
	}
	for k, wv := range want.All() {
		gv, ok := got.Get(k)
		if !ok {
			t.Fatalf("%s missing from the bulk path's result", k)
		}
		if !gv.Equal(wv) {
			t.Fatalf("%s = %v (%s), want %v (%s)", k, gv.AsRaw(), gv.Type(), wv.AsRaw(), wv.Type())
		}
	}
	if v, _ := got.Get(podLabelPrefix + "app3"); v.Str() != "v3" {
		t.Fatalf("a pre-existing %sapp3 was not overwritten by the label (got %q)", podLabelPrefix, v.Str())
	}
	if v, _ := got.Get("preint"); v.Type() != pcommon.ValueTypeInt || v.Int() != 7 {
		t.Fatalf("a pre-existing Int attribute did not survive the bulk path: %v (%s)", v.AsRaw(), v.Type())
	}
}

// The bulk path exists because the per-label PutStr loop is QUADRATIC in a
// tenant-controlled label count (each PutStr scans the map), and Build runs per
// resource per scrape cycle: 40k labels took ~7s a cycle. Coarse on purpose — a
// linear path does 40k labels in tens of milliseconds, the quadratic one in
// seconds. The ceiling sits well below the quadratic figure and ~50x above the
// linear one, because this runs beside the rest of the suite on a loaded
// machine and a load factor inflates both paths alike.
func TestPodWithManyLabelsIsLinear(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector multiplies every map operation's cost")
	}
	const n = 40_000
	pod := kubemeta.Pod{Name: "p", Namespace: "ns", UID: "u", Labels: make(map[string]string, n)}
	for i := range n {
		pod.Labels[fmt.Sprintf("label-%d", i)] = "v"
	}
	start := time.Now()
	res := pcommon.NewResource()
	Pod(res, pod)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("stamping %d labels took %v: the label write is quadratic again", n, d)
	}
	if got := res.Attributes().Len(); got < n {
		t.Fatalf("stamped %d attributes, want at least %d", got, n)
	}
}

// A partially-filled Pod must not mint empty-string resource attributes:
// every field is guarded, not just NodeName/PodIP.
func TestPodEmptyFieldsOmitted(t *testing.T) {
	res := pcommon.NewResource()
	Pod(res, kubemeta.Pod{UID: "u"})
	got := res.Attributes().AsRaw()
	if got["k8s.pod.uid"] != "u" {
		t.Errorf("k8s.pod.uid = %v, want u", got["k8s.pod.uid"])
	}
	for _, absent := range []string{"k8s.namespace.name", "k8s.pod.name", "service.name"} {
		if v, ok := got[absent]; ok {
			t.Errorf("%s = %q set from an empty field; must be omitted", absent, v)
		}
	}
	// The zero Pod yields no attributes at all.
	res = pcommon.NewResource()
	Pod(res, kubemeta.Pod{})
	if n := res.Attributes().Len(); n != 0 {
		t.Errorf("zero Pod produced %d attributes: %v", n, res.Attributes().AsRaw())
	}
}

// The same rule as TestPodEmptyFieldsOmitted, for the two functions that
// stamp on top of Pod's attributes, and for Pod's reason: an empty
// k8s.container.name or k8s.service.uid participates in series identity and
// log-metric label sets as if it were a value.
func TestContainerAndServiceEmptyFieldsOmitted(t *testing.T) {
	res := pcommon.NewResource()
	Container(res, kubemeta.Container{ID: "cid"})
	got := res.Attributes().AsRaw()
	if got["container.id"] != "cid" {
		t.Errorf("container.id = %v, want cid", got["container.id"])
	}
	if v, ok := got["k8s.container.name"]; ok {
		t.Errorf("k8s.container.name = %q set from an empty field; must be omitted", v)
	}

	res = pcommon.NewResource()
	Container(res, kubemeta.Container{})
	if n := res.Attributes().Len(); n != 0 {
		t.Errorf("zero Container produced %d attributes: %v", n, res.Attributes().AsRaw())
	}

	res = pcommon.NewResource()
	Service(res, &kubemeta.Service{Name: "checkout"})
	got = res.Attributes().AsRaw()
	if got["k8s.service.name"] != "checkout" {
		t.Errorf("k8s.service.name = %v, want checkout", got["k8s.service.name"])
	}
	if v, ok := got["k8s.service.uid"]; ok {
		t.Errorf("k8s.service.uid = %q set from an empty field; must be omitted", v)
	}

	res = pcommon.NewResource()
	Service(res, &kubemeta.Service{})
	if n := res.Attributes().Len(); n != 0 {
		t.Errorf("zero Service produced %d attributes: %v", n, res.Attributes().AsRaw())
	}
}

// ReservedIdentity is the boundary the tailer's pod-annotation filter (and any
// other workload/line-supplied attribute path) consults; the exact key set is
// load-bearing for those consumers, so pin it.
func TestReservedIdentity(t *testing.T) {
	want := []string{
		"container.id",
		"container.name",
		"k8s.container.name",
		"k8s.namespace.name",
		"k8s.node.name",
		"k8s.pod.ip",
		"k8s.pod.name",
		"k8s.pod.uid",
		"service.instance.id",
		"service.namespace",
	}
	if got := ReservedIdentityKeys(); !slices.Equal(got, want) {
		t.Errorf("ReservedIdentityKeys() = %v, want %v", got, want)
	}
	for _, k := range want {
		if !ReservedIdentity(k) {
			t.Errorf("ReservedIdentity(%q) = false, want true", k)
		}
	}
	// service.name is deliberately NOT reserved: descriptive, and overriding
	// it is the pod annotation's documented purpose.
	for _, k := range []string{"service.name", "k8s.cluster.name", ""} {
		if ReservedIdentity(k) {
			t.Errorf("ReservedIdentity(%q) = true, want false", k)
		}
	}
}

func TestIdentity(t *testing.T) {
	inst := func(seed map[string]string) string {
		res := pcommon.NewResource()
		for k, v := range seed {
			res.Attributes().PutStr(k, v)
		}
		Identity(res)
		id, _ := res.Attributes().Get("service.instance.id")
		return id.Str()
	}
	cases := []struct {
		name string
		seed map[string]string
		want string
	}{
		{"container.id wins", map[string]string{"container.id": "abc", "k8s.pod.uid": "u", "k8s.container.name": "c"}, "abc"},
		{"pod.uid + container", map[string]string{"k8s.pod.uid": "u", "k8s.container.name": "c"}, "u/c"},
		{"pod.uid alone", map[string]string{"k8s.pod.uid": "u"}, "u"},
		{"namespace/pod/container", map[string]string{"k8s.namespace.name": "ns", "k8s.pod.name": "p", "k8s.container.name": "c"}, "ns/p/c"},
		{"namespace/pod", map[string]string{"k8s.namespace.name": "ns", "k8s.pod.name": "p"}, "ns/p"},
		{"node fallback", map[string]string{"k8s.node.name": "n1"}, "n1"},
	}
	for _, c := range cases {
		if got := inst(c.seed); got != c.want {
			t.Errorf("%s: service.instance.id = %q, want %q", c.name, got, c.want)
		}
	}

	// service.namespace derived from the k8s namespace.
	res := pcommon.NewResource()
	res.Attributes().PutStr("k8s.namespace.name", "ns")
	Identity(res)
	if v, _ := res.Attributes().Get("service.namespace"); v.Str() != "ns" {
		t.Errorf("service.namespace = %q, want ns", v.Str())
	}
	// An explicit service.instance.id is not overwritten.
	res2 := pcommon.NewResource()
	res2.Attributes().PutStr("k8s.pod.uid", "u")
	res2.Attributes().PutStr("service.instance.id", "preset")
	Identity(res2)
	if v, _ := res2.Attributes().Get("service.instance.id"); v.Str() != "preset" {
		t.Errorf("preset instance overwritten: %q", v.Str())
	}
}

func TestServiceAttrs(t *testing.T) {
	res := pcommon.NewResource()
	Service(res, &kubemeta.Service{Name: "web-svc", UID: "svc-uid"})
	a := res.Attributes()
	if v, _ := a.Get("k8s.service.name"); v.Str() != "web-svc" {
		t.Errorf("k8s.service.name = %q", v.Str())
	}
	if v, _ := a.Get("k8s.service.uid"); v.Str() != "svc-uid" {
		t.Errorf("k8s.service.uid = %q", v.Str())
	}
	// nil Service is a no-op.
	res2 := pcommon.NewResource()
	Service(res2, nil)
	if res2.Attributes().Len() != 0 {
		t.Error("nil service must not set attributes")
	}
}

// FillAbsent is the shared "someone else knows more about this resource" merge
// (ingest enrichment, self-metadata stamping): it adds, never overwrites, and
// carries non-string values across intact.
func TestFillAbsent(t *testing.T) {
	src, dst := pcommon.NewMap(), pcommon.NewMap()
	src.PutStr("a", "from-src")
	src.PutStr("b", "added")
	src.PutInt("n", 7)
	dst.PutStr("a", "kept")

	FillAbsent(src, dst)
	if v, _ := dst.Get("a"); v.AsString() != "kept" {
		t.Errorf("a = %q; an existing key must not be overwritten", v.AsString())
	}
	if v, _ := dst.Get("b"); v.AsString() != "added" {
		t.Errorf("b = %q; want added", v.AsString())
	}
	if v, ok := dst.Get("n"); !ok || v.Int() != 7 {
		t.Errorf("n = %v; non-string values must survive the copy", v)
	}
}

// mergeLoop is Merge's per-key path, the definition the bulk path must agree
// with: add what dst lacks, replace what replace names.
func mergeLoop(src, dst pcommon.Map, replace func(string) bool) {
	src.Range(func(k string, v pcommon.Value) bool {
		if _, exists := dst.Get(k); !exists || (replace != nil && replace(k)) {
			v.CopyTo(dst.PutEmpty(k))
		}
		return true
	})
}

// A source past bulkLabelThreshold takes the one-pass rebuild, and it must
// write exactly what the per-key loop writes: every absent key added, a
// present key replaced only when replace says so, and every untouched dst
// attribute keeping its type and value — structured ones included, since the
// bulk path MOVES them rather than round-tripping them through AsRaw.
func TestMergeBulkPathMatchesTheLoop(t *testing.T) {
	for _, replace := range []func(string) bool{
		nil,
		func(string) bool { return true },
		func(k string) bool { return k == "shared-1" || k == "k8s.namespace.name" },
	} {
		src := pcommon.NewMap()
		for i := range bulkLabelThreshold + 8 {
			src.PutStr(fmt.Sprintf("src-%d", i), fmt.Sprintf("v%d", i))
		}
		src.PutStr("shared-1", "from-src")
		src.PutStr("shared-2", "from-src")
		src.PutStr("k8s.namespace.name", "resolved")
		src.PutEmptySlice("src-slice").AppendEmpty().SetInt(9)
		if src.Len() <= bulkLabelThreshold {
			t.Fatal("the fixture does not reach the bulk path")
		}
		dst := func() pcommon.Map {
			m := pcommon.NewMap()
			m.PutStr("shared-1", "from-dst")
			m.PutStr("shared-2", "from-dst")
			m.PutStr("k8s.namespace.name", "claimed")
			m.PutInt("dst-int", 7)
			m.PutEmptyMap("dst-map").PutStr("inner", "x")
			m.PutEmptyBytes("dst-bytes").FromRaw([]byte{1, 2, 3})
			return m
		}
		got, want := dst(), dst()
		Merge(src, got, replace)
		mergeLoop(src, want, replace)
		if got.Len() != want.Len() {
			t.Fatalf("bulk merge produced %d attributes, the loop %d", got.Len(), want.Len())
		}
		for k, wv := range want.All() {
			gv, ok := got.Get(k)
			if !ok {
				t.Fatalf("%s missing from the bulk merge", k)
			}
			if !gv.Equal(wv) {
				t.Fatalf("%s = %v (%s), the loop wrote %v (%s)", k, gv.AsRaw(), gv.Type(), wv.AsRaw(), wv.Type())
			}
		}
	}
}

// The bulk path rebuilds dst, so it must leave it untouched — order included —
// when there is nothing to write, and it collapses a key the sender repeated to
// ONE entry carrying the value Get reads (the first), where the loop would have
// left the later copy on the wire.
func TestMergeBulkPathEdges(t *testing.T) {
	src := pcommon.NewMap()
	for i := range bulkLabelThreshold + 1 {
		src.PutStr(fmt.Sprintf("k%d", i), "src")
	}
	dst := pcommon.NewMap()
	src.CopyTo(dst)
	dst.PutStr("tail", "dst")
	before := pcommon.NewMap()
	dst.CopyTo(before)
	FillAbsent(src, dst) // every key present, none replaced
	var gotKeys, wantKeys []string
	for k := range dst.All() {
		gotKeys = append(gotKeys, k)
	}
	for k := range before.All() {
		wantKeys = append(wantKeys, k)
	}
	if !slices.Equal(gotKeys, wantKeys) || !dst.Equal(before) {
		t.Fatalf("a merge with nothing to write changed dst: %v, want %v", gotKeys, wantKeys)
	}

	// A key the sender repeated (legal on the wire: pdata does not dedupe on
	// decode) collapses to one entry, and it is the one Get reads.
	var um plog.JSONUnmarshaler
	ld, err := um.UnmarshalLogs([]byte(`{"resourceLogs":[{"resource":{"attributes":[
		{"key":"dup","value":{"stringValue":"first"}},
		{"key":"dup","value":{"stringValue":"second"}}
	]}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	rep := ld.ResourceLogs().At(0).Resource().Attributes()
	FillAbsent(src, rep)
	n := 0
	for k, v := range rep.All() {
		if k == "dup" {
			n++
			if v.Str() != "first" {
				t.Errorf("the surviving dup = %q, want the first entry (what Get reads)", v.Str())
			}
		}
	}
	if n != 1 {
		t.Errorf("%d entries of dup after the bulk merge, want 1", n)
	}
	if rep.Len() != src.Len()+1 {
		t.Errorf("merged map has %d attributes, want %d", rep.Len(), src.Len()+1)
	}
}

// Merge exists so a merge is LINEAR in both sides: the per-key loop is
// O(|src| x (|dst|+|src|)), and on the ingest path the source is a resolved
// pod's labels — tenant-authored, bounded only by the API server's object size
// — merged once per resource of every push. Coarse on purpose, like
// TestPodWithManyLabelsIsLinear: a linear merge does 40k attributes in tens of
// milliseconds, the quadratic loop in seconds.
func TestMergeIsLinear(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector multiplies every map operation's cost")
	}
	const n = 40_000
	src, dst := pcommon.NewMap(), pcommon.NewMap()
	src.EnsureCapacity(n)
	raw := make(map[string]any, n)
	for i := range n {
		raw[fmt.Sprintf("k8s.pod.label.l-%d", i)] = "v"
	}
	if err := src.FromRaw(raw); err != nil {
		t.Fatal(err)
	}
	dst.PutStr("service.name", "sender")
	start := time.Now()
	FillAbsent(src, dst)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("merging %d attributes took %v: the merge is quadratic again", n, d)
	}
	if got := dst.Len(); got != n+1 {
		t.Fatalf("merged %d attributes, want %d", got, n+1)
	}
}
