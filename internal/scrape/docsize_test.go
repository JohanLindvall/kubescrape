package scrape

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// fatPod is the shape MaxTargetBytesPerPod exists for: a pod carrying a
// large-but-legal annotation. The API server permits 256 KiB of annotations per
// object, and kubemeta.FilterAnnotations only drops the deploy-tool copies of
// the applied object — everything else is served verbatim and embedded in every
// target.
func fatPod(annotationBytes int) kubemeta.Pod {
	p := basePod()
	p.Annotations["team.example.com/inventory"] = strings.Repeat("x", annotationBytes)
	return p
}

// sizedPods is the corpus both size assertions run over: empty, ordinary, and
// every shape whose size is driven by input a tenant supplies.
func sizedPods(t *testing.T) map[string]kubemeta.Pod {
	t.Helper()
	now := time.Now().UTC()
	full := basePod()
	full.NodeName = "node-1"
	full.PodIPs = []string{"10.1.2.3", "fd00::7"}
	full.HostIP = "192.168.0.9"
	full.Phase = "Running"
	full.Ready = true
	full.CreatedAt = now
	full.StartedAt = &now
	full.DeletionTimestamp = &now
	full.DeletedAt = &now
	full.NamespaceMetadata = &kubemeta.ObjectMeta{
		UID:         "ns-uid",
		Labels:      map[string]string{"kubernetes.io/metadata.name": "default"},
		Annotations: map[string]string{"openshift.io/sa.scc.uid-range": "1000/10000"},
	}
	for i := range 8 {
		full.Owners = append(full.Owners, kubemeta.Owner{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-" + strconv.Itoa(i),
			UID: "owner-uid-" + strconv.Itoa(i), Controller: true,
			Labels:      map[string]string{"app": "web", "pod-template-hash": "7d9f8c"},
			Annotations: map[string]string{"deployment.kubernetes.io/revision": "3"},
		})
	}
	for i := range 6 {
		full.Containers = append(full.Containers, kubemeta.Container{
			Name: "side-" + strconv.Itoa(i), Type: "container",
			ID: strings.Repeat("a", 64), RuntimeID: "containerd://" + strings.Repeat("a", 64),
			Image: "registry.example.com/team/app:v1.2.3", ImageID: "sha256:" + strings.Repeat("b", 64),
			State: "running", WaitingReason: "", StartedAt: &now,
			Ports: []kubemeta.ContainerPort{{Name: "metrics", Port: 9090, Protocol: "TCP"}},
		})
	}

	manyLabels := basePod()
	manyLabels.Labels = map[string]string{}
	for i := range 64 {
		manyLabels.Labels["label.example.com/"+strconv.Itoa(i)] = strconv.Itoa(i)
		manyLabels.Annotations["annotation.example.com/"+strconv.Itoa(i)] = strings.Repeat("v", 40)
	}

	return map[string]kubemeta.Pod{
		"empty":       {},
		"base":        basePod(),
		"full":        full,
		"manyLabels":  manyLabels,
		"fat200KiB":   fatPod(200 << 10),
		"emptyValues": {Name: "n", Annotations: map[string]string{"a": ""}, Labels: map[string]string{}},
	}
}

// THE CONTRACT: the estimate is never LESS than what the encoder produces, for
// any document whose strings need no JSON escaping. The byte ceiling is a bound
// on the response, and a bound computed from an estimate is only a bound while
// the estimate does not undercount.
//
// It is an estimate rather than a json.Marshal because it runs once per pod per
// agent poll per node, and marshalling to measure would allocate the whole
// 200 KiB document this ceiling exists for (see PodDocBytes).
func TestDocBytesIsNeverUnderTheMarshalledSize(t *testing.T) {
	for name, pod := range sizedPods(t) {
		want, err := json.Marshal(pod)
		if err != nil {
			t.Fatal(err)
		}
		if got := PodDocBytes(&pod); got < len(want) {
			t.Errorf("%s: PodDocBytes estimates %d bytes, encoder produces %d — the ceiling is computed "+
				"from this number, so it must never be under", name, got, len(want))
		}
		tgt := kubemeta.ScrapeTarget{
			URL: "http://10.1.2.3:9090/metrics", Scheme: "http", Address: "10.1.2.3:9090",
			Port: 9090, Path: "/metrics", Source: "servicemonitor",
			Monitor: "default/web", Monitors: []string{"default/web", "default/platform"},
			InsecureSkipVerify: true, AuthSecret: "default/tok/key",
			BasicAuthUser: "default/ba/user", BasicAuthPass: "default/ba/pass",
			AuthType: "Bearer", AuthCredentials: "default/auth/creds",
			TLSCA: "default/tls/ca", TLSCert: "default/tls/crt", TLSKey: "default/tls/key",
			TLSServerName: "web.default.svc", Interval: "30s", ScrapeTimeout: "10s",
			MetricRelabelings: []kubemeta.RelabelRule{
				{Action: "keep", SourceLabels: []string{"__name__", "job"}, Regex: strings.Repeat("a|", 200) + "z"},
			},
			Service: &kubemeta.Service{
				Name: "web", Namespace: "default", UID: "svc-uid",
				Labels:      map[string]string{"team": "obs"},
				Annotations: map[string]string{"prometheus.io/scrape": "true"},
			},
			Pod: pod,
		}
		wantT, err := json.Marshal(tgt)
		if err != nil {
			t.Fatal(err)
		}
		if got := TargetDocBytes(&tgt, PodDocBytes(&pod)); got < len(wantT) {
			t.Errorf("%s: TargetDocBytes estimates %d bytes, encoder produces %d", name, got, len(wantT))
		}
	}
}

// The estimate must stay CLOSE, or the ceiling stops meaning what its comment
// says: a bound that over-charges by an order of magnitude would refuse the
// second target of an ordinary pod.
func TestDocBytesIsNotWildlyOverTheMarshalledSize(t *testing.T) {
	for name, pod := range sizedPods(t) {
		want, err := json.Marshal(pod)
		if err != nil {
			t.Fatal(err)
		}
		got := PodDocBytes(&pod)
		// The fixed floors dominate a tiny document, so the ratio is only
		// meaningful once there is something to measure.
		if len(want) > 1024 && got > 2*len(want) {
			t.Errorf("%s: PodDocBytes estimates %d bytes for a %d-byte document; the charge must track the "+
				"real cost, not double it", name, got, len(want))
		}
	}
}

// The estimate is a hand-written walk, so a field added to the model must not
// silently escape it: a new unbounded string on kubemeta.Pod would be charged
// nothing and reopen exactly the multiplier MaxTargetBytesPerPod closes.
//
// The list below is what PodDocBytes/TargetOwnBytes account for. If this test
// fails, the model gained a field — charge it in internal/scrape/docsize.go and
// then add it here.
func TestDocSizeChargesEveryField(t *testing.T) {
	charged := map[reflect.Type][]string{
		reflect.TypeOf(kubemeta.Pod{}): {
			"Name", "Namespace", "UID", "NodeName", "PodIP", "PodIPs", "HostIP", "HostNetwork",
			"Phase", "Ready", "Labels", "Annotations", "CreatedAt", "StartedAt",
			"DeletionTimestamp", "DeletedAt", "NamespaceMetadata", "Owners", "OwnersOmitted", "Containers",
		},
		reflect.TypeOf(kubemeta.Container{}): {
			"Name", "Type", "ID", "RuntimeID", "Image", "ImageID", "Ports", "RestartCount",
			"Ready", "State", "WaitingReason", "StartedAt", "FinishedAt", "ExitCode",
		},
		reflect.TypeOf(kubemeta.ContainerPort{}): {"Name", "Port", "Protocol"},
		reflect.TypeOf(kubemeta.Owner{}): {
			"APIVersion", "Kind", "Name", "UID", "Controller", "Labels", "Annotations",
		},
		reflect.TypeOf(kubemeta.ObjectMeta{}):  {"UID", "Labels", "Annotations"},
		reflect.TypeOf(kubemeta.Service{}):     {"Name", "Namespace", "UID", "Labels", "Annotations"},
		reflect.TypeOf(kubemeta.RelabelRule{}): {"Action", "SourceLabels", "Regex"},
		reflect.TypeOf(kubemeta.ScrapeTarget{}): {
			"URL", "Scheme", "Address", "Port", "Path", "Source", "Service", "Monitor", "Monitors",
			"InsecureSkipVerify", "AuthSecret", "BasicAuthUser", "BasicAuthPass", "AuthType",
			"AuthCredentials", "TLSCA", "TLSCert", "TLSKey", "TLSServerName", "Interval",
			"ScrapeTimeout", "MetricRelabelings", "Pod",
		},
	}
	for typ, want := range charged {
		var got []string
		for i := range typ.NumField() {
			got = append(got, typ.Field(i).Name)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s has fields %v, but the size estimate accounts for %v — charge the new field in "+
				"internal/scrape/docsize.go (an uncharged string reopens the per-target multiplier) and then "+
				"list it here", typ, got, want)
		}
	}
}

// The measurement runs once per pod on the node-targets derivation, which is
// re-run for every agent poll. It walks the map ENTRIES, never their bytes, and
// allocates nothing — a json.Marshal to measure would allocate the whole
// document it is measuring.
func TestPodDocBytesIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector adds bookkeeping allocations")
	}
	pod := fatPod(200 << 10)
	if n := testing.AllocsPerRun(100, func() { _ = PodDocBytes(&pod) }); n != 0 {
		t.Errorf("PodDocBytes allocates %v per call; it runs once per pod per agent poll", n)
	}
}
