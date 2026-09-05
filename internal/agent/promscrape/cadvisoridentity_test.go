package promscrape

// The identity rule the two kubelet endpoints share: whether a pod the metadata
// service answers for BY NAME is the pod the kubelet was describing.

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// staticPodSlice is the fixture static pod's POD cgroup — the path a pod-level
// row (container_network_*, and every container family's pod-cgroup rollup)
// carries. Its container sibling is staticPodCgroup (summary_test.go).
const staticPodSlice = "/kubepods/besteffort/pod" + staticPodUID

// staticPodImpostor is an ordinary API pod that merely SHARES the static pod's
// namespace and name — the shape a name reuse takes, and the shape the live
// reproduction took: a static pod whose mirror object never made it into the
// API server (its namespace did not exist), and later an unrelated pod created
// under that name, on ANOTHER NODE.
func staticPodImpostor() kubemeta.Pod {
	p := apiserverMirrorPod
	p.UID = "77777777-8888-9999-aaaa-bbbbccccdddd"
	p.NodeName = "node2"
	p.PodIP = "10.244.2.247"
	// An ordinary API-created pod: no config.hash, no config.mirror, so nothing
	// ties it to the static pod whose UID the kubelet reported.
	p.Annotations = map[string]string{"kubernetes.io/config.source": "api"}
	p.Labels = map[string]string{"impostor": "yes"}
	p.Owners = []kubemeta.Owner{{Kind: "Deployment", Name: "successor"}}
	p.Containers = []kubemeta.Container{{Name: "kube-apiserver", ID: successorCID, Image: apiserverImage}}
	return p
}

// The two kubelet endpoints describe the SAME objects, and their series are
// only worth having while they JOIN — which is decided entirely by the resource
// attributes. So a pod one of them refuses to place must be a pod the other
// refuses to place, under one rule rather than two implementations of it.
//
// The case that pulls them apart is the STATIC pod, because the UID the kubelet
// reports for one is not a UID the API server ever assigned: /stats/summary
// carries it in podRef.uid and /metrics/cadvisor carries it in the cgroup path.
// The summary path checks a by-name answer against it and accepts a mismatch
// only from a pod that SAYS it mirrors that UID; the cadvisor path used to
// accept any pod of that name, because cgroupid dropped a static pod's undashed
// UID and a by-name lookup with nothing to check against accepts everything.
//
// Measured on a live kind cluster before the fix, from one static pod on
// kubescrape-worker2 and one same-named ordinary pod on kubescrape-worker: the
// cadvisor resource carried the OTHER pod's uid, its labels, its pod IP and its
// NODE — statistics measured on worker2 exported as worker's — while the
// summary resource for the same cgroup carried the kubelet's own identity.
//
// Both halves of the rule are pinned here: the impostor subtest fails if the
// cross-check is missing or vacuous, and the mirror subtest fails if it is
// merely strict (refusing the mirror pod loses the control plane, which is the
// half the summary pipeline was built to fix).
func TestCadvisorAndSummaryAgreeOnAStaticPodsIdentity(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pod     kubemeta.Pod
		wantUID string
		wantIn  map[string]any
		wantOut []string
	}{
		{
			name:    "mirror pod",
			pod:     apiserverMirrorPod,
			wantUID: mirrorPodUID, // the API server's, since the pod proved it is the mirror
			wantIn: map[string]any{
				"k8s.pod.label.component": "kube-apiserver",
				"k8s.node.name":           "node1",
			},
		},
		{
			name:    "impostor sharing the name",
			pod:     staticPodImpostor(),
			wantUID: staticPodUID, // the kubelet's own, i.e. the label fallback
			wantIn: map[string]any{
				// The statistics were measured by node1's kubelet; the impostor
				// runs on node2, and attrs.Pod would stamp ITS node.
				"k8s.node.name": "node1",
			},
			wantOut: []string{"k8s.pod.label.impostor", "k8s.deployment.name", "k8s.pod.ip"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cadvisorRes := cadvisorPodResource(t, tc.pod)
			summaryRes := summaryPodResource(t, tc.pod)

			if !reflect.DeepEqual(cadvisorRes, summaryRes) {
				t.Fatalf("the two kubelet endpoints resolved one pod differently, so their series will not join:\ncadvisor: %v\nsummary:  %v",
					cadvisorRes, summaryRes)
			}
			if got := cadvisorRes["k8s.pod.uid"]; got != tc.wantUID {
				t.Errorf("k8s.pod.uid = %v, want %v", got, tc.wantUID)
			}
			for k, want := range tc.wantIn {
				if got := cadvisorRes[k]; got != want {
					t.Errorf("%s = %v, want %v (resource %v)", k, got, want, cadvisorRes)
				}
			}
			for _, k := range tc.wantOut {
				if v, ok := cadvisorRes[k]; ok {
					t.Errorf("%s = %v: another pod's identity was stamped on these statistics (resource %v)", k, v, cadvisorRes)
				}
			}
		})
	}
}

// cadvisorPodResource scrapes one pod-level cadvisor row for the fixture's
// static pod and returns the resource it landed on.
func cadvisorPodResource(t *testing.T, pod kubemeta.Pod) map[string]any {
	t.Helper()
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &mirrorMetaSource{pod: pod}, exp)
	cb := newCadvisorBatcher(context.Background(), s, summaryScrape)
	row := "# TYPE container_memory_working_set_bytes gauge\n" +
		`container_memory_working_set_bytes{namespace="kube-system",pod="kube-apiserver-node1",id="` + staticPodSlice + `"} 1073741824` + "\n"
	if _, err := s.parseAndExport(context.Background(), strings.NewReader(row), false, false, cb, pipelineCadvisor, "u"); err != nil {
		t.Fatal(err)
	}
	pts := flattenSummary(exp.batches)
	if len(pts) != 1 {
		t.Fatalf("the cadvisor row produced %d points, want 1", len(pts))
	}
	return pts[0].res
}

// summaryPodResource converts the static-pod summary fixture and returns the
// resource its POD-LEVEL statistics landed on. Pod level, because that is where
// the two pipelines describe the same object from the same identity: a summary
// element carries no container id, so an UNRESOLVED container's resource
// legitimately differs from the cadvisor row's (the documented join loss), and
// comparing those would assert the wrong thing.
func summaryPodResource(t *testing.T, pod kubemeta.Pod) map[string]any {
	t.Helper()
	exp := &captureExporter{}
	s := newSummaryScraper(t, "http://unused", &mirrorMetaSource{pod: pod}, exp)
	for _, p := range flattenSummary(convertFixture(t, s, exp, loadSummary(t, "summary-static-pod.json"))) {
		if p.metric == metPodFilesystemUsage.name {
			return p.res
		}
	}
	t.Fatalf("the fixture exported no pod-level statistic")
	return nil
}
