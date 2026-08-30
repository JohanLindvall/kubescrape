package scrape

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// bombMonitor parses a real ServiceMonitor CR through the real parse door, so
// the test exercises what an API-server-delivered object does rather than a
// hand-built Endpoint the door never saw.
func bombMonitor(t *testing.T, ep map[string]any) servicemonitors.Endpoint {
	t.Helper()
	m, err := servicemonitors.Parse(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "bomb", "namespace": "tenant"},
		"spec": map[string]any{
			"selector":          map[string]any{},
			"namespaceSelector": map[string]any{"any": true},
			"endpoints":         []any{ep},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return m.Endpoints[0]
}

func bombService() *services.Service {
	return &services.Service{
		Name: "svc", Namespace: "default", UID: "svc-uid",
		Ports: []services.Port{{Name: "http", Port: 9090, TargetPortNum: 9090}},
	}
}

// THE ATTACK, end to end through the derivation the singleton runs: ONE
// cluster-wide ServiceMonitor with a 1 MiB `path` and no metricRelabelings
// yielded ONE target of 2,097,625 bytes (the path is copied into t.URL AND
// t.Path), embedded once per matched pod in /v1/nodes/{node}/targets — which is
// re-derived and re-marshalled on every agent poll, since writeCached must
// build the body to hash the ETag.
//
// Reverse-patch check: dropping the `ep.Refused != ""` arm from
// monitorEndpoint AND the enforceFieldBounds call in servicemonitors restores
// the 2 MiB target and this fails.
func TestOversizeMonitorPathYieldsNoTarget(t *testing.T) {
	ep := bombMonitor(t, map[string]any{"port": "http", "path": "/" + strings.Repeat("a", 1<<20)})
	svc := bombService()
	if ts := MonitorTargets(basePod(), svc, "tenant/bomb", ep); len(ts) != 0 {
		b, _ := json.Marshal(ts)
		t.Fatalf("a 1 MiB path produced %d target(s), %d bytes", len(ts), len(b))
	}
	if _, ok := MonitorTargetURL(basePod(), svc, ep); ok {
		t.Errorf("the identity half still resolves the endpoint: the dedup would hold a URL for a target nobody serves")
	}
	// And /v1/explain says WHY, naming the field rather than the port (which
	// the endpoint did name) and never the value.
	note := MonitorEndpointNote(ep)
	if !strings.Contains(note, "path") || !strings.Contains(note, "REFUSED") {
		t.Errorf("explain does not name the refusal: %q", note)
	}
	if strings.Contains(note, strings.Repeat("a", 64)) {
		t.Errorf("explain echoes the tenant's value back")
	}
}

// The PodMonitor half of the same door: it resolves through a different
// function (podMonitorEndpoint), which is exactly how a sibling stays open.
func TestOversizePodMonitorPathYieldsNoTarget(t *testing.T) {
	ep := bombMonitor(t, map[string]any{"port": "metrics", "path": "/" + strings.Repeat("a", 1<<20)})
	if ts := PodMonitorTargets(basePod(), "tenant/bomb", ep); len(ts) != 0 {
		t.Fatalf("a 1 MiB path produced %d PodMonitor target(s)", len(ts))
	}
	if _, ok := PodMonitorTargetURL(basePod(), ep); ok {
		t.Errorf("the identity half still resolves the endpoint")
	}
	if note := PodMonitorEndpointNote(ep); !strings.Contains(note, "REFUSED") {
		t.Errorf("explain does not name the refusal: %q", note)
	}
}

// A refused endpoint carries no credential either, so /v1/scrape-auth cannot be
// asked to serve one for a target that does not exist.
func TestRefusedEndpointCarriesNoAuthMaterial(t *testing.T) {
	ep := bombMonitor(t, map[string]any{
		"port": "http", "path": "/" + strings.Repeat("a", 1<<20),
		"bearerTokenSecret": map[string]any{"name": "tok", "key": "token"},
	})
	if ep.BearerSecret != "" {
		t.Errorf("a refused endpoint still names a secret: %q", ep.BearerSecret)
	}
}

// The ANNOTATION door, which the finding names as the sibling: prometheus.io/
// path is attacker-supplied by anyone who can annotate a pod or a Service, and
// the API server bounds it only at the 256 KiB total-annotations limit — two
// orders of magnitude above any real path, then multiplied by the targets the
// pod produces.
//
// Reverse-patch check: making defaultSchemePath return true unconditionally
// restores a target whose URL alone is a quarter-megabyte.
func TestOversizePathAnnotationYieldsNoTarget(t *testing.T) {
	long := "/" + strings.Repeat("a", MaxTargetPathBytes)
	pod := basePod()
	pod.Annotations[AnnotationPath] = long
	if ts := PodTargets(pod); len(ts) != 0 {
		b, _ := json.Marshal(ts)
		t.Fatalf("pod door produced %d target(s), %d bytes", len(ts), len(b))
	}
	// The explain mirror must refuse the same door, or /v1/explain answers
	// "why is my port not scraped?" with "it is" — the inversion the mirror
	// exists to prevent.
	verdicts, annotated := ExplainPodPorts(pod)
	if !annotated || len(verdicts) != 1 || len(verdicts[0].Ports) != 0 ||
		!strings.Contains(verdicts[0].Note, "over the ceiling") {
		t.Fatalf("explain does not mirror the pod-door refusal: %+v", verdicts)
	}

	svc := bombService()
	svc.Annotations = map[string]string{AnnotationScrape: "true", AnnotationPath: long}
	if ts := ServiceTargets(basePod(), svc); len(ts) != 0 {
		t.Fatalf("service door produced %d target(s)", len(ts))
	}
	sv, _ := ExplainServicePorts(basePod(), svc)
	if len(sv) != 1 || len(sv[0].Ports) != 0 || !strings.Contains(sv[0].Note, "over the ceiling") {
		t.Fatalf("explain does not mirror the service-door refusal: %+v", sv)
	}
}

// An ordinary path is untouched at every door, including one right at the
// ceiling: a bound that refused real configuration would be the worse outage.
func TestOrdinaryPathsAreUnaffectedAtEveryDoor(t *testing.T) {
	pod := basePod()
	pod.Annotations[AnnotationPath] = "/" + strings.Repeat("a", MaxTargetPathBytes-1)
	if ts := PodTargets(pod); len(ts) != 2 {
		t.Errorf("a path exactly at the ceiling was refused: %d targets", len(ts))
	}
	pod.Annotations[AnnotationPath] = "/actuator/prometheus"
	if ts := PodTargets(pod); len(ts) != 2 || !strings.HasSuffix(ts[0].URL, "/actuator/prometheus") {
		t.Errorf("ordinary annotation path mangled: %+v", ts)
	}
	ep := bombMonitor(t, map[string]any{"port": "http", "path": "/actuator/prometheus"})
	ts := MonitorTargets(basePod(), bombService(), "tenant/ok", ep)
	if len(ts) != 1 || ts[0].Path != "/actuator/prometheus" {
		t.Errorf("ordinary monitor path mangled: %+v", ts)
	}
}

// The two doors must not disagree about what a path is: servicemonitors
// enforces its own ceiling at parse time and scrape enforces this one at
// resolution, so a path the parse door ADMITS must be one this door serves.
// (The relabel ceilings are held together the same way — the parse-time bound
// is strictly under the merged one.)
func TestMonitorPathDoorIsNoLooserThanTheAnnotationDoor(t *testing.T) {
	ep := bombMonitor(t, map[string]any{"port": "http", "path": "/" + strings.Repeat("a", MaxTargetPathBytes*2)})
	if ep.Refused == "" {
		t.Fatalf("servicemonitors admitted a path longer than scrape.MaxTargetPathBytes (%d): the two ceilings have drifted", MaxTargetPathBytes)
	}
}
