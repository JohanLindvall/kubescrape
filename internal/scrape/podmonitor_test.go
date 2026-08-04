package scrape

import (
	"reflect"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
)

func TestPodMonitorTargets(t *testing.T) {
	pod := basePod() // declares container port "metrics" = 9090

	// Port by container port name.
	ts := PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{Port: "metrics"})
	if len(ts) != 1 || ts[0].Port != 9090 || ts[0].Source != "podmonitor" || ts[0].Monitor != "ns/pm" {
		t.Fatalf("by name: %+v", ts)
	}
	// Numeric targetPort.
	tp := intstr.FromInt32(8080)
	ts = PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{TargetPort: &tp})
	if len(ts) != 1 || ts[0].Port != 8080 {
		t.Fatalf("by number: %+v", ts)
	}
	// Neither port nor targetPort: the phantom-target guard.
	if ts := PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{}); ts != nil {
		t.Fatalf("empty endpoint produced targets: %+v", ts)
	}
	// Unknown port name resolves to nothing.
	if ts := PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{Port: "nope"}); ts != nil {
		t.Fatalf("unknown port name produced targets: %+v", ts)
	}
	// Endpoint auth/TLS/relabelings are stamped onto the target.
	ts = PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{
		Port: "metrics", Scheme: "https", InsecureSkipVerify: true,
		BearerSecret: "ns/tok/token",
		MetricRelabelings: []servicemonitors.RelabelRule{
			{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "go_.*"},
		},
	})
	if len(ts) != 1 || !ts[0].InsecureSkipVerify || ts[0].AuthSecret != "ns/tok/token" ||
		len(ts[0].MetricRelabelings) != 1 || ts[0].Scheme != "https" {
		t.Fatalf("endpoint stamping: %+v", ts)
	}
}

// notStampedVerbatim are the Endpoint string fields that deliberately do NOT
// carry an arbitrary sentinel to the target: Port resolves to a port NUMBER,
// Path is folded into the URL, Scheme is normalised by defaultSchemePath
// (anything but "https" becomes "http"), and Ignored is reporting metadata the
// target never carries. Each is asserted separately below with a legal value.
var notStampedVerbatim = map[string]bool{
	"Port": true, "Path": true, "Scheme": true, "Ignored": true,
}

// stampEndpoint is the DELIVERY site of every endpoint field, and it is a
// fourth hand-written copy of the list that servicemonitors.Endpoint.secretRefs
// exists to keep singular. secretRefs makes a new secret-bearing field a
// compile-visible omission at the namespacing and allowlisting sites, but
// stampEndpoint is a plain sequence of assignments: an author who adds a field
// and updates Endpoint, secretRefs and toEndpoint gets green tests, correct
// namespacing, correct allowlisting — and a target that silently carries no
// credential, scraping unauthenticated with up=0 as the only signal.
//
// So assert it structurally rather than field by field: every string field of
// Endpoint gets a distinct sentinel, and each one must resurface somewhere on
// the produced target. A new field that stampEndpoint forgets fails HERE.
func TestEveryEndpointFieldReachesTheTarget(t *testing.T) {
	ep := servicemonitors.Endpoint{Port: "metrics"}
	v := reflect.ValueOf(&ep).Elem()
	typ := v.Type()
	want := map[string]string{} // field name -> sentinel
	for i := range typ.NumField() {
		f := typ.Field(i)
		if f.Type.Kind() != reflect.String || notStampedVerbatim[f.Name] {
			continue
		}
		sentinel := "sentinel-" + f.Name
		v.Field(i).SetString(sentinel)
		want[f.Name] = sentinel
	}
	ep.Path = "/sentinel-path"
	ep.Scheme = "https"

	ts := PodMonitorTargets(basePod(), "ns/pm", ep)
	if len(ts) != 1 {
		t.Fatalf("want 1 target, got %d", len(ts))
	}
	got := ts[0]

	// Collect every string the target carries.
	var carried []string
	tv := reflect.ValueOf(got)
	for i := range tv.NumField() {
		if tv.Field(i).Kind() == reflect.String {
			carried = append(carried, tv.Field(i).String())
		}
	}
	haystack := strings.Join(carried, "\x00")
	for field, sentinel := range want {
		if !strings.Contains(haystack, sentinel) {
			t.Errorf("Endpoint.%s never reaches the target: stampEndpoint does not copy it "+
				"(add it there, beside the other endpoint fields)", field)
		}
	}
	if !strings.Contains(got.URL, "/sentinel-path") {
		t.Errorf("Endpoint.Path did not reach the target URL: %q", got.URL)
	}
	if got.Scheme != "https" || !strings.HasPrefix(got.URL, "https://") {
		t.Errorf("Endpoint.Scheme did not reach the target: scheme=%q url=%q", got.Scheme, got.URL)
	}
}
