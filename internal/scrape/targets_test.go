package scrape

import (
	"testing"

	"time"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func basePod() kubemeta.Pod {
	return kubemeta.Pod{
		Name:      "pod1",
		Namespace: "default",
		PodIP:     "10.0.0.5",
		Phase:     "Running",
		Annotations: map[string]string{
			AnnotationScrape: "true",
		},
		Containers: []kubemeta.Container{{
			Name: "app",
			Ports: []kubemeta.ContainerPort{
				{Name: "metrics", Port: 9090},
				{Name: "web", Port: 8080},
			},
		}},
	}
}

func TestPodTargetsWithPortAnnotation(t *testing.T) {
	pod := basePod()
	pod.Annotations[AnnotationPort] = "9102"
	pod.Annotations[AnnotationPath] = "custom/metrics"
	pod.Annotations[AnnotationScheme] = "https"

	targets := PodTargets(pod)
	if len(targets) != 1 {
		t.Fatalf("got %d targets, want 1", len(targets))
	}
	tg := targets[0]
	if tg.URL != "https://10.0.0.5:9102/custom/metrics" {
		t.Errorf("URL = %q", tg.URL)
	}
	if tg.Port != 9102 || tg.Scheme != "https" || tg.Path != "/custom/metrics" || tg.Source != "pod" {
		t.Errorf("target = %+v", tg)
	}
	if tg.Pod.Name != "pod1" {
		t.Errorf("pod metadata not embedded")
	}
}

func TestPodTargetsMultiplePorts(t *testing.T) {
	pod := basePod()
	pod.Annotations[AnnotationPort] = "9102, metrics,web, bogus,0"

	targets := PodTargets(pod)
	if len(targets) != 3 {
		t.Fatalf("got %d targets, want 3: %+v", len(targets), targets)
	}
	want := []int32{9102, 9090, 8080}
	for i, tg := range targets {
		if tg.Port != want[i] {
			t.Errorf("target %d port = %d, want %d", i, tg.Port, want[i])
		}
	}
}

func TestPodTargetsWithNamedPort(t *testing.T) {
	pod := basePod()
	pod.Annotations[AnnotationPort] = "metrics"

	targets := PodTargets(pod)
	if len(targets) != 1 || targets[0].Port != 9090 {
		t.Fatalf("targets = %+v", targets)
	}
}

func TestPodTargetsDefaultToContainerPorts(t *testing.T) {
	targets := PodTargets(basePod())
	if len(targets) != 2 {
		t.Fatalf("got %d targets, want 2 (one per container port)", len(targets))
	}
	if targets[0].URL != "http://10.0.0.5:9090/metrics" {
		t.Errorf("URL = %q", targets[0].URL)
	}
}

func TestPodTargetsIPv6(t *testing.T) {
	pod := basePod()
	pod.PodIP = "fd00::1"
	pod.Annotations[AnnotationPort] = "9090"

	targets := PodTargets(pod)
	if len(targets) != 1 || targets[0].URL != "http://[fd00::1]:9090/metrics" {
		t.Fatalf("targets = %+v", targets)
	}
}

func TestPodTargetsSkipped(t *testing.T) {
	cases := map[string]func(*kubemeta.Pod){
		"scrape annotation missing": func(p *kubemeta.Pod) { delete(p.Annotations, AnnotationScrape) },
		"scrape annotation false":   func(p *kubemeta.Pod) { p.Annotations[AnnotationScrape] = "false" },
		"no pod IP":                 func(p *kubemeta.Pod) { p.PodIP = "" },
		"succeeded":                 func(p *kubemeta.Pod) { p.Phase = "Succeeded" },
		"failed":                    func(p *kubemeta.Pod) { p.Phase = "Failed" },
		"unknown named port":        func(p *kubemeta.Pod) { p.Annotations[AnnotationPort] = "nosuchport" },
		"port out of range":         func(p *kubemeta.Pod) { p.Annotations[AnnotationPort] = "70000" },
		"no ports declared": func(p *kubemeta.Pod) {
			p.Containers[0].Ports = nil
		},
	}
	for name, mutate := range cases {
		pod := basePod()
		mutate(&pod)
		if targets := PodTargets(pod); len(targets) != 0 {
			t.Errorf("%s: got %d targets, want 0", name, len(targets))
		}
	}
}

func TestInvalidSchemeFallsBackToHTTP(t *testing.T) {
	pod := basePod()
	pod.Annotations[AnnotationScheme] = "gopher"
	pod.Annotations[AnnotationPort] = "9090"

	targets := PodTargets(pod)
	if len(targets) != 1 || targets[0].Scheme != "http" {
		t.Fatalf("targets = %+v", targets)
	}
}

func baseService() *services.Service {
	return &services.Service{
		Name:      "svc1",
		Namespace: "default",
		UID:       "svc-uid",
		Labels:    map[string]string{"app": "web"},
		Annotations: map[string]string{
			AnnotationScrape: "true",
		},
		Selector: map[string]string{"app": "web"},
		Ports: []services.Port{
			{Name: "metrics", Port: 80, TargetPortName: "metrics"},
			{Name: "web", Port: 8081, TargetPortNum: 8080},
		},
	}
}

func TestServiceTargetsNamedTargetPort(t *testing.T) {
	pod := basePod()
	delete(pod.Annotations, AnnotationScrape) // pod itself not annotated

	svc := baseService()
	svc.Annotations[AnnotationPort] = "metrics"
	svc.Annotations[AnnotationPath] = "/svc-metrics"

	targets := ServiceTargets(pod, svc)
	if len(targets) != 1 {
		t.Fatalf("targets = %+v", targets)
	}
	tg := targets[0]
	// Service port 80 -> targetPort "metrics" -> container port 9090.
	if tg.URL != "http://10.0.0.5:9090/svc-metrics" || tg.Port != 9090 {
		t.Errorf("target = %+v", tg)
	}
	if tg.Source != "service" || tg.Service == nil || tg.Service.Name != "svc1" || tg.Service.UID != "svc-uid" {
		t.Errorf("service metadata = %+v", tg.Service)
	}
}

func TestServiceTargetsAllPortsByDefault(t *testing.T) {
	targets := ServiceTargets(basePod(), baseService())
	if len(targets) != 2 {
		t.Fatalf("got %d targets, want 2: %+v", len(targets), targets)
	}
	// Named targetPort "metrics" -> 9090; numeric targetPort -> 8080.
	if targets[0].Port != 9090 || targets[1].Port != 8080 {
		t.Errorf("ports = %d, %d", targets[0].Port, targets[1].Port)
	}
}

func TestServiceTargetsPortSelection(t *testing.T) {
	svc := baseService()
	svc.Annotations[AnnotationPort] = "8081" // select service port by number
	targets := ServiceTargets(basePod(), svc)
	if len(targets) != 1 || targets[0].Port != 8080 {
		t.Fatalf("targets = %+v", targets)
	}

	svc.Annotations[AnnotationPort] = "metrics, 8081" // list of name and number
	targets = ServiceTargets(basePod(), svc)
	if len(targets) != 2 {
		t.Fatalf("targets = %+v", targets)
	}
}

func TestServiceTargetsDefaultTargetPort(t *testing.T) {
	svc := baseService()
	svc.Ports = []services.Port{{Name: "plain", Port: 9999}} // no targetPort: pod port = service port
	targets := ServiceTargets(basePod(), svc)
	if len(targets) != 1 || targets[0].Port != 9999 {
		t.Fatalf("targets = %+v", targets)
	}
}

func TestServiceTargetsSkipped(t *testing.T) {
	pod := basePod()
	if got := ServiceTargets(pod, nil); got != nil {
		t.Fatal("nil service should produce no targets")
	}

	svc := baseService()
	svc.Annotations[AnnotationScrape] = "false"
	if got := ServiceTargets(pod, svc); got != nil {
		t.Fatal("unannotated service should produce no targets")
	}

	svc = baseService()
	svc.Ports = []services.Port{{Name: "x", Port: 80, TargetPortName: "missing"}}
	if got := ServiceTargets(pod, svc); len(got) != 0 {
		t.Fatalf("unresolvable named targetPort should be skipped, got %+v", got)
	}

	pod.Phase = "Succeeded"
	if got := ServiceTargets(pod, baseService()); got != nil {
		t.Fatal("finished pod should produce no targets")
	}
}

func monitorService() *services.Service {
	return &services.Service{
		Name: "svc1", Namespace: "default", UID: "svc-uid",
		Labels: map[string]string{"team": "core"},
		Ports: []services.Port{
			{Name: "metrics", Port: 80, TargetPortName: "metrics"},
			{Name: "direct", Port: 9091, TargetPortNum: 9091},
		},
	}
}

func TestMonitorTargets(t *testing.T) {
	pod := basePod()
	svc := monitorService()

	// Service-port name -> named container port.
	ts := MonitorTargets(pod, svc, "mon/m1", servicemonitors.Endpoint{Port: "metrics"})
	if len(ts) != 1 {
		t.Fatalf("targets = %+v", ts)
	}
	tg := ts[0]
	if tg.URL != "http://10.0.0.5:9090/metrics" || tg.Source != "servicemonitor" || tg.Monitor != "mon/m1" {
		t.Fatalf("target = %+v", tg)
	}
	if tg.Service == nil || tg.Service.Name != "svc1" || tg.Service.Labels["team"] != "core" {
		t.Fatalf("service info = %+v", tg.Service)
	}

	// Numeric targetPort override + scheme/path handling (missing leading /).
	tp := intstr.FromInt32(8080)
	ts = MonitorTargets(pod, svc, "mon/m1", servicemonitors.Endpoint{
		TargetPort: &tp, Scheme: "https", Path: "custom",
	})
	if len(ts) != 1 || ts[0].URL != "https://10.0.0.5:8080/custom" {
		t.Fatalf("targetPort target = %+v", ts)
	}

	// Named targetPort resolves against container ports.
	tpn := intstr.FromString("web")
	ts = MonitorTargets(pod, svc, "mon/m1", servicemonitors.Endpoint{TargetPort: &tpn})
	if len(ts) != 1 || ts[0].URL != "http://10.0.0.5:8080/metrics" {
		t.Fatalf("named targetPort target = %+v", ts)
	}

	// Unresolvable ports and nil services yield nothing.
	if ts := MonitorTargets(pod, svc, "m", servicemonitors.Endpoint{Port: "nope"}); ts != nil {
		t.Fatalf("unknown service port = %+v", ts)
	}
	bad := intstr.FromString("nope")
	if ts := MonitorTargets(pod, svc, "m", servicemonitors.Endpoint{TargetPort: &bad}); ts != nil {
		t.Fatalf("unknown targetPort = %+v", ts)
	}
	if ts := MonitorTargets(pod, nil, "m", servicemonitors.Endpoint{Port: "metrics"}); ts != nil {
		t.Fatalf("nil service = %+v", ts)
	}

	// Deleted or IP-less pods are never targets.
	gone := basePod()
	now := time.Now()
	gone.DeletedAt = &now
	if ts := MonitorTargets(gone, svc, "m", servicemonitors.Endpoint{Port: "metrics"}); ts != nil {
		t.Fatalf("deleted pod = %+v", ts)
	}
}
