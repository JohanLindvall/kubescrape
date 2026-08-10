package promscrape

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/pkg/cgroupid"
)

// The cgroup layouts a live kubelet's /metrics/cadvisor actually emits. These
// are the SPECIFICATION for sandbox detection, so they are built from captured
// shapes rather than invented: the systemd driver's kubepods.slice tree, the
// same tree rooted under kubelet.slice (a kubelet run with --cgroup-root, which
// is what kind and several distros do), and the cgroupfs driver's plain
// directories.
func systemdPodSlice(uid string) string {
	return "/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod" +
		strings.ReplaceAll(uid, "-", "_") + ".slice"
}

func kubeletSlicePodSlice(uid string) string {
	return "/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-besteffort.slice/kubelet-kubepods-besteffort-pod" +
		strings.ReplaceAll(uid, "-", "_") + ".slice"
}

func cgroupfsPodSlice(uid string) string { return "/kubepods/besteffort/pod" + uid }

// containerdScope is the containerd container scope beneath a pod slice; the
// pause container gets one exactly like a workload container's.
func containerdScope(podSlice, cid string) string {
	if strings.HasSuffix(podSlice, ".slice") {
		return podSlice + "/cri-containerd-" + cid + ".scope"
	}
	return podSlice + "/" + cid
}

const pauseImage = "registry.k8s.io/pause:3.10"

// containerdCadvisorBody renders the THREE row shapes one pod produces on a
// modern containerd node, in cadvisor's own label order:
//
//	the workload container   container="app", its own scope, the workload image
//	the pause container      container="",    its own scope, the pause image
//	the pod cgroup           container="",    the pod slice,  no image, no name
//
// The last two are what a container="POD"-only predicate conflated: they are
// different cgroups measuring different things, and only the cgroup path tells
// them apart.
func containerdCadvisorBody(podSlice string) string {
	appScope := containerdScope(podSlice, appCID)
	pauseScope := containerdScope(podSlice, pauseCID)
	return fmt.Sprintf(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{container="app",id=%q,image="img:1",name=%q,namespace="ns1",pod="pod1"} 12.5
container_cpu_usage_seconds_total{container="",id=%q,image=%q,name=%q,namespace="ns1",pod="pod1"} 0.03
container_cpu_usage_seconds_total{container="",id=%q,image="",name="",namespace="ns1",pod="pod1"} 12.9
# TYPE container_network_receive_bytes_total counter
container_network_receive_bytes_total{container="",id=%q,image="",interface="eth0",name="",namespace="ns1",pod="pod1"} 5000
`, appScope, appCID, pauseScope, pauseImage, pauseCID, podSlice, podSlice)
}

// legacyCadvisorBody is the same pod as dockershim (and CRI-O) reports it: the
// infra container is labelled container="POD". Removed from Kubernetes in 1.24
// and emitted by zero rows on a live containerd node, but CRI-O still annotates
// its infra container this way, so the convention must keep working.
func legacyCadvisorBody() string {
	podSlice := cgroupfsPodSlice(uid1)
	return fmt.Sprintf(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{container="app",id=%q,image="img:1",name="k8s_app_pod1_ns1_%s_0",namespace="ns1",pod="pod1"} 12.5
container_cpu_usage_seconds_total{container="POD",id=%q,image="k8s.gcr.io/pause:3.2",name="k8s_POD_pod1_ns1_%s_0",namespace="ns1",pod="pod1"} 0.03
container_cpu_usage_seconds_total{container="",id=%q,image="",name="",namespace="ns1",pod="pod1"} 12.9
# TYPE container_network_receive_bytes_total counter
container_network_receive_bytes_total{container="",id=%q,image="",interface="eth0",name="",namespace="ns1",pod="pod1"} 5000
`, podSlice+"/"+appCID, uid1, podSlice+"/"+pauseCID, uid1, podSlice, podSlice)
}

// cadvisorPoints scrapes body and flattens every exported point into
// "<metric>|<data point attrs>|<value>" strings, grouped by the same
// pod-uid/container-id/container-name resource key resourcesByIdentity uses.
// The second result is each resource's container.image.name — how a phantom
// pause resource announces itself — and the third its whole attribute set,
// which is what a wrongly folded row strips.
func cadvisorPoints(t *testing.T, body string, disableRollups bool) (map[string][]string, map[string]string, map[string]map[string]any) {
	t.Helper()
	srv := serveBody(t, body)
	exp := &captureExporter{}
	s := newKubeletScraper(t, srv.URL, &fakeMetaSource{}, exp, disableRollups)
	if _, err := s.scrapeCadvisor(context.Background()); err != nil {
		t.Fatal(err)
	}
	points := map[string][]string{}
	images := map[string]string{}
	resAttrs := map[string]map[string]any{}
	for _, md := range exp.batches {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			rm := rms.At(i)
			res := rm.Resource()
			key := attrStr(res, "k8s.pod.uid") + "/" + attrStr(res, "container.id") + "/" + attrStr(res, "k8s.container.name")
			images[key] = attrStr(res, "container.image.name")
			resAttrs[key] = res.Attributes().AsRaw()
			sms := rm.ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					dps := m.Sum().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						points[key] = append(points[key],
							fmt.Sprintf("%s|%v|%v", m.Name(), dps.At(d).Attributes().AsRaw(), dps.At(d).DoubleValue()))
					}
				}
			}
		}
	}
	return points, images, resAttrs
}

// TestCadvisorSandboxFoldsIntoThePodResourceOnEveryRuntime is the regression
// pin for the phantom pause resource. container="POD" is the dockershim and
// CRI-O convention and was never containerd's: there the sandbox is reported as
// container="" under its own cri-containerd scope, so the fold never fired, the
// pause container id resolved against nothing, and every pod grew a third,
// unresolved resource carrying container.image.name = the pause image.
func TestCadvisorSandboxFoldsIntoThePodResourceOnEveryRuntime(t *testing.T) {
	cases := []struct{ name, body string }{
		{"containerd/systemd", containerdCadvisorBody(systemdPodSlice(uid1))},
		{"containerd/systemd-kubelet-root", containerdCadvisorBody(kubeletSlicePodSlice(uid1))},
		{"containerd/cgroupfs", containerdCadvisorBody(cgroupfsPodSlice(uid1))},
		{"dockershim-crio/container=POD", legacyCadvisorBody()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			points, images, _ := cadvisorPoints(t, c.body, false)

			container := uid1 + "/" + appCID + "/app"
			pod := uid1 + "//"
			for key := range points {
				if key != container && key != pod {
					t.Errorf("phantom resource %q (image %q): the sandbox row must fold onto the pod resource %q, not mint its own",
						key, images[key], pod)
				}
			}
			if strings.Contains(images[pod], "pause") || strings.Contains(images[container], "pause") {
				t.Errorf("pause image leaked onto a resource: %v", images)
			}
			if len(points[container]) != 1 {
				t.Errorf("workload container points = %v, want just its own cpu row", points[container])
			}
			// Three rows land on the pod: the pod cgroup's cpu, the pause
			// container's cpu, and the pod-level network counter.
			if len(points[pod]) != 3 {
				t.Fatalf("pod resource points = %v, want the pod-cgroup cpu row, the pause cpu row and the network row", points[pod])
			}
			// No double-counting: the pause point and the pod-cgroup point are
			// two different cgroups of the same family on one resource, so they
			// must stay two series — identical attribute sets would make them one
			// series downstream and the two values would collide.
			var cpu []string
			for _, p := range points[pod] {
				if strings.HasPrefix(p, "container_cpu_usage_seconds_total|") {
					cpu = append(cpu, p)
				}
			}
			if len(cpu) != 2 || cpu[0] == cpu[1] {
				t.Fatalf("pod-cgroup and pause cpu points = %v, want two distinguishable points", cpu)
			}
			// The distinguisher is the sandbox row's `id`: the pod-cgroup row's
			// is elided as resource-redundant.
			if !strings.Contains(strings.Join(cpu, " "), pauseCID) {
				t.Errorf("the sandbox point must keep its cgroup id, its only distinguisher: %v", cpu)
			}
		})
	}
}

// Network counters are measured on the pod's sandbox network namespace. Which
// cgroup cadvisor hangs them off differs by runtime and version — the pod slice
// on the nodes captured for this fix, the pause scope elsewhere — so BOTH must
// reach the pod resource, and both must survive -cadvisor-rollups=false, which
// keeps genuinely pod-scoped families.
func TestCadvisorNetworkOnTheSandboxScopeReachesThePod(t *testing.T) {
	podSlice := systemdPodSlice(uid1)
	body := fmt.Sprintf(`# TYPE container_network_receive_bytes_total counter
container_network_receive_bytes_total{container="",id=%q,image=%q,interface="eth0",name=%q,namespace="ns1",pod="pod1"} 5000
`, containerdScope(podSlice, pauseCID), pauseImage, pauseCID)

	for _, disableRollups := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollups=%v", !disableRollups), func(t *testing.T) {
			points, images, _ := cadvisorPoints(t, body, disableRollups)
			pod := uid1 + "//"
			if len(points) != 1 || len(points[pod]) != 1 {
				t.Fatalf("network points = %v (images %v), want one point on the pod resource %q", points, images, pod)
			}
			if !strings.Contains(points[pod][0], "eth0") {
				t.Errorf("network point lost its interface label: %v", points[pod])
			}
		})
	}
}

// What -cadvisor-rollups=false does with the folded sandbox row is a DECISION,
// not a consequence, and this pins it: the row is dropped with the pod-cgroup
// row, so the pause container's own cpu/memory/fs is not reported at all in
// that mode.
//
// It is NOT dropped as a duplicate — unlike the pod-cgroup row it is a
// COMPONENT of the pod's sum, and nothing else reports it once the pod row is
// gone. It is dropped because the flag's contract is per-WORKLOAD-container
// series and the sandbox is pod infrastructure whose ~1 mcore no dashboard
// queries, distinguishable from the pod row only by the raw cgroup path in
// `id`; keeping it would also give back most of what the flag saves here, one
// point per family per pod. Anyone tempted to let it survive should change
// this test deliberately, not by accident.
func TestCadvisorSandboxCPURowObeysTheRollupFilter(t *testing.T) {
	points, images, _ := cadvisorPoints(t, containerdCadvisorBody(systemdPodSlice(uid1)), true)
	container := uid1 + "/" + appCID + "/app"
	pod := uid1 + "//"
	if len(points) != 2 {
		t.Fatalf("resources = %v (images %v), want only the workload container %q and the pod %q",
			keys(points), images, container, pod)
	}
	if len(points[container]) != 1 {
		t.Errorf("container points = %v, want the workload cpu row", points[container])
	}
	for _, p := range points[pod] {
		if strings.HasPrefix(p, "container_cpu_usage_seconds_total|") {
			t.Errorf("pod-level cpu row survived -cadvisor-rollups=false: %v", points[pod])
		}
	}
	if len(points[pod]) != 1 || !strings.HasPrefix(points[pod][0], "container_network_receive_bytes_total|") {
		t.Fatalf("pod points = %v, want only the pod-scoped network family", points[pod])
	}
}

// The structural rule stands alone: containerd's sandbox_image is configurable,
// so a fleet running its own mirror under its own name must not regrow the
// phantom. Detection may never REQUIRE the pause image.
func TestCadvisorSandboxWithACustomSandboxImageStillFolds(t *testing.T) {
	podSlice := systemdPodSlice(uid1)
	body := fmt.Sprintf(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{container="",id=%q,image="registry.internal:5000/infra/sandbox:1.2",name=%q,namespace="ns1",pod="pod1"} 0.03
`, containerdScope(podSlice, pauseCID), pauseCID)

	points, images, _ := cadvisorPoints(t, body, false)
	pod := uid1 + "//"
	if len(points) != 1 || len(points[pod]) != 1 {
		t.Fatalf("points = %v (images %v), want the sandbox row folded onto the pod resource %q", points, images, pod)
	}
}

// The image is the FALLBACK when the pod segment does not parse: a kubelet run
// with a custom --cgroup-root can produce a container scope whose ancestors
// carry no pod<uid>, and then only the image plus the pod label say the row is
// the sandbox. Without it that row would mint the phantom resource again.
func TestCadvisorSandboxUnderAnUnparseableCgroupRootFoldsByImage(t *testing.T) {
	body := fmt.Sprintf(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{container="app",id=%q,image="img:1",name=%q,namespace="ns1",pod="pod1"} 12.5
container_cpu_usage_seconds_total{container="",id=%q,image=%q,name=%q,namespace="ns1",pod="pod1"} 0.03
`, "/customroot/workload/cri-containerd-"+appCID+".scope", appCID,
		"/customroot/workload/cri-containerd-"+pauseCID+".scope", pauseImage, pauseCID)

	points, images, _ := cadvisorPoints(t, body, false)
	// The workload container still resolves by its own container id; the pause
	// row folds onto the pod, which podMeta resolves by namespace/name.
	container := uid1 + "/" + appCID + "/app"
	pod := uid1 + "//"
	if len(points) != 2 || len(points[container]) != 1 || len(points[pod]) != 1 {
		t.Fatalf("points = %v (images %v), want the pause row on the pod resource %q", points, images, pod)
	}
	if strings.Contains(images[pod], "pause") {
		t.Errorf("pause image leaked onto the pod resource: %q", images[pod])
	}
}

// A workload container is never mistaken for the sandbox: the rule keys on the
// ABSENCE of a container name under a pod slice, and cadvisor always carries
// io.kubernetes.container.name for a real CRI container. Losing this would fold
// every container of a pod onto one resource and merge their series.
func TestCadvisorWorkloadContainersAreNotSandboxes(t *testing.T) {
	podSlice := systemdPodSlice(uid1)
	// A second container of the same pod, unknown to the metadata service, whose
	// image name starts with "pause-" — the image is corroboration only and must
	// never override a present container name.
	body := fmt.Sprintf(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{container="app",id=%q,image="img:1",name=%q,namespace="ns1",pod="pod1"} 12.5
container_cpu_usage_seconds_total{container="sidecar",id=%q,image="registry.example.com/pause-service:1",name=%q,namespace="ns1",pod="pod1"} 3
`, containerdScope(podSlice, appCID), appCID, containerdScope(podSlice, ghostCID), ghostCID)

	points, _, _ := cadvisorPoints(t, body, false)
	if len(points) != 2 {
		t.Fatalf("two workload containers must keep two resources, got %v", points)
	}
	if len(points[uid1+"/"+appCID+"/app"]) != 1 || len(points[uid1+"/"+ghostCID+"/sidecar"]) != 1 {
		t.Fatalf("containers merged or mislabelled: %v", points)
	}
}

// impostorRow is a cgroup that an absence-based sandbox rule ("no container
// name under a pod slice") folds onto the pod's resource. Every shape here is
// one a kubelet's /metrics/cadvisor really carries.
type impostorRow struct {
	name   string
	labels []Label
	why    string
	// podCgroupShape marks a row whose path yields no container id at all, so it
	// is indistinguishable from the POD's OWN cgroup and shares the pod resource
	// however this predicate is written. That is a pre-existing property of
	// cgroupid parsing, not of the fold, and it is why such a row is excluded
	// from the strip test below — it is documented as a known gap on isSandbox.
	podCgroupShape bool
}

// impostors are the rows that must keep their pre-fold behaviour: their own
// resource, never the pod's. All but the last carry NO pod attribution, which
// is exactly what makes folding them destructive — see
// TestCadvisorImpostorRowsCannotStripThePodResource.
func impostors(podSlice string) []impostorRow {
	return []impostorRow{
		{
			name: "raw-handler-scope",
			// cadvisor's raw handler reports every cgroup it cannot resolve to a CRI
			// container — a container whose CRI lookup failed included. Same shape as
			// the sandbox, no pod attribution whatsoever.
			labels: []Label{
				{Name: "id", Value: containerdScope(podSlice, ghostCID)},
				{Name: "name", Value: ghostCID},
			},
			why: "cadvisor could not attribute it to a pod, so nothing says it is that pod's sandbox",
		},
		{
			name: "crio-conmon-scope",
			labels: []Label{
				{Name: "id", Value: podSlice + "/crio-conmon-" + ghostCID + ".scope"},
				{Name: "name", Value: "crio-conmon-" + ghostCID + ".scope"},
			},
			why: "conmon is an immediate child of the pod slice and parses to a container id, so only the missing pod attribution tells it apart",
		},
		{
			name: "kata-vmm-cgroup",
			labels: []Label{
				{Name: "id", Value: podSlice + "/kata_" + pauseCID},
				{Name: "name", Value: "kata_" + pauseCID},
			},
			why:            "a sandboxing runtime's VMM cgroup parked in the pod slice",
			podCgroupShape: true,
		},
		{
			name: "nested-helper-scope",
			// Pod-attributed, but the scope hangs below a helper slice rather than
			// directly under the pod's. The sandbox is always an immediate child.
			labels: []Label{
				{Name: "container", Value: ""},
				{Name: "id", Value: podSlice + "/helper.slice/cri-containerd-" + ghostCID + ".scope"},
				{Name: "namespace", Value: "ns1"},
				{Name: "pod", Value: "pod1"},
			},
			why: "nested below the pod's own children, where no runtime puts the sandbox",
		},
	}
}

// cadvisorRow renders one exposition line from a label set, so the impostor
// shapes are declared ONCE and drive both the predicate test and the end-to-end
// one.
func cadvisorRow(family string, labels []Label, value float64) string {
	var sb strings.Builder
	sb.WriteString(family)
	sb.WriteByte('{')
	for i, l := range labels {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%s=%q", l.Name, l.Value)
	}
	fmt.Fprintf(&sb, "} %v\n", value)
	return sb.String()
}

// TestIsSandboxIdentifiesTheSandboxPositively pins the predicate itself. The
// rule must key on evidence a sandbox HAS, never on what a workload container
// lacks: "a container cgroup under a pod slice with no container label" is
// satisfied by every cgroup cadvisor cannot attribute, and folding one of those
// rewrites the identity of the pod's whole cadvisor resource.
func TestIsSandboxIdentifiesTheSandboxPositively(t *testing.T) {
	podSlice := systemdPodSlice(uid1)
	pauseScope := containerdScope(podSlice, pauseCID)

	sandboxes := []struct {
		name   string
		labels []Label
	}{
		{"containerd/systemd", []Label{
			{Name: "container", Value: ""}, {Name: "id", Value: pauseScope},
			{Name: "image", Value: pauseImage}, {Name: "name", Value: pauseCID},
			{Name: "namespace", Value: "ns1"}, {Name: "pod", Value: "pod1"},
		}},
		{"containerd/cgroupfs", []Label{
			{Name: "container", Value: ""}, {Name: "id", Value: containerdScope(cgroupfsPodSlice(uid1), pauseCID)},
			{Name: "image", Value: pauseImage}, {Name: "namespace", Value: "ns1"}, {Name: "pod", Value: "pod1"},
		}},
		{"containerd/custom-sandbox-image", []Label{
			{Name: "container", Value: ""}, {Name: "id", Value: pauseScope},
			{Name: "image", Value: "registry.internal:5000/infra/sandbox:1.2"},
			{Name: "namespace", Value: "ns1"}, {Name: "pod", Value: "pod1"},
		}},
		{"dockershim-crio/container=POD", []Label{
			{Name: "container", Value: "POD"}, {Name: "id", Value: pauseScope},
			{Name: "image", Value: "k8s.gcr.io/pause:3.2"}, {Name: "namespace", Value: "ns1"}, {Name: "pod", Value: "pod1"},
		}},
		{"unparseable-cgroup-root/by-image", []Label{
			{Name: "container", Value: ""}, {Name: "id", Value: "/customroot/workload/cri-containerd-" + pauseCID + ".scope"},
			{Name: "image", Value: pauseImage}, {Name: "namespace", Value: "ns1"}, {Name: "pod", Value: "pod1"},
		}},
	}
	for _, c := range sandboxes {
		cb := &cadvisorBatcher{cgroupMemo: map[string]cgroupPair{}}
		ident := cb.identityOf(c.labels)
		if !ident.sandbox {
			t.Errorf("%s: not detected as the sandbox — it mints a phantom third resource per pod", c.name)
			continue
		}
		if ident.containerID != "" || ident.image != "" {
			t.Errorf("%s: the pause container id/image must be cleared so the row shares the pod resource, got %q/%q",
				c.name, ident.containerID, ident.image)
		}
	}

	notSandboxes := []struct {
		name   string
		labels []Label
		why    string
	}{
		{"workload-container", []Label{
			{Name: "container", Value: "app"}, {Name: "id", Value: containerdScope(podSlice, appCID)},
			{Name: "image", Value: "img:1"}, {Name: "namespace", Value: "ns1"}, {Name: "pod", Value: "pod1"},
		}, "it names its container"},
		{"workload-container/pause-ish-image", []Label{
			{Name: "container", Value: "sidecar"}, {Name: "id", Value: containerdScope(podSlice, ghostCID)},
			{Name: "image", Value: "registry.example.com/pause-service:1"},
			{Name: "namespace", Value: "ns1"}, {Name: "pod", Value: "pod1"},
		}, "the image is corroboration only and can never override a container name"},
		{"pod-cgroup-row", []Label{
			{Name: "container", Value: ""}, {Name: "id", Value: podSlice},
			{Name: "namespace", Value: "ns1"}, {Name: "pod", Value: "pod1"},
		}, "the pod's own cgroup is not a container cgroup"},
		{"standalone-container", []Label{
			{Name: "id", Value: "/system.slice/docker-" + ghostCID + ".scope"},
		}, "no pod slice above it at all"},
		{"container=POD/unattributed", []Label{
			{Name: "container", Value: "POD"}, {Name: "id", Value: containerdScope(podSlice, pauseCID)},
			{Name: "image", Value: pauseImage},
		}, "even the explicit convention must name the pod whose resource it would join"},
	}
	for _, imp := range impostors(podSlice) {
		notSandboxes = append(notSandboxes, struct {
			name   string
			labels []Label
			why    string
		}{imp.name, imp.labels, imp.why})
	}
	for _, c := range notSandboxes {
		cb := &cadvisorBatcher{cgroupMemo: map[string]cgroupPair{}}
		ident := cb.identityOf(c.labels)
		if ident.sandbox {
			t.Errorf("%s: folded onto the pod resource, but %s", c.name, c.why)
		}
		// Not folding must leave the row exactly as it was: its own container id
		// and image are what keep it on its own resource.
		var wantCID, wantImage string
		for _, l := range c.labels {
			switch l.Name {
			case "id":
				_, wantCID = cgroupid.Identity(l.Value)
			case "image":
				wantImage = l.Value
			}
		}
		if ident.containerID != wantCID || ident.image != wantImage {
			t.Errorf("%s: identity was rewritten (container id %q want %q, image %q want %q)",
				c.name, ident.containerID, wantCID, ident.image, wantImage)
		}
	}
}

// TestCadvisorImpostorRowsCannotStripThePodResource is the pin for the damage a
// too-broad fold does, which is strictly worse than the phantom resource the
// fold removes.
//
// scope() fills a resource from the FIRST identity that reaches its key, and a
// folded row is keyed exactly like the pod's own cgroup row. An impostor
// carrying no pod attribution therefore CREATES the pod resource without
// k8s.pod.name, k8s.namespace.name, the owner chain or service.name — the
// Prometheus job — and every container_cpu/memory series of that pod exports
// unidentified for the whole scrape. So the impostor is placed FIRST here.
func TestCadvisorImpostorRowsCannotStripThePodResource(t *testing.T) {
	podSlice := systemdPodSlice(uid1)
	const impostorValue = 99
	for _, imp := range impostors(podSlice) {
		if imp.podCgroupShape {
			continue // see impostorRow.podCgroupShape
		}
		t.Run(imp.name, func(t *testing.T) {
			const head = "# TYPE container_cpu_usage_seconds_total counter\n"
			body := head + cadvisorRow("container_cpu_usage_seconds_total", imp.labels, impostorValue) +
				strings.TrimPrefix(containerdCadvisorBody(podSlice), head)

			points, _, attrs := cadvisorPoints(t, body, false)
			pod := uid1 + "//"
			for _, want := range []struct{ key, value string }{
				{"k8s.pod.name", "pod1"},
				{"k8s.namespace.name", "ns1"},
				{"k8s.deployment.name", "dep1"},
				{"service.name", "dep1"},
			} {
				if got := attrs[pod][want.key]; got != want.value {
					t.Errorf("the pod's cadvisor resource lost %s (= %v, want %q): a row that names no pod folded onto it and named it instead\nresource = %v",
						want.key, got, want.value, attrs[pod])
				}
			}
			// The impostor keeps its pre-fold behaviour: its own resource, and its
			// point is not on the pod's.
			onPod, elsewhere := false, false
			for key, ps := range points {
				for _, p := range ps {
					if !strings.HasSuffix(p, fmt.Sprintf("|%v", float64(impostorValue))) {
						continue
					}
					if key == pod {
						onPod = true
					} else {
						elsewhere = true
					}
				}
			}
			if onPod {
				t.Errorf("the impostor row folded onto the pod resource: %v", points[pod])
			}
			if !elsewhere {
				t.Errorf("the impostor row vanished; it must keep its own resource: %v", points)
			}
			// The real sandbox still folds with an impostor in the scrape: the pod
			// keeps the pod-cgroup cpu row, the pause cpu row and the network row.
			if len(points[pod]) != 3 {
				t.Errorf("pod resource points = %v, want the pod-cgroup cpu row, the pause cpu row and the network row", points[pod])
			}
		})
	}
}

func TestIsPauseImage(t *testing.T) {
	cases := []struct {
		image string
		want  bool
	}{
		{"registry.k8s.io/pause:3.10", true},
		{"k8s.gcr.io/pause:3.2", true},
		{"gcr.io/google_containers/pause-amd64:3.1", true},
		{"rancher/mirrored-pause:3.6", true},
		{"mcr.microsoft.com/oss/kubernetes/pause:3.9", true},
		{"registry.internal:5000/pause@sha256:0123456789abcdef", true},
		{"pause", true},
		{"", false},
		{"img:1", false},
		{"registry.k8s.io/kube-proxy:v1.33.1", false},
		// The match is on the repository's LAST path segment: a registry host or
		// a namespace containing "pause" must not turn a workload into a sandbox.
		{"pause.example.com/myapp:1", false},
		{"registry.example.com/pause-images/myapp:1", false},
		{"registry.example.com/pauseless:1", false},
	}
	for _, c := range cases {
		if got := isPauseImage(c.image); got != c.want {
			t.Errorf("isPauseImage(%q) = %v, want %v", c.image, got, c.want)
		}
	}
}
