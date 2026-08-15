package scrape

import (
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// pod builds the pod half of a target: a namespace, a name and — the half that
// decides the JOB — a workload owner. deployment == "" leaves the pod bare, so
// attrs.ServiceName falls back to the pod name.
func pod(namespace, name, deployment string) kubemeta.Pod {
	p := kubemeta.Pod{Namespace: namespace, Name: name}
	if deployment != "" {
		p.Owners = []kubemeta.Owner{
			{Kind: "ReplicaSet", Name: deployment + "-abc"},
			{Kind: "Deployment", Name: deployment},
		}
	}
	return p
}

// tgt is a target of one pod of the "web" Deployment in "default": the shape
// nearly every case here varies only the URL and address of.
func tgt(url, address, source, monitor string) kubemeta.ScrapeTarget {
	return podTgt(pod("default", "web-1", "web"), url, address, source, monitor)
}

func podTgt(p kubemeta.Pod, url, address, source, monitor string) kubemeta.ScrapeTarget {
	return kubemeta.ScrapeTarget{URL: url, Address: address, Source: source, Monitor: monitor, Pod: p}
}

func collisions(targets []kubemeta.ScrapeTarget) []InstanceCollision {
	var s InstanceScan
	return s.Collisions(targets)
}

// The defect this file exists for: the dedup keys by URL, the exported identity
// is (job, host:port), and two paths on one container port are therefore two
// targets with one identity. Nothing named the pair before this did.
func TestTwoPathsOnOnePortCollide(t *testing.T) {
	got := collisions([]kubemeta.ScrapeTarget{
		tgt("http://10.9.9.9:9090/actuator/prometheus", "10.9.9.9:9090", "service", ""),
		tgt("http://10.9.9.9:9090/metrics", "10.9.9.9:9090", "pod", ""),
	})
	if len(got) != 1 {
		t.Fatalf("collisions = %+v, want exactly one group", got)
	}
	if got[0].Address != "10.9.9.9:9090" {
		t.Errorf("address = %q, want 10.9.9.9:9090", got[0].Address)
	}
	if got[0].Job != "default/web" {
		t.Errorf("job = %q, want the workload's default/web — the other half of the identity", got[0].Job)
	}
	// Served order, so the log line reads in the order the response does.
	want := []string{"http://10.9.9.9:9090/actuator/prometheus", "http://10.9.9.9:9090/metrics"}
	if len(got[0].Targets) != 2 ||
		got[0].Targets[0].URL != want[0] || got[0].Targets[1].URL != want[1] {
		t.Errorf("group = %+v, want the two URLs in served order %v", got[0].Targets, want)
	}
	if got[0].Targets[0].Source != "service" || got[0].Targets[1].Source != "pod" {
		t.Errorf("group lost the sources that say which declaration produced each: %+v", got[0].Targets)
	}
}

// Scheme is the other half of the same defect: https://host:port/metrics and
// http://host:port/metrics are two URLs, one host:port, one exported instance.
func TestTwoSchemesOnOnePortCollide(t *testing.T) {
	got := collisions([]kubemeta.ScrapeTarget{
		tgt("http://10.9.9.9:9090/metrics", "10.9.9.9:9090", "pod", ""),
		tgt("https://10.9.9.9:9090/metrics", "10.9.9.9:9090", "servicemonitor", "default/sm"),
	})
	if len(got) != 1 || len(got[0].Targets) != 2 {
		t.Fatalf("collisions = %+v, want one group of two", got)
	}
	if got[0].Targets[1].Monitor != "default/sm" {
		t.Errorf("group lost the monitor that declared the second URL: %+v", got[0].Targets[1])
	}
}

// THE STRONGEST FORM, and the one a pod-scoped scan could not see while its own
// doc asserted it could not happen: two hostNetwork REPLICAS of one workload,
// annotated for one port. They share the node's address, and — being replicas —
// one Deployment, so they share the job too. Their URLs are identical, so not
// even url.full tells the two exports apart.
//
// The port needs no bind to collide: it comes from the annotation.
func TestTwoHostNetworkReplicasOfOneWorkloadCollide(t *testing.T) {
	got := collisions([]kubemeta.ScrapeTarget{
		podTgt(pod("default", "web-1", "web"), "http://10.0.0.5:9100/metrics", "10.0.0.5:9100", "pod", ""),
		podTgt(pod("default", "web-2", "web"), "http://10.0.0.5:9100/metrics", "10.0.0.5:9100", "pod", ""),
	})
	if len(got) != 1 || len(got[0].Targets) != 2 {
		t.Fatalf("collisions = %+v, want one group of two", got)
	}
	if got[0].Job != "default/web" || got[0].Address != "10.0.0.5:9100" {
		t.Errorf("group identity = (%q, %q), want (default/web, 10.0.0.5:9100)", got[0].Job, got[0].Address)
	}
	// URL, Source and Monitor are identical here, so the pod is the ONLY thing
	// that distinguishes the two members: without it the report is one line
	// printed twice, naming neither pod.
	if got[0].Targets[0].Pod != "default/web-1" || got[0].Targets[1].Pod != "default/web-2" {
		t.Errorf("members do not name their pods: %+v", got[0].Targets)
	}
}

// A StatefulSet owns its pods DIRECTLY, so its replicas have stable distinct
// names and still one workload — one job. The owner chain shape must not be
// what the detection depends on.
func TestTwoHostNetworkStatefulSetPodsCollide(t *testing.T) {
	sts := func(name string) kubemeta.Pod {
		return kubemeta.Pod{Namespace: "default", Name: name,
			Owners: []kubemeta.Owner{{Kind: "StatefulSet", Name: "db"}}}
	}
	got := collisions([]kubemeta.ScrapeTarget{
		podTgt(sts("db-0"), "http://10.0.0.5:9100/metrics", "10.0.0.5:9100", "pod", ""),
		podTgt(sts("db-1"), "http://10.0.0.5:9100/metrics", "10.0.0.5:9100", "pod", ""),
	})
	if len(got) != 1 || got[0].Job != "default/db" {
		t.Fatalf("collisions = %+v, want one group under job default/db", got)
	}
}

// ...and the decision that bounds all of the above: a collision is the same
// instance AND the same job. Two DIFFERENT workloads sharing a node address on
// one annotated port export up{job="default/web"} and up{job="default/other"},
// which are two series — nothing overwrites anything, and the warning's own
// words ("their samples collide") would be false. Their real fault, one URL
// fetched twice under two jobs, is visible in the data; this one is not, which
// is the whole reason it is reported.
func TestTwoWorkloadsSharingANodeAddressDoNotCollide(t *testing.T) {
	got := collisions([]kubemeta.ScrapeTarget{
		podTgt(pod("default", "web-1", "web"), "http://10.0.0.5:9100/metrics", "10.0.0.5:9100", "pod", ""),
		podTgt(pod("default", "other-1", "other"), "http://10.0.0.5:9100/metrics", "10.0.0.5:9100", "pod", ""),
	})
	if got != nil {
		t.Errorf("collisions = %+v, want none: two workloads on one address export two jobs", got)
	}
}

// The namespace is half the job, so the same workload name in two namespaces is
// two jobs — the hostNetwork case again, one level up.
func TestSameWorkloadNameInTwoNamespacesDoesNotCollide(t *testing.T) {
	got := collisions([]kubemeta.ScrapeTarget{
		podTgt(pod("a", "web-1", "web"), "http://10.0.0.5:9100/metrics", "10.0.0.5:9100", "pod", ""),
		podTgt(pod("b", "web-1", "web"), "http://10.0.0.5:9100/metrics", "10.0.0.5:9100", "pod", ""),
	})
	if got != nil {
		t.Errorf("collisions = %+v, want none: service.namespace is half the job", got)
	}
}

// Two BARE pods (no workload owner) fall back to the pod NAME for service.name,
// so they are two jobs. This is the same rule as the two-workloads case and is
// pinned separately because it is the fallback arm of attrs.ServiceName.
func TestTwoBarePodsSharingAnAddressDoNotCollide(t *testing.T) {
	got := collisions([]kubemeta.ScrapeTarget{
		podTgt(pod("default", "static-a", ""), "http://10.0.0.5:9100/metrics", "10.0.0.5:9100", "pod", ""),
		podTgt(pod("default", "static-b", ""), "http://10.0.0.5:9100/metrics", "10.0.0.5:9100", "pod", ""),
	})
	if got != nil {
		t.Errorf("collisions = %+v, want none: a bare pod's job is its own name", got)
	}
}

// Targets on DIFFERENT ports are the ordinary multi-port pod
// (prometheus.io/port: "8080,9100") and must not be reported: their exported
// instances differ, which is exactly what host:port buys.
func TestDifferentPortsDoNotCollide(t *testing.T) {
	got := collisions([]kubemeta.ScrapeTarget{
		tgt("http://10.9.9.9:8080/metrics", "10.9.9.9:8080", "pod", ""),
		tgt("http://10.9.9.9:9100/metrics", "10.9.9.9:9100", "pod", ""),
	})
	if got != nil {
		t.Errorf("collisions = %+v, want none for two distinct ports", got)
	}
}

// Three paths on one port are ONE group of three, not three groups or a group
// per pair: the operator has one problem, on one address.
func TestThreeWayCollisionIsOneGroup(t *testing.T) {
	got := collisions([]kubemeta.ScrapeTarget{
		tgt("http://10.9.9.9:9090/a", "10.9.9.9:9090", "pod", ""),
		tgt("http://10.9.9.9:9090/b", "10.9.9.9:9090", "service", ""),
		tgt("http://10.9.9.9:9090/c", "10.9.9.9:9090", "servicemonitor", "default/sm"),
	})
	if len(got) != 1 {
		t.Fatalf("collisions = %+v, want one group", got)
	}
	if len(got[0].Targets) != 3 {
		t.Errorf("group holds %d targets, want all 3: %+v", len(got[0].Targets), got[0].Targets)
	}
}

// Two independent collisions on two ports are two groups.
func TestTwoCollidingPortsAreTwoGroups(t *testing.T) {
	got := collisions([]kubemeta.ScrapeTarget{
		tgt("http://10.9.9.9:8080/a", "10.9.9.9:8080", "pod", ""),
		tgt("http://10.9.9.9:9100/a", "10.9.9.9:9100", "pod", ""),
		tgt("http://10.9.9.9:8080/b", "10.9.9.9:8080", "service", ""),
		tgt("http://10.9.9.9:9100/b", "10.9.9.9:9100", "service", ""),
	})
	if len(got) != 2 {
		t.Fatalf("collisions = %+v, want two groups", got)
	}
	if got[0].Address != "10.9.9.9:8080" || got[1].Address != "10.9.9.9:9100" {
		t.Errorf("groups = %q/%q, want them in first-target order", got[0].Address, got[1].Address)
	}
}

func TestNoTargetsAndOneTargetCollideWithNothing(t *testing.T) {
	if got := collisions(nil); got != nil {
		t.Errorf("collisions(nil) = %+v, want none", got)
	}
	one := []kubemeta.ScrapeTarget{tgt("http://10.9.9.9:9090/metrics", "10.9.9.9:9090", "pod", "")}
	if got := collisions(one); got != nil {
		t.Errorf("collisions(one target) = %+v, want none", got)
	}
}

// A reused scan must not remember the previous call's targets: the same scratch
// serves every pod of a request and every request of a process.
func TestScanScratchIsNotCarriedBetweenCalls(t *testing.T) {
	var s InstanceScan
	colliding := []kubemeta.ScrapeTarget{
		tgt("http://10.9.9.9:9090/a", "10.9.9.9:9090", "pod", ""),
		tgt("http://10.9.9.9:9090/b", "10.9.9.9:9090", "service", ""),
	}
	if got := s.Collisions(colliding); len(got) != 1 {
		t.Fatalf("first scan = %+v, want one group", got)
	}
	clean := []kubemeta.ScrapeTarget{
		tgt("http://10.9.9.9:9090/a", "10.9.9.9:9090", "pod", ""),
		tgt("http://10.9.9.9:9091/b", "10.9.9.9:9091", "service", ""),
	}
	if got := s.Collisions(clean); got != nil {
		t.Errorf("second scan = %+v, want none — the scratch leaked the first call's targets", got)
	}
	if got := s.Collisions(colliding); len(got) != 1 || len(got[0].Targets) != 2 {
		t.Errorf("third scan = %+v, want the same one group of two", got)
	}
}

// The answer "no collision" is what essentially every node gets, on a path that
// runs per targets request per agent per scrape cycle. It must cost nothing
// once the scan's scratch exists — which is what makes the scratch worth having.
func TestInstanceCollisionsIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector adds bookkeeping allocations")
	}
	targets := nodeTargetFixture(110)
	var s InstanceScan
	var sink []InstanceCollision
	// AllocsPerRun calls f once as a warm-up, which is where the scan's one map
	// is built; every measured run reuses it.
	if n := testing.AllocsPerRun(50, func() { sink = s.Collisions(targets) }); n != 0 {
		t.Errorf("Collisions allocated %v times on the no-collision path, want 0", n)
	}
	if sink != nil {
		t.Fatalf("fixture collides: %+v", sink)
	}
}

// nodeTargetFixture is a node's worth of NON-colliding targets: n pods, each
// with its own IP, most with one target and every fifth with a second port.
func nodeTargetFixture(pods int) []kubemeta.ScrapeTarget {
	out := make([]kubemeta.ScrapeTarget, 0, pods+pods/5)
	for i := range pods {
		p := pod("default", "web-"+itoa(i), "web-"+itoa(i%17))
		ip := "10.244.1." + itoa(i)
		out = append(out, podTgt(p, "http://"+ip+":9090/metrics", ip+":9090", "pod", ""))
		if i%5 == 0 {
			out = append(out, podTgt(p, "http://"+ip+":9100/metrics", ip+":9100", "pod", ""))
		}
	}
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [8]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	return string(b[n:])
}

// BenchmarkInstanceScan reports the per-request cost at two node sizes: a
// standard node's 110 pods, and the busy node (many multi-port pods, several
// monitors each) the pairwise scan this replaced could not have carried.
func BenchmarkInstanceScan(b *testing.B) {
	for _, n := range []int{110, 420} {
		targets := nodeTargetFixture(n)
		b.Run(itoa(len(targets))+"targets", func(b *testing.B) {
			var s InstanceScan
			s.Collisions(targets)
			b.ReportAllocs()
			for b.Loop() {
				_ = s.Collisions(targets)
			}
		})
	}
}
