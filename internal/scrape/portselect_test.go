package scrape

import (
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/cli"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// strconvParsePort is parsePort's DEFINITION: what it was before it was
// hand-written, and what it must keep accepting, exactly.
func strconvParsePort(entry string) (int32, bool) {
	n, err := strconv.ParseInt(entry, 10, 32)
	if err != nil || n < 1 || n > 65535 {
		return 0, false
	}
	return int32(n), true
}

// parsePortEdgeCases are the inputs where a hand-written decimal reader is
// likeliest to part company with strconv: signs, leading zeros, the range
// boundaries, int32 overflow, and every non-decimal spelling ParseInt refuses
// in base 10.
var parsePortEdgeCases = []string{
	"", "+", "-", "++80", "+-80", "-+80", "80", "+80", "-80", "-0", "+0", "0",
	"00", "000080", "+000080", "1", "65535", "65536", "065535", "99999",
	"2147483647", "2147483648", "-2147483648", "99999999999999999999",
	"0x50", "0X50", "0o120", "0b1010000", "8_0", "80 ", " 80", "8 0", "80\t",
	"８０", "٨٠", "metrics", "http-metrics", "80a", "a80", "1e3", "80.0",
	strings.Repeat("0", 4096) + "80", strings.Repeat("9", 4096),
}

// parsePort is hand-written so a REJECTED entry costs no allocation, which is
// the whole reason it is not a strconv call — and the one thing that must not
// change with it is the accepted set: "+80" and "000080" are ports today, and
// a stricter reader would stop scraping them.
func TestParsePortAgreesWithStrconv(t *testing.T) {
	for _, in := range parsePortEdgeCases {
		gotN, gotOK := parsePort(in)
		wantN, wantOK := strconvParsePort(in)
		if gotN != wantN || gotOK != wantOK {
			t.Errorf("parsePort(%q) = (%d, %v), strconv says (%d, %v)", clipForLog(in), gotN, gotOK, wantN, wantOK)
		}
	}
	for n := -2; n <= 70000; n++ {
		in := strconv.Itoa(n)
		if gotN, gotOK := parsePort(in); gotOK != (n >= 1 && n <= 65535) || (gotOK && gotN != int32(n)) {
			t.Fatalf("parsePort(%q) = (%d, %v)", in, gotN, gotOK)
		}
	}
}

func FuzzParsePortAgreesWithStrconv(f *testing.F) {
	for _, in := range parsePortEdgeCases {
		f.Add(in)
	}
	f.Fuzz(func(t *testing.T, in string) {
		gotN, gotOK := parsePort(in)
		wantN, wantOK := strconvParsePort(in)
		if gotN != wantN || gotOK != wantOK {
			t.Fatalf("parsePort(%q) = (%d, %v), strconv says (%d, %v)", in, gotN, gotOK, wantN, wantOK)
		}
	})
}

// A rejected entry is the COMMON case — every named entry ("metrics") is one —
// so it must cost nothing; strconv's error path allocated a *NumError and a
// copy of the input per entry.
// portSink and portOKSink keep parsePort's results live, so the compiler cannot
// drop the call and make the allocation budget below vacuous.
var (
	portSink   int32
	portOKSink bool
)

func TestParsePortRejectionIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("allocation counts are meaningless under -race")
	}
	for _, in := range []string{"metrics", "x", "80a", "99999999", "-80", "80"} {
		if n := testing.AllocsPerRun(100, func() { portSink, portOKSink = parsePort(in) }); n != 0 {
			t.Errorf("parsePort(%q) allocates %.0f times, want 0", in, n)
		}
	}
}

func clipForLog(s string) string {
	if len(s) > 32 {
		return s[:32] + "..."
	}
	return s
}

// listEntries is the derivation's lazy reader and cli.SplitList the one every
// other comma list goes through; they must yield the same entries, or the
// derivation and /v1/explain (which still materialises the list) disagree
// about what an annotation says.
func TestListEntriesMatchesSplitList(t *testing.T) {
	for _, in := range []string{
		"", ",", " , ,", "a", "a,b", " a , b ", "a,,b", ",a,", "a, ,b", "\ta\t,\nb\n",
		"8," + strings.Repeat("8,", 100), "metrics, 9090 ,http",
	} {
		got := slices.Collect(listEntries(in))
		want := cli.SplitList(in)
		if len(got) != len(want) || (len(got) > 0 && !reflect.DeepEqual(got, want)) {
			t.Errorf("listEntries(%q) = %q, cli.SplitList = %q", in, got, want)
		}
	}
	// And an early stop is honoured: the derivation breaks out at its ceiling.
	n := 0
	for range listEntries("a,b,c,d") {
		if n++; n == 2 {
			break
		}
	}
	if n != 2 {
		t.Errorf("listEntries kept yielding after the consumer stopped: %d", n)
	}
}

// nestedLoopSelection is selectServicePorts as it was: every (entry, port)
// match in entry order, repeats included, through strconv. The new selection
// must equal it with each Service port's REPEATS removed (first occurrence
// kept) — the order ServiceTargets consumes it in, and therefore the targets.
func nestedLoopSelection(svc *services.Service) []int {
	var out []int
	for _, entry := range cli.SplitList(svc.Annotations[AnnotationPort]) {
		n, numeric := strconvParsePort(entry)
		for i, sp := range svc.Ports {
			if sp.Name == entry || (numeric && sp.Port == n) {
				out = append(out, i)
			}
		}
	}
	return out
}

// nestedLoopServiceTargets is ServiceTargets as it was, over the nested loop's
// selection (repeats and all) — the targets the fixed derivation must still
// serve, in the same order.
func nestedLoopServiceTargets(pod kubemeta.Pod, svc *services.Service) []kubemeta.ScrapeTarget {
	if svc.Annotations[AnnotationScrape] != "true" || !Scrapeable(pod) {
		return nil
	}
	scheme, path, ok := schemeAndPath(svc.Annotations)
	if !ok {
		return nil
	}
	selection := svc.Ports
	if ann := svc.Annotations[AnnotationPort]; strings.TrimSpace(ann) != "" {
		selection = nil
		for _, i := range nestedLoopSelection(svc) {
			selection = append(selection, svc.Ports[i])
		}
	}
	info := serviceInfo(svc)
	var targets []kubemeta.ScrapeTarget
	seen := map[int32]struct{}{}
	for _, sp := range selection {
		if len(targets) >= MaxPortsPerPod {
			break
		}
		port, ok := targetPodPort(pod, sp)
		if !ok {
			continue
		}
		if _, dup := seen[port]; dup {
			continue
		}
		seen[port] = struct{}{}
		t := makeTarget(pod, scheme, path, port)
		t.Source = "service"
		t.Service = info
		targets = append(targets, t)
	}
	return targets
}

// randomSelectionCase builds a Service and a pod out of a SMALL alphabet, so
// names and numbers collide constantly — repeated entries, ports sharing a
// name or a number, a name that is also a number's spelling — and port counts
// on both sides of indexServicePortsOver, so the linear scan and the index are
// both exercised.
func randomSelectionCase(r *rand.Rand) (kubemeta.Pod, *services.Service) {
	names := []string{"", "a", "b", "http", "metrics", "80", "0x50"}
	numbers := []int32{1, 80, 443, 8080, 9090, 65535}
	containerNames := []string{"metrics", "http", "a", "zz"}

	pod := basePod()
	pod.Containers = []kubemeta.Container{{Name: "app"}}
	for range r.IntN(6) {
		pod.Containers[0].Ports = append(pod.Containers[0].Ports, kubemeta.ContainerPort{
			Name: containerNames[r.IntN(len(containerNames))], Port: numbers[r.IntN(len(numbers))],
		})
	}

	svc := baseService()
	svc.Ports = nil
	nports := r.IntN(3 * indexServicePortsOver)
	for range nports {
		sp := services.Port{Name: names[r.IntN(len(names))], Port: numbers[r.IntN(len(numbers))]}
		switch r.IntN(3) {
		case 0:
			sp.TargetPortName = containerNames[r.IntN(len(containerNames))]
		case 1:
			sp.TargetPortNum = numbers[r.IntN(len(numbers))]
		}
		svc.Ports = append(svc.Ports, sp)
	}
	entries := []string{"a", "b", "http", "metrics", "80", "+80", "080", "-80", "443", "9090", "x", " ", ""}
	var ann []string
	for range r.IntN(40) {
		ann = append(ann, entries[r.IntN(len(entries))])
	}
	if r.IntN(8) != 0 {
		svc.Annotations[AnnotationPort] = strings.Join(ann, ",")
	}
	return pod, svc
}

// The selection keeps each Service port at most once, in the order the entries
// first name it, and goes through an index past indexServicePortsOver ports.
// Neither may change an answer: over randomised inputs built to collide, the
// selection must be the nested loop's with repeats removed, and ServiceTargets
// — through both ServiceTargets and a memoised ServiceDoor, the server's path —
// must serve exactly what the nested loop served.
func TestServicePortSelectionMatchesTheNestedLoop(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for i := range 20000 {
		pod, svc := randomSelectionCase(r)

		if ann := svc.Annotations[AnnotationPort]; strings.TrimSpace(ann) != "" {
			var want []services.Port
			picked := map[int]bool{}
			for _, j := range nestedLoopSelection(svc) {
				if !picked[j] {
					picked[j] = true
					want = append(want, svc.Ports[j])
				}
			}
			if got := selectServicePorts(svc); !reflect.DeepEqual(got, want) {
				t.Fatalf("case %d: annotation %q over ports %+v:\n selection %+v\n nested loop (deduped) %+v",
					i, ann, svc.Ports, got, want)
			}
		}

		want := nestedLoopServiceTargets(pod, svc)
		if got := ServiceTargets(pod, svc); !reflect.DeepEqual(got, want) {
			t.Fatalf("case %d: annotation %q over ports %+v:\n ServiceTargets %+v\n nested loop %+v",
				i, svc.Annotations[AnnotationPort], svc.Ports, got, want)
		}
		door := NewServiceDoor(svc)
		if got := door.Targets(pod); !reflect.DeepEqual(got, want) {
			t.Fatalf("case %d: a memoised ServiceDoor serves %+v, ServiceTargets %+v", i, got, want)
		}
	}
}

// hostilePortList is a prometheus.io/port value of n bytes (up to the
// per-value annotation ceiling) repeating one entry.
func hostilePortList(entry string, n int) string {
	var b strings.Builder
	for b.Len()+len(entry)+1 <= n {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(entry)
	}
	return b.String()
}

// A Service's prometheus.io/port list is tenant-authored and up to
// kubemeta.MaxAnnotationValueBytes long, and ServiceTargets runs once per
// (pod, matched Service) on every node-targets derivation. So its cost must be
// bounded by the Service's PORTS, never by the list's ENTRIES: an 8 KiB
// `8,8,...,8` used to cost ~3 MB and milliseconds per call to yield one target
// (the selection kept every repeat), and an 8 KiB `x,x,...` ~8,000 allocations
// (strconv's error path, once per rejected entry). Both must cost what a
// one-entry list costs.
func TestServiceTargetsCostIsBoundedByPortsNotEntries(t *testing.T) {
	if testrace.Enabled {
		t.Skip("allocation counts are meaningless under -race")
	}
	pod := basePod()
	delete(pod.Annotations, AnnotationScrape)
	svcWith := func(ann string) *services.Service {
		svc := baseService()
		svc.Ports = []services.Port{
			{Name: "a", Port: 8, TargetPortNum: 9090},
			{Name: "b", Port: 8, TargetPortNum: 9090},
			{Name: "c", Port: 8, TargetPortNum: 9090},
		}
		svc.Annotations[AnnotationPort] = ann
		return svc
	}
	allocs := func(ann string) float64 {
		svc := svcWith(ann)
		return testing.AllocsPerRun(20, func() { ServiceTargets(pod, svc) })
	}
	baseline := allocs("8")
	if got := len(ServiceTargets(pod, svcWith("8"))); got != 1 {
		t.Fatalf("baseline fixture serves %d targets, want 1", got)
	}
	for _, entry := range []string{"8", "x", "metrics", "99999"} {
		ann := hostilePortList(entry, kubemeta.MaxAnnotationValueBytes)
		if got := allocs(ann); got > baseline {
			t.Errorf("an %d-byte %q list costs %.0f allocs per ServiceTargets call against %.0f for one entry: "+
				"the cost must be bounded by the Service's ports, not the annotation's entries",
				len(ann), entry+","+entry+",...", got, baseline)
		}
	}
	// And the pod-annotation door, which walks the same kind of list.
	podAnn := basePod()
	podAnn.Annotations[AnnotationPort] = "9090"
	one := testing.AllocsPerRun(20, func() { PodTargets(podAnn) })
	for _, entry := range []string{"9090", "x"} {
		p := basePod()
		p.Annotations[AnnotationPort] = hostilePortList(entry, kubemeta.MaxAnnotationValueBytes)
		if got := testing.AllocsPerRun(20, func() { PodTargets(p) }); got > one {
			t.Errorf("a %q pod port list costs %.0f allocs per PodTargets call against %.0f for one entry", entry, got, one)
		}
	}
}

// Explain lists one verdict per DISTINCT annotation entry and folds a repeat
// into its first occurrence, because the derivation resolves a repeat to
// nothing new — and one formatted verdict per repeat of a ~4,000-entry
// tenant-authored list is what an unauthenticated request could make this
// materialise. A distinct entry must still get its own verdict.
func TestExplainFoldsRepeatedPortEntries(t *testing.T) {
	pod := basePod()
	pod.Annotations[AnnotationPort] = "9090, metrics,9090,9090,web"
	verdicts, _ := ExplainPodPorts(pod)
	if len(verdicts) != 3 {
		t.Fatalf("pod: %d verdicts for 3 distinct entries: %+v", len(verdicts), verdicts)
	}
	if !strings.Contains(verdicts[0].Note, "repeated 2 more time") {
		t.Errorf("pod: the repeated entry does not say it was folded: %+v", verdicts[0])
	}
	if len(verdicts[0].Ports) != 1 || verdicts[0].Ports[0] != 9090 {
		t.Errorf("pod: folding changed the first verdict's resolution: %+v", verdicts[0])
	}

	svc := baseService()
	svc.Annotations[AnnotationPort] = hostilePortList("metrics", kubemeta.MaxAnnotationValueBytes) + ",web"
	sv, _ := ExplainServicePorts(pod, svc)
	if len(sv) != 2 {
		t.Fatalf("service: %d verdicts for 2 distinct entries: %+v", len(sv), sv)
	}
	if !strings.Contains(sv[0].Note, "a repeat adds no target") || len(sv[0].Ports) == 0 {
		t.Errorf("service: the repeated entry lost its resolution or its fold note: %+v", sv[0])
	}
	// The explanation and the derivation still agree on what is served.
	if got, want := resolvedPorts(sv), len(ServiceTargets(pod, svc)); got != want {
		t.Errorf("service: explain claims %d resolving ports, the derivation serves %d", got, want)
	}
}

// prometheus-operator's Scheme enum admits HTTP/HTTPS as well as http/https,
// and its own SchemeHTTPS constant is the upper-case spelling. The monitor
// doors must fold it, or `scheme: HTTPS` is served as plaintext http:// — an
// up=0 target with tlsConfig silently unused and any credential sent in the
// clear. Anything else still falls back to http, and the annotation door stays
// case-sensitive (documented lower-case, like Prometheus' relabel regex).
func TestMonitorSchemeIsCaseInsensitive(t *testing.T) {
	pod := basePod()
	svc := monitorService()
	for _, tc := range []struct{ scheme, want string }{
		{"HTTPS", "https"}, {"Https", "https"}, {"https", "https"},
		{"HTTP", "http"}, {"http", "http"}, {"", "http"}, {"gopher", "http"}, {"HTTPSX", "http"},
	} {
		ep := servicemonitors.Endpoint{Port: "metrics", Scheme: tc.scheme}
		ts := MonitorTargets(pod, svc, "m", ep)
		if len(ts) != 1 || ts[0].Scheme != tc.want || !strings.HasPrefix(ts[0].URL, tc.want+"://") {
			t.Errorf("ServiceMonitor scheme %q: targets %+v, want %s://", tc.scheme, ts, tc.want)
		}
		if url, ok := MonitorTargetURL(pod, svc, ep); !ok || !strings.HasPrefix(url, tc.want+"://") {
			t.Errorf("ServiceMonitor scheme %q: MonitorTargetURL = %q, want %s://", tc.scheme, url, tc.want)
		}
		pts := PodMonitorTargets(pod, "pm", ep)
		if len(pts) != 1 || pts[0].Scheme != tc.want || !strings.HasPrefix(pts[0].URL, tc.want+"://") {
			t.Errorf("PodMonitor scheme %q: targets %+v, want %s://", tc.scheme, pts, tc.want)
		}
		if url, ok := PodMonitorTargetURL(pod, ep); !ok || !strings.HasPrefix(url, tc.want+"://") {
			t.Errorf("PodMonitor scheme %q: PodMonitorTargetURL = %q, want %s://", tc.scheme, url, tc.want)
		}
	}
	p := basePod()
	p.Annotations[AnnotationScheme] = "HTTPS"
	if ts := PodTargets(p); len(ts) == 0 || ts[0].Scheme != "http" {
		t.Errorf("the annotation door changed: prometheus.io/scheme=HTTPS gave %+v", ts)
	}
}

// BenchmarkSelectServicePortsWide is the entries x ports product the index
// removes: a ~4,000-entry list against a 1,000-port Service, both
// tenant-authored, where no entry matches so nothing cuts the walk short. The
// nested loop measured 31.8 ms per call at this shape, per (pod, Service).
func BenchmarkSelectServicePortsWide(b *testing.B) {
	svc := baseService()
	svc.Ports = nil
	for i := range 1000 {
		svc.Ports = append(svc.Ports, services.Port{Name: "p" + strconv.Itoa(i), Port: int32(10000 + i)})
	}
	svc.Annotations[AnnotationPort] = hostilePortList("x", kubemeta.MaxAnnotationValueBytes)
	b.ReportAllocs()
	for b.Loop() {
		selectServicePorts(svc)
	}
}
