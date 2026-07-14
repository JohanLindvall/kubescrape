package promscrape

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/promparse"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// k8sScrapeBody synthesizes a typical Kubernetes workload exposition: repeated
// namespace/pod labels, counters, gauges and histograms with 12 buckets.
func k8sScrapeBody(series int) string {
	var sb strings.Builder
	sb.WriteString("# TYPE http_requests_total counter\n")
	for i := 0; i < series; i++ {
		fmt.Fprintf(&sb, "http_requests_total{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",method=\"GET\",code=\"200\",path=\"/api/v1/orders\"} %d\n", i%40, i*7)
	}
	sb.WriteString("# TYPE process_resident_memory_bytes gauge\n")
	for i := 0; i < series/4; i++ {
		fmt.Fprintf(&sb, "process_resident_memory_bytes{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\"} %d\n", i%40, 100000000+i)
	}
	sb.WriteString("# TYPE http_request_duration_seconds histogram\n")
	bounds := []string{"0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf"}
	for i := 0; i < series/8; i++ {
		for bi, le := range bounds {
			fmt.Fprintf(&sb, "http_request_duration_seconds_bucket{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",handler=\"/api\",le=\"%s\"} %d\n", i%40, le, (bi+1)*10)
		}
		fmt.Fprintf(&sb, "http_request_duration_seconds_sum{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",handler=\"/api\"} 42.5\n", i%40)
		fmt.Fprintf(&sb, "http_request_duration_seconds_count{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",handler=\"/api\"} 120\n", i%40)
	}
	return sb.String()
}

// ksmSplitBody synthesizes a kube-state-metrics style exposition: family-major
// order, several rows per object for the phase-style families.
func ksmSplitBody(pods int) string {
	var sb strings.Builder
	sb.WriteString("# TYPE kube_pod_info gauge\n")
	for i := 0; i < pods; i++ {
		fmt.Fprintf(&sb, "kube_pod_info{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",uid=\"0a1b2c3d-1111-2222-3333-4444555%05d\",node=\"node9\"} 1\n", i, i)
	}
	sb.WriteString("# TYPE kube_pod_status_phase gauge\n")
	for i := 0; i < pods; i++ {
		for _, phase := range []string{"Pending", "Running", "Succeeded", "Failed", "Unknown"} {
			fmt.Fprintf(&sb, "kube_pod_status_phase{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",uid=\"0a1b2c3d-1111-2222-3333-4444555%05d\",phase=\"%s\"} 0\n", i, i, phase)
		}
	}
	return sb.String()
}

// BenchmarkSplitConvert measures the splitter routing path: parse -> convert
// -> per-object resources.
func BenchmarkSplitConvert(b *testing.B) {
	input := ksmSplitBody(200)
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodName: "ksm-.+"},
		Rules: []SplitRule{{
			Metrics: `kube_pod_.+`,
			GroupBy: map[string]string{
				"namespace": "k8s.namespace.name", "pod": "k8s.pod.name", "uid": "k8s.pod.uid",
			},
		}},
	}})
	if err != nil {
		b.Fatal(err)
	}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: time.Second,
		Targets: staticTargets{}, Exporter: &captureExporter{},
		Splitters: sp, Kubelet: KubeletConfig{Meta: &fakeMetaSource{}},
		StartTime: time.Unix(1, 0),
	})
	target := testTarget("http://ksm:8080/metrics")
	target.Pod.Name = "ksm-abc"
	var points int
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb := newSplitBatcher(s, context.Background(), target, sp[0], time.Unix(2, 0))
		conv := newConverter(cb, nil)
		p := promparse.Get(promparse.Options{MaxLineBytes: 1 << 20})
		_, err := p.Parse(strings.NewReader(input), func(smp Sample) error {
			_ = conv.add(smp)
			return nil
		})
		promparse.Put(p)
		if err != nil {
			b.Fatal(err)
		}
		_ = conv.finish()
		points = cb.count()
	}
	if points == 0 {
		b.Fatal("no points")
	}
}

// cadvisorBenchBody synthesizes a cadvisor exposition: family-major order,
// per-container rows with cgroup ids, plus a per-device family.
func cadvisorBenchBody(containers int) string {
	var sb strings.Builder
	cg := func(i int) string {
		return fmt.Sprintf("/kubepods/burstable/pod0a1b2c3d-1111-2222-3333-4444555%05d/d4f00c1e8a2b4c5d6e7f80912a3b4c5d6e7f80912a3b4c5d6e7f80912a3%05d", i, i)
	}
	sb.WriteString("# TYPE container_cpu_usage_seconds_total counter\n")
	for i := 0; i < containers; i++ {
		fmt.Fprintf(&sb, "container_cpu_usage_seconds_total{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",id=\"%s\",image=\"img:1\"} 12.5\n", i, cg(i))
	}
	sb.WriteString("# TYPE container_fs_usage_bytes gauge\n")
	for i := 0; i < containers; i++ {
		for _, dev := range []string{"/dev/sda1", "/dev/sda2", "overlay"} {
			fmt.Fprintf(&sb, "container_fs_usage_bytes{namespace=\"prod-payments\",pod=\"payments-6f7b9c%03d\",container=\"app\",id=\"%s\",device=\"%s\"} 4096\n", i, cg(i), dev)
		}
	}
	return sb.String()
}

// BenchmarkCadvisorConvert measures the cadvisor routing path: parse ->
// identity from the cgroup id -> per-pod/container resources.
func BenchmarkCadvisorConvert(b *testing.B) {
	input := cadvisorBenchBody(100)
	for _, rollups := range []bool{true, false} {
		name := "rollups"
		if !rollups {
			name = "norollups"
		}
		b.Run(name, func(b *testing.B) {
			s := New(Config{
				Node: "node1", Interval: time.Hour, Timeout: time.Second,
				Targets: staticTargets{}, Exporter: &captureExporter{},
				Kubelet:   KubeletConfig{Meta: &fakeMetaSource{}, DisableRollups: !rollups},
				StartTime: time.Unix(1, 0),
			})
			var points int
			b.SetBytes(int64(len(input)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cb := newCadvisorBatcher(s, time.Unix(2, 0), context.Background())
				conv := newConverter(cb, nil)
				p := promparse.Get(promparse.Options{MaxLineBytes: 1 << 20})
				_, err := p.Parse(strings.NewReader(input), func(smp Sample) error {
					_ = conv.add(smp)
					return nil
				})
				promparse.Put(p)
				if err != nil {
					b.Fatal(err)
				}
				_ = conv.finish()
				points = cb.count()
			}
			if points == 0 {
				b.Fatal("no points")
			}
		})
	}
}

// BenchmarkConvertScrape measures the full parse -> filter -> convert -> OTLP
// pipeline for a typical Kubernetes exposition.
func BenchmarkConvertScrape(b *testing.B) {
	input := k8sScrapeBody(4000)
	filter, err := newMetricFilter([]FilterRule{
		{Action: "keep", Metrics: "http_request_duration_seconds_bucket", Labels: map[string]string{"handler": "/api"}},
		{Action: "drop", Metrics: "(go_|promhttp_|process_start_).+"},
	})
	if err != nil {
		b.Fatal(err)
	}
	var points int
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bt := newBatcher(func(pcommon.Resource) {}, 1<<30, time.Unix(1, 0), time.Unix(2, 0))
		conv := newConverter(bt, nil)
		fs := filter.session()
		p := promparse.Get(promparse.Options{MaxLineBytes: 1 << 20}) // the production path: pooled parser + reader
		_, err := p.Parse(strings.NewReader(input), func(s Sample) error {
			if !fs.Keep(s.Name, s.Labels) {
				return nil
			}
			_ = conv.add(s)
			return nil
		})
		promparse.Put(p)
		if err != nil {
			b.Fatal(err)
		}
		_ = conv.finish()
		points = bt.count()
	}
	if points == 0 {
		b.Fatal("no points")
	}
}
