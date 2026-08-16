// Command nhexporter is an e2e FIXTURE: a scrape target serving a Prometheus
// NATIVE (sparse) histogram, so hack/e2e.sh can exercise the agent's protobuf
// scrape path against a real exposition rather than a hand-built fake.
//
// Why a fixture is needed at all: client_golang emits native-histogram fields
// ONLY over the protobuf exposition, and promhttp negotiates that format from
// the scraper's Accept header — which is exactly what kubescrape sends when
// -scrape-native-histograms is on. No busybox one-liner can produce that, and
// the conversion under test (Prometheus schema/spans/deltas -> OTLP
// exponential histogram scale/offset/bucketCounts) is the kind of arithmetic
// that unit tests can pin but only a live scrape can prove end to end.
//
// Alongside the native histogram it exposes a CLASSIC histogram, a counter
// carrying an exemplar, a gauge and a summary, so ONE target proves the
// protobuf path converts every family kind — and that -scrape-exemplars is
// honoured there too, not silently dropped.
//
// FORCE_PROTO=1 makes it answer protobuf whatever the scraper asked for. That
// is the MISBEHAVING target the agent must refuse when the operator has not
// enabled -scrape-native-histograms: the decode materialises the whole message
// and is gzip-amplified, so the format is the operator's choice, never the
// target's.
//
// It lives in the main module because client_golang is already a direct
// dependency (internal/obs), so the fixture costs no new deps and is covered
// by `make vet` and `make lint` like everything else.
package main

import (
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)

	// The NATIVE histogram: a bucket factor turns on sparse buckets, which
	// client_golang serialises only into the protobuf exposition.
	native := factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "e2e_native_latency_seconds",
		Help:                            "Native (sparse) histogram of request latency.",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  160,
		NativeHistogramMinResetDuration: time.Hour,
		NativeHistogramZeroThreshold:    1e-6,
	}, []string{"route"})

	// A CLASSIC histogram on the same target: the protobuf path must convert
	// it through the same converter the text path uses.
	classic := factory.NewHistogram(prometheus.HistogramOpts{
		Name:    "e2e_classic_latency_seconds",
		Help:    "Classic bucketed histogram of request latency.",
		Buckets: []float64{0.01, 0.1, 0.5, 1, 5},
	})

	counter := factory.NewCounterVec(prometheus.CounterOpts{
		Name: "e2e_native_requests_total",
		Help: "Requests observed by the native-histogram exporter.",
	}, []string{"code"})

	gauge := factory.NewGauge(prometheus.GaugeOpts{
		Name: "e2e_native_inflight",
		Help: "In-flight requests.",
	})

	summary := factory.NewSummary(prometheus.SummaryOpts{
		Name:       "e2e_native_size_bytes",
		Help:       "Summary of response sizes.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01},
	})

	go func() {
		r := rand.New(rand.NewSource(1))
		for i := 0; ; i++ {
			// A wide spread so the sparse buckets actually populate across
			// several powers of two (that is what makes scale/offset
			// non-trivial on the OTLP side).
			v := math.Abs(r.NormFloat64())*0.15 + 0.001
			native.WithLabelValues("/api").Observe(v)
			native.WithLabelValues("/health").Observe(v / 10)
			classic.Observe(v)
			// Exemplars ride the protobuf exposition too, under the same
			// -scrape-exemplars gate as the text path.
			if o, ok := counter.WithLabelValues("200").(prometheus.ExemplarAdder); ok {
				o.AddWithExemplar(1, prometheus.Labels{
					"trace_id": "abcdef0123456789abcdef0123456789",
				})
			} else {
				counter.WithLabelValues("200").Inc()
			}
			if i%17 == 0 {
				counter.WithLabelValues("500").Inc()
			}
			gauge.Set(float64(i % 7))
			summary.Observe(v * 1000)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// promhttp negotiates protobuf-delimited when the scraper asks for it.
	inner := promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true})
	if os.Getenv("FORCE_PROTO") == "1" {
		// A MISBEHAVING target: answer protobuf whatever the scraper asked
		// for. This is the shape the agent must refuse when the operator did
		// not enable -scrape-native-histograms — the decode is unbounded and
		// gzip-amplified, and the TARGET must not get to choose it.
		http.Handle("/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Header.Set("Accept", "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited")
			inner.ServeHTTP(w, r)
		}))
	} else {
		http.Handle("/metrics", inner)
	}
	log.Println("nhexporter listening on :9400/metrics")
	srv := &http.Server{Addr: ":9400", ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
