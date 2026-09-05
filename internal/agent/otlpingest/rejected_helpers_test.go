package otlpingest

import "github.com/JohanLindvall/kubescrape/internal/obs"

// ingestRejectedTotal sums kubescrape_ingest_rejected_total over its three
// reasons, for the tests that assert a refusal happened without caring which
// bound refused; the per-bound tests read the label directly.
func ingestRejectedTotal() float64 {
	var n float64
	for _, reason := range []string{shedInFlight, shedBuffer, shedDecoded} {
		n += obs.IngestRejected.WithLabelValues(reason).Value()
	}
	return n
}
