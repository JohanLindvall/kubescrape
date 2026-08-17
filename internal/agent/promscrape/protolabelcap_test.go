package promscrape

import (
	"strconv"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

// The protobuf front runs the same O(n²) duplicate-name scan as the text front,
// and it is the worse of the two: the whole message is resident before the
// first comparison, so no interleaved socket read gives the scrape deadline a
// place to land. Uncapped, 120k labels in one metric (1.38 MiB — well inside
// maxProtoMessageBytes) measured 80s against a 1s timeout, and cycle() waits
// for every scrape it starts.
func TestProtoLabelsAreCapped(t *testing.T) {
	mk := func(n int) *dto.Metric {
		m := &dto.Metric{Label: make([]*dto.LabelPair, 0, n)}
		for i := 0; i < n; i++ {
			m.Label = append(m.Label, &dto.LabelPair{
				Name:  proto.String("l" + strconv.Itoa(i)),
				Value: proto.String("v"),
			})
		}
		return m
	}

	if _, ok := protoLabels(mk(maxLabelsPerSample)); !ok {
		t.Errorf("a metric with exactly %d labels was refused; the cap must be inclusive", maxLabelsPerSample)
	}
	if _, ok := protoLabels(mk(maxLabelsPerSample + 1)); ok {
		t.Errorf("a metric with %d labels was accepted; it must be malformed past the cap", maxLabelsPerSample+1)
	}

	// And the refusal must be CHEAP — the whole point is that the quadratic
	// scan never runs. 200k labels is ~1.5x the size that measured 80s.
	start := time.Now()
	if _, ok := protoLabels(mk(200_000)); ok {
		t.Fatal("a 200k-label metric was accepted")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("refusing a 200k-label metric took %v; the cap is not short-circuiting the scan", d)
	}
}
