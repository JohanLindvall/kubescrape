package promparse_test

import (
	"fmt"
	"strings"

	"github.com/JohanLindvall/kubescrape/pkg/promparse"
)

func Example() {
	body := `# TYPE http_requests_total counter
http_requests_total{code="200"} 1027
http_requests_total{code="500"} 3
`
	p := promparse.New(promparse.Options{})
	malformed, err := p.Parse(strings.NewReader(body), func(s promparse.Sample) error {
		// s (including Labels and Exemplar) is only valid inside the callback.
		fmt.Printf("%s %v = %v\n", s.Name, s.Labels, s.Value)
		return nil
	})
	fmt.Println("malformed:", malformed, "err:", err)
	// Output:
	// http_requests_total [{code 200}] = 1027
	// http_requests_total [{code 500}] = 3
	// malformed: 0 err: <nil>
}

// On a hot path, pooled parsers keep the intern tables and read buffer warm
// across scrapes.
func ExampleGet() {
	p := promparse.Get(promparse.Options{OpenMetrics: true, Exemplars: true})
	defer promparse.Put(p)

	body := "reqs_total 17.0 # {trace_id=\"4bf92f3577b34da6a3ce929d0e0e4736\"} 0.5\n# EOF\n"
	_, _ = p.Parse(strings.NewReader(body), func(s promparse.Sample) error {
		if s.Exemplar != nil {
			// The exemplar aliases parser memory; copy it to keep it.
			kept := promparse.CopyExemplar(*s.Exemplar)
			fmt.Println("exemplar:", kept.Labels[0].Value)
		}
		return nil
	})
	// Output:
	// exemplar: 4bf92f3577b34da6a3ce929d0e0e4736
}
