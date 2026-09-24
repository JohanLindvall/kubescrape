package chartcheck

import (
	"regexp"
	"strings"
	"testing"
)

// Every containerPort, NetworkPolicy port and prometheus.io/port the chart
// renders is derived from a listen address, and the 32 inline copies read
// `(split ":" X)._1` — the SECOND colon-separated field. A bracketed IPv6
// address (`[::]:9090`) therefore rendered containerPort 0, a NetworkPolicy
// `port: 0` and `prometheus.io/port: ""`; the first two fail the install, the
// last silently stops the workload's own metrics being scraped. This renders
// every workload with IPv6 listen addresses on every derived port and requires
// the real port numbers everywhere.
func TestIPv6ListenAddressesRenderTheirPorts(t *testing.T) {
	helm := helmBin(t)
	out := renderAllWorkloads(t, helm,
		"networkPolicy.enabled=true",
		"metrics.listen=[::]:9191",
		"service.listen=[::]:8181",
		"agent.listen=[::1]:8182",
		"agent.ingest.enabled=true",
		"agent.ingest.hostPort=true",
		"agent.ingest.grpcEndpoint=[::]:4417",
		"agent.ingest.httpEndpoint=[fd00::1]:4418",
		"serviceGraph.ingest.enabled=true",
		"serviceGraph.ingest.grpcEndpoint=[::]:4517",
		"serviceGraph.ingest.httpEndpoint=0.0.0.0:4518",
	)
	for _, bad := range []*regexp.Regexp{
		regexp.MustCompile(`(?m)^\s*-?\s*(containerPort|hostPort|port): 0\s*$`),
		regexp.MustCompile(`(?m)^\s*prometheus\.io/port: ""\s*$`),
	} {
		for _, m := range bad.FindAllString(out, -1) {
			t.Errorf("rendered %q from an IPv6 listen address", strings.TrimSpace(m))
		}
	}
	for _, want := range []string{
		`prometheus.io/port: "9191"`,
		"containerPort: 9191", "containerPort: 8181", "containerPort: 8182",
		"containerPort: 4417", "hostPort: 4417", "containerPort: 4418", "hostPort: 4418",
		"containerPort: 4517", "containerPort: 4518",
		"- port: 9191", "- port: 8181", "- port: 8182", "- port: 4417", "- port: 4418", "- port: 4517", "- port: 4518",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered output has no %q", want)
		}
	}

	// A listen address with no port is one the binary cannot listen on either;
	// the render refuses it instead of emitting port 0.
	if out, err := templateAllWorkloads(helm, "metrics.listen=localhost"); err == nil || !strings.Contains(string(out), "names no port") {
		t.Errorf("metrics.listen=localhost rendered (err=%v) instead of failing with a named reason:\n%s", err, out)
	}
}
