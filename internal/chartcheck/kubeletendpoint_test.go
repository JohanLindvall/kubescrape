package chartcheck

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Both shipped install paths point the kubelet scrapes at the node's own
// address through a downward-API env var: `https://$(NODE_IP):10250`, with
// NODE_IP from status.hostIP. On an IPv6 node that expands to a BARE IPv6
// literal — https://fd00:10::5:10250 — which net/url refuses with "invalid port
// after host", taking cadvisor, node-metrics and /stats/summary down on every
// node of an IPv6 cluster.
//
// The remedy that suggests itself in a manifest is the WRONG one and this test
// exists to refuse it: `https://[$(NODE_IP)]:10250` renders `[10.0.0.5]` on an
// IPv4 cluster, which Go rejects as an invalid IP-literal, so a static bracket
// simply moves the outage from one family to the other. One literal cannot
// spell both — which is why the bracketing belongs in the agent, which sees the
// EXPANDED value and can re-form the authority with net.JoinHostPort.
//
// Pinned as a relation rather than a spelling: the two surfaces must agree, and
// neither may bracket an unexpanded `$(...)`. An operator writing a literal
// IPv6 host in a values override is a different case entirely — there the
// brackets are correct and required, which is why the check keys on the env-var
// expansion and not on brackets as such.
func TestKubeletEndpointIsNotStaticallyBracketedAroundTheEnvVar(t *testing.T) {
	helm := helmBin(t)
	out, err := exec.Command(helm, "template", "kubescrape", "../../charts/kubescrape",
		"--namespace", "monitoring", "--show-only", "templates/agent.yaml").CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, out)
	}
	deploy, err := os.ReadFile(filepath.Join("..", "..", "deploy", "agent.yaml"))
	if err != nil {
		t.Fatalf("reading deploy/agent.yaml: %v", err)
	}

	arg := regexp.MustCompile(`(?m)^\s*-\s+-kubelet-endpoint=(\S+)\s*$`)
	surfaces := map[string]string{
		"rendered chart":    string(out),
		"deploy/agent.yaml": string(deploy),
	}
	values := map[string]string{}
	for name, body := range surfaces {
		m := arg.FindStringSubmatch(body)
		if m == nil {
			t.Fatalf("%s passes no -kubelet-endpoint; the default that this guard is about has moved", name)
		}
		values[name] = m[1]
		// The literal a manifest can safely carry is the UNBRACKETED one. A
		// bracketed env-var expansion is the naive IPv6 "fix" that breaks every
		// IPv4 cluster instead.
		if strings.Contains(m[1], "[$(") || strings.Contains(m[1], "[${") {
			t.Errorf("%s renders -kubelet-endpoint=%s: bracketing the env-var expansion makes the URL "+
				"`https://[10.0.0.5]:10250` on an IPv4 cluster, which Go rejects as an invalid IP-literal. "+
				"One static value cannot serve both families — the agent brackets the host itself.", name, m[1])
		}
	}
	// The two install paths are separate copies of one decision; the raw
	// manifests are the half no rendering test would otherwise reach.
	if a, b := values["rendered chart"], values["deploy/agent.yaml"]; a != b {
		t.Errorf("-kubelet-endpoint differs between the install paths: chart %q, deploy/agent.yaml %q — "+
			"an IPv6 caveat fixed in one and not the other is exactly the drift this pins", a, b)
	}
}
