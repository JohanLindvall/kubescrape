package chartcheck

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/docscheck"
	"github.com/JohanLindvall/kubescrape/internal/manifestcheck"
)

// Three workloads run the agent binary — the DaemonSet, the events/Azure
// singleton and the trace-tier StatefulSet — and all three take their exporter
// from the ONE agent.otlp block, their log level, metrics and pprof listeners
// from the same top-level values, and their -config from the same rendered
// agent config. The flag lines were hand-copied into three templates and
// DRIFTED three times (retry/max-send on the DaemonSet only; the log-metrics
// knobs on the DaemonSet only; pprofListen missing from the singletons), each
// time silently: a flag a workload does not render is that workload running on
// the binary's default while the operator's value applies elsewhere.
//
// The -otlp-* and -logs-metrics-* blocks are now written once, as argument-block
// helpers in _helpers.tpl (which the text-scanning guards read where they are
// included: manifestcheck.ExpandIncludes). A helper removes the copies, not the
// rule — a workload can still stop including one, and the log level and the
// listeners are still per-template lines — so the RULE is pinned here, on the
// rendered output: with every optional value set, the three render the same
// shared flag NAMES; the DaemonSet and the singleton (which runs the same
// logMetrics set) the same -logs-metrics-* names; the agent-binary set is every
// -otlp-* flag the agent registers; and the metadata service renders every
// -otlp-* flag the two binaries SHARE (cli.RegisterOTLPFlags).
func TestAgentBinaryWorkloadsRenderTheSameSharedFlags(t *testing.T) {
	helm := helmBin(t)
	vals, err := filepath.Abs(filepath.Join("testdata", "values", "everything.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"-f", vals}
	for _, s := range []string{
		"agent.otlp.retryAttempts=7",
		"agent.otlp.retryBackoff=3s",
		"agent.otlp.maxSendBytes=2000000",
		"agent.otlp.caFile=/etc/ssl/collector-ca.pem",
		"agent.otlp.bearerTokenSecret.name=otlp-token",
		"service.otlp.caFile=/etc/ssl/collector-ca.pem",
		"service.otlp.bearerTokenSecret.name=otlp-token",
		"pprofListen=localhost:6060",
	} {
		args = append(args, "--set", s)
	}
	out := renderWith(t, helm, args)

	shared := func(name string) bool {
		return strings.HasPrefix(name, "otlp-") || slices.Contains([]string{"log-level", "metrics-listen", "pprof-listen", "config"}, name)
	}
	logsMetrics := func(name string) bool { return strings.HasPrefix(name, "logs-metrics-") }

	agentBinary := map[string][]string{} // "<kind>/<name>" -> all flag names
	var serviceFlags []string
	for _, doc := range manifestcheck.Documents(out) {
		var w struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Command []string `json:"command"`
							Args    []string `json:"args"`
						} `json:"containers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal([]byte(doc), &w); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(w.Spec.Template.Spec.Containers) == 0 {
			continue
		}
		c := w.Spec.Template.Spec.Containers[0]
		names := flagNames(c.Args)
		switch {
		case slices.Contains(c.Command, "/kubescrape-agent"):
			agentBinary[w.Kind+"/"+w.Metadata.Name] = names
		case w.Kind == "Deployment":
			serviceFlags = names
		}
	}
	if len(agentBinary) != 3 {
		t.Fatalf("found %d agent-binary workloads (%v), want 3: the DaemonSet, the events singleton and the trace tier", len(agentBinary), mapKeys(agentBinary))
	}
	if serviceFlags == nil {
		t.Fatal("found no metadata-service Deployment")
	}

	// 1. The three agent-binary workloads render the same shared flag names.
	var ref string
	var refShared []string
	for _, key := range mapKeys(agentBinary) {
		got := filterSorted(agentBinary[key], shared)
		if refShared == nil {
			ref, refShared = key, got
			continue
		}
		if !slices.Equal(got, refShared) {
			t.Errorf("%s renders shared flags %v, %s renders %v — a workload missing one runs on the binary's default while the operator's value applies elsewhere",
				key, got, ref, refShared)
		}
	}

	// 2. ... and that set is COMPLETE: every -otlp-* flag the agent registers.
	agentDoc, serviceDoc := flagsDocSections(t)
	wantAgentOTLP := filterSorted(agentDoc, func(n string) bool { return strings.HasPrefix(n, "otlp-") })
	if got := filterSorted(refShared, func(n string) bool { return strings.HasPrefix(n, "otlp-") }); !slices.Equal(got, wantAgentOTLP) {
		t.Errorf("the agent-binary workloads render -otlp-* flags %v, but the agent registers %v — a flag missing from all three is invisible to a comparison between them",
			got, wantAgentOTLP)
	}

	// 3. The DaemonSet and the singleton run the same logMetrics set, so they
	// render the same -logs-metrics-* knobs (the trace tier runs none).
	var ds, ev []string
	for key, names := range agentBinary {
		switch {
		case strings.HasPrefix(key, "DaemonSet/"):
			ds = filterSorted(names, logsMetrics)
		case strings.HasPrefix(key, "Deployment/"):
			ev = filterSorted(names, logsMetrics)
		}
	}
	if len(ds) == 0 || !slices.Equal(ds, ev) {
		t.Errorf("DaemonSet renders -logs-metrics-* %v, the events singleton %v; they run the same rules and must agree (and the fixture sets all three knobs)", ds, ev)
	}

	// 4. The metadata service renders every -otlp-* flag the two binaries share.
	var wantShared []string
	for _, n := range serviceDoc {
		if strings.HasPrefix(n, "otlp-") && slices.Contains(agentDoc, n) {
			wantShared = append(wantShared, n)
		}
	}
	slices.Sort(wantShared)
	if got := filterSorted(serviceFlags, func(n string) bool { return strings.HasPrefix(n, "otlp-") }); !slices.Equal(got, wantShared) {
		t.Errorf("the metadata service renders -otlp-* flags %v, want the shared set %v (cli.RegisterOTLPFlags)", got, wantShared)
	}
	for _, n := range []string{"log-level", "metrics-listen", "pprof-listen"} {
		if !slices.Contains(serviceFlags, n) {
			t.Errorf("the metadata service does not render -%s", n)
		}
	}
}

var argFlagName = regexp.MustCompile(`^--?([a-z0-9-]+)(=|$)`)

func flagNames(args []string) []string {
	var out []string
	for _, a := range args {
		if m := argFlagName.FindStringSubmatch(a); m != nil {
			out = append(out, m[1])
		}
	}
	return out
}

func filterSorted(names []string, keep func(string) bool) []string {
	out := []string{}
	for _, n := range names {
		if keep(n) && !slices.Contains(out, n) {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out
}

func mapKeys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// flagsDocSections returns the flag names docs/FLAGS.md (generated from the
// registered flag sets) documents for the agent and for the metadata service.
func flagsDocSections(t *testing.T) (agent, service []string) {
	t.Helper()
	b, err := os.ReadFile(flagsDocPath)
	if err != nil {
		t.Fatalf("reading docs/FLAGS.md: %v", err)
	}
	var cur *[]string
	for line := range strings.SplitSeq(string(b), "\n") {
		switch {
		case strings.HasPrefix(line, "## ") && strings.Contains(line, "`kubescrape-agent`"):
			cur = &agent
		case strings.HasPrefix(line, "## ") && strings.Contains(line, "`kubescrape`"):
			cur = &service
		case strings.HasPrefix(line, "## "):
			cur = nil
		}
		if row, ok := docscheck.ParseFlagRow(line); ok && cur != nil {
			*cur = append(*cur, row.Name)
		}
	}
	if len(agent) == 0 || len(service) == 0 {
		t.Fatalf("docs/FLAGS.md: parsed %d agent and %d service flags; the section headings changed", len(agent), len(service))
	}
	return agent, service
}

func renderWith(t *testing.T, helm string, args []string) string {
	t.Helper()
	out, err := helmTemplate(helm, "monitoring", args...)
	if err != nil {
		t.Fatalf("helm template %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}
