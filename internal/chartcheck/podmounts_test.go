package chartcheck

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/manifestcheck"
)

// renderedPods returns the pod template of every rendered workload, keyed
// "<kind>/<name>".
func renderedPods(t *testing.T, rendered string) map[string]corev1.PodSpec {
	t.Helper()
	pods := map[string]corev1.PodSpec{}
	for _, doc := range manifestcheck.Documents(rendered) {
		var w struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				Template struct {
					Spec corev1.PodSpec `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal([]byte(doc), &w); err != nil {
			t.Fatalf("unmarshal: %v\n%s", err, doc)
		}
		switch w.Kind {
		case "DaemonSet", "Deployment", "StatefulSet":
			pods[w.Kind+"/"+w.Metadata.Name] = w.Spec.Template.Spec
		}
	}
	return pods
}

// agentBinaryPods are the rendered pods whose container runs the agent binary.
func agentBinaryPods(pods map[string]corev1.PodSpec) map[string]corev1.PodSpec {
	out := map[string]corev1.PodSpec{}
	for key, spec := range pods {
		for _, c := range spec.Containers {
			if slices.Contains(c.Command, manifestcheck.AgentCommand) {
				out[key] = spec
			}
		}
	}
	return out
}

func writeValues(t *testing.T, text string) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), "values.yaml")
	if err := os.WriteFile(f, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

// Every workload that runs the agent binary runs its processing chain —
// transforms included — so each must be able to mount what a -transforms-file
// needs. The events/Azure singleton could not: it was the one agent-binary
// workload with no extraVolumes/extraVolumeMounts, and the schema refused the
// keys, so passing -transforms-file through events.extraArgs pointed the agent
// at a file nothing could mount, which validateConfig refuses at startup.
func TestEveryAgentBinaryWorkloadMountsItsExtraVolumes(t *testing.T) {
	helm := helmBin(t)
	// No agent.config and no token Secret beside it: the extra volume is the
	// singleton's ONLY volume, so its volume list must not hinge on anything
	// else being set.
	const transforms = `
  extraArgs: ["-transforms-file=/etc/kubescrape/transforms/transforms.yaml"]
  extraVolumes:
    - {name: transforms, configMap: {name: my-transforms}}
  extraVolumeMounts:
    - {name: transforms, mountPath: /etc/kubescrape/transforms, readOnly: true}
`
	f := writeValues(t, "agent:"+transforms+
		"events:\n  enabled: true"+transforms+
		"serviceGraph:\n  enabled: true\n  tokenSecret: {name: sg-token}"+transforms)
	out, err := helmTemplate(helm, "monitoring", "-f", f)
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, out)
	}
	pods := agentBinaryPods(renderedPods(t, string(out)))
	if len(pods) != 3 {
		t.Fatalf("rendered %d agent-binary workloads, want 3 (the DaemonSet, the events singleton, the trace tier)", len(pods))
	}
	for key, spec := range pods {
		vol := slices.IndexFunc(spec.Volumes, func(v corev1.Volume) bool {
			return v.Name == "transforms" && v.ConfigMap != nil && v.ConfigMap.Name == "my-transforms"
		})
		if vol < 0 {
			t.Errorf("%s: no `transforms` ConfigMap volume; its extraVolumes were not rendered", key)
		}
		c := spec.Containers[0]
		mount := slices.IndexFunc(c.VolumeMounts, func(m corev1.VolumeMount) bool {
			return m.Name == "transforms" && m.MountPath == "/etc/kubescrape/transforms" && m.ReadOnly
		})
		if mount < 0 {
			t.Errorf("%s: no `transforms` mount at /etc/kubescrape/transforms; its extraVolumeMounts were not rendered", key)
		}
		if !slices.Contains(c.Args, "-transforms-file=/etc/kubescrape/transforms/transforms.yaml") {
			t.Errorf("%s: -transforms-file from extraArgs was not rendered", key)
		}
	}
}

// configArg, configMountPath and the ConfigMap they pair with: the rendered
// agent config is useful to a workload only as all three at once.
const (
	configArg       = "-config=/etc/kubescrape/config/config.yaml"
	configMountPath = "/etc/kubescrape/config"
)

// A -config flag without its mount is a CrashLoopBackOff on every pod of that
// workload, and a mount without the flag is config nothing reads — so the three
// must agree PER WORKLOAD. agent.staticAttrs alone once rendered the flag and
// the ConfigMap but neither the volume nor the mount, on every node. This used
// to be a bash loop in CI that compared whole-render COUNTS (so a flag missing
// on one workload and a mount missing on another cancelled out), never enabled
// the events singleton or the trace tier, and was run by nothing `make check`
// runs.
func TestEveryConfigFlagHasItsMount(t *testing.T) {
	helm := helmBin(t)
	fixtures, err := filepath.Glob(filepath.Join("testdata", "values", "*.yaml"))
	if err != nil || len(fixtures) == 0 {
		t.Fatalf("no value fixtures under testdata/values (err=%v)", err)
	}
	// Every workload on, on top of whatever a case sets.
	on := []string{"--set", "events.enabled=true", "--set", "serviceGraph.enabled=true",
		"--set", "serviceGraph.tokenSecret.name=sg-token"}
	cases := map[string][]string{}
	for _, f := range fixtures {
		cases["fixture "+filepath.Base(f)] = []string{"-f", f}
	}
	for _, set := range []string{
		"",
		"agent.staticAttrs.foo=bar",
		"networkPolicy.enabled=true",
		"agent.ingest.enabled=true,networkPolicy.enabled=true",
		"service.scrapeAuthSecrets=true,service.scrapeAuthToken.existingSecret=tok",
		"service.scrapeAuthSecrets=true,service.scrapeAuthToken.value=tok",
		"events.enabled=false,azure.enabled=true,agent.staticAttrs.foo=bar",
	} {
		if set == "" {
			cases["defaults"] = nil
			continue
		}
		cases["--set "+set] = []string{"--set", set}
	}
	configMap := releaseName + "-agent-config"
	for name, args := range cases {
		out, err := helmTemplate(helm, "monitoring", slices.Concat(on, args)...)
		if err != nil {
			t.Fatalf("%s: helm template failed: %v\n%s", name, err, out)
		}
		pods := renderedPods(t, string(out))
		withFlag := 0
		for key, spec := range pods {
			var flag, mount bool
			for _, c := range spec.Containers {
				flag = flag || slices.Contains(c.Args, configArg)
				mount = mount || slices.ContainsFunc(c.VolumeMounts, func(m corev1.VolumeMount) bool {
					return m.Name == "config" && m.MountPath == configMountPath
				})
			}
			volume := slices.ContainsFunc(spec.Volumes, func(v corev1.Volume) bool {
				return v.Name == "config" && v.ConfigMap != nil && v.ConfigMap.Name == configMap
			})
			if flag != mount || flag != volume {
				t.Errorf("%s: %s renders %s=%v, a `config` mount at %s=%v and the %s volume=%v — all three or none",
					name, key, configArg, flag, configMountPath, mount, configMap, volume)
			}
			if flag {
				withFlag++
			}
		}
		// Not vacuous: staticAttrs is exactly the value that renders a config,
		// and every agent-binary workload then reads it.
		if strings.Contains(name, "staticAttrs") {
			if want := len(agentBinaryPods(pods)); withFlag != want || want == 0 {
				t.Errorf("%s: %d workloads render %s, want every agent-binary workload (%d)", name, withFlag, configArg, want)
			}
		}
	}
}

// An empty consumer group is refused by the agent (azurediag.ValidateGroup, so
// -check-config catches it) — and at template time, so `helm upgrade` fails
// instead of the rollout.
func TestEmptyAzureConsumerGroupIsRefusedAtTemplateTime(t *testing.T) {
	helm := helmBin(t)
	base := []string{"--set", "azure.enabled=true", "--set", "azure.eventhub.namespace=myns.servicebus.windows.net"}
	if out, err := helmTemplate(helm, "monitoring", base...); err != nil {
		t.Fatalf("the default group was refused: %v\n%s", err, out)
	}
	out, err := helmTemplate(helm, "monitoring", append(base, "--set-string", "azure.eventhub.group=")...)
	if err == nil {
		t.Fatalf("azure.eventhub.group=\"\" rendered; the agent refuses an empty group at startup:\n%s", out)
	}
	if !strings.Contains(string(out), jsonPointer("azure.eventhub.group")) {
		t.Errorf("the refusal does not name azure.eventhub.group: %s", out)
	}
}

// Every token file the chart points a binary at is `<mount>/<key>`, and an
// empty key means the Secret's conventional `token` key — on EVERY such flag.
// -otlp-bearer-token-file was the one that did not apply the default, so
// `bearerTokenSecret.key: ""` (which the schema allows) pointed the exporter at
// the mount DIRECTORY, and every export failed its token read.
func TestEmptySecretKeysFallBackToTheTokenKey(t *testing.T) {
	helm := helmBin(t)
	out := renderAllWorkloads(t, helm,
		"agent.otlp.bearerTokenSecret.name=otlp", "agent.otlp.bearerTokenSecret.key=",
		"service.otlp.bearerTokenSecret.name=otlp", "service.otlp.bearerTokenSecret.key=",
		"service.scrapeAuthSecrets=true", "service.scrapeAuthToken.value=tok", "service.scrapeAuthToken.key=",
		"agent.debug.tokenSecret.name=dbg", "agent.debug.tokenSecret.key=",
		"serviceGraph.tokenSecret.key=")
	seen := map[string]int{}
	for key, spec := range renderedPods(t, out) {
		for _, arg := range spec.Containers[0].Args {
			name, value, ok := strings.Cut(strings.TrimPrefix(arg, "-"), "=")
			if !ok || !strings.HasSuffix(name, "token-file") {
				continue
			}
			seen[name]++
			if !strings.HasSuffix(value, "/token") {
				t.Errorf("%s: -%s=%s; an empty key must name the `token` key, not the mount directory", key, name, value)
			}
		}
	}
	// otlp-bearer on all four workloads, scrape-auth on the service and the
	// agent, debug on the three agent-binary workloads, the tier's own once.
	for name, want := range map[string]int{
		"otlp-bearer-token-file":   4,
		"scrape-auth-token-file":   2,
		"debug-token-file":         3,
		"service-graph-token-file": 1,
	} {
		if seen[name] != want {
			t.Errorf("-%s rendered on %d workloads, want %d", name, seen[name], want)
		}
	}
}
