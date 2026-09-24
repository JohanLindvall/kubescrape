package chartcheck

import (
	"regexp"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/manifestcheck"
)

// agent.cgroupStats.root names where the agent LOOKS for the cgroup v2
// hierarchy, and the chart is the only thing that can put a hierarchy there.
//
// It used to render the flag alone, against a mountPath hardcoded to
// /sys/fs/cgroup — so every value except the default handed the agent a
// -cgroup-stats-root pointing at nothing, and an explicitly configured root
// with no cgroup hierarchy behind it is FATAL by design (it is an operator
// error, identical on every node, unlike a v1 node which merely disables the
// pipeline at any root). The only value the chart exposed therefore CrashLooped the
// DaemonSet unless the operator also hand-rolled a volume through
// extraVolumes, and the everything.yaml golden froze exactly that pair.
//
// The golden pins the current rendering; this pins the RULE, so a regeneration
// cannot quietly bless the contradiction again.
//
// The DEFAULT is /host/sys/fs/cgroup, never /sys/fs/cgroup, and the flag is
// always rendered: the agent runs in its own cgroup namespace, its own
// /sys/fs/cgroup is where internal/cli reads its memory limit for GOMEMLIMIT,
// and the node's root mounted over it made that read find no limit — every
// cgroup-stats agent ran without the soft memory limit, silently.
func TestCgroupStatsRootDrivesItsMount(t *testing.T) {
	helm := helmBin(t)
	for _, root := range []string{"", "/host/sys/fs/cgroup", "/run/cgroup2"} {
		args := []string{"--set", "agent.cgroupStats.enabled=true"}
		want := "/host/sys/fs/cgroup"
		if root != "" {
			args = append(args, "--set", "agent.cgroupStats.root="+root)
			want = root
		}
		out, err := helmTemplate(helm, "monitoring", args...)
		if err != nil {
			t.Fatalf("helm template with root=%q failed: %v\n%s", root, err, out)
		}
		doc := agentDaemonSet(t, string(out))

		if !strings.Contains(doc, "- -cgroup-stats-root="+want+"\n") {
			t.Errorf("root=%q did not render -cgroup-stats-root=%s", root, want)
		}
		for _, m := range volumeMountPaths(doc) {
			if m == "/sys/fs/cgroup" || m == "/sys/fs/cgroup/" {
				t.Errorf("root=%q: a volumeMount targets the container's own /sys/fs/cgroup, which hides its memory limit from the GOMEMLIMIT reader", root)
			}
		}
		mounts := cgroupMountPaths(doc)
		if len(mounts) != 1 {
			t.Fatalf("root=%q rendered %d cgroup volumeMounts (%v), want exactly 1", root, len(mounts), mounts)
		}
		if mounts[0] != want {
			t.Errorf("root=%q mounts the host hierarchy at %s but tells the agent to read %s: "+
				"the flag and the mount must name the same path or the pipeline refuses to start on every node",
				root, mounts[0], want)
		}
		// The host side is the node's real hierarchy whatever the mount point
		// is: relocating the container-side path must not start reading some
		// other host directory.
		if !strings.Contains(doc, "path: /sys/fs/cgroup") {
			t.Errorf("root=%q: the hostPath is no longer the node's /sys/fs/cgroup", root)
		}
	}
}

// mountPathRe matches every volumeMount's mountPath line.
var mountPathRe = regexp.MustCompile(`(?m)^\s*mountPath: (\S+)`)

func volumeMountPaths(doc string) []string {
	var out []string
	for _, m := range mountPathRe.FindAllStringSubmatch(doc, -1) {
		out = append(out, m[1])
	}
	return out
}

// cgroupMountRe matches the mountPath line of the `cgroup` volumeMount.
var cgroupMountRe = regexp.MustCompile(`(?m)^\s*- name: cgroup\n\s*mountPath: (\S+)`)

func cgroupMountPaths(doc string) []string {
	var out []string
	for _, m := range cgroupMountRe.FindAllStringSubmatch(doc, -1) {
		out = append(out, m[1])
	}
	return out
}

// agentDaemonSet returns the one rendered DaemonSet document, so an assertion
// about "the agent's mounts" cannot accidentally match the singleton
// Deployment or the trace tier.
func agentDaemonSet(t *testing.T, rendered string) string {
	t.Helper()
	var found []string
	for _, doc := range manifestcheck.Documents(rendered) {
		if strings.Contains(doc, "kind: DaemonSet") {
			found = append(found, doc)
		}
	}
	if len(found) != 1 {
		t.Fatalf("found %d DaemonSet documents, want 1", len(found))
	}
	return found[0]
}
