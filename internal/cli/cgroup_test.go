package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeCgroup points the readers at a fixture tree. procLines is written
// verbatim as /proc/self/cgroup.
func fakeCgroup(t *testing.T, procLines string, files map[string]string) {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	proc := filepath.Join(root, "proc-self-cgroup")
	if err := os.WriteFile(proc, []byte(procLines), 0o644); err != nil {
		t.Fatal(err)
	}
	// No mount table unless a test writes one (fakeMountinfo): the real
	// /proc/self/mountinfo describes this machine, not the fixture.
	oldRoot, oldProc, oldMounts := cgroupRoot, procCgroup, procMountinfo
	cgroupRoot, procCgroup, procMountinfo = root, proc, filepath.Join(root, "proc-self-mountinfo")
	t.Cleanup(func() { cgroupRoot, procCgroup, procMountinfo = oldRoot, oldProc, oldMounts })
}

// fakeMountinfo writes the fixture's /proc/self/mountinfo; %s in lines is the
// fixture's cgroup mount point.
func fakeMountinfo(t *testing.T, lines string) {
	t.Helper()
	body := strings.ReplaceAll(lines, "%s", cgroupRoot)
	if err := os.WriteFile(procMountinfo, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The mount table is read the way the kernel writes it: the LAST entry at a
// mount point is the visible one, and paths are octal-escaped.
func TestCgroupMountShadowedReadsTheVisibleMount(t *testing.T) {
	fakeCgroup(t, "0::/\n", map[string]string{})
	for _, tc := range []struct {
		name, lines string
		want        bool
	}{
		{"own namespace root", "30 25 0:26 / %s rw - cgroup2 cgroup2 rw\n", false},
		{"ancestor", "30 25 0:26 /../.. %s rw - cgroup2 cgroup2 rw\n", true},
		{"one level up", "30 25 0:26 /.. %s rw - cgroup2 cgroup2 rw\n", true},
		{"a dotted name is not a parent", "30 25 0:26 /..foo %s rw - cgroup2 cgroup2 rw\n", false},
		{"shadowed, then shadowed again by the container's own", "30 25 0:26 /../.. %s rw - cgroup2 cgroup2 rw\n31 25 0:27 / %s rw - cgroup2 cgroup2 rw\n", false},
		{"v1 memory controller", "30 25 0:26 /.. %s/memory rw master:1 - cgroup cgroup rw,memory\n", true},
		{"not a cgroup filesystem", "30 25 0:26 /../.. %s rw - tmpfs tmpfs rw\n", false},
		{"another mount point", "30 25 0:26 /../.. %s-other rw - cgroup2 cgroup2 rw\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeMountinfo(t, tc.lines)
			if _, got := cgroupMountShadowed(); got != tc.want {
				t.Errorf("shadowed = %v, want %v", got, tc.want)
			}
		})
	}
	if got := unescapeMountinfo(`/a\040b\134c`); got != `/a b\c` {
		t.Errorf("unescapeMountinfo = %q", got)
	}
}

// The shape a container gets when the runtime gave it a cgroup namespace: its
// own limit is the mount point's own memory.max and the path is "/".
func TestCgroupLimitInACgroupNamespace(t *testing.T) {
	fakeCgroup(t, "0::/\n", map[string]string{"memory.max": "536870912\n"})
	got, path, ok := CgroupMemoryLimit()
	if !ok || got != 536870912 {
		t.Fatalf("limit = %d, %q, %v; want 536870912", got, path, ok)
	}
}

// cgroupns=host: the mount point is the node's root cgroup and reads "max",
// while the container's real limit sits several levels down at the path
// /proc/self/cgroup names. Reading only the top-level file — which is what a
// naive implementation does — finds nothing here.
func TestCgroupLimitWithoutACgroupNamespace(t *testing.T) {
	const path = "kubepods.slice/kubepods-burstable.slice/pod123.slice/cri-containerd-abc.scope"
	fakeCgroup(t, "0::/"+path+"\n", map[string]string{
		"memory.max":                       "max\n",
		path + "/memory.max":               "268435456\n",
		filepath.Dir(path) + "/memory.max": "1073741824\n",
	})
	got, _, ok := CgroupMemoryLimit()
	if !ok || got != 268435456 {
		t.Fatalf("limit = %d, %v; want 268435456", got, ok)
	}
}

// An UNCAPPED container must report no limit even though every cgroup above it
// carries one. This is the shape a real node has: --enforce-node-allocatable
// puts a memory limit on kubepods.slice equal to the node's whole allocatable
// memory and --cgroups-per-qos puts one on the QoS slice, so a walk up the
// hierarchy hands an uncapped process ~0.9x the NODE's RAM as its heap goal —
// the node-scale ceiling cgroup.go's file comment refuses, arriving by the
// back door. The metadata service ships uncapped on purpose and is exactly
// this case.
func TestAncestorLimitIsNotInherited(t *testing.T) {
	const path = "kubepods.slice/kubepods-burstable.slice/pod123.slice/cri-containerd-abc.scope"
	fakeCgroup(t, "0::/"+path+"\n", map[string]string{
		"memory.max":                "max\n",
		"kubepods.slice/memory.max": "67386466304\n", // node allocatable
		"kubepods.slice/kubepods-burstable.slice/memory.max": "50000000000\n",
		filepath.Dir(path) + "/memory.max":                   "1073741824\n", // pod slice
		path + "/memory.max":                                 "max\n",        // this container: uncapped
	})
	if v, p, ok := CgroupMemoryLimit(); ok {
		t.Fatalf("an uncapped container inherited %d from %q", v, p)
	}
}

func TestCgroupLimitV1(t *testing.T) {
	fakeCgroup(t, "8:memory:/docker/abc\n", map[string]string{
		"memory/docker/abc/memory.limit_in_bytes": "268435456\n",
	})
	got, _, ok := CgroupMemoryLimit()
	if !ok || got != 268435456 {
		t.Fatalf("limit = %d, %v; want 268435456", got, ok)
	}
}

// cgroup v1 in Kubernetes and Docker bind-mounts the CONTAINER's own cgroup
// directory at /sys/fs/cgroup/memory while /proc/self/cgroup keeps naming the
// host path, which does not exist inside the container. The controller root is
// then the container's own file, which is why it is the one fallback.
func TestCgroupLimitV1BindMountedController(t *testing.T) {
	fakeCgroup(t, "8:memory:/kubepods/burstable/pod123/abcdef\n", map[string]string{
		"memory/memory.limit_in_bytes": "268435456\n",
	})
	got, path, ok := CgroupMemoryLimit()
	if !ok || got != 268435456 {
		t.Fatalf("limit = %d, %q, %v; want 268435456 from the mounted controller root", got, path, ok)
	}
}

// The three ways a hierarchy says "not capped". None may become a limit: a
// soft limit derived from a sentinel is worse than none at all.
func TestUncappedReportsNoLimit(t *testing.T) {
	cases := map[string]map[string]string{
		"v2 max":        {"memory.max": "max\n"},
		"v1 sentinel":   {"memory/memory.limit_in_bytes": "9223372036854771712\n"},
		"nothing there": {},
	}
	for name, files := range cases {
		t.Run(name, func(t *testing.T) {
			fakeCgroup(t, "0::/\n8:memory:/\n", files)
			if v, _, ok := CgroupMemoryLimit(); ok {
				t.Fatalf("reported a limit of %d for an uncapped hierarchy", v)
			}
		})
	}
}

// A /proc/self/cgroup that would leave the mount point must not read anything
// above it: the escaping candidate is dropped and only the mount root itself
// remains.
func TestPathEscapeStaysUnderTheMount(t *testing.T) {
	fakeCgroup(t, "0::/../../../../etc\n", map[string]string{"memory.max": "max\n"})
	if v, p, ok := CgroupMemoryLimit(); ok {
		t.Fatalf("escaped the mount: %d from %q", v, p)
	}
	for _, p := range limitFiles(cgroupRoot, "/../../../../etc", "memory.max") {
		if p != filepath.Join(cgroupRoot, "memory.max") {
			t.Fatalf("limitFiles offered %q, outside %q", p, cgroupRoot)
		}
	}
}
