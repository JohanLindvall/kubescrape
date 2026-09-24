package cli

// Reading THIS CONTAINER'S OWN cgroup memory limit: the one reader behind
// SetMemoryLimit (memlimit.go) and internal/agent/tailbuffer's maxSpans sizing.
//
// An ANCESTOR cgroup's limit is not read, and that is memlimit.go's
// no-MemTotal-fallback rule rather than a second one. This code walked from
// the leaf up to the mount taking the minimum, which reads a node-scale number
// under another name: with --enforce-node-allocatable=pods (the kubelet
// default) kubepods.slice carries a memory limit of the node's whole
// allocatable memory, and --cgroups-per-qos puts one on the QoS slice below it
// — so an uncapped container inherited ~0.9 x the NODE's RAM as its heap goal
// and memlimit.go's file comment described a behaviour the code did not have.
// A pod-level limit is no better a number to derive from: the kubelet only
// sets one when EVERY container in the pod is capped (in which case this
// container's own cgroup already carries the tighter answer), and it is shared
// with the siblings, so a fraction of it is not a ceiling this process owns
// either. Reading only the container's own cgroup is also what keeps this in
// step with tailbuffer's maxSpans sizing, which reads the container's
// memory.max and nothing above it.

import (
	"bufio"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// cgroupRoot is the mount point the limit is read under; a var so the test can
// point it at a fixture tree.
var cgroupRoot = "/sys/fs/cgroup"

// procCgroup is this process's cgroup membership file; a var for the same
// reason.
var procCgroup = "/proc/self/cgroup"

// procMountinfo is this process's mount table; a var for the same reason.
var procMountinfo = "/proc/self/mountinfo"

// CgroupMemoryLimit reports THIS CONTAINER'S OWN memory limit and the file it
// was read from, or ok=false when the workload is uncapped or the limit cannot
// be read. It never reads an ancestor's — see this file's comment for why a
// pod-slice or kubepods.slice limit is the wrong number to hand the Go runtime.
//
// It is the ONE reader behind both SetMemoryLimit and
// internal/agent/tailbuffer's maxSpans sizing, so that the claim in
// memlimit.go's file comment — that the Go soft limit and the span buffer
// cannot disagree about how much memory this pod has — is held by one
// implementation rather than by two that happen to read the same path.
// tailbuffer's own reader looked at the mount root alone, so under
// cgroupns=host it saw no limit at all and sized the span buffer against the
// NODE's RAM; it calls this now.
//
// The container's own cgroup is named by /proc/self/cgroup, which covers both
// layouts: inside a cgroup namespace the path is "/" and the file is the mount
// point's own, and with cgroupns=host — still a supported kubelet
// configuration, and what any systemd scope on a plain host looks like — it is
// the full path several levels down, whose file is still the container's.
//
// The mount ROOT is a fallback for one layout that would otherwise read
// nothing: cgroup v1 in Kubernetes and Docker bind-mounts the container's own
// cgroup directory at /sys/fs/cgroup/memory while /proc/self/cgroup keeps
// naming the host path, which does not exist inside the container. That
// fallback cannot smuggle a node-scale number back in: a genuine host root has
// no memory.max at all (cgroup v2 does not give the root cgroup controller
// files) and spells v1's limit as the unlimited sentinel, both of which
// readCgroupBytes reports as no limit.
//
// A caller that wants a fallback to the host's MemTotal has to add it itself:
// this package deliberately has none (see memlimit.go's file comment).
func CgroupMemoryLimit() (int64, string, bool) {
	v2, v1 := cgroupPaths()
	for _, p := range limitFiles(cgroupRoot, v2, "memory.max") {
		if v, ok := readCgroupBytes(p); ok {
			return v, p, true
		}
	}
	// cgroup v1 puts the controller in its own subtree.
	if v1 != "" {
		for _, p := range limitFiles(filepath.Join(cgroupRoot, "memory"), v1, "memory.limit_in_bytes") {
			if v, ok := readCgroupBytes(p); ok {
				return v, p, true
			}
		}
	}
	return 0, "", false
}

// limitFiles lists the files that may hold this container's own limit, most
// specific first: the cgroup /proc/self/cgroup names, then the mount root
// itself. A rel that would leave base is dropped entirely, so a malformed
// /proc/self/cgroup can never read a file outside the cgroup mount.
func limitFiles(base, rel, name string) []string {
	base = filepath.Clean(base)
	out := make([]string, 0, 2)
	if dir := filepath.Join(base, rel); dir != base && strings.HasPrefix(dir, base+string(filepath.Separator)) {
		out = append(out, filepath.Join(dir, name))
	}
	return append(out, filepath.Join(base, name))
}

// cgroupMountShadowed reports whether a cgroup filesystem mounted at
// cgroupRoot (or at its v1 memory controller directory) is rooted ABOVE this
// process's cgroup namespace — mountinfo's root field then starts with "/..".
// That is the shape of the node's /sys/fs/cgroup bind-mounted over the
// container's own: the mount shows the node's hierarchy, /proc/self/cgroup
// names "/" relative to the namespace, and the container's own directory is
// unreachable by any path this package can form. The last matching entry wins,
// because a later mount at the same point is the one that shadows the earlier.
func cgroupMountShadowed() (mountRoot string, shadowed bool) {
	b, err := os.ReadFile(procMountinfo)
	if err != nil {
		return "", false
	}
	targets := []string{filepath.Clean(cgroupRoot), filepath.Join(cgroupRoot, "memory")}
	found := map[string]string{}
	for line := range strings.Lines(string(b)) {
		// id parent major:minor root mountpoint options [optional...] - fstype source super
		pre, post, ok := strings.Cut(line, " - ")
		if !ok {
			continue
		}
		fields, fs := strings.Fields(pre), strings.Fields(post)
		if len(fields) < 5 || len(fs) < 1 || (fs[0] != "cgroup2" && fs[0] != "cgroup") {
			continue
		}
		point := unescapeMountinfo(fields[4])
		if slices.Contains(targets, point) {
			found[point] = unescapeMountinfo(fields[3])
		}
	}
	for _, point := range targets {
		if root, ok := found[point]; ok && (root == "/.." || strings.HasPrefix(root, "/../")) {
			return root, true
		}
	}
	return "", false
}

// unescapeMountinfo undoes the kernel's octal escaping of mountinfo paths
// (space, tab, newline and backslash are written as \040, \011, \012, \134).
func unescapeMountinfo(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var out strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				out.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		out.WriteByte(s[i])
	}
	return out.String()
}

// cgroupPaths reads /proc/self/cgroup and returns this process's path in the
// v2 hierarchy and in the v1 memory controller. Either may be empty; "/" is
// the normal answer inside a cgroup namespace and is what makes the walk above
// degenerate to reading the mount point itself.
func cgroupPaths() (v2, v1 string) {
	f, err := os.Open(procCgroup)
	if err != nil {
		return "/", ""
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		// hierarchy-ID:controller-list:path
		parts := strings.SplitN(sc.Text(), ":", 3)
		if len(parts) != 3 {
			continue
		}
		switch {
		case parts[0] == "0" && parts[1] == "":
			v2 = parts[2]
		case slices.Contains(strings.Split(parts[1], ","), "memory"):
			v1 = parts[2]
		}
	}
	if v2 == "" {
		v2 = "/"
	}
	return v2, v1
}

// readCgroupBytes reads one cgroup limit file. "max" and the sentinel v1 uses
// for unlimited (a number near the int64 maximum) report not-ok: neither is a
// limit anything can be planned against.
func readCgroupBytes(path string) (int64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(b))
	if s == "max" {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 || v >= 1<<62 {
		return 0, false
	}
	return v, true
}
