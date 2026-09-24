package cgroupstats

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// The chart mounts the node's hierarchy at /host/sys/fs/cgroup and renders
// -cgroup-stats-root pointing at it BY DEFAULT — mounting it over the
// container's own /sys/fs/cgroup hid the container's memory limit from
// internal/cli, so a capped agent ran without GOMEMLIMIT. An explicit root is
// fatal when it is wrong, so a cgroup v1 node would then CrashLoop the whole
// DaemonSet, logs included, for a metric: exactly what ErrUnsupportedNode
// exists to prevent. A root holding a GENUINE v1 hierarchy is not an operator
// error — it proves the operator pointed at the node's cgroup mount — so it
// must take the node-property path at any root.
func TestAnExplicitRootOnACgroupV1NodeDisablesThePipelineInsteadOfFailing(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	const root = "/host/sys/fs/cgroup"
	for name, v1 := range map[string]error{
		"root is a v1 controller mount":   &v1Error{root: root},
		"root is the tmpfs above v1 ones": &v1Error{root: root, controller: root + "/memory"},
	} {
		check := func(string) error { return v1 }
		_, err := New(Config{Root: root, Resolver: &fakeResolver{}, Logger: quiet, check: check})
		if !errors.Is(err, ErrUnsupportedNode) {
			t.Errorf("%s: err = %v at an explicit root, want ErrUnsupportedNode so a v1 node disables this pipeline rather than CrashLooping the DaemonSet", name, err)
			continue
		}
		if !strings.Contains(err.Error(), "v1") {
			t.Errorf("%s: the error does not name the version: %v", name, err)
		}
	}

	// What stays fatal at an explicit root is a path that is not a usable
	// cgroup hierarchy at all — the operator's typo, identical on every node.
	notMounted := func(string) error { return errors.New("/host/sys/fs/cgorup is not a cgroup v2 hierarchy") }
	_, err := New(Config{Root: root, Resolver: &fakeResolver{}, Logger: quiet, check: notMounted})
	if err == nil || errors.Is(err, ErrUnsupportedNode) {
		t.Errorf("err = %v for an explicit root with nothing mounted, want a fatal (non-ErrUnsupportedNode) refusal", err)
	}
}

// A directory merely NAMED `memory` is not evidence of a v1 node: the tmpfs
// probe must read the controller's filesystem, or an operator's typo landing on
// any tree with such a directory would silently disable the pipeline instead of
// refusing the value.
func TestADirectoryNamedMemoryIsNotACgroupV1Hierarchy(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root+"/memory/placeholder", "")
	err := checkCgroup2(root)
	var v1 *v1Error
	if err == nil || errors.As(err, &v1) {
		t.Errorf("checkCgroup2 = %v over a plain directory tree, want a non-v1 refusal", err)
	}
}
