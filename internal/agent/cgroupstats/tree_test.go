package cgroupstats

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// A fabricated cgroup v2 tree in t.TempDir(). Everything this package reads is
// a plain file, so a real hierarchy is only needed for the statfs branch —
// which is exactly why checkCgroup2 falls back to the cgroup.controllers probe
// (see version.go).

// hexID builds a syntactically valid 64-hex runtime container id from a seed,
// so tests can name containers readably without hand-typing 64 characters.
// DISTINCT per seed — the sampler keys its tracked set by container id, so
// colliding fixtures would silently test one container instead of n.
func hexID(seed int) string {
	b := make([]byte, 0, 64)
	b = fmt.Appendf(b, "%08x", uint32(seed))
	for len(b) < 64 {
		b = append(b, "0123456789abcdef"[(seed+len(b))%16])
	}
	return string(b)
}

// podUID is a canonical 8-4-4-4-12 uid derived from a seed.
func podUID(seed int) string {
	h := hexID(seed)
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[:8], h[8:12], h[12:16], h[16:20], h[20:32])
}

// systemdUID is the same uid as a systemd slice spells it.
func systemdUID(seed int) string {
	u := []byte(podUID(seed))
	for i, c := range u {
		if c == '-' {
			u[i] = '_'
		}
	}
	return string(u)
}

// newRoot creates a cgroup v2 root: an empty hierarchy that checkCgroup2
// accepts.
func newRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, filepath.Join(root, controllersFile), "cpu memory pids\n")
	return root
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// cpuStat renders a realistic cpu.stat. The field list matches a current
// kernel's, so a reader that assumed "the first line" would pass while a reader
// that assumed a fixed offset would not.
func cpuStat(usageUsec uint64) string {
	return fmt.Sprintf("usage_usec %d\nuser_usec %d\nsystem_usec %d\nnr_periods 0\nnr_throttled 0\nthrottled_usec 0\nnr_bursts 0\nburst_usec 0\n",
		usageUsec, usageUsec*3/4, usageUsec/4)
}

// memStat renders a memory.stat with inactive_file where the kernel puts it:
// well past the head, after twenty-odd other fields. That is what makes the
// "scan as the bytes arrive, stop at the key" read path meaningful rather than
// a first-line special case.
func memStat(inactiveFile uint64) string {
	s := ""
	for _, k := range []string{
		"anon", "file", "kernel", "kernel_stack", "pagetables", "sec_pagetables",
		"percpu", "sock", "vmalloc", "shmem", "zswap", "zswapped", "file_mapped",
		"file_dirty", "file_writeback", "swapcached", "anon_thp", "file_thp",
		"shmem_thp", "inactive_anon", "active_anon",
	} {
		s += k + " 4096\n"
	}
	s += fmt.Sprintf("inactive_file %d\n", inactiveFile)
	for _, k := range []string{"active_file", "unevictable", "slab_reclaimable", "slab_unreclaimable", "slab"} {
		s += k + " 8192\n"
	}
	return s
}

// makeContainer writes the three sampled files into a container cgroup dir.
func makeContainer(t *testing.T, dir string, usageUsec, memCurrent, inactiveFile uint64) {
	t.Helper()
	writeFile(t, filepath.Join(dir, fileCPUStat), cpuStat(usageUsec))
	writeFile(t, filepath.Join(dir, fileMemCurrent), fmt.Sprintf("%d\n", memCurrent))
	writeFile(t, filepath.Join(dir, fileMemStat), memStat(inactiveFile))
}

// kindContainerDir is the path a kind node uses: the whole tree nested under
// kubelet.slice, with each systemd slice named for its full ancestry.
func kindContainerDir(root string, pod int, cid string) string {
	u := systemdUID(pod)
	return filepath.Join(root,
		"kubelet.slice",
		"kubelet-kubepods.slice",
		"kubelet-kubepods-besteffort.slice",
		"kubelet-kubepods-besteffort-pod"+u+".slice",
		"cri-containerd-"+cid+".scope")
}

// systemdContainerDir is a stock systemd-driver node (burstable QoS).
func systemdContainerDir(root string, pod int, cid string) string {
	u := systemdUID(pod)
	return filepath.Join(root,
		"kubepods.slice",
		"kubepods-burstable.slice",
		"kubepods-burstable-pod"+u+".slice",
		"cri-containerd-"+cid+".scope")
}

// guaranteedContainerDir has NO QoS segment, which is what the Guaranteed class
// looks like on a systemd-driver node.
func guaranteedContainerDir(root string, pod int, cid string) string {
	u := systemdUID(pod)
	return filepath.Join(root,
		"kubepods.slice",
		"kubepods-pod"+u+".slice",
		"cri-containerd-"+cid+".scope")
}

// cgroupfsContainerDir is the cgroupfs driver's layout: bare names, no slices.
func cgroupfsContainerDir(root string, pod int, cid string) string {
	return filepath.Join(root, "kubepods", "besteffort", "pod"+podUID(pod), cid)
}

// fakeResolver records what it was asked about and writes a resource that a
// test can recognise. The real one is *promscrape.Scraper.
//
// refuse is how a test spells "the metadata service does not know this
// container id" — a DEFINITIVE 404, the pod sandbox's permanent condition and a
// new container's for its first second. down is the other refusal, and the one
// the retry policy treats completely differently: the service could not be
// reached at all, which says nothing about any container. It is mutex-guarded
// because Run resolves on the sampler goroutine while the test reads the calls.
type fakeResolver struct {
	mu     sync.Mutex
	calls  []resolveCall
	refuse map[string]bool
	// down makes every lookup fail as UNANSWERED, whatever refuse says: the
	// metadata service is unreachable.
	down bool
	// delay is charged per call, for the resolution-budget test.
	delay time.Duration
	// extra is stamped on every resolved resource. It stands for everything the
	// real resolver reads out of live metadata and can therefore CHANGE under a
	// running container — an edited pod or namespace label, a `.Node` attribute
	// template that only had node metadata to read from on the second attempt.
	extra map[string]string
}

type resolveCall struct{ containerID, podUID string }

func (f *fakeResolver) FillContainerResource(ctx context.Context, res pcommon.Resource, containerID, podUID string) (bool, bool) {
	f.mu.Lock()
	f.calls = append(f.calls, resolveCall{containerID, podUID})
	refused := f.refuse[containerID]
	down := f.down
	delay := f.delay
	extra := make(map[string]string, len(f.extra))
	for k, v := range f.extra {
		extra[k] = v
	}
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			// The real resolver's lookups are context-bounded, and the export
			// rebuild leans on that: its pass deadline only bounds anything if
			// a lookup actually gives up when the context does. A lookup cut
			// short answered nothing, which is what the second false says.
			return false, false
		}
	}
	if down {
		// Unreachable: no verdict about this container at all.
		return false, false
	}
	if refused {
		// The real resolver still writes what the cgroup path told it; the
		// sampler must decline on the VERDICT, not on the resource being empty.
		res.Attributes().PutStr("container.id", containerID)
		return false, true
	}
	if containerID != "" {
		res.Attributes().PutStr("container.id", containerID)
	}
	if podUID != "" {
		res.Attributes().PutStr("k8s.pod.uid", podUID)
	}
	// A resolved container carries a service.name, which is the whole point:
	// without one the OTLP→Prometheus translation gives the series no `job`.
	res.Attributes().PutStr("service.name", "workload")
	for k, v := range extra {
		res.Attributes().PutStr(k, v)
	}
	return true, true
}

// setDown is how a test spells "the metadata service is unreachable": every
// lookup fails without answering anything, which is the failure class that must
// never abandon a container.
func (f *fakeResolver) setDown(down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = down
}

// set installs an attribute every later resolution will carry, which is how a
// test spells "somebody edited this pod's labels".
func (f *fakeResolver) set(key, value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.extra == nil {
		f.extra = map[string]string{}
	}
	f.extra[key] = value
}

// resolved reports the ids the resolver was asked about.
func (f *fakeResolver) asked() []resolveCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]resolveCall(nil), f.calls...)
}

func (f *fakeResolver) reject(ids ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse == nil {
		f.refuse = map[string]bool{}
	}
	for _, id := range ids {
		f.refuse[id] = true
	}
}

func (f *fakeResolver) accept(ids ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		delete(f.refuse, id)
	}
}
