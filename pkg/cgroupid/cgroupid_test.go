package cgroupid_test

import (
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/pkg/cgroupid"
)

const (
	uid1   = "0a1b2c3d-1111-2222-3333-444455556666"
	appCID = "d4f00c1e8a2b4c5d6e7f80912a3b4c5d6e7f80912a3b4c5d6e7f80912a3b4c5d"
)

func TestCgroupIdentity(t *testing.T) {
	cases := []struct {
		id       string
		uid, cid string
	}{
		{"/kubepods/burstable/pod" + uid1 + "/" + appCID, uid1, appCID},
		{"/kubepods/pod" + uid1 + "/" + appCID, uid1, appCID}, // Guaranteed QoS
		{"/kubepods/burstable/pod" + uid1, uid1, ""},
		{"/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod" +
			strings.ReplaceAll(uid1, "-", "_") + ".slice/cri-containerd-" + appCID + ".scope", uid1, appCID},
		{"/kubepods.slice/kubepods-pod" + strings.ReplaceAll(uid1, "-", "_") + ".slice/docker-" + appCID + ".scope", uid1, appCID},
		{"/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod" +
			strings.ReplaceAll(uid1, "-", "_") + ".slice/crio-" + appCID + ".scope", uid1, appCID},
		// A kubelet run with --cgroup-root (kind and several distros): the whole
		// kubepods tree hangs under kubelet.slice and every segment gains a
		// "kubelet-" prefix. Telling the POD SLICE from the container SCOPE
		// beneath it is what cadvisor sandbox detection turns on, so both halves
		// of this layout are pinned.
		{"/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-besteffort.slice/kubelet-kubepods-besteffort-pod" +
			strings.ReplaceAll(uid1, "-", "_") + ".slice", uid1, ""},
		{"/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-besteffort.slice/kubelet-kubepods-besteffort-pod" +
			strings.ReplaceAll(uid1, "-", "_") + ".slice/cri-containerd-" + appCID + ".scope", uid1, appCID},
		{"/", "", ""},
		{"/kubepods", "", ""},
		{"/kubepods.slice/kubepods-burstable.slice", "", ""},
		{"/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-besteffort.slice", "", ""},
		{"/system.slice/containerd.service", "", ""},
	}
	for _, c := range cases {
		uid, cid := cgroupid.Identity(c.id)
		if uid != c.uid || cid != c.cid {
			t.Errorf("cgroupid.Identity(%q) = (%q, %q), want (%q, %q)", c.id, uid, cid, c.uid, c.cid)
		}
	}
}

// A STATIC pod's cgroup is named after a UID no API server ever assigned: the
// kubelet mints it from the manifest as an md5, so it is 32 bare hex digits
// rather than the canonical 8-4-4-4-12. Dropping it is not a cosmetic loss of
// one attribute — it is the loss of the only thing a caller can cross-check a
// by-name metadata lookup against, and the pods it happens to are the control
// plane's, on every node.
//
// The path and both ids are copied from a live kind v1.33 node (kubelet run
// with --cgroup-root, hence the kubelet- prefixes): the pod is
// kube-system/kube-apiserver-<node>, whose mirror pod carries this very value
// as kubernetes.io/config.hash.
func TestIdentityReadsAStaticPodsKubeletMintedUID(t *testing.T) {
	const (
		staticUID = "690540eda46e8f874de849cc6a40a909"
		cid       = "5573a6c340a7927c3195da3397600c3d15cd15b6008cafd98441fbf3fa781251"
		podSlice  = "/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-burstable.slice/kubelet-kubepods-burstable-pod" + staticUID + ".slice"
	)
	cases := []struct {
		id       string
		uid, cid string
		child    bool
	}{
		{podSlice, staticUID, "", false},
		{podSlice + "/cri-containerd-" + cid + ".scope", staticUID, cid, true},
		// The cgroupfs driver's spelling of the same pod.
		{"/kubepods/burstable/pod" + staticUID + "/" + cid, staticUID, cid, true},
		// Not a UID: 31 and 33 hex digits, and one with a non-hex byte. The
		// 32-hex form is accepted because the kubelet writes it, not because
		// "a hex-looking tail" is enough.
		{"/kubepods/burstable/pod" + staticUID[:31] + "/" + cid, "", cid, false},
		{"/kubepods/burstable/pod" + staticUID + "aa/" + cid, "", cid, false},
		{"/kubepods/burstable/pod" + staticUID[:31] + "g/" + cid, "", cid, false},
	}
	for _, c := range cases {
		uid, gotCID, child := cgroupid.Parse(c.id)
		if uid != c.uid || gotCID != c.cid || child != c.child {
			t.Errorf("cgroupid.Parse(%q) = (%q, %q, %v), want (%q, %q, %v)",
				c.id, uid, gotCID, child, c.uid, c.cid, c.child)
		}
	}
}

// Parse's shape verdict is what tells a container OF a pod from anything else
// living under the pod slice. cadvisor's sandbox detection folds such a row onto
// the pod's resource, and folding something that merely sits beside the
// containers renames every series of that pod (see isSandbox in
// internal/agent/promscrape), so "(uid, cid) both parsed" is not enough — the
// container segment must be the pod segment's immediate child and the last one.
func TestParseReportsAContainerDirectlyUnderThePodSlice(t *testing.T) {
	systemdPod := "/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod" +
		strings.ReplaceAll(uid1, "-", "_") + ".slice"
	kubeletRootPod := "/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-besteffort.slice/kubelet-kubepods-besteffort-pod" +
		strings.ReplaceAll(uid1, "-", "_") + ".slice"
	cases := []struct {
		id   string
		want bool
		why  string
	}{
		{systemdPod + "/cri-containerd-" + appCID + ".scope", true, "a container scope of the pod"},
		{"/kubepods/burstable/pod" + uid1 + "/" + appCID, true, "the cgroupfs spelling of the same"},
		{"/kubepods//burstable//pod" + uid1 + "//" + appCID, true, "empty segments are not segments"},
		{kubeletRootPod + "/cri-containerd-" + appCID + ".scope", true, "the same under a custom --cgroup-root"},
		// The shape cannot see the difference and deliberately does not try: the
		// caller excludes conmon on evidence the path does not carry.
		{systemdPod + "/crio-conmon-" + appCID + ".scope", true, "CRI-O's conmon scope is shaped exactly like a container's"},
		{systemdPod, false, "the pod slice itself names no container"},
		{systemdPod + "/helper.slice/cri-containerd-" + appCID + ".scope", false, "nested below the pod's own children"},
		{"/system.slice/docker-" + appCID + ".scope", false, "a standalone container, no pod slice above it"},
		{"/customroot/workload/cri-containerd-" + appCID + ".scope", false, "no parseable pod segment"},
		{"/kubepods", false, "no ids at all"},
	}
	for _, c := range cases {
		podUID, containerID, podContainer := cgroupid.Parse(c.id)
		if podContainer != c.want {
			t.Errorf("cgroupid.Parse(%q) podContainer = %v, want %v (%s)", c.id, podContainer, c.want, c.why)
		}
		if uid, cid := cgroupid.Identity(c.id); uid != podUID || cid != containerID {
			t.Errorf("Identity(%q) = (%q, %q) but Parse says (%q, %q): the two must be one parse",
				c.id, uid, cid, podUID, containerID)
		}
	}
}

// FuzzCgroupIdentity feeds arbitrary strings through the cadvisor cgroup-path
// parser. Invariants: no panics; a non-empty pod UID is always one of the two
// forms IsPodUID takes (canonical 8-4-4-4-12, or a static pod's 32 bare hex
// digits); a non-empty container ID is always 64 hex characters; and a
// podContainer verdict always carries both ids, since it claims to have placed
// one segment relative to the other.
func FuzzCgroupIdentity(f *testing.F) {
	seeds := []string{
		"/kubepods/burstable/pod12345678-1234-1234-1234-123456789012/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod12345678_1234_1234_1234_123456789012.slice/cri-containerd-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.scope",
		"/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-besteffort.slice/kubelet-kubepods-besteffort-pod12345678_1234_1234_1234_123456789012.slice",
		"/kubepods/pod12345678-1234-1234-1234-123456789012",
		"/", "", "//", "pod", "podX", "/system.slice/docker-.scope",
		"/kubepods.slice/kubepods-pod_.slice", "pod\x00", "/kubepods/pod£züß/…",
		"/a/pod12345678-1234-1234-1234-12345678901G/b",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, id string) {
		podUID, containerID, podContainer := cgroupid.Parse(id)
		if podUID != "" && !cgroupid.IsPodUID(podUID) {
			t.Fatalf("cgroupid.Parse(%q) returned a pod UID of neither accepted form: %q", id, podUID)
		}
		if containerID != "" && !cgroupid.IsContainerID(containerID) {
			t.Fatalf("cgroupid.Parse(%q) returned non-hex container ID %q", id, containerID)
		}
		if podContainer && (podUID == "" || containerID == "") {
			t.Fatalf("cgroupid.Parse(%q) = (%q, %q, podContainer) — the verdict claims a container of a pod",
				id, podUID, containerID)
		}
	})
}
