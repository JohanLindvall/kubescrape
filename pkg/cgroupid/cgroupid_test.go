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
		{"/", "", ""},
		{"/kubepods", "", ""},
		{"/kubepods.slice/kubepods-burstable.slice", "", ""},
		{"/system.slice/containerd.service", "", ""},
	}
	for _, c := range cases {
		uid, cid := cgroupid.Identity(c.id)
		if uid != c.uid || cid != c.cid {
			t.Errorf("cgroupid.Identity(%q) = (%q, %q), want (%q, %q)", c.id, uid, cid, c.uid, c.cid)
		}
	}
}

// FuzzCgroupIdentity feeds arbitrary strings through the cadvisor cgroup-path
// parser. Invariants: no panics; a non-empty pod UID is always canonical
// 8-4-4-4-12 form; a non-empty container ID is always 64 hex characters.
func FuzzCgroupIdentity(f *testing.F) {
	seeds := []string{
		"/kubepods/burstable/pod12345678-1234-1234-1234-123456789012/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod12345678_1234_1234_1234_123456789012.slice/cri-containerd-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.scope",
		"/kubepods/pod12345678-1234-1234-1234-123456789012",
		"/", "", "//", "pod", "podX", "/system.slice/docker-.scope",
		"/kubepods.slice/kubepods-pod_.slice", "pod\x00", "/kubepods/pod£züß/…",
		"/a/pod12345678-1234-1234-1234-12345678901G/b",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, id string) {
		podUID, containerID := cgroupid.Identity(id)
		if podUID != "" && !cgroupid.IsPodUID(podUID) {
			t.Fatalf("cgroupid.Identity(%q) returned non-canonical pod UID %q", id, podUID)
		}
		if containerID != "" && !cgroupid.IsContainerID(containerID) {
			t.Fatalf("cgroupid.Identity(%q) returned non-hex container ID %q", id, containerID)
		}
	})
}
