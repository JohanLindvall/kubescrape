package promscrape

import "strings"

// cgroupIdentity extracts the pod UID and container ID from a cadvisor "id"
// label (the cgroup path). Both the cgroupfs and the systemd driver layouts
// are understood:
//
//	/kubepods/burstable/pod<uid>/<containerID>
//	/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod<uid_with_underscores>.slice/cri-containerd-<containerID>.scope
//
// The QoS segment is absent for Guaranteed pods; container scopes may be
// prefixed cri-containerd-, crio- or docker-. Unrecognized segments yield
// empty results.
func cgroupIdentity(id string) (podUID, containerID string) {
	for _, seg := range strings.Split(id, "/") {
		if seg == "" {
			continue
		}
		seg = strings.TrimSuffix(seg, ".slice")
		seg = strings.TrimSuffix(seg, ".scope")

		// Pod segment: "pod<uid>" (cgroupfs) or
		// "kubepods-<qos>-pod<uid_with_underscores>" (systemd).
		if i := strings.LastIndex(seg, "pod"); i >= 0 {
			if uid := strings.ReplaceAll(seg[i+3:], "_", "-"); isPodUID(uid) {
				podUID = uid
				continue
			}
		}
		// Container segment: a bare hex ID (cgroupfs) or
		// "<runtime>-<hexID>" (systemd scope).
		cand := seg
		if i := strings.LastIndexByte(cand, '-'); i >= 0 {
			cand = cand[i+1:]
		}
		if isContainerID(cand) {
			containerID = cand
		}
	}
	return podUID, containerID
}

// isPodUID matches the canonical UID form 8-4-4-4-12 (hex and dashes).
func isPodUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch i {
		case 8, 13, 18, 23:
			if s[i] != '-' {
				return false
			}
		default:
			if !isHexByte(s[i]) {
				return false
			}
		}
	}
	return true
}

// isContainerID matches a 64-character hex runtime container ID.
func isContainerID(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isHexByte(s[i]) {
			return false
		}
	}
	return true
}

func isHexByte(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
