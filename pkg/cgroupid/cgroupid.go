// Package cgroupid extracts Kubernetes pod and container identity from
// cgroup paths, understanding both the cgroupfs and the systemd cgroup
// driver layouts. It is the parsing behind cadvisor's `id` label but has no
// dependency on scraping — any code holding a cgroup path can use it.
package cgroupid

import "strings"

// Identity extracts the pod UID and container ID from a cadvisor "id"
// label (the cgroup path). Both the cgroupfs and the systemd driver layouts
// are understood:
//
//	/kubepods/burstable/pod<uid>/<containerID>
//	/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod<uid_with_underscores>.slice/cri-containerd-<containerID>.scope
//
// The QoS segment is absent for Guaranteed pods; container scopes may be
// prefixed cri-containerd-, crio- or docker-. Unrecognized segments yield
// empty results.
//
// Runs once per cadvisor sample, so the path is walked in place (substring
// slices) rather than split into an allocated segment slice.
func Identity(id string) (podUID, containerID string) {
	for start := 0; start < len(id); {
		var seg string
		if i := strings.IndexByte(id[start:], '/'); i >= 0 {
			seg = id[start : start+i]
			start += i + 1
		} else {
			seg = id[start:]
			start = len(id)
		}
		if seg == "" {
			continue
		}
		seg = strings.TrimSuffix(seg, ".slice")
		seg = strings.TrimSuffix(seg, ".scope")

		// Pod segment: "pod<uid>" (cgroupfs) or
		// "kubepods-<qos>-pod<uid_with_underscores>" (systemd).
		if i := strings.LastIndex(seg, "pod"); i >= 0 {
			if uid := strings.ReplaceAll(seg[i+3:], "_", "-"); IsPodUID(uid) {
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
		if IsContainerID(cand) {
			containerID = cand
		}
	}
	return podUID, containerID
}

// IsPodUID matches the canonical UID form 8-4-4-4-12 (hex and dashes).
func IsPodUID(s string) bool {
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

// IsContainerID matches a 64-character hex runtime container ID.
func IsContainerID(s string) bool {
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
