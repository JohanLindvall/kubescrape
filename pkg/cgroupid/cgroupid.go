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
func Identity(id string) (podUID, containerID string) {
	podUID, containerID, _ = Parse(id)
	return podUID, containerID
}

// Parse is Identity plus the SHAPE of the path: podContainer reports that the
// segment the container ID came from is the pod segment's IMMEDIATE child and
// the path's LAST segment — the layout every runtime uses for a container of
// that pod, and nothing deeper.
//
// `podUID != "" && containerID != ""` does NOT imply it, which is why the flag
// exists: any descendant of a pod slice carrying a hex-looking tail satisfies
// that pair, so a caller deciding what a row DESCRIBES (cadvisor's sandbox
// detection) would otherwise read a runtime helper cgroup parked beside the
// containers as one of them. It is deliberately a SHAPE verdict and nothing
// more — the last segment's prefix is not vetted, so CRI-O's
// crio-conmon-<containerID>.scope, which really is an immediate child of the pod
// slice, still reports true; a caller that must exclude it needs evidence the
// path does not carry (see isSandbox in internal/agent/promscrape).
//
// Runs once per cadvisor sample, so the path is walked in place (substring
// slices) rather than split into an allocated segment slice.
func Parse(id string) (podUID, containerID string, podContainer bool) {
	segs, podSeg, cidSeg := 0, -1, -1
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
		// Empty segments are skipped above, so the index counted here is the one
		// "immediate child" must be measured in: "/kubepods//pod<uid>//<cid>" is
		// the same path as its single-slash spelling.
		idx := segs
		segs++
		seg = strings.TrimSuffix(seg, ".slice")
		seg = strings.TrimSuffix(seg, ".scope")

		// Pod segment: "pod<uid>" (cgroupfs) or
		// "kubepods-<qos>-pod<uid_with_underscores>" (systemd).
		if i := strings.LastIndex(seg, "pod"); i >= 0 {
			if uid := strings.ReplaceAll(seg[i+3:], "_", "-"); IsPodUID(uid) {
				podUID, podSeg = uid, idx
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
			containerID, cidSeg = cand, idx
		}
	}
	return podUID, containerID, podSeg >= 0 && cidSeg == podSeg+1 && cidSeg == segs-1
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
