package kubemeta

import (
	"strings"
	"unicode"
)

// NormalizeContainerID strips the runtime scheme prefix from a container ID,
// so "containerd://abc", "docker://abc" and "abc" all normalize to "abc".
// It also tolerates a collapsed "containerd:/abc" form (as produced by HTTP
// path cleaning). Container runtime IDs themselves never contain a colon.
//
// It is idempotent — the same ID may be normalized again on another path
// (agent, HTTP handler), which must be a no-op. Hence the cut at the LAST
// colon (the result can never retain one) and the space trim after the
// slashes (a malformed "scheme:// id" must not need two passes).
func NormalizeContainerID(id string) string {
	id = strings.TrimSpace(id)
	if i := strings.LastIndexByte(id, ':'); i >= 0 {
		id = strings.TrimLeftFunc(id[i+1:], func(r rune) bool { return r == '/' || unicode.IsSpace(r) })
	}
	return id
}
