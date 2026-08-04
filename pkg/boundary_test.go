// Package pkg_test enforces the one structural rule of the public packages.
package pkg_test

import (
	"os/exec"
	"strings"
	"testing"
)

// modulePath is this module. A dependency under modulePath + "/internal/" is
// the violation; the standard library's own internal packages (crypto/internal
// /..., internal/godebug, and ~110 others) are not, which is why a bare
// "/internal/" grep cannot be the check.
const modulePath = "github.com/JohanLindvall/kubescrape"

// Everything under pkg/ is importable by other modules. Go's own internal rule
// would NOT stop pkg/ from importing this module's internal/ — the import is
// legal inside the module — but it breaks every external consumer at compile
// time, and it breaks them at THEIR build, not ours. So nothing here would
// fail: the offending import compiles, vets, lints and tests cleanly in this
// repo forever.
//
// CLAUDE.md states the rule ("They must never import internal/"); this is what
// makes it true rather than remembered.
func TestPublicPackagesDoNotImportInternal(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "./...").Output()
	if err != nil {
		t.Fatalf("go list -deps ./...: %v", err)
	}
	var bad []string
	for _, dep := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if strings.HasPrefix(dep, modulePath+"/internal/") {
			bad = append(bad, dep)
		}
	}
	if len(bad) > 0 {
		t.Errorf("pkg/ transitively imports internal packages, which breaks every external consumer:\n\t%s",
			strings.Join(bad, "\n\t"))
	}
}
