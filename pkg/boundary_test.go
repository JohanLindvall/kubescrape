// Package pkg_test enforces the one structural rule of the public packages.
package pkg_test

import (
	"errors"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// modulePath is this module. A dependency under modulePath + "/internal/" is
// the violation; the standard library's own internal packages (crypto/internal
// /..., internal/godebug, and ~110 others) are not, which is why a bare
// "/internal/" grep cannot be the check.
const modulePath = "github.com/JohanLindvall/kubescrape"

// Everything under pkg/ is importable by other modules. Go's own internal rule
// does NOT stop pkg/ from importing this module's internal/: the rule is
// checked per import edge against the IMPORTER's path, pkg/ sits inside the
// tree internal/ belongs to, so the import is legal — here, and equally in an
// external consumer's build, which still compiles. What the import costs is
// the reason for the rule: every internal package pkg/ reaches, with its own
// dependency tree, is dragged into every consumer's build; an internal type
// surfacing in a pkg/ signature is one the consumer cannot name; and internal/
// carries no compatibility promise, so its churn silently becomes a change to
// a public package. Nothing in the toolchain objects — the offending import
// compiles, vets, lints and tests cleanly here and downstream forever.
//
// AGENTS.md states the rule ("They must never import internal/"); this is what
// makes it true rather than remembered.
func TestPublicPackagesDoNotImportInternal(t *testing.T) {
	// Pin the directory to THIS file's rather than inheriting the process CWD.
	// A Go test binary runs in its package directory under `go test ./...`, but
	// not when the compiled binary is run from elsewhere (`go test -c`, a CI
	// harness, a debugger) — and there "./..." would resolve to a different
	// package set, or to no module at all, and this check would fail loudly
	// while proving nothing.
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("cannot locate the test source; skipping the import-boundary check")
	}
	cmd := exec.Command("go", "list", "-deps", "./...")
	cmd.Dir = filepath.Dir(thisFile)
	out, err := cmd.Output()
	if err != nil {
		// Skip rather than fail: an environment without a usable toolchain or
		// module cache cannot answer the question, and a false alarm here would
		// train people to ignore the one check that guards the pkg/ boundary.
		var stderr string
		if ee, ok := errors.AsType[*exec.ExitError](err); ok {
			stderr = string(ee.Stderr)
		}
		t.Skipf("cannot run `go list -deps ./...` in %s (%v): %s", cmd.Dir, err, stderr)
	}
	var bad []string
	for dep := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		if strings.HasPrefix(dep, modulePath+"/internal/") {
			bad = append(bad, dep)
		}
	}
	if len(bad) > 0 {
		t.Errorf("pkg/ transitively imports internal packages, which drags them (and their dependencies) into every external consumer's build and makes their unversioned APIs part of the public packages:\n\t%s",
			strings.Join(bad, "\n\t"))
	}
}
