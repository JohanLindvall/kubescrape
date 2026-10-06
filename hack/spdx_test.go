// SPDX-License-Identifier: MIT

package hack

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// spdxID is the license every source file declares. It is the LICENSE file's,
// and TestEverySourceFileCarriesTheLicenseIdentifier holds the two together.
const spdxID = "SPDX-License-Identifier: MIT"

// TestEverySourceFileCarriesTheLicenseIdentifier holds the rule that every
// source file states its license in its first line (after a shebang, the line
// after it): `// SPDX-License-Identifier: MIT` in Go, the `#` form in shell,
// Python, Makefiles and Dockerfiles. A legal review approves a dependency by
// scanning file headers rather than the repository root, and the pkg/
// packages are built to be imported elsewhere, where a file travels without
// the LICENSE beside it.
//
// It lists files through git — tracked ones and new ones not ignored — so a
// file that is not committed yet fails here before it is, and build output
// never takes part. testdata/ holds fixtures and fuzz corpora, not source.
func TestEverySourceFileCarriesTheLicenseIdentifier(t *testing.T) {
	git := lookPathOrSkip(t, "git")
	license, err := os.ReadFile(filepath.Join("..", "LICENSE"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(license, []byte("MIT License\n")) {
		t.Fatalf("LICENSE no longer starts with %q: change spdxID and every source file's header with it", "MIT License")
	}
	cmd := exec.Command(git, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	cmd.Dir = ".."
	out, err := cmd.Output()
	if err != nil {
		t.Skipf("cannot list the repository's files with git (%v)", err)
	}
	var checked int
	var missing []string
	for path := range strings.SplitSeq(strings.TrimSuffix(string(out), "\x00"), "\x00") {
		marker := commentMarker(path)
		if marker == "" {
			continue
		}
		data, err := os.ReadFile(filepath.Join("..", path))
		if errors.Is(err, fs.ErrNotExist) {
			continue // deleted in the working tree, not yet in the index
		}
		if err != nil {
			t.Fatal(err)
		}
		checked++
		if !hasLicenseHeader(data, marker) {
			missing = append(missing, path)
		}
	}
	if checked == 0 {
		t.Fatal("no source files found: the listing is not this repository's")
	}
	if len(missing) > 0 {
		t.Errorf("%d of %d source files do not start with %q (after a shebang, on the next line):\n  %s",
			len(missing), checked, spdxID, strings.Join(missing, "\n  "))
	}
}

// commentMarker is the line-comment syntax of a file the license rule covers,
// or "" for one it does not.
func commentMarker(path string) string {
	if strings.Contains("/"+path, "/testdata/") {
		return ""
	}
	base := filepath.Base(path)
	switch {
	case strings.HasSuffix(base, ".go"):
		return "//"
	case strings.HasSuffix(base, ".sh"), strings.HasSuffix(base, ".py"),
		base == "Makefile", strings.HasPrefix(base, "Dockerfile"):
		return "#"
	}
	return ""
}

// hasLicenseHeader reports whether the file's first line — the one after the
// shebang, when there is one — is the license identifier.
func hasLicenseHeader(data []byte, marker string) bool {
	lines := strings.SplitN(string(data), "\n", 3)
	if strings.HasPrefix(lines[0], "#!") {
		lines = lines[1:]
	}
	return len(lines) > 0 && strings.TrimRight(lines[0], "\r") == marker+" "+spdxID
}
