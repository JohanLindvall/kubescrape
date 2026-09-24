// Package hack holds the tests for the repository's build and development
// tooling — the Makefile and the hack/*.sh scripts — which have no Go package
// of their own to sit beside. Each test runs the REAL file: `make -n` from the
// repository root, and each script as a copy in a temporary directory (the
// scripts locate hack/bin from their own path, so the checkout's hack/bin is
// never touched) against stub tools on a PATH that holds nothing else.
package hack

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// TestImageTargetsBuildTheDockerfileTheirTagsNeed pins which Dockerfile each
// image target reaches. `image` follows journald to the cgo Dockerfile, and
// `image-static` is `image` with TAGS_STATIC and a `-static` tag, so a
// TAGS_STATIC carrying journald would build the distroless/base image under a
// `-static` name without Dockerfile.static's own refusal ever running. The
// Makefile refuses that before delegating.
func TestImageTargetsBuildTheDockerfileTheirTagsNeed(t *testing.T) {
	makeBin := lookPathOrSkip(t, "make")
	for _, tc := range []struct {
		name           string
		args           []string
		wantDockerfile string // "" = the target must refuse
		wantTag        string
		wantRefusal    string
	}{
		{"image with journald", []string{"image", "TAGS=journald,azure,events"}, "Dockerfile", "img:t", ""},
		{"image without journald", []string{"image", "TAGS=azure,events"}, "Dockerfile.static", "img:t", ""},
		{"image-static default", []string{"image-static"}, "Dockerfile.static", "img:t-static", ""},
		{"image-static without tags", []string{"image-static", "TAGS_STATIC="}, "Dockerfile.static", "img:t-static", ""},
		{"image-static with journald", []string{"image-static", "TAGS_STATIC=journald,azure"}, "", "", "image-static cannot carry journald"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(makeBin, append([]string{"-n", "IMAGE=img", "TAG=t"}, tc.args...)...)
			cmd.Dir = ".."
			// A `make test` running this test exports its own MAKEFLAGS — its
			// jobserver, and command-line variables such as CI's `TAGS=` — which
			// the child would otherwise inherit as if they were typed here.
			cmd.Env = withoutEnv(os.Environ(), "MAKEFLAGS", "MFLAGS", "GNUMAKEFLAGS", "MAKELEVEL", "MAKEOVERRIDES", "TAGS", "TAGS_STATIC", "IMAGE", "TAG")
			out, err := cmd.CombinedOutput()
			builds := dockerBuilds(string(out))
			if tc.wantRefusal != "" {
				if err == nil || !strings.Contains(string(out), tc.wantRefusal) {
					t.Fatalf("make %v: want a refusal containing %q, got err=%v and:\n%s", tc.args, tc.wantRefusal, err, out)
				}
				if len(builds) != 0 {
					t.Fatalf("make %v refused but still ran a build: %q", tc.args, builds)
				}
				return
			}
			if err != nil {
				t.Fatalf("make %v: %v\n%s", tc.args, err, out)
			}
			want := fmt.Sprintf("docker build -f %s ", tc.wantDockerfile)
			if len(builds) != 1 || !strings.HasPrefix(builds[0], want) || !strings.HasSuffix(builds[0], " -t "+tc.wantTag+" .") {
				t.Fatalf("make %v: want exactly one %q... -t %s build, got %q", tc.args, want, tc.wantTag, builds)
			}
		})
	}
}

// TestClusterUpTreatsAnUnrunnableToolAsAMismatch: ensure_tool promises that a
// kind or kubectl which is not the pinned version is replaced by a download.
// A tool that cannot run its version subcommand AT ALL — a wrong-architecture
// copy in hack/bin, a version-manager shim on PATH with no version selected —
// is the same case, and under errexit + pipefail a failing probe used to end
// the script at the assignment with 126 and no message.
func TestClusterUpTreatsAnUnrunnableToolAsAMismatch(t *testing.T) {
	const kindV, kubectlV = "v0.99.0", "v1.33.1"
	for _, tc := range []struct {
		name string
		// broken is the tool that cannot run; inBin places it in hack/bin
		// (checked first) rather than on the caller's PATH.
		broken string
		inBin  bool
		want   []string
	}{
		{"wrong-architecture kind in hack/bin", "kind", true, []string{"kind unknown in ", " is not the pinned " + kindV}},
		{"kubectl shim on PATH with no version set", "kubectl", false, []string{"kubectl unknown on PATH is not the pinned " + kubectlV}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bash := lookPathOrSkip(t, "bash")
			root := t.TempDir()
			hackDir := filepath.Join(root, "hack")
			binDir := filepath.Join(hackDir, "bin")
			stubDir := filepath.Join(root, "stubs")
			for _, d := range []string{binDir, stubDir} {
				mustMkdir(t, d)
			}
			script := copyScript(t, "cluster-up.sh", hackDir)
			for _, f := range []string{"kind-config.yaml", "test-workloads.yaml"} {
				writeFile(t, filepath.Join(hackDir, f), "", 0o644)
			}

			// Every pinned tool the stub curl "downloads", and every working one
			// the case starts with: answers `version` at the pin, succeeds at
			// everything else.
			good := filepath.Join(root, "good-tool")
			writeFile(t, good, fmt.Sprintf(`#!/bin/sh
case "${0##*/} $1" in
"kind version") echo "kind %s go1.27 linux/amd64" ;;
"kubectl version") echo "Client Version: %s" ;;
esac
exit 0
`, kindV, kubectlV), 0o755)
			writeFile(t, filepath.Join(stubDir, "curl"), fmt.Sprintf("#!/bin/sh\n# curl -fsSLo <out> <url>\ncp %q \"$2\"\n", good), 0o755)
			writeFile(t, filepath.Join(stubDir, "docker"), "#!/bin/sh\nexit 0\n", 0o755)

			brokenAt := filepath.Join(stubDir, tc.broken)
			if tc.inBin {
				brokenAt = filepath.Join(binDir, tc.broken)
				// An ELF header the kernel refuses (ENOEXEC): bash reports
				// "cannot execute binary file" and exits 126 — the wrong-arch case.
				writeFile(t, brokenAt, "\x7fELF\x02\x01\x01"+strings.Repeat("\x00", 57), 0o755)
			} else {
				writeFile(t, brokenAt, "#!/bin/sh\necho \"No version is set for command ${0##*/}\" >&2\nexit 126\n", 0o755)
			}
			// The other tool starts out working, on PATH.
			other := map[string]string{"kind": "kubectl", "kubectl": "kind"}[tc.broken]
			copyFile(t, good, filepath.Join(stubDir, other), 0o755)

			cmd := exec.Command(bash, script)
			cmd.Env = []string{
				"PATH=" + stubDir + string(os.PathListSeparator) + systemTools(t, root, "dirname", "uname", "tr", "awk", "sed", "cut", "grep", "mkdir", "mktemp", "chmod", "mv", "rm", "cp"),
				"HOME=" + root,
				"TMPDIR=" + root,
				"KIND_VERSION=" + kindV,
				"KUBECTL_VERSION=" + kubectlV,
			}
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("cluster-up.sh failed (%v) — an unrunnable %s must read as a mismatch, not end the script:\n%s", err, tc.broken, out)
			}
			for _, want := range append(tc.want, "downloading "+tc.broken+" ") {
				if !strings.Contains(string(out), want) {
					t.Errorf("output lacks %q:\n%s", want, out)
				}
			}
			// The download replaced the broken tool in hack/bin.
			got, err := os.ReadFile(filepath.Join(binDir, tc.broken))
			want, _ := os.ReadFile(good)
			if err != nil || !bytes.Equal(got, want) {
				t.Errorf("hack/bin/%s is not the downloaded pinned copy (err=%v)", tc.broken, err)
			}
		})
	}
}

// TestEnsureHelmTreatsAnUnrunnableHelmAsAMismatch is the same property for
// hack/ensure-helm.sh, whose `path_version="$(helm_version_of ...)"` had the
// same errexit trap: a helm shim on PATH that cannot answer `version` must
// read as "unknown" and take the download path (here, a failed download, so
// the documented fallback to the PATH helm with a warning).
func TestEnsureHelmTreatsAnUnrunnableHelmAsAMismatch(t *testing.T) {
	bash := lookPathOrSkip(t, "bash")
	root := t.TempDir()
	hackDir := filepath.Join(root, "hack")
	stubDir := filepath.Join(root, "stubs")
	for _, d := range []string{hackDir, stubDir} {
		mustMkdir(t, d)
	}
	script := copyScript(t, "ensure-helm.sh", hackDir)
	writeFile(t, filepath.Join(hackDir, "helm-version"), "v3.99.0\n", 0o644)
	helm := filepath.Join(stubDir, "helm")
	writeFile(t, helm, "#!/bin/sh\necho 'No version is set for command helm' >&2\nexit 126\n", 0o755)
	writeFile(t, filepath.Join(stubDir, "curl"), "#!/bin/sh\nexit 22\n", 0o755)

	cmd := exec.Command(bash, script)
	cmd.Env = []string{
		"PATH=" + stubDir + string(os.PathListSeparator) + systemTools(t, root, "dirname", "tr", "cut", "uname", "mkdir", "mktemp", "rm", "mv", "chmod", "tar", "gzip"),
		"HOME=" + root,
		"TMPDIR=" + root,
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("ensure-helm.sh failed (%v) — an unrunnable helm must read as a mismatch, not end the script:\n%s", err, stderr.String())
	}
	for _, want := range []string{"helm unknown on PATH is not the pinned v3.99.0", "downloading helm v3.99.0", "could not fetch helm v3.99.0; using " + helm} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr lacks %q:\n%s", want, stderr.String())
		}
	}
	if got := strings.TrimSpace(stdout.String()); got != helm {
		t.Errorf("printed %q, want the PATH fallback %q", got, helm)
	}
}

// dockerBuilds returns the `docker build` lines of a `make -n` run.
func dockerBuilds(out string) []string {
	var builds []string
	for line := range strings.SplitSeq(out, "\n") {
		if strings.HasPrefix(line, "docker build ") {
			builds = append(builds, line)
		}
	}
	return builds
}

func withoutEnv(env []string, names ...string) []string {
	out := env[:0:0]
	for _, kv := range env {
		if name, _, _ := strings.Cut(kv, "="); !slices.Contains(names, name) {
			out = append(out, kv)
		}
	}
	return out
}

func lookPathOrSkip(t *testing.T, name string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the tooling is POSIX shell and make")
	}
	p, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("%s not found: %v", name, err)
	}
	return p
}

// systemTools returns a directory holding a link to each named system utility
// and nothing else, so the script under test cannot find the machine's own
// kind, kubectl, helm or curl on its PATH. A link keeps the tool's name, which
// multi-call binaries (busybox, uutils coreutils) dispatch on.
func systemTools(t *testing.T, root string, names ...string) string {
	t.Helper()
	dir := filepath.Join(root, "tools")
	mustMkdir(t, dir)
	for _, n := range names {
		p, err := exec.LookPath(n)
		if err != nil {
			t.Skipf("%s not found: %v", n, err)
		}
		if err := os.Symlink(p, filepath.Join(dir, n)); err != nil && !errors.Is(err, os.ErrExist) {
			t.Fatal(err)
		}
	}
	return dir
}

func copyScript(t *testing.T, name, dir string) string {
	t.Helper()
	dst := filepath.Join(dir, name)
	copyFile(t, name, dst, 0o755)
	return dst
}

func copyFile(t *testing.T, src, dst string, mode os.FileMode) {
	t.Helper()
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, dst, string(b), mode)
}

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func mustMkdir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
}
