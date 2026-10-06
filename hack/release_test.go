// SPDX-License-Identifier: MIT

package hack

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestEveryBuildStampsTheVersion pins VERSION reaching the link step of every
// target that builds a binary. Nothing else would notice it going missing: a
// binary without it still builds, starts and passes every test, and reports
// "unknown" — in an image always, because the image build cannot see .git.
func TestEveryBuildStampsTheVersion(t *testing.T) {
	const stamp = "-X github.com/JohanLindvall/kubescrape/internal/obs.Version=v1.2.3"
	for _, tc := range []struct {
		target   string
		goBuilds int // go build commands, every one of which must carry the stamp
		want     []string
	}{
		{"build", 2, nil},
		{"image", 0, []string{"--build-arg VERSION=v1.2.3"}},
		{"dist", 1, []string{ // one go build, in the per-binary loop
			// The chart: SemVer version, appVersion the image tag it then pins.
			"package charts/kubescrape --version 1.2.3 --app-version v1.2.3",
			// The manifests, pinned to the same image.
			"s#ghcr.io/johanlindvall/kubescrape:latest#ghcr.io/johanlindvall/kubescrape:v1.2.3#",
			// The binaries: the static variant, stripped like the images.
			`-tags "azure,events"`,
			`-ldflags "-s -w ` + stamp + `"`,
			"sha256sum",
		}},
	} {
		t.Run(tc.target, func(t *testing.T) {
			out, err := makeDryRun(t, tc.target, "VERSION=v1.2.3")
			if err != nil {
				t.Fatalf("make -n %s: %v\n%s", tc.target, err, out)
			}
			// Join backslash-continued lines, so one command is one line.
			var builds int
			for line := range strings.SplitSeq(strings.ReplaceAll(out, "\\\n", " "), "\n") {
				if !strings.Contains(line, "go build") {
					continue
				}
				builds++
				if !strings.Contains(line, stamp) {
					t.Errorf("make -n %s: a go build without the version stamp %q:\n%s", tc.target, stamp, line)
				}
			}
			if builds != tc.goBuilds {
				t.Errorf("make -n %s ran %d go build commands, want %d:\n%s", tc.target, builds, tc.goBuilds, out)
			}
			for _, w := range tc.want {
				if !strings.Contains(out, w) {
					t.Errorf("make -n %s VERSION=v1.2.3 does not run %q; got:\n%s", tc.target, w, out)
				}
			}
		})
	}
}

// TestDistRefusesANonReleaseVersion: the release assets pin the image tag
// VERSION names and package the chart as a SemVer version, so a `git describe`
// value (a bare commit, a -dirty tree) must be refused before anything is
// built, not discovered as a Helm error halfway through or, worse, published.
func TestDistRefusesANonReleaseVersion(t *testing.T) {
	for _, v := range []string{"54f1172", "v1.2.3-4-g54f1172-dirty x", "1.2.3", "v1.2", ""} {
		out, err := makeDryRun(t, "dist", "VERSION="+v)
		if err == nil || !strings.Contains(out, "dist needs VERSION=vMAJOR.MINOR.PATCH") {
			t.Errorf("make -n dist VERSION=%q: want the refusal, got err=%v and:\n%s", v, err, out)
		}
	}
	for _, v := range []string{"v0.1.0", "v1.2.3-rc.1", "v10.20.30"} {
		if out, err := makeDryRun(t, "dist", "VERSION="+v); err != nil {
			t.Errorf("make -n dist VERSION=%q refused a release version: %v\n%s", v, err, out)
		}
	}
}

// makeDryRun is `make -n <args>` from the repository root, with the variables
// these tests set stripped from the inherited environment (see
// TestImageTargetsBuildTheDockerfileTheirTagsNeed for why).
func makeDryRun(t *testing.T, args ...string) (string, error) {
	t.Helper()
	makeBin := lookPathOrSkip(t, "make")
	cmd := exec.Command(makeBin, append([]string{"-n"}, args...)...)
	cmd.Dir = ".."
	cmd.Env = withoutEnv(os.Environ(), "MAKEFLAGS", "MFLAGS", "GNUMAKEFLAGS", "MAKELEVEL", "MAKEOVERRIDES",
		"TAGS", "TAGS_STATIC", "IMAGE", "TAG", "VERSION", "DIST_ARCHES")
	out, err := cmd.CombinedOutput()
	return string(out), err
}
