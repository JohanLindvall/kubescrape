package chartcheck

import (
	"regexp"
	"strings"
	"testing"
)

// The documented pin for the floating-tag residual is a DIGEST, and the image
// helper used to join repository and tag with ':' unconditionally — so
// `image.tag: "@sha256:..."` rendered `repo:@sha256:...`, an empty tag
// followed by a digest, and every one of the four workloads failed with
// InvalidImageName. This renders all four under each pin spelling and checks
// the reference each one actually gets, plus the pull policy a digest implies.
func TestImagePinsRenderValidReferences(t *testing.T) {
	helm := helmBin(t)
	const (
		repo   = "ghcr.io/johanlindvall/kubescrape"
		digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	)
	for _, tc := range []struct {
		name       string
		set        []string
		wantImage  string
		wantPolicy string
	}{
		{"unset (appVersion latest)", nil, repo + ":latest", "Always"},
		{"released tag", []string{"image.tag=v1.2.3"}, repo + ":v1.2.3", "IfNotPresent"},
		{"digest", []string{"image.digest=" + digest}, repo + "@" + digest, "IfNotPresent"},
		{"tag beside a digest", []string{"image.tag=v1.2.3", "image.digest=" + digest}, repo + ":v1.2.3@" + digest, "IfNotPresent"},
		{"digest spelled as an @ tag", []string{"image.tag=@" + digest}, repo + "@" + digest, "IfNotPresent"},
		{"digest spelled as a bare tag", []string{"image.tag=" + digest}, repo + "@" + digest, "IfNotPresent"},
	} {
		out := renderAllWorkloads(t, helm, tc.set...)
		images := imageLineRe.FindAllStringSubmatch(out, -1)
		if len(images) != 4 {
			t.Fatalf("%s: rendered %d image lines, want 4 (one per workload)", tc.name, len(images))
		}
		for _, m := range images {
			if m[1] != tc.wantImage {
				t.Errorf("%s: image %q, want %q", tc.name, m[1], tc.wantImage)
			}
			if strings.Contains(m[1], ":@") {
				t.Errorf("%s: image %q has an empty tag before its digest — InvalidImageName", tc.name, m[1])
			}
		}
		for _, m := range pullPolicyRe.FindAllStringSubmatch(out, -1) {
			if m[1] != tc.wantPolicy {
				t.Errorf("%s: imagePullPolicy %s, want %s", tc.name, m[1], tc.wantPolicy)
			}
		}
	}

	// Two pins that could disagree are refused rather than guessed between.
	if out, err := templateAllWorkloads(helm, "image.digest="+digest, "image.tag=@"+digest); err == nil {
		t.Errorf("image.digest beside a digest-shaped image.tag rendered instead of failing:\n%s", out)
	}
	// And the schema refuses a digest that is not one.
	if out, err := templateAllWorkloads(helm, "image.digest=latest"); err == nil {
		t.Errorf("image.digest=latest rendered instead of failing the schema:\n%s", out)
	}
}

var (
	imageLineRe  = regexp.MustCompile(`(?m)^\s+image: (\S+)$`)
	pullPolicyRe = regexp.MustCompile(`(?m)^\s+imagePullPolicy: (\S+)$`)
)

// templateAllWorkloads renders all four workloads — the metadata service, the
// agent DaemonSet, the events singleton and the trace-tier StatefulSet (which
// refuses to render without a token Secret name) — with each of set added as a
// --set value. A caller asserting a refusal reads err; renderAllWorkloads is
// the one that must render.
func templateAllWorkloads(helm string, set ...string) ([]byte, error) {
	args := []string{
		"--set", "events.enabled=true",
		"--set", "serviceGraph.enabled=true", "--set", "serviceGraph.tokenSecret.name=sg-token",
	}
	for _, s := range set {
		args = append(args, "--set", s)
	}
	return helmTemplate(helm, "monitoring", args...)
}

func renderAllWorkloads(t *testing.T, helm string, set ...string) string {
	t.Helper()
	out, err := templateAllWorkloads(helm, set...)
	if err != nil {
		t.Fatalf("helm template with every workload and --set %q failed: %v\n%s", set, err, out)
	}
	return string(out)
}
