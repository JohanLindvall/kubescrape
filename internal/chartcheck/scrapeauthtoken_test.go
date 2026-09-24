package chartcheck

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/manifestcheck"
)

// service.scrapeAuthToken.autoGenerate: false exists so a template-rendering
// GitOps flow (no live cluster, so lookup() reads nothing back) FAILS instead
// of minting a fresh token on every render (templates/service.yaml explains the
// 401 storm that causes). The serviceGraph token refusal beside it is pinned by
// TestServiceGraphWithoutTokenIsRefusedAtRender; this one was not, in either
// direction.
func TestScrapeAuthAutoGenerateFalseRefusesAtRender(t *testing.T) {
	helm := helmBin(t)
	render := func(set ...string) (string, error) {
		args := []string{"--set", "service.scrapeAuthSecrets=true"}
		for _, s := range set {
			args = append(args, "--set", s)
		}
		out, err := helmTemplate(helm, "monitoring", args...)
		return string(out), err
	}

	out, err := render("service.scrapeAuthToken.autoGenerate=false")
	if err == nil {
		t.Errorf("autoGenerate=false with no token rendered fine; it must refuse rather than mint one that differs on the next render:\n%s", out)
	} else {
		for _, want := range []string{"autoGenerate", "service.scrapeAuthToken.value", "service.scrapeAuthToken.existingSecret"} {
			if !strings.Contains(out, want) {
				t.Errorf("the refusal does not name %q, the value an operator has to set: %s", want, out)
			}
		}
	}

	// Either supplied form renders, and the supplied value is what ships.
	out, err = render("service.scrapeAuthToken.autoGenerate=false", "service.scrapeAuthToken.value=pinned-token")
	if err != nil {
		t.Errorf("autoGenerate=false with a value must render: %v\n%s", err, out)
	} else if !strings.Contains(out, `token: "pinned-token"`) {
		t.Errorf("the rendered Secret does not carry the supplied token:\n%s", out)
	}
	out, err = render("service.scrapeAuthToken.autoGenerate=false", "service.scrapeAuthToken.existingSecret=mine")
	if err != nil {
		t.Errorf("autoGenerate=false with an existingSecret must render: %v\n%s", err, out)
	} else if strings.Contains(out, "name: kubescrape-scrape-auth") {
		t.Errorf("with existingSecret the chart must not render a Secret of its own:\n%s", out)
	}

	// The default still generates — the branch the refusal replaces.
	out, err = render()
	if err != nil {
		t.Fatalf("the default (autoGenerate: true) must render: %v\n%s", err, out)
	}
	var secret *corev1.Secret
	for _, doc := range manifestcheck.Documents(out) {
		if renderedKind(t, doc) != "Secret" {
			continue
		}
		var s corev1.Secret
		if err := yaml.Unmarshal([]byte(doc), &s); err != nil {
			t.Fatalf("unmarshal Secret: %v", err)
		}
		if s.Name == "kubescrape-scrape-auth" {
			secret = &s
		}
	}
	if secret == nil {
		t.Fatal("no kubescrape-scrape-auth Secret rendered with scrapeAuthSecrets on and no token supplied")
	}
	tok := secret.StringData["token"]
	if len(tok) != 48 || strings.Trim(tok, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") != "" {
		t.Errorf("generated token %q: want 48 alphanumerics (randAlphaNum 48)", tok)
	}
}
