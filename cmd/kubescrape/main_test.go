package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	discoveryfake "k8s.io/client-go/discovery/fake"
	coretesting "k8s.io/client-go/testing"
)

// The -servicemonitors pre-check must verify the servicemonitors resource
// itself, not just the group/version: a cluster with only other
// monitoring.coreos.com/v1 CRDs (e.g. PrometheusRule) serves the group, but a
// servicemonitor informer there can never sync and would wedge readiness.
func TestCheckServiceMonitorCRD(t *testing.T) {
	disc := func(resources ...metav1.APIResource) *discoveryfake.FakeDiscovery {
		fake := &coretesting.Fake{}
		if resources != nil {
			fake.Resources = []*metav1.APIResourceList{{
				GroupVersion: "monitoring.coreos.com/v1",
				APIResources: resources,
			}}
		}
		return &discoveryfake.FakeDiscovery{Fake: fake}
	}

	if err := checkServiceMonitorCRD(disc(metav1.APIResource{Name: "servicemonitors"})); err != nil {
		t.Errorf("CRD present: %v", err)
	}
	if err := checkServiceMonitorCRD(disc(metav1.APIResource{Name: "prometheusrules"})); err == nil {
		t.Error("group present without servicemonitors must error")
	}
	if err := checkServiceMonitorCRD(disc()); err == nil {
		t.Error("absent group must error")
	}
}

// -scrape-auth-secrets serves Secret keys to anything that can reach the
// service, so the bearer token guarding that endpoint is mandatory: no flag,
// an unreadable file or a blank file must all fail startup rather than leave
// /v1/scrape-auth open.
func TestLoadScrapeAuthToken(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	if _, err := loadScrapeAuthToken(""); err == nil {
		t.Error("no -scrape-auth-token-file must be a startup error")
	} else if !strings.Contains(err.Error(), "-scrape-auth-token-file") {
		t.Errorf("error should name the flag: %v", err)
	}
	if _, err := loadScrapeAuthToken(filepath.Join(dir, "missing")); err == nil {
		t.Error("unreadable token file must be a startup error")
	}
	for _, blank := range []string{"", "\n", "   \n\t"} {
		if _, err := loadScrapeAuthToken(write("blank", blank)); err == nil {
			t.Errorf("blank token file %q must be a startup error", blank)
		}
	}
	// A Secret-mounted token keeps its trailing newline; the header carries
	// the trimmed value, so the file is trimmed too.
	got, err := loadScrapeAuthToken(write("token", "s3cr3t\n"))
	if err != nil || got != "s3cr3t" {
		t.Fatalf("token = %q, err = %v", got, err)
	}
}
