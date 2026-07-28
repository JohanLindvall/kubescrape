package main

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	discoveryfake "k8s.io/client-go/discovery/fake"
	coretesting "k8s.io/client-go/testing"
)

// The -servicemonitors pre-check must verify the servicemonitors resource
// itself, not just the group/version: a cluster with only other
// monitoring.coreos.com/v1 CRDs (e.g. PrometheusRule) serves the group, but a
// servicemonitor informer there can never sync and would wedge readiness.
//
// It must ALSO tell "the cluster says no" apart from "the cluster could not be
// asked". Collapsing the two let one 503 from a rolling API server, or one
// throttled request, permanently disable an explicitly requested feature: every
// monitor-derived target vanished for the life of the process behind a single
// startup log line.
func TestServiceMonitorCRDPresent(t *testing.T) {
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

	for _, tc := range []struct {
		name        string
		d           discovery.DiscoveryInterface
		wantPresent bool
		wantErr     bool
	}{
		{"CRD served", disc(metav1.APIResource{Name: "servicemonitors"}), true, false},
		{"group served without servicemonitors", disc(metav1.APIResource{Name: "prometheusrules"}), false, false},
		{"group absent", disc(), false, false},
		{"api server unreachable", errDiscovery{err: errors.New("connection refused")}, false, true},
		{"api server 503", errDiscovery{err: apierrors.NewServiceUnavailable("rolling")}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			present, err := serviceMonitorCRDPresent(tc.d)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if present != tc.wantPresent {
				t.Errorf("present = %v, want %v", present, tc.wantPresent)
			}
		})
	}
}

// errDiscovery fails every discovery call. Only ServerResourcesForGroupVersion
// is exercised, so the embedded nil interface is never dereferenced.
type errDiscovery struct {
	discovery.DiscoveryInterface
	err error
}

func (e errDiscovery) ServerResourcesForGroupVersion(string) (*metav1.APIResourceList, error) {
	return nil, e.err
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

// A ServiceMonitor is an instruction to every node agent to issue a GET, and
// there is no equivalent of prometheus-operator's admin-owned
// serviceMonitorSelector — so -monitor-namespaces is what stops a tenant who
// can create a CR in their own namespace from pointing `selector: {}` +
// `namespaceSelector.any: true` at an arbitrary path cluster-wide.
func TestMonitorNamespaceGate(t *testing.T) {
	if got := parseNamespaceSet(""); got != nil {
		t.Errorf("empty flag = %v, want nil (no restriction)", got)
	}
	if got := parseNamespaceSet("  ,  "); got != nil {
		t.Errorf("blank-only flag = %v, want nil", got)
	}
	set := parseNamespaceSet(" monitoring , platform ")
	if !set["monitoring"] || !set["platform"] || len(set) != 2 {
		t.Fatalf("parseNamespaceSet = %v", set)
	}

	mon := func(ns string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"namespace": ns, "name": "m"},
		}}
	}
	if !monitorAllowed(nil, mon("team-a")) {
		t.Error("nil set must allow everything (backward compatible default)")
	}
	if !monitorAllowed(set, mon("monitoring")) {
		t.Error("listed namespace must be allowed")
	}
	if monitorAllowed(set, mon("team-a")) {
		t.Error("unlisted namespace must be refused")
	}
}

// rotatingToken: a changed file promotes the old token to a grace-window
// second value, and the window expires. Without the grace, agents (which
// re-read on their own cadence) would 401 until they happened to reload.
func TestRotatingTokenGraceWindow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("old-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rt := &rotatingToken{path: path, log: slog.Default(), cur: "old-token", fetched: time.Now()}

	if got := rt.tokens(); len(got) != 1 || got[0] != "old-token" {
		t.Fatalf("fresh: %v, want [old-token]", got)
	}

	// Rotate the file; the cached value is still fresh, so nothing changes yet.
	if err := os.WriteFile(path, []byte("new-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := rt.tokens(); len(got) != 1 || got[0] != "old-token" {
		t.Fatalf("within the read interval: %v, want the cached [old-token]", got)
	}

	// Past the read interval: both are accepted.
	rt.mu.Lock()
	rt.fetched = time.Now().Add(-2 * scrapeAuthReadInterval)
	rt.mu.Unlock()
	got := rt.tokens()
	if len(got) != 2 || got[0] != "new-token" || got[1] != "old-token" {
		t.Fatalf("during the grace window: %v, want [new-token old-token]", got)
	}

	// A re-read that finds no change must not re-arm the window.
	rt.mu.Lock()
	rt.fetched = time.Now().Add(-2 * scrapeAuthReadInterval)
	until := rt.prevUntil
	rt.mu.Unlock()
	rt.tokens()
	rt.mu.Lock()
	reArmed := rt.prevUntil.After(until)
	rt.mu.Unlock()
	if reArmed {
		t.Fatal("an unchanged re-read must not extend the grace window")
	}

	// Past the window: only the current token.
	rt.mu.Lock()
	rt.prevUntil = time.Now().Add(-time.Second)
	rt.mu.Unlock()
	if got := rt.tokens(); len(got) != 1 || got[0] != "new-token" {
		t.Fatalf("after the grace window: %v, want [new-token]", got)
	}

	// An unreadable file keeps the last good token rather than 401ing the fleet.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	rt.mu.Lock()
	rt.fetched = time.Now().Add(-2 * scrapeAuthReadInterval)
	rt.mu.Unlock()
	if got := rt.tokens(); len(got) != 1 || got[0] != "new-token" {
		t.Fatalf("unreadable file: %v, want the last good [new-token]", got)
	}
}
