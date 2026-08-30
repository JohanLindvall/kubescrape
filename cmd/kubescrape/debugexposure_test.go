package main

// The metadata service's exposure inventory, asserted rather than assumed.
//
// The agent gates its data-bearing debug surfaces (cmd/kubescrape-agent/
// debugauth.go) because /debug/otlp streams the node's telemetry bodies. This
// binary has no such surface — its /debug is a static page of forms and its
// data lives on the /v1 routes, which are unauthenticated BY DESIGN (see the
// block above api.HTTPServer in main.go). "By design" is only true while it is
// checked: a route added here that serves bodies would inherit the openness of
// the ones that legitimately have it, silently.

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// metadataRoutes classifies every route Server.Handler registers.
//
//	true  = authenticated (the shared -scrape-auth-token-file bearer token)
//	false = open on purpose, and the purpose is in the comment beside it
var metadataRoutes = map[string]bool{
	// Pod/owner/namespace metadata the agents poll on a cycle. No log line, no
	// metric sample, no credential; the deploy-tool annotations that could
	// smuggle an applied spec through are dropped by kubemeta.FilterAnnotations.
	"/v1/containers/{id...}":         false,
	"/v1/pods/{namespace}/{name}":    false,
	"/v1/pod-uids/{uid}":             false,
	"/v1/pod-ips/{ip}":               false,
	"/v1/nodes/{node}/targets":       false,
	"/v1/nodes/{node}/metadata":      false,
	"/v1/self":                       false, // answers about the CALLER's own connection
	"/v1/explain/{namespace}/{name}": false, // the same metadata as a scrape-decision narrative
	// The one route that hands back resolved Secret material.
	"/v1/scrape-auth/{namespace}/{name}/{key}": true,
	// A static page of forms plus its redirect, and the kubelet's probes.
	"/debug":     false,
	"/debug/{$}": false,
	"/{$}":       false,
	"/healthz":   false,
	"/readyz":    false,
}

// TestMetadataServiceServesNoDataBearingDebugSurface reads the real routing
// table. A new route must be classified here before this passes, which is the
// point: the decision is made deliberately, once, in a place a reviewer reads.
func TestMetadataServiceServesNoDataBearingDebugSurface(t *testing.T) {
	src, err := os.ReadFile("../../internal/server/server.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	start := strings.Index(body, "func (s *Server) Handler() http.Handler {")
	if start < 0 {
		t.Fatal("Server.Handler is gone; this test can no longer see the routing table it audits")
	}
	end := strings.Index(body[start:], "\n\treturn mux\n}")
	if end < 0 {
		t.Fatal("cannot find the end of Server.Handler")
	}
	body = body[start : start+end]

	re := regexp.MustCompile(`mux\.HandleFunc\("GET ([^"]+)"`)
	seen := map[string]bool{}
	for _, m := range re.FindAllStringSubmatch(body, -1) {
		pattern := m[1]
		authed, known := metadataRoutes[pattern]
		if !known {
			t.Errorf("the metadata service registers GET %s, which metadataRoutes does not classify: this port "+
				"is unauthenticated apart from /v1/scrape-auth, so decide — and write down — whether the new "+
				"route may be read by any pod in the cluster", pattern)
			continue
		}
		seen[pattern] = true
		if authed && !strings.Contains(body, "handleScrapeAuth") {
			t.Errorf("GET %s is classified authenticated but the handler is gone", pattern)
		}
	}
	for pattern := range metadataRoutes {
		if !seen[pattern] {
			t.Errorf("metadataRoutes classifies GET %s but the metadata service no longer registers it", pattern)
		}
	}

	// The debug page itself: a template with no data in it. If it ever grows a
	// store read, this is where that decision has to be made.
	home, err := os.ReadFile("../../internal/server/debughome.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(home), "s.store") || strings.Contains(string(home), "s.cfg.Store") {
		t.Error("the metadata service's /debug homepage now reads the store: it is served without the readiness " +
			"gate and to any caller, so anything it renders is served to any caller too")
	}
}
