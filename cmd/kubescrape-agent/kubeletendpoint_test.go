package main

// -kubelet-endpoint normalisation (kubeletBase, config.go).
//
// The shipped default is `https://$(NODE_IP):10250` in both the chart and
// deploy/agent.yaml, and NODE_IP is status.hostIP — so on an IPv6 node it
// expands to a BARE IPv6 literal, which net/url refuses for http/https. One
// static manifest value cannot spell both families (a pre-bracketed
// `[$(NODE_IP)]` renders `[10.0.0.5]`, which the same parser rejects as an
// invalid IP-literal), so the bracketing has to happen in the agent.

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Only a host that genuinely parses as an IP literal is bracketed: an IPv4
// address and a DNS name must come back untouched, or the repair would mint
// exactly the "invalid IP-literal" the manifests cannot avoid.
func TestKubeletBaseBracketsOnlyAnIPv6Literal(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"bare IPv6, as $(NODE_IP) expands on an IPv6 node", "https://fd00:10::5:10250", "https://[fd00:10::5]:10250"},
		{"IPv6 already bracketed", "https://[fd00:10::5]:10250", "https://[fd00:10::5]:10250"},
		{"IPv4", "https://10.0.0.5:10250", "https://10.0.0.5:10250"},
		{"hostname", "https://node-1.example:10250", "https://node-1.example:10250"},
		{"bare IPv6 with no port", "https://fd00:10::5", "https://[fd00:10::5]"},
		{"bare IPv6 with a path", "https://fd00:10::5:10250/", "https://[fd00:10::5]:10250/"},
		{"empty disables the kubelet scrapes and stays empty", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := kubeletBase(tc.in)
			if err != nil {
				t.Fatalf("kubeletBase(%q) = %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("kubeletBase(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if got == "" {
				return
			}
			// What the scraper actually builds: base + the pipeline's path.
			// This is where the un-normalised form dies, before any request
			// goes out.
			if _, err := http.NewRequest(http.MethodGet, strings.TrimRight(got, "/")+"/metrics/cadvisor", nil); err != nil {
				t.Fatalf("the normalised base still does not make a request: %v", err)
			}
		})
	}
}

// An endpoint no request can be built from is refused by -check-config rather
// than by every scrape cycle on every node for the process lifetime. Nothing
// parsed it at all before, so the dry run passed the one value that guarantees
// total kubelet-metric loss.
func TestValidateConfigParsesTheKubeletEndpoint(t *testing.T) {
	restoreKubeletFlags(t)

	// The shipped default on an IPv6 node: normalisable, therefore accepted.
	*kubeletEndpoint = "https://fd00:10::5:10250"
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("a bare IPv6 endpoint is what the manifests render on an IPv6 cluster and must be accepted: %v", err)
	}

	for _, bad := range []string{
		"10.0.0.5:10250",           // no scheme: url.Parse reads the host as one
		"https://[10.0.0.5]:10250", // bracketed IPv4 — the naive manifest "fix"
	} {
		*kubeletEndpoint = bad
		err := validateConfig(agentConfig{}, "")
		if err == nil || !strings.Contains(err.Error(), "-kubelet-endpoint") {
			t.Fatalf("validateConfig(%q) = %v, want a refusal naming the flag", bad, err)
		}
	}
}

// End to end through the shipped path: startScraper is the one place the flag
// is read, so the normalisation has to be there for a kubelet on an IPv6 node
// to be scraped at all. The endpoint is built the way the manifests build it —
// scheme://host:port with a bare host — against a listener on ::1.
func TestKubeletScrapeReachesAnIPv6KubeletEndpoint(t *testing.T) {
	restoreKubeletFlags(t)

	l, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback on this machine: %v", err)
	}
	paths := make(chan string, 8)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case paths <- r.URL.Path:
		default:
		}
		w.WriteHeader(http.StatusForbidden) // first-contact failure; the request is the evidence
	}))
	srv.Listener = l
	srv.Start()
	t.Cleanup(srv.Close)

	host, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// The manifest's own concatenation: `https://$(NODE_IP):10250` with hostIP
	// substituted verbatim, brackets nowhere.
	endpoint := "http://" + host + ":" + port
	if _, rerr := http.NewRequest(http.MethodGet, endpoint+"/metrics/cadvisor", nil); rerr == nil {
		t.Fatalf("%q already parses, so this test would pass without the normalisation", endpoint)
	}

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	*kubeletEndpoint, *kubeletToken = endpoint, tokenFile
	*cadvisorOn, *nodeOn, *summaryOn = true, false, false
	*metricsOn, *logsOn = false, false
	*healthMetrics = false
	*scrapeInterval = time.Hour // one cycle, then park

	var wg sync.WaitGroup
	p := &pipelines{wg: &wg, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	if sc := p.startScraper(ctx); sc == nil {
		cancel()
		t.Fatal("no Scraper was built for an agent whose only pipeline is -cadvisor")
	}
	defer func() {
		cancel()
		wg.Wait()
	}()

	select {
	case got := <-paths:
		if got != "/metrics/cadvisor" {
			t.Fatalf("the kubelet was asked for %q, want /metrics/cadvisor", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the IPv6 kubelet was never asked for anything: every request died in net/url before it was issued")
	}
}
