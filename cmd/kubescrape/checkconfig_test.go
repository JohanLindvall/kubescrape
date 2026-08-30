package main

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// discardLog is a logger whose records go nowhere: these tests are about the
// REFUSALS, and the warnings beside them have their own.
func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// restoreFlags puts every flag validateConfig reads back the way it found it —
// they are package globals, so a case that mutates one would otherwise decide
// every later test in the package.
func restoreFlags(t *testing.T) {
	t.Helper()
	old := struct {
		listen, metrics, pprof, namespaces, tokenFile string
		monitors, auth                                bool
		resync, probe, wait, cache, meta              time.Duration
	}{*listen, *metricsListen, *pprofListen, *monitorNamespaces, *scrapeAuthTokenFile,
		*monitorsOn, *scrapeAuthOn, *resync, *apiserverProbeInterval, *maxWait, *cacheTTL, *metaCacheTTL}
	t.Cleanup(func() {
		*listen, *metricsListen, *pprofListen = old.listen, old.metrics, old.pprof
		*monitorNamespaces, *scrapeAuthTokenFile = old.namespaces, old.tokenFile
		*monitorsOn, *scrapeAuthOn = old.monitors, old.auth
		*resync, *apiserverProbeInterval = old.resync, old.probe
		*maxWait, *cacheTTL, *metaCacheTTL = old.wait, old.cache, old.meta
	})
}

// Every refusal, and the substring that makes it actionable. All of them are
// silent at runtime otherwise: the process starts, serves, and does the wrong
// thing for its whole life — a glob in -monitor-namespaces indexes no monitor
// at all, an empty -listen binds port 80 while the probes address 8080, and a
// negative duration is read by each consumer as something quieter than an
// error.
func TestValidateConfigRefusesWhatARealStartCannotHonour(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func()
		want string
	}{
		{"scrape auth without a token file", func() { *scrapeAuthOn = true }, "-scrape-auth-token-file"},
		{"monitor namespace glob", func() { *monitorNamespaces = "kube-*" }, "EXACT namespace list"},
		{"monitor namespace that is not a name", func() { *monitorNamespaces = "Prod" }, "not a Kubernetes namespace name"},
		{"negative resync", func() { *resync = -time.Second }, "-resync"},
		{"negative probe interval", func() { *apiserverProbeInterval = -time.Second }, "-apiserver-probe-interval"},
		{"negative wait timeout", func() { *maxWait = -time.Second }, "-wait-timeout"},
		{"negative cache ttl", func() { *cacheTTL = -time.Second }, "-cache-ttl"},
		{"negative metadata cache ttl", func() { *metaCacheTTL = -time.Second }, "-metadata-cache-ttl"},
		{"empty listen", func() { *listen = "" }, "-listen is empty"},
		{"unparseable listener", func() { *pprofListen = "nonsense" }, "-pprof-listen"},
		{"two listeners on one address", func() { *metricsListen = *listen }, "address already in use"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restoreFlags(t)
			tc.set()
			err := validateConfig(discardLog())
			if err == nil {
				t.Fatal("accepted; a real start would serve, silently, the wrong thing")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name %q", err, tc.want)
			}
		})
	}
}

// The shipped defaults, and the two combinations that must stay legal: a
// namespace list of real names, and -scrape-auth-secrets WITH a token file (the
// file's readability is the real start's business — the dry run is run from a
// laptop, where a Secret projection does not exist).
func TestValidateConfigAcceptsTheShippedShapes(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func()
	}{
		{"defaults", func() {}},
		{"namespace list", func() { *monitorNamespaces = "monitoring, kube-system" }},
		{"scrape auth with a token file that does not exist here", func() {
			*scrapeAuthOn, *monitorsOn = true, true
			*scrapeAuthTokenFile = "/var/run/secrets/kubescrape/scrape-auth-token"
		}},
		{"observability listeners disabled", func() { *metricsListen, *pprofListen = "", "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restoreFlags(t)
			tc.set()
			if err := validateConfig(discardLog()); err != nil {
				t.Fatalf("refused a legal configuration: %v", err)
			}
		})
	}
}

// checkConfigArgsEnv carries the child process' command line.
const checkConfigArgsEnv = "KUBESCRAPE_TEST_CHECK_CONFIG_ARGS"

// runChild runs the real run() in a child process with argv, and returns its
// combined output and whether it exited non-zero. A child is the only place the
// exit status — the thing a CI job and a pre-rollout check actually read — is
// observable, and the only place flag.Parse sees a real command line.
func runChild(t *testing.T, argv ...string) (string, bool) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestCheckConfigChild")
	cmd.Env = append(os.Environ(),
		checkConfigArgsEnv+"="+strings.Join(argv, " "),
		// No kubeconfig anywhere: the pre-flight is documented as a local
		// binary, so the dry run must reach its verdict without one.
		"KUBECONFIG=/nonexistent",
		"HOME="+t.TempDir(),
	)
	out, err := cmd.CombinedOutput()
	return string(out), err != nil
}

// A refusal is only worth anything if the dry run EXITS NON-ZERO on it, and a
// pass is only worth anything if it reaches the same summary a real start
// prints. Both are the child's own exit status and output.
func TestCheckConfigExitStatus(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		wantErr bool
		want    string
	}{
		{"scrape auth without a token file", []string{"-scrape-auth-secrets"}, true, "-scrape-auth-token-file"},
		{"monitor namespace glob", []string{"-monitor-namespaces=kube-*"}, true, "EXACT namespace list"},
		{"negative resync", []string{"-resync=-1s"}, true, "-resync"},
		{"nothing typed", nil, false, "config is valid"},
		{"the summary is printed", nil, false, "effective configuration"},
		// No kubeconfig on this machine is not a verdict on the CONFIG: the
		// destination is named unresolved and the check still passes.
		{"no api server here", nil, false, "no API server could be resolved"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, failed := runChild(t, append([]string{"-check-config"}, tc.args...)...)
			if tc.wantErr != failed {
				t.Fatalf("-check-config %v exited failed=%v, want %v:\n%s", tc.args, failed, tc.wantErr, out)
			}
			if !strings.Contains(out, tc.want) {
				t.Fatalf("-check-config output does not name %q:\n%s", tc.want, out)
			}
		})
	}
}

// "Acquires nothing" is the property that makes a dry run safe to run anywhere,
// and the listener is the acquisition that would bite: -check-config must pass
// on an address something else already holds.
func TestCheckConfigBindsNothing(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()

	out, failed := runChild(t, "-check-config", "-listen="+addr)
	if failed {
		t.Fatalf("-check-config tried to bind %s, which this test already holds:\n%s", addr, out)
	}
	if !strings.Contains(out, "config is valid") {
		t.Fatalf("-check-config did not reach its verdict:\n%s", out)
	}
}

// The whole value of a dry run is that it runs what a start runs. A refusal
// reachable only through -check-config would let a rollout through; one
// reachable only through a start would make the check a lie. This asserts the
// second direction — a real start (no -check-config) refuses the same value,
// with the same message, BEFORE it touches a kubeconfig.
func TestARealStartRunsTheSameValidation(t *testing.T) {
	out, failed := runChild(t, "-resync=-1s")
	if !failed {
		t.Fatalf("a real start accepted -resync=-1s:\n%s", out)
	}
	if !strings.Contains(out, "-resync") {
		t.Fatalf("a real start's refusal does not name the flag:\n%s", out)
	}
	if strings.Contains(out, "building kubernetes client config") {
		t.Fatalf("a real start reached the kubeconfig before validating its flags, so -check-config is not the same path:\n%s", out)
	}
}

// TestCheckConfigChild is the child half of the tests above: it runs the real
// run(), so the exit status under test is the binary's own. Skipped unless the
// parent asked for it.
func TestCheckConfigChild(t *testing.T) {
	argv := os.Getenv(checkConfigArgsEnv)
	if argv == "" {
		t.Skip("child half of TestCheckConfigExitStatus")
	}
	os.Args = append([]string{"kubescrape"}, strings.Fields(argv)...)
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
