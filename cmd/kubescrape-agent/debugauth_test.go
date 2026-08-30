package main

// These tests ARE the attack: a neighbour pod on the cluster network opening a
// plain GET against the agent's -listen port and reading the node's exported
// log records. It succeeded before debugauth.go existed, and the first test
// here fails the moment the gate is removed from either the handler or the
// guard.

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/debugtap"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailer"
	"github.com/JohanLindvall/kubescrape/internal/cli"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// victimLine is what a workload on this node printed. It must never leave the
// process through an unauthenticated read.
const victimLine = "victim-tenant: db_password=hunter2 for finance"

type nopExporter struct{}

func (nopExporter) ExportLogs(context.Context, plog.Logs) error          { return nil }
func (nopExporter) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }
func (nopExporter) ExportTraces(context.Context, ptrace.Traces) error    { return nil }

func victimLogs() plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("k8s.namespace.name", "finance-prod")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(victimLine)
	return ld
}

// remoteConn reports a pod IP as the connection's peer, which is what a
// neighbour pod's connection actually looks like to net/http. The address of an
// accepted TCP connection is not the sender's to choose, which is exactly why
// the local exemption is safe — and why a test has to fake it at the listener.
type remoteConn struct{ net.Conn }

func (c remoteConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(10, 244, 3, 7), Port: 34512}
}

type remoteListener struct{ net.Listener }

func (l remoteListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return remoteConn{c}, nil
}

// tapServer serves the agent's REAL debug routing table (p.debugMux), not a
// hand-wrapped handler: the gate is only as good as its registration. fromPod
// makes every connection look like it came from another pod rather than from
// loopback.
func tapServer(t *testing.T, tokenFile string, fromPod bool) (*httptest.Server, *debugtap.Tap) {
	t.Helper()
	tap := debugtap.New(nopExporter{})
	log := slog.New(cli.NewLogfmtHandler(os.Stderr, slog.LevelError))
	guard, err := newDebugGuard(context.Background(), tokenFile, log)
	if err != nil {
		t.Fatal(err)
	}
	p := &pipelines{log: log, ready: newReadiness(), debugTap: tap}
	srv := httptest.NewUnstartedServer(p.debugMux(guard, nil, nil))
	if fromPod {
		srv.Listener = remoteListener{srv.Listener}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, tap
}

// streamCarriesTheVictimLine drives the tap until a payload line arrives, or
// fails. It is the "did the attacker get the data" half.
func streamCarriesTheVictimLine(t *testing.T, resp *http.Response, tap *debugtap.Tap) bool {
	t.Helper()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	if !sc.Scan() || !strings.HasPrefix(sc.Text(), "#") {
		t.Fatalf("no banner line: %q", sc.Text())
	}
	// The subscriber is registered before the banner goes out (ServeHTTP
	// subscribes before the response header), so exporting now cannot race it.
	deadline := time.Now().Add(5 * time.Second)
	go func() {
		for time.Now().Before(deadline) {
			_ = tap.ExportLogs(context.Background(), victimLogs())
			time.Sleep(5 * time.Millisecond)
		}
	}()
	for sc.Scan() {
		if strings.Contains(sc.Text(), victimLine) {
			return true
		}
	}
	return false
}

// THE ATTACK. Attacker A — any pod in the cluster — opens an unauthenticated
// GET against the agent's -listen port and asks for the node's log stream. With
// no -debug-token-file configured it must be refused, and the refusal must not
// carry a byte of what it asked for.
func TestNeighbourPodCannotReadTheOtlpStream(t *testing.T) {
	before := obs.DebugRefused.WithLabelValues("no_token").Value()
	srv, _ := tapServer(t, "", true)

	resp, err := http.Get(srv.URL + "/debug/otlp?signal=logs&sample=100&attr=k8s.namespace.name%3Dfinance-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("an unauthenticated pod got %d from /debug/otlp, want 403: the node's whole log feed is readable",
			resp.StatusCode)
	}
	body := make([]byte, 4096)
	n, _ := resp.Body.Read(body)
	if strings.Contains(string(body[:n]), victimLine) {
		t.Fatalf("the refusal leaked the payload: %q", string(body[:n]))
	}
	if !strings.Contains(string(body[:n]), "-debug-token-file") {
		t.Errorf("the refusal does not name the flag that grants access, so it reads as a bug: %q", string(body[:n]))
	}
	if d := obs.DebugRefused.WithLabelValues("no_token").Value() - before; d != 1 {
		t.Errorf("kubescrape_debug_refused_total{reason=no_token} moved by %v, want 1: a refusal nobody can see gets configured away", d)
	}
}

// A configured token means a credential exists to present, so a request without
// one is 401 (with a challenge), not 403 — and still no data.
func TestNeighbourPodWithoutTheTokenIsChallenged(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("s3cret-debug-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	before := obs.DebugRefused.WithLabelValues("unauthenticated").Value()
	srv, _ := tapServer(t, tokenFile, true)

	for _, header := range []string{"", "Bearer wrong-token", "Basic s3cret-debug-token"} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/debug/otlp?signal=logs", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("Authorization %q got %d, want 401", header, resp.StatusCode)
		}
		if resp.Header.Get("WWW-Authenticate") == "" {
			t.Errorf("no challenge on the 401, so a client cannot tell wrong-credentials from wrong-URL")
		}
	}
	if d := obs.DebugRefused.WithLabelValues("unauthenticated").Value() - before; d != 3 {
		t.Errorf("kubescrape_debug_refused_total{reason=unauthenticated} moved by %v, want 3", d)
	}
}

// The other half of the fix: the operator's own access must survive it. A
// central debug pod holding the token reads the stream from anywhere.
func TestTheTokenGrantsTheStreamFromAnotherPod(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("s3cret-debug-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv, tap := tapServer(t, tokenFile, true)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/debug/otlp?signal=logs&sample=100", nil)
	req.Header.Set("Authorization", "Bearer s3cret-debug-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("a token-bearing read got %d, want 200", resp.StatusCode)
	}
	if !streamCarriesTheVictimLine(t, resp, tap) {
		t.Error("the token-bearing client never received a payload: the fix broke the surface it was protecting")
	}
}

// kubectl port-forward is how every doc in this repo reaches these surfaces:
// the kubelet dials 127.0.0.1 inside the pod, so it arrives as loopback and
// must keep working with no flag set. hack/e2e.sh asserts exactly this shape.
func TestPortForwardStillReadsTheStreamWithNoToken(t *testing.T) {
	srv, tap := tapServer(t, "", false)

	resp, err := http.Get(srv.URL + "/debug/otlp?signal=logs&sample=100")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("a local (port-forward) read got %d, want 200", resp.StatusCode)
	}
	if !streamCarriesTheVictimLine(t, resp, tap) {
		t.Error("a local read never received a payload")
	}
}

// A loopback address that arrived through a relay proves nothing about the
// caller — the same evidence /v1/self refuses on.
func TestALocalButForwardedRequestIsRefused(t *testing.T) {
	before := obs.DebugRefused.WithLabelValues("forwarded").Value()
	srv, _ := tapServer(t, "", false)

	for _, h := range []string{"X-Forwarded-For", "Forwarded", "X-Real-Ip"} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/debug/otlp?signal=logs", nil)
		req.Header.Set(h, "10.244.3.7")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s got %d, want 403: a relayed request wears the relay's address", h, resp.StatusCode)
		}
	}
	if d := obs.DebugRefused.WithLabelValues("forwarded").Value() - before; d != 3 {
		t.Errorf("kubescrape_debug_refused_total{reason=forwarded} moved by %v, want 3", d)
	}
}

// A named token file that cannot be read must stop the process rather than
// silently downgrading to local-only: an operator who asked for a token has to
// learn their mount is wrong.
func TestAnUnreadableDebugTokenFileIsFatal(t *testing.T) {
	_, err := newDebugGuard(context.Background(), filepath.Join(t.TempDir(), "absent"),
		slog.New(cli.NewLogfmtHandler(os.Stderr, slog.LevelError)))
	if err == nil {
		t.Fatal("an unreadable -debug-token-file was accepted; the gate would fall back to local-only unannounced")
	}
}

// The gate covers /debug/tailer (which enumerates every log file, and so every
// namespace/pod/container, on the node) and covers NOTHING ELSE: the kubelet's
// probes and the homepage must keep answering an unauthenticated caller, or a
// security fix becomes a broken rollout.
func TestOnlyTheDataBearingSurfacesAreGated(t *testing.T) {
	tap := debugtap.New(nopExporter{})
	log := slog.New(cli.NewLogfmtHandler(os.Stderr, slog.LevelError))
	guard, err := newDebugGuard(context.Background(), "", log)
	if err != nil {
		t.Fatal(err)
	}
	p := &pipelines{log: log, ready: newReadiness(), debugTap: tap}
	srv := httptest.NewUnstartedServer(p.debugMux(guard, tailer.New(tailer.Config{}), nil))
	srv.Listener = remoteListener{srv.Listener}
	srv.Start()
	defer srv.Close()

	for path, want := range map[string]int{
		"/debug/tailer":  http.StatusForbidden,
		"/debug/otlp":    http.StatusForbidden,
		"/debug/otlp/ui": http.StatusForbidden,
		"/healthz":       http.StatusOK,
		"/readyz":        http.StatusOK,
		"/debug":         http.StatusOK,
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s from a pod IP = %d, want %d", path, resp.StatusCode, want)
		}
	}
}

// "Who may read this node's telemetry feed" is a property of the deployment,
// so it has to be answerable BEFORE the rollout (-check-config prints this
// summary) and off a running agent's log, not by diffing flags.
func TestTheStartupSummaryNamesWhoMayReadTheDebugStream(t *testing.T) {
	restoreSummaryFlags(t)
	tok := *debugToken
	t.Cleanup(func() { *debugToken = tok })

	*debugToken = ""
	if got := summaryLines(t, agentConfig{})["effective listeners"]["debugAccess"]; got != "local-only" {
		t.Errorf("debugAccess = %q with no token file, want local-only", got)
	}
	*debugToken = "/var/run/secrets/kubescrape/debug-token"
	if got := summaryLines(t, agentConfig{})["effective listeners"]["debugAccess"]; got != "token" {
		t.Errorf("debugAccess = %q with a token file, want token", got)
	}
}

// THE SECOND ATTACK, on the local exemption itself. The operator is running
// `kubectl port-forward` and, in the same browser, visits a page whose DNS name
// is rebound to 127.0.0.1. The browser then opens a genuinely LOCAL connection
// to the forwarded port and, being same-origin with the page, hands the
// response back to the attacker's script — every log line on the node. The one
// thing rebinding cannot launder is the Host header: the browser keeps sending
// the name the page was loaded from. Reverse the debugLocalHost check in
// refuse() and this test hands over victimLine.
func TestARebindingBrowserPageCannotReadTheStream(t *testing.T) {
	before := obs.DebugRefused.WithLabelValues("host").Value()
	// fromPod=false: the connection really is loopback, exactly as the
	// kubelet's port-forward delivers it.
	srv, _ := tapServer(t, "", false)

	for _, host := range []string{"evil.example.com", "evil.example.com:8081", "attacker.test"} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/debug/otlp?signal=logs&sample=100", nil)
		// http.Request.Host overrides the URL's authority on the wire, which is
		// precisely what a rebound DNS name does.
		req.Host = host
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body := make([]byte, 4096)
		n, _ := resp.Body.Read(body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("a loopback connection with Host %q got %d, want 403: a rebound browser page reads the "+
				"node's whole log feed", host, resp.StatusCode)
		}
		if strings.Contains(string(body[:n]), victimLine) {
			t.Fatalf("the refusal leaked the payload: %q", string(body[:n]))
		}
	}
	if d := obs.DebugRefused.WithLabelValues("host").Value() - before; d != 3 {
		t.Errorf("kubescrape_debug_refused_total{reason=host} moved by %v, want 3", d)
	}
}

// The Host check must not cost the operator anything: every shape a client that
// dialled the port directly can send still reads the stream with no flag set.
func TestEveryLoopbackHostFormStillReadsTheStream(t *testing.T) {
	srv, _ := tapServer(t, "", false)
	_, port, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}

	for _, host := range []string{
		fmt.Sprintf("localhost:%s", port),
		fmt.Sprintf("127.0.0.1:%s", port),
		fmt.Sprintf("[::1]:%s", port),
		fmt.Sprintf("127.0.0.7:%s", port), // the whole 127/8, which is what a stray bind uses
		"localhost",                       // a client that omits the port
		"localhost.",                      // fully qualified
	} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/debug/otlp?signal=logs", nil)
		req.Host = host
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Host %q got %d, want 200: the rebinding fix broke a legitimate port-forward", host,
				resp.StatusCode)
		}
	}
}

// A Host header absent altogether (HTTP/1.0) claims no name, so there is
// nothing to have been rebound. Unit-level because Go's client always sends one.
func TestDebugLocalHostAcceptsOnlyLoopbackNames(t *testing.T) {
	for host, want := range map[string]bool{
		"":                    true,
		"localhost":           true,
		"LocalHost:8081":      true,
		"127.0.0.1:8081":      true,
		"[::1]:8081":          true,
		"::1":                 true,
		"[::ffff:127.0.0.1]":  true,
		"evil.example.com":    false,
		"kubescrape-agent":    false,
		"10.244.3.7:8081":     false,
		"localhost.evil.test": false,
		"notlocalhost":        false,
		"[::ffff:10.244.3.7]": false,
	} {
		if got := debugLocalHost(host); got != want {
			t.Errorf("debugLocalHost(%q) = %v, want %v", host, got, want)
		}
	}
}

// The credential answers the question the Host was standing in for, so a
// token-bearing read is unaffected by it: a proxied UI legitimately rewrites
// Host, and refusing that would push operators back to no token at all.
func TestTheTokenIsUnaffectedByTheHostCheck(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("s3cret-debug-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv, tap := tapServer(t, tokenFile, false)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/debug/otlp?signal=logs&sample=100", nil)
	req.Host = "debug.internal.example.com"
	req.Header.Set("Authorization", "Bearer s3cret-debug-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("a token-bearing read through a proxy that rewrote Host got %d, want 200", resp.StatusCode)
	}
	if !streamCarriesTheVictimLine(t, resp, tap) {
		t.Error("the token-bearing client never received a payload")
	}
}

// debugSurfaces is the INVENTORY of this port: every route debugMux registers,
// and whether it goes through the gate. It is the deliberate half of "verify
// the gate covers every data-bearing surface" — a new /debug endpoint has to be
// classified here, with a reason, before this test will pass, and a gate lost
// from an existing registration fails it too.
//
// The open ones are open for stated reasons, not by omission:
//   - /healthz and /readyz are the kubelet's probes. Gating them turns a
//     security fix into a fleet-wide failed rollout, and they carry no data
//     beyond the pending readiness gates' NAMES.
//   - /debug/targets is scrape-target state (URLs, namespace/pod names, the
//     last scrape's error): the same class the metadata service's /v1 routes
//     already serve to any caller by design. It carries no telemetry BODIES and
//     no credential — the resolved auth material never reaches it.
//   - /debug/transforms is one content hash of the operator's own script.
//   - /debug, /debug/{$} and / are the homepage and its redirect: a static link
//     list with no data in it, which is also where a refused reader is told
//     which key opens the rest.
var debugSurfaces = map[string]bool{ // pattern -> must be behind guard.protect
	"/healthz":          false,
	"/readyz":           false,
	"/debug/tailer":     true,
	"/debug/targets":    false,
	"/debug/otlp":       true,
	"/debug/otlp/ui":    true,
	"/debug/transforms": false,
	"/debug":            false,
	"/debug/{$}":        false,
	"/{$}":              false,
}

// TestEveryDebugRouteIsClassified reads the REAL routing table's source. A
// runtime probe can only ask about paths the test already knows; this fails on
// a surface nobody thought to ask about, which is how an ungated one gets added.
func TestEveryDebugRouteIsClassified(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	start := strings.Index(body, "func (p *pipelines) debugMux(")
	if start < 0 {
		t.Fatal("debugMux is gone from main.go; this test can no longer see the routing table it audits")
	}
	end := strings.Index(body[start:], "\n\treturn mux\n}")
	if end < 0 {
		t.Fatal("cannot find the end of debugMux")
	}
	body = body[start : start+end]

	re := regexp.MustCompile(`mux\.HandleFunc\("GET ([^"]+)",\s*(\S*)`)
	seen := map[string]bool{}
	for _, m := range re.FindAllStringSubmatch(body, -1) {
		pattern, handler := m[1], m[2]
		gated, known := debugSurfaces[pattern]
		if !known {
			t.Errorf("debugMux registers GET %s, which debugSurfaces does not classify: decide whether it is "+
				"data-bearing (guard.protect) or open, and say why here", pattern)
			continue
		}
		seen[pattern] = true
		if got := strings.HasPrefix(handler, "guard.protect("); got != gated {
			t.Errorf("GET %s behind guard.protect = %v, want %v", pattern, got, gated)
		}
	}
	for pattern := range debugSurfaces {
		if !seen[pattern] {
			t.Errorf("debugSurfaces classifies GET %s but debugMux no longer registers it", pattern)
		}
	}
}

// The refused caller chooses the Host, so it is cut before it reaches the log.
// (Quoting is internal/cli's: TextHandler escapes a value holding spaces, '='
// or newlines, which logfmt_test.go pins. Length is what is left.)
func TestARefusedHostIsCutBeforeItIsLogged(t *testing.T) {
	hostile := strings.Repeat("a", 64*1024) + ".evil.test"
	got := debugLogHost(hostile)
	if len(got) > maxLoggedHostBytes+len("...(truncated)") {
		t.Errorf("a %d-byte Host logged as %d bytes", len(hostile), len(got))
	}
	if !strings.HasSuffix(got, "...(truncated)") {
		t.Errorf("the cut is invisible in the log line: %q", got[max(0, len(got)-32):])
	}
	if short := "localhost:8081"; debugLogHost(short) != short {
		t.Errorf("an ordinary Host was rewritten: %q", debugLogHost(short))
	}
}
