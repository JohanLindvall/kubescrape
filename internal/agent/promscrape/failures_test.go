package promscrape

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func debugScraper(cfg Config, buf *strings.Builder) *Scraper {
	cfg.Logger = slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return New(cfg)
}

// The classification is by TYPE, never by message text: a library rewording an
// error must not silently re-bucket a fleet's failures.
func TestFailureReasonClassifiesByType(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"forbidden", &statusError{code: 403}, reasonUnauthorized},
		{"unauthorized", &statusError{code: 401}, reasonUnauthorized},
		{"other status", &statusError{code: 500}, reasonStatus},
		{"wrapped status", fmt.Errorf("scraping: %w", &statusError{code: 404}), reasonStatus},
		{"sample limit", ErrTooManySamples, reasonSampleLimit},
		{"deadline", fmt.Errorf("get: %w", context.DeadlineExceeded), reasonTimeout},
		{"canceled", fmt.Errorf("get: %w", context.Canceled), reasonCanceled},
		{"unknown authority", fmt.Errorf("get: %w", x509.UnknownAuthorityError{}), reasonTLS},
		{"record header", fmt.Errorf("get: %w", tls.RecordHeaderError{Msg: "first record does not look like TLS"}), reasonTLS},
		{"dns", fmt.Errorf("get: %w", &net.DNSError{Err: "no such host", IsNotFound: true}), reasonDNS},
		{"dns timeout", fmt.Errorf("get: %w", &net.DNSError{Err: "timeout", IsTimeout: true}), reasonTimeout},
		{"refused", fmt.Errorf("get: %w", &net.OpError{Op: "dial", Err: errors.New("connection refused")}), reasonConnect},
		// A body that ended mid-stream is the TARGET's fault, and every reader
		// in the chain (net/http's Content-Length check, gzip, the protobuf
		// framing) reports it the same way.
		{"body cut mid-stream", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), reasonBody},
		// ...unless the scrape's own deadline is what cut it.
		{"cut by the deadline", errors.Join(io.ErrUnexpectedEOF, context.DeadlineExceeded), reasonTimeout},
		{"classified", classify(reasonExport, errors.New("collector said no")), reasonExport},
		// The explicit wrapper wins over anything the classifier could infer:
		// an export that failed with a deadline is still an export failure, and
		// pointing an operator at the target would be the wrong diagnosis.
		{"classified beats inference", classify(reasonExport, context.DeadlineExceeded), reasonExport},
		{"unknown", errors.New("something new"), reasonOther},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := failureReason(c.err); got != c.want {
				t.Errorf("failureReason(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
}

// Every failure reason this package can emit must be DEFINED in the help text
// of kubescrape_scrape_failures_total — the metric an operator is told to read
// first when targets are up=0, and whose help is the only definition of each
// value (docs/METRICS.md is generated from it). The repo-wide guard for this,
// obs.TestHelpEnumeratingLabelValuesNamesThemAll, says plainly that it cannot
// see a value reached through a constant, and reportScrapeFailure passes a
// variable, so this is that guard for this one metric.
//
// The domain is read from failures.go's SOURCE (every string constant named
// reason*) rather than from a hand-kept list, so a new reason cannot be added
// without either being defined or failing here. The help is matched in its
// DEFINITION form, `<reason> (`, not as a bare word: several values are common
// English (other, status, body, export, timeout) and `auth` sits inside the
// help's own `-scrape-auth-secrets`, so a bare-word match would pass on
// incidental prose — exactly for the values most worth checking.
func TestFailureReasonsAreDocumented(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "failures.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	var reasons []string
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, spec := range gd.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, name := range vs.Names {
				if !strings.HasPrefix(name.Name, "reason") || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				v, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatal(err)
				}
				reasons = append(reasons, v)
			}
		}
	}
	// The vacuity check: a broken extractor must fail, not pass silently.
	if len(reasons) < 14 {
		t.Fatalf("found %d reason constants in failures.go (%v); the extractor is broken", len(reasons), reasons)
	}

	// The package DIRECTORY, never one file: registrations live in several
	// files there, and a single-file read stops seeing one the moment it moves.
	docs, err := obs.ParseMetricDocs("../../obs")
	if err != nil {
		t.Fatal(err)
	}
	help := ""
	for _, d := range docs {
		if d.Name == "kubescrape_scrape_failures_total" {
			help = d.Help
		}
	}
	if help == "" {
		t.Fatal("kubescrape_scrape_failures_total is not registered in internal/obs, or has no help")
	}
	for _, r := range reasons {
		if !definesReason(help, r) {
			t.Errorf("reason %q is emitted by this package but kubescrape_scrape_failures_total's help never defines it as `%s (...)`", r, r)
		}
	}
}

// definesReason reports whether help contains `reason (` with reason standing
// as its own token (not the tail of a longer name such as `basicAuth`).
func definesReason(help, reason string) bool {
	def := reason + " ("
	for i := 0; ; {
		j := strings.Index(help[i:], def)
		if j < 0 {
			return false
		}
		at := i + j
		if at == 0 || !isNameByte(help[at-1]) {
			return true
		}
		i = at + 1
	}
}

func isNameByte(c byte) bool {
	return c == '_' || c == '-' || c == '.' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// A response body that ENDS mid-stream is a body failure, on both fronts —
// counted under `body`, not under `other`, the value whose help reads a rate as
// a gap in this classifier. The text front sees it as net/http's short-body
// error when a target declares a Content-Length it does not deliver; the
// protobuf front as a length prefix promising more than arrives, including the
// case where NOTHING of the promised message arrives (io.ReadFull's bare EOF,
// which must not be mistaken for the clean end of the exposition).
func TestTruncatedBodyIsABodyFailure(t *testing.T) {
	msg := protoBody(t, &dto.MetricFamily{Name: new("g"), Type: dto.MetricType_GAUGE.Enum(),
		Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: new(1.0)}}}})
	prefixOnly := binary.AppendUvarint(nil, 10)
	for _, tc := range []struct {
		name        string
		contentType string
		declared    int // Content-Length, 0 = chunked
		body        []byte
	}{
		{"text body shorter than its Content-Length", "text/plain; version=0.0.4", 1000, []byte("up 1\nother 2\n")},
		{"proto message cut mid-message", "application/vnd.google.protobuf; encoding=delimited", 0, msg[:len(msg)-3]},
		{"proto length prefix with nothing after it", "application/vnd.google.protobuf; encoding=delimited", 0, prefixOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				if tc.declared > 0 {
					w.Header().Set("Content-Length", strconv.Itoa(tc.declared))
				}
				_, _ = w.Write(tc.body)
			}))
			defer srv.Close()
			var buf strings.Builder
			before := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonBody).Value()
			beforeOther := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonOther).Value()
			s := debugScraper(Config{
				Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second, NativeHistograms: true,
				Exporter: &captureExporter{}, Targets: staticTargets{testTarget(srv.URL)},
			}, &buf)
			s.cycle(context.Background())
			if got := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonOther).Value(); got != beforeOther {
				t.Errorf("other failures moved by %v: a truncated body went unclassified", got-beforeOther)
			}
			if got := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonBody).Value(); got != before+1 {
				t.Errorf("body failures = %v, want %v; log: %s", got, before+1, buf.String())
			}
		})
	}
}

// A protobuf message whose length prefix is over maxProtoMessageBytes is
// exactly "a response body over this pipeline's cap", the definition of
// reason=body — it used to be a bare error counted under `other`.
func TestOverCapProtoMessageIsABodyFailure(t *testing.T) {
	s := New(Config{Node: "n1", Interval: time.Hour, Timeout: time.Hour, NativeHistograms: true,
		Targets: staticTargets{}, Exporter: &captureExporter{}, StartTime: time.Now()})
	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	body := binary.AppendUvarint(nil, maxProtoMessageBytes+1)
	_, err := s.scrapeProto(context.Background(), bytes.NewReader(body), cb, nil, "t", "t")
	if err == nil {
		t.Fatal("an over-cap message was accepted")
	}
	if got := failureReason(err); got != reasonBody {
		t.Fatalf("reason = %q (%v), want %q", got, err, reasonBody)
	}
}

// Wrapping must not change what anything downstream reads: the message is the
// cause's verbatim, and errors.Is still reaches through.
func TestClassifiedErrorIsTransparent(t *testing.T) {
	inner := errors.New("boom")
	err := classify(reasonAuth, inner)
	if err.Error() != "boom" {
		t.Errorf("Error() = %q, want the cause verbatim", err.Error())
	}
	if !errors.Is(err, inner) {
		t.Error("errors.Is does not reach the cause")
	}
}

// A 403 on a target must move the counter under `unauthorized` and put the URL
// — the one thing the counter cannot hold — into the log.
func TestScrapeFailureIsCountedAndNamed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	var buf strings.Builder
	before := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonUnauthorized).Value()
	s := debugScraper(Config{
		Node: "node1", Interval: time.Hour, Exporter: &captureExporter{},
		Targets: staticTargets{testTarget(srv.URL)},
	}, &buf)
	s.cycle(context.Background())

	if got := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonUnauthorized).Value(); got != before+1 {
		t.Errorf("unauthorized failures = %v, want %v", got, before+1)
	}
	got := buf.String()
	for _, want := range []string{"scrape failed", "reason=unauthorized", srv.URL, "note="} {
		if !strings.Contains(got, want) {
			t.Errorf("log is missing %q; got %q", want, got)
		}
	}
}

// A kubelet token that has never been readable is reason=auth — nothing was
// sent, so nothing refused it — but its note must be the KUBELET's remedy. The
// shared note told this operator to run the metadata service with
// -scrape-auth-secrets and share -scrape-auth-token-file, neither of which the
// cadvisor/node/summary pipelines ever use, and it re-warned every 5 minutes
// while the accurate token-file line was said once.
func TestKubeletTokenAuthFailureNamesTheTokenFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("up 1\n"))
	}))
	defer srv.Close()

	var buf strings.Builder
	before := obs.ScrapeFailures.WithLabelValues(pipelineNode, reasonAuth).Value()
	s := debugScraper(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Exporter: &captureExporter{}, Targets: staticTargets{},
		Kubelet: KubeletConfig{
			Endpoint: srv.URL, NodeMetrics: true,
			TokenFile: filepath.Join(t.TempDir(), "never-existed"),
		},
	}, &buf)
	s.cycle(context.Background())

	if got := obs.ScrapeFailures.WithLabelValues(pipelineNode, reasonAuth).Value(); got != before+1 {
		t.Fatalf("node auth failures = %v, want %v", got, before+1)
	}
	var line string
	for l := range strings.SplitSeq(buf.String(), "\n") {
		if strings.Contains(l, "scrape failed") && strings.Contains(l, "pipeline=node") {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("no scrape-failed line for the node pipeline; log:\n%s", buf.String())
	}
	if !strings.Contains(line, "-kubelet-token-file") || strings.Contains(line, "-scrape-auth-secrets") {
		t.Errorf("the kubelet token failure's note must name -kubelet-token-file and not the metadata service's secret flags; got %q", line)
	}

	// A discovered target's unresolvable secret ref keeps its own remedy.
	if note := failureNote(pipelineTargets, reasonAuth); !strings.Contains(note, "-scrape-auth-secrets") {
		t.Errorf("targets auth note = %q, want the secret-ref remedy", note)
	}
	for _, p := range []string{pipelineCadvisor, pipelineNode, pipelineSummary} {
		if note := failureNote(p, reasonAuth); !strings.Contains(note, "-kubelet-token-file") {
			t.Errorf("%s auth note = %q, want the kubelet token remedy", p, note)
		}
	}
}

// The line is throttled per (target, reason) — fifty broken targets on two
// hundred nodes would otherwise be 20k identical lines a minute — while the
// counter keeps moving on every cycle.
func TestRepeatedScrapeFailuresAreThrottledButStillCounted(t *testing.T) {
	var buf strings.Builder
	s := debugScraper(Config{Node: "node1", Exporter: &captureExporter{}, Targets: staticTargets{}}, &buf)
	t1 := testTarget("http://a:1")
	before := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonStatus).Value()
	for range 5 {
		s.reportScrapeFailure(pipelineTargets, t1.URL, warnTarget(t1), &statusError{code: 500}, false)
	}
	if got := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonStatus).Value(); got != before+5 {
		t.Errorf("failures = %v, want %v (the counter is the rate)", got, before+5)
	}
	if n := strings.Count(buf.String(), "scrape failed"); n != 1 {
		t.Errorf("logged %d times, want 1 (the rest are inside the window)", n)
	}
	// A failure that changes SHAPE reports at once rather than hiding behind
	// the previous message's window: the remedy has changed.
	s.reportScrapeFailure(pipelineTargets, t1.URL, warnTarget(t1), &statusError{code: 403}, false)
	if n := strings.Count(buf.String(), "scrape failed"); n != 2 {
		t.Errorf("logged %d times, want 2 (a new reason re-fires)", n)
	}
}

// A rolling update cancels every in-flight scrape. That is not the target's
// fault and must not put one accusation per target into the last seconds of
// every deploy — but the counter still records it.
func TestShutdownCancellationIsCountedButNotLogged(t *testing.T) {
	var buf strings.Builder
	s := debugScraper(Config{Node: "node1", Exporter: &captureExporter{}, Targets: staticTargets{}}, &buf)
	before := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonCanceled).Value()
	s.reportScrapeFailure(pipelineTargets, "http://a:1", "cfg", &statusError{code: 500}, true)
	if got := obs.ScrapeFailures.WithLabelValues(pipelineTargets, reasonCanceled).Value(); got != before+1 {
		t.Errorf("canceled failures = %v, want %v", got, before+1)
	}
	if strings.Contains(buf.String(), "scrape failed") {
		t.Errorf("a shutdown-cancelled scrape logged: %q", buf.String())
	}
}
