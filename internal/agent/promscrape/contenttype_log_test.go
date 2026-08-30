package promscrape

// The response Content-Type is the TARGET's bytes: a header a workload chooses,
// of whatever length the transport accepted. It reaches three log lines here —
// the protobuf refusal (Warn) and reportNegotiation's two Debug lines — and
// every other target-supplied value on a line in this package goes through
// clipForLog. These are that rule applied to the header, not a paraphrase of
// it: a megabyte of chosen bytes in a log record is a second flood wearing the
// shape of one line.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// padding is long enough that an unclipped header is unmistakable in the
// output and short enough to survive any transport header limit.
const ctPadding = 600

func scraperWithLog(t *testing.T, url string, native bool) (*Scraper, func() string) {
	t.Helper()
	log, dump := recordingLogger()
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{testTarget(url)}, Exporter: &captureExporter{},
		StartTime: time.Now(), Logger: log, NativeHistograms: native,
	})
	return s, dump
}

func TestRefusedProtoContentTypeIsClippedOnTheLine(t *testing.T) {
	pad := strings.Repeat("a", ctPadding)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", protoContentType+";pad="+pad)
		_, _ = w.Write([]byte{0x00})
	}))
	t.Cleanup(srv.Close)

	s, dump := scraperWithLog(t, srv.URL, false)
	if _, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), s.cfg.Timeout); err == nil {
		t.Fatal("a protobuf response was accepted without -scrape-native-histograms")
	}
	out := dump()
	if !strings.Contains(out, "refusing to decode it") {
		t.Fatalf("the refusal did not warn, so nothing was clipped:\n%s", firstLines(out, 3))
	}
	if strings.Contains(out, pad) {
		t.Errorf("the target's Content-Type went onto the line unclipped (%d bytes of it)", len(pad))
	}
}

// The two Debug lines are the same header on a path an operator turns on
// DURING an incident, i.e. the worst moment to multiply every scrape's log
// volume by the size of a header the target picked.
func TestNegotiationDebugClipsTheContentType(t *testing.T) {
	pad := strings.Repeat("b", ctPadding)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain;version=0.0.4;pad="+pad)
		_, _ = w.Write([]byte("up 1\n"))
	}))
	t.Cleanup(srv.Close)

	// NativeHistograms on: the agent asks for protobuf and the target answers
	// classic text, which is reportNegotiation's first arm.
	s, dump := scraperWithLog(t, srv.URL, true)
	if _, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), s.cfg.Timeout); err != nil {
		t.Fatal(err)
	}
	out := dump()
	if !strings.Contains(out, "negotiated down") {
		t.Fatalf("the negotiation report did not fire, so nothing was clipped:\n%s", firstLines(out, 3))
	}
	if strings.Contains(out, pad) {
		t.Errorf("the target's Content-Type went onto the Debug line unclipped (%d bytes of it)", len(pad))
	}
}
