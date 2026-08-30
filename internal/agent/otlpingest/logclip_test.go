package otlpingest

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"
)

// The OTLP/HTTP ingest listener is unauthenticated by design, and net/http
// admits a request line plus header block up to Server.MaxHeaderBytes — 1 MiB
// by default. Every value this refusal line carries from the request is
// therefore sender-chosen and sender-sized, and the throttle around it bounds
// only how OFTEN the line is written, never how big it is. One megabyte per
// reason per window per node is a log bill and a stalled collector, not a
// diagnostic.
//
// Reverse-patch check: drop the clipForLog calls in noteRejected and this
// fails on the line length.
func TestRefusalLineClipsSenderSuppliedValues(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	huge := strings.Repeat("A", 512*1024)
	req := httptest.NewRequest(http.MethodPost, "/v1/logs/"+huge, nil)
	req.Header.Set("Content-Type", "application/x-"+huge)
	req.Header.Set("Content-Encoding", "br-"+huge)

	br := newBodyReader(1<<20, nil, log)
	br.noteRejected(req, ErrBodyTooLarge)

	out := buf.String()
	if out == "" {
		t.Fatal("the refusal was not reported at all")
	}
	// Generous: the message and the fixed fields are a few hundred bytes, and
	// three clipped values add ~300 more. A megabyte is what this rejects.
	if len(out) > 4096 {
		t.Errorf("one refusal produced %d bytes of log; the sender is choosing the line length", len(out))
	}
	if strings.Contains(out, strings.Repeat("A", maxLoggedValueBytes+1)) {
		t.Error("a sender-supplied value reached the line unclipped")
	}
	// The value must still be recognisable — clipping is not dropping.
	for _, want := range []string{"path=", "contentType=", "contentEncoding="} {
		if !strings.Contains(out, want) {
			t.Errorf("the line lost %q, so the clip dropped the diagnosis instead of bounding it", want)
		}
	}
}

// A clip that lands mid-rune would put a replacement character into whatever
// reads the line, so the cut walks back to a rune start.
func TestClipForLogCutsOnARuneBoundary(t *testing.T) {
	for _, in := range []string{
		strings.Repeat("é", 200),          // 2-byte runes: every odd cut is mid-rune
		strings.Repeat("😀", 100),          // 4-byte runes
		strings.Repeat("a", 95) + "😀tail", // the boundary lands inside the emoji
	} {
		got := clipForLog(in)
		if !utf8.ValidString(got) {
			t.Errorf("clipForLog(%.10q...) produced invalid UTF-8: %q", in, got)
		}
		if len(got) > maxLoggedValueBytes+len("…") {
			t.Errorf("clipForLog returned %d bytes, over the %d-byte bound", len(got), maxLoggedValueBytes)
		}
	}
	if got := clipForLog("short"); got != "short" {
		t.Errorf("a value inside the bound was rewritten: %q", got)
	}
}
