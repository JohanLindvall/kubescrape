package main

import (
	"context"
	"html"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/debugtap"
	"github.com/JohanLindvall/kubescrape/internal/agent/promscrape"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailer"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
)

func TestDebugHomeRendersLinksAndEscapes(t *testing.T) {
	h := debugHome([]debugLink{
		{"/debug/tailer", "/debug/tailer", "positions & lag"},
		{"/debug/otlp/ui", "/debug/otlp/ui", "<script>alert(1)</script>"},
	}, []string{"pprof on :6060 /debug/pprof/."})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/debug", nil))
	body := rec.Body.String()
	if rec.Code != 200 || !strings.Contains(body, `href="/debug/tailer"`) || !strings.Contains(body, `href="/debug/otlp/ui"`) {
		t.Fatalf("homepage = %d %q", rec.Code, body)
	}
	if strings.Contains(body, "<script>alert") {
		t.Fatal("descriptions are not HTML-escaped")
	}
	if !strings.Contains(body, "pprof on :6060") {
		t.Fatal("notes missing")
	}
}

// The homepage's claim — it "cannot list an endpoint this process does not
// serve" — rests on where lines sit in debugMux, inside three conditionals
// (the tailer, the scraper, the transforms). Nothing held it: the render test
// above feeds hand-made links, and the route inventory in debugauth_test.go
// audits registrations, not links. So this builds the REAL mux both ways —
// every optional surface absent, then present — renders /debug through it and
// resolves every advertised href against the same mux. No request is served
// (mux.Handler only routes), so the streaming /debug/otlp needs no timeout, and
// the zero-valued tailer/scraper/wrapper are only captured, never called.
func TestDebugHomeLinksAreAllServed(t *testing.T) {
	guard, err := newDebugGuard(context.Background(), "", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	hrefRE := regexp.MustCompile(`href="([^"]*)"`)
	optional := []string{"/debug/tailer", "/debug/targets", "/debug/transforms"}

	for _, tc := range []struct {
		name string
		tl   *tailer.Tailer
		sc   *promscrape.Scraper
		tr   *transform.Wrapper
	}{
		{"no optional surfaces", nil, nil, nil},
		{"every optional surface", &tailer.Tailer{}, &promscrape.Scraper{}, &transform.Wrapper{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &pipelines{log: slog.New(slog.DiscardHandler), ready: newReadiness(),
				debugTap: debugtap.New(nopExporter{}), transforms: tc.tr}
			mux := p.debugMux(guard, tc.tl, tc.sc)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug", nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("GET /debug = %d", rec.Code)
			}
			var hrefs []string
			for _, m := range hrefRE.FindAllStringSubmatch(rec.Body.String(), -1) {
				hrefs = append(hrefs, html.UnescapeString(m[1]))
			}
			if len(hrefs) == 0 {
				t.Fatal("the homepage advertises nothing; the check would pass vacuously")
			}
			for _, href := range hrefs {
				if _, pat := mux.Handler(httptest.NewRequest(http.MethodGet, href, nil)); pat == "" {
					t.Errorf("the homepage links %q, which no registration in debugMux serves", href)
				}
			}
			for _, path := range optional {
				linked := slices.ContainsFunc(hrefs, func(h string) bool { return strings.HasPrefix(h, path) })
				if want := tc.tl != nil; linked != want {
					t.Errorf("%s linked = %v, want %v", path, linked, want)
				}
			}
		})
	}
}
