package main

import (
	"net/http/httptest"
	"strings"
	"testing"
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
