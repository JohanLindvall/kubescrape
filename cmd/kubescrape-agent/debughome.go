package main

// The /debug homepage: one place that links every debug surface this agent
// serves, so an operator port-forwarding to -listen discovers what exists
// instead of memorizing paths from the docs. Entries are appended exactly
// where the handlers are registered (startDebugServer), so the page cannot
// list an endpoint this process does not serve.

import (
	"fmt"
	"html"
	"net/http"
	"strings"
)

// debugLink is one homepage entry.
type debugLink struct {
	href  string
	title string
	desc  string
}

// debugHome renders the index. notes carry the surfaces that live on OTHER
// ports (metrics, pprof) and cannot be linked relative to this one.
func debugHome(links []debugLink, notes []string) http.HandlerFunc {
	var b strings.Builder
	b.WriteString(`<!doctype html>
<meta charset="utf-8">
<title>kubescrape-agent debug</title>
<style>
  body { font: 14px/1.5 system-ui, sans-serif; margin: 1.5rem; max-width: 46rem; }
  li { margin-bottom: .5rem; }
  code, a { font-family: ui-monospace, monospace; }
  .d { color: #555; }
  .n { color: #777; font-size: 13px; }
</style>
<h1>kubescrape-agent debug</h1>
<ul>
`)
	for _, l := range links {
		fmt.Fprintf(&b, `  <li><a href="%s">%s</a> — <span class="d">%s</span></li>`+"\n",
			html.EscapeString(l.href), html.EscapeString(l.title), html.EscapeString(l.desc))
	}
	b.WriteString("</ul>\n")
	for _, n := range notes {
		fmt.Fprintf(&b, `<p class="n">%s</p>`+"\n", html.EscapeString(n))
	}
	page := b.String()
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(page))
	}
}
